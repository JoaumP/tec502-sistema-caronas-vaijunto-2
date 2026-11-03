package servidor

import (
	"log"
	"net/http"
	"time"

	"github.com/JoaumP/tec502-sistema-caronas-vaijunto/internal/consorcio"
	"github.com/JoaumP/tec502-sistema-caronas-vaijunto/internal/modelos"
)

// Papel de PARTICIPANTE (empresa dona de algum trecho do itinerário)

// PrepararParticipante roda a Fase 1 do 3PC nos trechos locais desta empresa.
//
// Regra de precedência (Wound-Wait usando o Ticket de Lamport):
//   - Se sobrou vaga livre, já segura o assento na hora;
//   - Se lotou, mas tem assento apenas "preparado" por um pedido mais novo
//     (ticket maior), este pedido "fere" (aborta) aquele mais novo e fica com a vaga.
//     Quem já passou pra "pre_confirmada" ou "confirmada" nunca perde a vaga;
//   - Se quem está segurando a pré-reserva é mais antigo que nós, esperamos um
//     pouquinho (até 250ms), porque ele pode abortar em outra empresa e soltar a vaga.
func (e *Estado) PrepararParticipante(txID string, ped consorcio.PedidoPreparar) (int, consorcio.RespostaTx) {
	if len(ped.Trechos) == 0 {
		return http.StatusBadRequest, consorcio.RespostaTx{ID: txID, Erro: "lista de trechos vazia"}
	}
	vistos := make(map[int64]bool, len(ped.Trechos))
	for _, id := range ped.Trechos {
		if vistos[id] {
			return http.StatusBadRequest, consorcio.RespostaTx{ID: txID, Erro: "trechos repetidos no pedido"}
		}
		vistos[id] = true
	}

	lease := e.cfg.Lease
	if ped.LeaseSegundos > 0 {
		if d := time.Duration(ped.LeaseSegundos) * time.Second; d > 0 {
			lease = d
		}
	}

	prazoEspera := time.Now().Add(250 * time.Millisecond)

	for {
		codigo, resp, esperar := e.tentarPrepararUmaVez(txID, ped, lease)
		if !esperar || time.Now().After(prazoEspera) {
			if esperar {
				return http.StatusConflict, consorcio.RespostaTx{
					ID:     txID,
					Estado: modelos.TxAbortada,
					Erro:   "assento indisponível em um dos trechos",
				}
			}
			if codigo == http.StatusOK {
				e.salvar()
				e.pontoDeFalha(consorcio.FalhaPartAposPreparar)
			}
			return codigo, resp
		}
		time.Sleep(15 * time.Millisecond)
	}
}

// tentarPrepararUmaVez faz toda a verificação e reserva dos trechos locais de
// forma atômica dentro do lock (em duas passadas, pra não alterar nada se faltar vaga)
func (e *Estado) tentarPrepararUmaVez(txID string, ped consorcio.PedidoPreparar, lease time.Duration) (int, consorcio.RespostaTx, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// chamada repetida (retentativa de rede): responde de forma idempotente
	if existente, ok := e.txPart[txID]; ok {
		if existente.Estado == modelos.TxAbortada || existente.Estado == modelos.TxCancelada {
			return http.StatusConflict, consorcio.RespostaTx{
				ID:     txID,
				Estado: existente.Estado,
				Erro:   "transação já foi abortada",
			}, false
		}
		infos := make([]modelos.TrechoInfo, 0, len(existente.Trechos))
		for _, id := range existente.Trechos {
			if t := e.trechos[id]; t != nil {
				infos = append(infos, e.infoTrecho(t))
			}
		}
		return http.StatusOK, consorcio.RespostaTx{ID: txID, Estado: existente.Estado, Trechos: infos}, false
	}

	// 1ª passada: verifica todos os trechos antes de alterar qualquer coisa
	vitimas := make(map[string]*modelos.TxParticipante)
	podeEsperar := false

	for _, idTrecho := range ped.Trechos {
		t, existe := e.trechos[idTrecho]
		if !existe {
			return http.StatusNotFound, consorcio.RespostaTx{ID: txID, Erro: "trecho não encontrado"}, false
		}
		if !e.caronaAtiva(t.IDCarona) {
			return http.StatusConflict, consorcio.RespostaTx{ID: txID, Erro: "um dos trechos pertence a uma carona cancelada"}, false
		}

		// conta se alguma vítima já escolhida em outro trecho também libera vaga aqui
		vagasLiberadasAqui := 0
		for vID := range vitimas {
			if e.ocupacoes[idTrecho][vID] {
				vagasLiberadasAqui++
			}
		}
		if t.AssentosLivres+vagasLiberadasAqui > 0 {
			continue
		}

		// sem vaga livre: procura entre as pré-reservas ("preparada") quem tem menor prioridade
		var piorPreparada *modelos.TxParticipante
		haPreparadaMaior := false
		for ocupanteID := range e.ocupacoes[idTrecho] {
			if vitimas[ocupanteID] != nil {
				continue
			}
			oc := e.txPart[ocupanteID]
			if oc == nil || oc.Estado != modelos.TxPreparada {
				continue
			}
			if ped.Ticket.Precede(oc.Ticket) {
				if piorPreparada == nil || piorPreparada.Ticket.Precede(oc.Ticket) {
					piorPreparada = oc
				}
			} else {
				haPreparadaMaior = true
			}
		}

		if piorPreparada != nil {
			vitimas[piorPreparada.ID] = piorPreparada
		} else if haPreparadaMaior {
			// há assento em pré-reserva de alguém mais antigo; vale aguardar um instante
			podeEsperar = true
		} else {
			// todos os assentos já estão pré-confirmados ou confirmados
			return http.StatusConflict, consorcio.RespostaTx{
				ID:   txID,
				Erro: "assento indisponível em um dos trechos",
			}, false
		}
	}

	if podeEsperar {
		return 0, consorcio.RespostaTx{}, true
	}

	// 2ª passada: tudo certo, então aborta as vítimas feridas e ocupa os assentos
	agora := time.Now()
	for _, v := range vitimas {
		log.Printf("[%s] tx %s (lamport=%d) preemptou pré-reserva %s (lamport=%d)",
			e.cfg.Empresa, txID, ped.Ticket.Relogio, v.ID, v.Ticket.Relogio)
		v.Estado = modelos.TxAbortada
		v.AtualizadoEm = agora
		e.liberarAssentos(v)
	}

	copiaTrechos := append([]int64(nil), ped.Trechos...)
	copiaPart := append([]string(nil), ped.Participantes...)
	tx := &modelos.TxParticipante{
		ID:            txID,
		Coordenador:   ped.Coordenador,
		Participantes: copiaPart,
		Ticket:        ped.Ticket,
		Trechos:       copiaTrechos,
		Passageiro:    ped.Passageiro,
		Estado:        modelos.TxPreparada,
		LeaseAte:      agora.Add(lease),
		AtualizadoEm:  agora,
	}
	e.txPart[txID] = tx

	infos := make([]modelos.TrechoInfo, 0, len(copiaTrechos))
	for _, idTrecho := range copiaTrechos {
		e.ocupar(idTrecho, txID)
		infos = append(infos, e.infoTrecho(e.trechos[idTrecho]))
	}

	return http.StatusOK, consorcio.RespostaTx{
		ID:      txID,
		Estado:  modelos.TxPreparada,
		Trechos: infos,
	}, false
}

// PreConfirmarParticipante roda a Fase 2 do 3PC: a partir daqui todas as empresas
// já separaram seus assentos e essa pré-reserva não pode mais ser ferida na fase 1
func (e *Estado) PreConfirmarParticipante(txID string) (int, consorcio.RespostaTx) {
	e.mu.Lock()
	tx, ok := e.txPart[txID]
	if !ok {
		e.mu.Unlock()
		return http.StatusNotFound, consorcio.RespostaTx{ID: txID, Estado: consorcio.EstadoDesconhecida, Erro: "transação desconhecida"}
	}
	switch tx.Estado {
	case modelos.TxPreparada:
		// confere se nenhuma carona dos trechos foi cancelada nesse intervalo
		for _, idTrecho := range tx.Trechos {
			t := e.trechos[idTrecho]
			if t == nil || !e.caronaAtiva(t.IDCarona) {
				tx.Estado = modelos.TxAbortada
				tx.AtualizadoEm = time.Now()
				e.liberarAssentos(tx)
				e.mu.Unlock()
				e.salvar()
				return http.StatusConflict, consorcio.RespostaTx{ID: txID, Estado: modelos.TxAbortada, Erro: "carona foi cancelada"}
			}
		}
		agora := time.Now()
		tx.Estado = modelos.TxPreConfirmada
		tx.LeaseAte = agora.Add(e.cfg.Lease)
		tx.AtualizadoEm = agora
		e.mu.Unlock()
		e.salvar()
		return http.StatusOK, consorcio.RespostaTx{ID: txID, Estado: modelos.TxPreConfirmada}

	case modelos.TxPreConfirmada, modelos.TxConfirmada:
		est := tx.Estado
		e.mu.Unlock()
		return http.StatusOK, consorcio.RespostaTx{ID: txID, Estado: est}

	default: // abortada ou cancelada (ex.: foi ferida na fase 1)
		est := tx.Estado
		e.mu.Unlock()
		return http.StatusConflict, consorcio.RespostaTx{
			ID:     txID,
			Estado: est,
			Erro:   "pré-reserva não está mais ativa",
		}
	}
}

// ConfirmarParticipante roda a Fase 3 do 3PC, efetivando de vez o assento
func (e *Estado) ConfirmarParticipante(txID string) (int, consorcio.RespostaTx) {
	e.mu.Lock()
	tx, ok := e.txPart[txID]
	if !ok {
		e.mu.Unlock()
		return http.StatusNotFound, consorcio.RespostaTx{ID: txID, Estado: consorcio.EstadoDesconhecida, Erro: "transação desconhecida"}
	}
	// se o motorista cancelou a carona bem na hora que o coordenador mandou o
	// confirmar da fase 3, já transforma em cancelamento em cascata pras outras empresas
	if tx.Estado != modelos.TxCancelada && e.algumaCaronaCanceladaLocked(tx.Trechos) {
		tx.Estado = modelos.TxCancelada
		tx.AtualizadoEm = time.Now()
		e.liberarAssentos(tx)
		e.propagarCancelamentoCaronaLocked(tx, 0)
		e.mu.Unlock()
		e.salvar()
		e.avisarOutbox()
		return http.StatusOK, consorcio.RespostaTx{ID: txID, Estado: modelos.TxCancelada}
	}

	switch tx.Estado {
	case modelos.TxPreConfirmada, modelos.TxPreparada:
		tx.Estado = modelos.TxConfirmada
		tx.AtualizadoEm = time.Now()
		e.mu.Unlock()
		e.salvar()
		return http.StatusOK, consorcio.RespostaTx{ID: txID, Estado: modelos.TxConfirmada}

	case modelos.TxConfirmada, modelos.TxCancelada:
		est := tx.Estado
		e.mu.Unlock()
		return http.StatusOK, consorcio.RespostaTx{ID: txID, Estado: est}

	default:
		est := tx.Estado
		e.mu.Unlock()
		return http.StatusConflict, consorcio.RespostaTx{ID: txID, Estado: est, Erro: "transação já abortada"}
	}
}

// AbortarParticipante desfaz uma pré-reserva e deixa o registro "abortada" salvo
// pra impedir que algum preparar atrasado de rede tente reservar depois
func (e *Estado) AbortarParticipante(txID string) (int, consorcio.RespostaTx) {
	e.mu.Lock()
	tx, ok := e.txPart[txID]
	if !ok {
		e.txPart[txID] = &modelos.TxParticipante{
			ID:           txID,
			Estado:       modelos.TxAbortada,
			AtualizadoEm: time.Now(),
		}
		e.mu.Unlock()
		e.salvar()
		return http.StatusOK, consorcio.RespostaTx{ID: txID, Estado: modelos.TxAbortada}
	}
	switch tx.Estado {
	case modelos.TxPreparada, modelos.TxPreConfirmada:
		tx.Estado = modelos.TxAbortada
		tx.AtualizadoEm = time.Now()
		e.liberarAssentos(tx)
		e.mu.Unlock()
		e.salvar()
		return http.StatusOK, consorcio.RespostaTx{ID: txID, Estado: modelos.TxAbortada}

	case modelos.TxAbortada, modelos.TxCancelada:
		est := tx.Estado
		e.mu.Unlock()
		return http.StatusOK, consorcio.RespostaTx{ID: txID, Estado: est}

	default: // já confirmada
		e.mu.Unlock()
		return http.StatusConflict, consorcio.RespostaTx{ID: txID, Estado: modelos.TxConfirmada, Erro: "transação já confirmada"}
	}
}

// ExpirarSePreparada é usada pelos pares quando o coordenador caiu: se a gente
// ainda estava só em "preparada" (ou nem viu a transação), não tem como a fase 2
// ter fechado, então já aborta aqui; se já estava pré-confirmada/confirmada, só devolve o estado
func (e *Estado) ExpirarSePreparada(txID string) (int, consorcio.RespostaTx) {
	e.mu.Lock()
	tx, ok := e.txPart[txID]
	if !ok {
		e.txPart[txID] = &modelos.TxParticipante{
			ID:           txID,
			Estado:       modelos.TxAbortada,
			AtualizadoEm: time.Now(),
		}
		e.mu.Unlock()
		e.salvar()
		return http.StatusOK, consorcio.RespostaTx{ID: txID, Estado: modelos.TxAbortada}
	}
	if tx.Estado == modelos.TxPreparada {
		tx.Estado = modelos.TxAbortada
		tx.AtualizadoEm = time.Now()
		e.liberarAssentos(tx)
		e.mu.Unlock()
		e.salvar()
		return http.StatusOK, consorcio.RespostaTx{ID: txID, Estado: modelos.TxAbortada}
	}
	est := tx.Estado
	e.mu.Unlock()
	return http.StatusOK, consorcio.RespostaTx{ID: txID, Estado: est}
}

// CancelarReservaParticipante solta os assentos de uma reserva já confirmada
// quando o passageiro desiste ou quando alguma carona do itinerário é cancelada
func (e *Estado) CancelarReservaParticipante(txID string) (int, consorcio.RespostaTx) {
	e.mu.Lock()
	tx, ok := e.txPart[txID]
	if !ok {
		// guarda tombstone para o caso de um commit atrasado da outbox chegar depois
		e.txPart[txID] = &modelos.TxParticipante{
			ID:           txID,
			Estado:       modelos.TxCancelada,
			AtualizadoEm: time.Now(),
		}
		e.mu.Unlock()
		e.salvar()
		return http.StatusOK, consorcio.RespostaTx{ID: txID, Estado: modelos.TxCancelada}
	}
	if tx.Estado == modelos.TxConfirmada || tx.Estado == modelos.TxPreConfirmada || tx.Estado == modelos.TxPreparada {
		tx.Estado = modelos.TxCancelada
		tx.AtualizadoEm = time.Now()
		e.liberarAssentos(tx)
		e.mu.Unlock()
		e.salvar()
		return http.StatusOK, consorcio.RespostaTx{ID: txID, Estado: modelos.TxCancelada}
	}
	est := tx.Estado
	e.mu.Unlock()
	return http.StatusOK, consorcio.RespostaTx{ID: txID, Estado: est}
}

// ConsultarTransacao devolve o que esta empresa sabe sobre a transação (seja
// como participante, como coordenadora, ou os dois)
func (e *Estado) ConsultarTransacao(txID string) consorcio.RespostaTx {
	e.mu.RLock()
	defer e.mu.RUnlock()

	resp := consorcio.RespostaTx{ID: txID}
	if p, ok := e.txPart[txID]; ok {
		resp.Estado = p.Estado
	} else {
		resp.Estado = consorcio.EstadoDesconhecida
	}
	resp.Fase = e.faseCoordenadorVisivelLocked(txID)
	return resp
}

// faseCoordenadorVisivelLocked separa transação que está rodando agora em memória
// daquelas carregadas do estado.json depois de uma queda:
//   - CoordIniciada sem goroutine ativa morreu na fase 1 -> já conta como abortada
//   - CoordPreConfirmando sem goroutine ativa está esperando reconciliar com os pares -> devolve "recuperando"
func (e *Estado) faseCoordenadorVisivelLocked(txID string) string {
	c, ok := e.txCoord[txID]
	if !ok {
		return ""
	}
	if !e.txAtivas[txID] {
		switch c.Fase {
		case modelos.CoordIniciada:
			return modelos.CoordAbortada
		case modelos.CoordPreConfirmando:
			return consorcio.FaseRecuperando
		}
	}
	return c.Fase
}


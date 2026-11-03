package servidor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/JoaumP/tec502-sistema-caronas-vaijunto/internal/consorcio"
	"github.com/JoaumP/tec502-sistema-caronas-vaijunto/internal/modelos"
)

// erroPreemptada avisa que nossa pré-reserva perdeu a vaga na fase 1 pra outra
// transação mais antiga no relógio de Lamport. Serve pra gente tentar uma vez
// de novo caso aquela vencedora acabe falhando num outro trecho.
var erroPreemptada = errors.New("pré-reserva preemptada por transação prioritária")

// Papel de COORDENADOR DINÂMICO (empresa onde o passageiro pediu a reserva)

// confirmarItinerario coordena a reserva atômica entre todas as empresas que têm
// trecho na viagem escolhida pelo passageiro
func (e *Estado) confirmarItinerario(idPassageiro int64, refs []modelos.RefTrecho) (string, error) {
	if len(refs) == 0 {
		return "", errors.New("itinerário vazio")
	}

	// normaliza empresa vazia (assume a própria empresa) e checa duplicatas
	normalizados := make([]modelos.RefTrecho, len(refs))
	vistos := make(map[modelos.RefTrecho]bool, len(refs))
	for i, r := range refs {
		if r.Empresa == "" {
			r.Empresa = e.cfg.Empresa
		}
		if r.Empresa != e.cfg.Empresa && !e.rede.Conhece(r.Empresa) {
			return "", fmt.Errorf("empresa desconhecida no itinerário: %s", r.Empresa)
		}
		if vistos[r] {
			return "", errors.New("itinerário contém trechos repetidos")
		}
		vistos[r] = true
		normalizados[i] = r
	}

	// relógio de Lamport no nascimento da requisição define a precedência do passageiro
	relogioInicial := e.relogio.Tick()

	// se formos preemptados na fase 1 por alguém mais antigo que depois acabe
	// falhando em outro trecho, fazemos uma única nova tentativa mantendo o
	// carimbo de relógio original do passageiro
	for tentativa := 0; tentativa < 2; tentativa++ {
		txID, err := e.executar3PC(idPassageiro, normalizados, relogioInicial)
		if errors.Is(err, erroPreemptada) && tentativa == 0 {
			time.Sleep(40 * time.Millisecond)
			continue
		}
		return txID, err
	}
	return "", errors.New("assento indisponível em um dos trechos")
}

// executar3PC roda as 3 fases do protocolo (preparar -> pré-confirmar -> confirmar)
// falando em paralelo com as empresas participantes
func (e *Estado) executar3PC(idPassageiro int64, refs []modelos.RefTrecho, relogioTicket int64) (string, error) {
	// agrupa os IDs de trecho por empresa participante
	porEmpresa := make(map[string][]int64)
	for _, r := range refs {
		porEmpresa[r.Empresa] = append(porEmpresa[r.Empresa], r.ID)
	}
	participantes := make([]string, 0, len(porEmpresa))
	for emp := range porEmpresa {
		participantes = append(participantes, emp)
	}
	sort.Strings(participantes)

	e.mu.Lock()
	e.proximoIDTx++
	txID := fmt.Sprintf("%s-%d", e.cfg.Empresa, e.proximoIDTx)
	ticket := modelos.Ticket{
		Relogio: relogioTicket,
		Empresa: e.cfg.Empresa,
		TxID:    txID,
	}
	coord := &modelos.TxCoordenador{
		ID:            txID,
		Ticket:        ticket,
		PassageiroID:  idPassageiro,
		Participantes: participantes,
		Fase:          modelos.CoordIniciada,
		CriadoEm:      time.Now(),
	}
	e.txCoord[txID] = coord
	e.txAtivas[txID] = true
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		delete(e.txAtivas, txID)
		e.mu.Unlock()
	}()
	e.salvar()

	passageiroGlobal := fmt.Sprintf("%d@%s", idPassageiro, e.cfg.Empresa)
	leaseSeg := int(e.cfg.Lease.Seconds())
	if leaseSeg < 1 {
		leaseSeg = 1
	}

	// fase 1: preparar (pré-reserva os assentos em todas as empresas envolvidas)
	type resultadoPrep struct {
		empresa string
		infos   []modelos.TrechoInfo
		err     error
	}
	resChan := make(chan resultadoPrep, len(participantes))
	for _, emp := range participantes {
		emp := emp
		ped := consorcio.PedidoPreparar{
			Coordenador:   e.cfg.Empresa,
			Participantes: participantes,
			Ticket:        ticket,
			Trechos:       porEmpresa[emp],
			Passageiro:    passageiroGlobal,
			LeaseSegundos: leaseSeg,
		}
		go func() {
			infos, err := e.enviarPreparar(emp, txID, ped)
			resChan <- resultadoPrep{empresa: emp, infos: infos, err: err}
		}()
	}

	mapaInfos := make(map[modelos.RefTrecho]modelos.TrechoInfo, len(refs))
	var erroFase1 error
	for range participantes {
		r := <-resChan
		if r.err != nil && erroFase1 == nil {
			erroFase1 = r.err
		}
		for _, info := range r.infos {
			mapaInfos[info.Ref()] = info
		}
	}

	if erroFase1 != nil {
		e.abortarCoordenador(txID, participantes)
		return "", erroFase1
	}

	// reconstrói o itinerário na ordem da viagem e valida encadeamento de cidades e horários
	trechosOrdenados := make([]modelos.TrechoInfo, len(refs))
	for i, ref := range refs {
		info, ok := mapaInfos[ref]
		if !ok {
			e.abortarCoordenador(txID, participantes)
			return "", errors.New("trecho não retornado na preparação")
		}
		if i > 0 {
			ant := trechosOrdenados[i-1]
			if info.Origem != ant.Destino {
				e.abortarCoordenador(txID, participantes)
				return "", fmt.Errorf("itinerário desconexo: %s não conecta com %s", ant.Destino, info.Origem)
			}
			if info.HorarioSaida < ant.HorarioChegada {
				e.abortarCoordenador(txID, participantes)
				return "", errors.New("conexão inválida: trecho parte antes da chegada do anterior")
			}
		}
		trechosOrdenados[i] = info
	}

	// ponto de injeção de falha de teste: coordenador cai após todos prepararem
	e.pontoDeFalha(consorcio.FalhaCoordAposPreparar)

	// fase 2: pré-confirmar (barreira do 3PC antes do commit)
	e.mu.Lock()
	coord.Trechos = trechosOrdenados
	coord.Fase = modelos.CoordPreConfirmando
	e.mu.Unlock()
	e.salvar()

	type resultadoPre struct {
		rejeitado bool
		err       error
	}
	errosPre := make(chan resultadoPre, len(participantes))
	for _, emp := range participantes {
		emp := emp
		go func() {
			rej, err := e.enviarPreConfirmar(emp, txID)
			errosPre <- resultadoPre{rejeitado: rej, err: err}
		}()
	}
	var erroFase2 error
	algumRejeitou := false
	for range participantes {
		res := <-errosPre
		if res.rejeitado {
			algumRejeitou = true
		}
		if res.err != nil && erroFase2 == nil {
			erroFase2 = res.err
		}
	}
	if erroFase2 != nil {
		if algumRejeitou {
			// pelo menos um participante confirmou que não está pré-confirmado:
			// a terminação cooperativa jamais poderá confirmar, aborto 100% seguro
			e.abortarCoordenador(txID, participantes)
		} else {
			// apenas erros de rede/timeout na fase 2: só grava CoordAbortada se
			// conseguirmos abortar ao menos um participante; se o coordenador
			// ficou isolado de todos os pares, mantém CoordPreConfirmando para
			// reconciliar com a decisão cooperativa deles quando a rede voltar
			e.tentarAbortarFase2(txID, participantes)
		}
		return "", erroFase2
	}

	// ponto de injeção de falha de teste: coordenador cai após todos pré-confirmarem
	e.pontoDeFalha(consorcio.FalhaCoordAposPreConfirmar)

	// fase 3: confirmar (decisão final irreversível)
	e.finalizarCommitCoordenador(txID)
	return txID, nil
}

// abortarCoordenador grava a decisão de aborto e avisa todos os participantes
// (jogando na outbox se algum estiver fora do ar na hora)
func (e *Estado) abortarCoordenador(txID string, participantes []string) {
	e.mu.Lock()
	if c := e.txCoord[txID]; c != nil {
		c.Fase = modelos.CoordAbortada
	}
	var remotos []string
	for _, emp := range participantes {
		if emp != e.cfg.Empresa {
			remotos = append(remotos, emp)
		}
	}
	e.mu.Unlock()
	e.salvar()

	for _, emp := range participantes {
		if emp == e.cfg.Empresa {
			e.AbortarParticipante(txID)
		}
	}

	var wg sync.WaitGroup
	for _, emp := range remotos {
		emp := emp
		wg.Add(1)
		go func() {
			defer wg.Done()
			caminho := fmt.Sprintf("%s/transacoes/%s/abortar", consorcio.Prefixo, txID)
			ctx, cancel := context.WithTimeout(context.Background(), e.cfg.TimeoutRede)
			defer cancel()
			var resp consorcio.RespostaTx
			cod, err := e.rede.Chamar(ctx, emp, http.MethodPost, caminho, nil, &resp)
			if err != nil || (cod != http.StatusOK && cod != http.StatusConflict) {
				e.mu.Lock()
				e.enfileirar(emp, caminho, nil)
				e.mu.Unlock()
				e.salvar()
				e.avisarOutbox()
			}
		}()
	}
	wg.Wait()
}

// tentarAbortarFase2 trata falha de rede na fase 2: tenta abortar quem estiver
// acessível primeiro e só marca CoordAbortada se pelo menos um participante
// confirmar o aborto (evita split-brain se o coordenador tiver ficado isolado)
func (e *Estado) tentarAbortarFase2(txID string, participantes []string) {
	algumAbortou := false
	var remotos []string
	for _, emp := range participantes {
		if emp == e.cfg.Empresa {
			cod, _ := e.AbortarParticipante(txID)
			if cod == http.StatusOK {
				algumAbortou = true
			}
		} else {
			remotos = append(remotos, emp)
		}
	}

	var mu sync.Mutex
	var pendentesOutbox []string
	var wg sync.WaitGroup
	for _, emp := range remotos {
		emp := emp
		wg.Add(1)
		go func() {
			defer wg.Done()
			caminho := fmt.Sprintf("%s/transacoes/%s/abortar", consorcio.Prefixo, txID)
			ctx, cancel := context.WithTimeout(context.Background(), e.cfg.TimeoutRede)
			defer cancel()
			var resp consorcio.RespostaTx
			cod, err := e.rede.Chamar(ctx, emp, http.MethodPost, caminho, nil, &resp)
			mu.Lock()
			defer mu.Unlock()
			if err == nil && cod == http.StatusOK {
				algumAbortou = true
			} else if err != nil || cod != http.StatusConflict {
				pendentesOutbox = append(pendentesOutbox, emp)
			}
		}()
	}
	wg.Wait()

	if !algumAbortou {
		return
	}

	caminho := fmt.Sprintf("%s/transacoes/%s/abortar", consorcio.Prefixo, txID)
	e.mu.Lock()
	if c := e.txCoord[txID]; c != nil {
		c.Fase = modelos.CoordAbortada
	}
	for _, emp := range pendentesOutbox {
		e.enfileirar(emp, caminho, nil)
	}
	e.mu.Unlock()
	e.salvar()
	if len(pendentesOutbox) > 0 {
		e.avisarOutbox()
	}
}

// finalizarCommitCoordenador salva a reserva confirmada na empresa do passageiro
// e manda o "confirmar" pra todos os participantes (usando outbox se a rede falhar)
func (e *Estado) finalizarCommitCoordenador(txID string) {
	e.mu.Lock()
	coord := e.txCoord[txID]
	if coord == nil {
		e.mu.Unlock()
		return
	}
	coord.Fase = modelos.CoordConfirmada
	if _, jaExiste := e.reservas[txID]; !jaExiste {
		e.reservas[txID] = &modelos.Reserva{
			ID:           txID,
			PassageiroID: coord.PassageiroID,
			Trechos:      coord.Trechos,
			Status:       modelos.ReservaConfirmada,
			CriadoEm:     time.Now(),
		}
	}
	participantes := append([]string(nil), coord.Participantes...)
	e.mu.Unlock()
	e.salvar()

	var wg sync.WaitGroup
	for _, emp := range participantes {
		if emp == e.cfg.Empresa {
			e.ConfirmarParticipante(txID)
			continue
		}
		emp := emp
		wg.Add(1)
		go func() {
			defer wg.Done()
			caminho := fmt.Sprintf("%s/transacoes/%s/confirmar", consorcio.Prefixo, txID)
			ctx, cancel := context.WithTimeout(context.Background(), e.cfg.TimeoutRede)
			defer cancel()
			var resp consorcio.RespostaTx
			cod, err := e.rede.Chamar(ctx, emp, http.MethodPost, caminho, nil, &resp)
			if err != nil || cod != http.StatusOK {
				e.mu.Lock()
				e.enfileirar(emp, caminho, nil)
				e.mu.Unlock()
				e.salvar()
				e.avisarOutbox()
			}
		}()
	}
	wg.Wait()
}

// enviarPreparar executa a fase 1 localmente (se o trecho for nosso) ou via REST nas outras empresas
func (e *Estado) enviarPreparar(emp, txID string, ped consorcio.PedidoPreparar) ([]modelos.TrechoInfo, error) {
	if emp == e.cfg.Empresa {
		cod, resp := e.PrepararParticipante(txID, ped)
		if cod != http.StatusOK {
			return nil, errors.New(resp.Erro)
		}
		return resp.Trechos, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), e.cfg.TimeoutRede)
	defer cancel()
	var resp consorcio.RespostaTx
	caminho := fmt.Sprintf("%s/transacoes/%s/preparar", consorcio.Prefixo, txID)
	cod, err := e.rede.Chamar(ctx, emp, http.MethodPost, caminho, ped, &resp)
	if err != nil {
		return nil, fmt.Errorf("empresa %s indisponível", emp)
	}
	if cod != http.StatusOK {
		if resp.Erro != "" {
			return nil, errors.New(resp.Erro)
		}
		return nil, fmt.Errorf("empresa %s recusou reserva", emp)
	}
	return resp.Trechos, nil
}

// enviarPreConfirmar manda o pré-confirmar pro participante e indica se houve
// recusa explícita (4xx), garantindo que ele não entrou em pre_confirmada
func (e *Estado) enviarPreConfirmar(emp, txID string) (bool, error) {
	if emp == e.cfg.Empresa {
		cod, resp := e.PreConfirmarParticipante(txID)
		if cod != http.StatusOK {
			if resp.Estado == modelos.TxAbortada {
				return true, erroPreemptada
			}
			return true, errors.New(resp.Erro)
		}
		return false, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), e.cfg.TimeoutRede)
	defer cancel()
	var resp consorcio.RespostaTx
	caminho := fmt.Sprintf("%s/transacoes/%s/pre-confirmar", consorcio.Prefixo, txID)
	cod, err := e.rede.Chamar(ctx, emp, http.MethodPost, caminho, nil, &resp)
	if err != nil {
		return false, fmt.Errorf("empresa %s indisponível na pré-confirmação", emp)
	}
	if cod != http.StatusOK {
		if resp.Estado == modelos.TxAbortada {
			return true, erroPreemptada
		}
		if resp.Erro != "" {
			return true, errors.New(resp.Erro)
		}
		return true, fmt.Errorf("empresa %s recusou pré-confirmação", emp)
	}
	return false, nil
}


package servidor

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/consorcio"
	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/modelos"
)

// Recuperação pós-queda, verificação de leases vencidos e entrega da outbox

// IniciarWorkers dispara em background a reconciliação inicial de boot, o
// verificador periódico de leases e o entregador da fila de saída (outbox)
func (e *Estado) IniciarWorkers(ctx context.Context) {
	go func() {
		e.reconciliarNoBoot()
		e.avisarOutbox()
	}()
	go e.loopLeases(ctx)
	go e.loopOutbox(ctx)
}

// reconciliarNoBoot resolve transações que ficaram no meio do caminho quando um
// servidor cai e sobe de novo
func (e *Estado) reconciliarNoBoot() {
	e.mu.RLock()
	var coordsIniciadas []struct {
		id   string
		part []string
	}
	var coordsPre []struct {
		id   string
		part []string
	}
	var partsPendentes []string

	for _, c := range e.txCoord {
		switch c.Fase {
		case modelos.CoordIniciada:
			coordsIniciadas = append(coordsIniciadas, struct {
				id   string
				part []string
			}{c.ID, append([]string(nil), c.Participantes...)})
		case modelos.CoordPreConfirmando:
			coordsPre = append(coordsPre, struct {
				id   string
				part []string
			}{c.ID, append([]string(nil), c.Participantes...)})
		}
	}
	for _, p := range e.txPart {
		if p.Estado == modelos.TxPreparada || p.Estado == modelos.TxPreConfirmada {
			partsPendentes = append(partsPendentes, p.ID)
		}
	}
	e.mu.RUnlock()

	// transações que morreram ainda na fase 1 nunca chegaram perto do commit
	for _, c := range coordsIniciadas {
		log.Printf("[recuperação] abortando transação %s interrompida na fase 1", c.id)
		e.abortarCoordenador(c.id, c.part)
	}

	// transações que morreram durante a fase 2: pergunta aos participantes se
	// todos chegaram em pre_confirmada/confirmada (ou se já decidiram entre si)
	for _, c := range coordsPre {
		e.dispararReconciliacaoCoord(c.id, c.part)
	}

	for _, txID := range partsPendentes {
		e.dispararResolucao(txID)
	}
}

// loopLeases acorda periodicamente pra procurar pré-reservas cujo prazo (lease)
// estourou e coordenadores que ficaram parados na fase 2 precisando reconciliar
func (e *Estado) loopLeases(ctx context.Context) {
	intervalo := e.cfg.Lease / 3
	if intervalo < 150*time.Millisecond {
		intervalo = 150 * time.Millisecond
	}
	if intervalo > time.Second {
		intervalo = time.Second
	}
	ticker := time.NewTicker(intervalo)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case agora := <-ticker.C:
			var vencidas []string
			e.mu.RLock()
			for _, tx := range e.txPart {
				if (tx.Estado == modelos.TxPreparada || tx.Estado == modelos.TxPreConfirmada) && agora.After(tx.LeaseAte) {
					vencidas = append(vencidas, tx.ID)
				}
			}
			// também reconcilia coordenadores em CoordPreConfirmando que não têm
			// mais goroutine ativa (herdadas de queda ou de partição na fase 2)
			var coordsPre []struct {
				id   string
				part []string
			}
			for _, c := range e.txCoord {
				if c.Fase == modelos.CoordPreConfirmando && !e.txAtivas[c.ID] {
					coordsPre = append(coordsPre, struct {
						id   string
						part []string
					}{c.ID, append([]string(nil), c.Participantes...)})
				}
			}
			e.mu.RUnlock()

			for _, txID := range vencidas {
				e.dispararResolucao(txID)
			}
			for _, c := range coordsPre {
				e.dispararReconciliacaoCoord(c.id, c.part)
			}
		}
	}
}

// dispararReconciliacaoCoord garante que só exista uma goroutine por vez tentando
// fechar a decisão de um coordenador que ficou pendente na fase 2
func (e *Estado) dispararReconciliacaoCoord(txID string, participantes []string) {
	chave := "coord:" + txID
	e.resolvendoMu.Lock()
	if e.resolvendo[chave] {
		e.resolvendoMu.Unlock()
		return
	}
	e.resolvendo[chave] = true
	e.resolvendoMu.Unlock()

	go func() {
		defer func() {
			e.resolvendoMu.Lock()
			delete(e.resolvendo, chave)
			e.resolvendoMu.Unlock()
		}()
		e.mu.RLock()
		c := e.txCoord[txID]
		aindaPendente := c != nil && c.Fase == modelos.CoordPreConfirmando && !e.txAtivas[txID]
		e.mu.RUnlock()
		if !aindaPendente {
			return
		}
		dec := e.consultarDecisaoParticipantes(txID, participantes)
		switch dec {
		case modelos.TxConfirmada:
			log.Printf("[recuperação] concluindo commit da transação %s após reconciliação", txID)
			e.finalizarCommitCoordenador(txID)
		case modelos.TxAbortada:
			log.Printf("[recuperação] abortando transação %s após reconciliação com pares", txID)
			e.abortarCoordenador(txID, participantes)
		}
	}()
}

// dispararResolucao dispara em background a resolução de um participante com lease
// vencido, evitando abrir duas goroutines pra mesma transação
func (e *Estado) dispararResolucao(txID string) {
	e.resolvendoMu.Lock()
	if e.resolvendo[txID] {
		e.resolvendoMu.Unlock()
		return
	}
	e.resolvendo[txID] = true
	e.resolvendoMu.Unlock()

	go func() {
		defer func() {
			e.resolvendoMu.Lock()
			delete(e.resolvendo, txID)
			e.resolvendoMu.Unlock()
		}()
		e.resolverTransacaoExpirada(txID)
	}()
}

// resolverTransacaoExpirada decide o que fazer com um assento pré-reservado cujo
// lease venceu: primeiro pergunta pro coordenador e, se ele tiver caído, fecha
// a decisão conversando com os outros participantes (terminação cooperativa)
func (e *Estado) resolverTransacaoExpirada(txID string) {
	e.mu.RLock()
	tx, ok := e.txPart[txID]
	if !ok || (tx.Estado != modelos.TxPreparada && tx.Estado != modelos.TxPreConfirmada) {
		e.mu.RUnlock()
		return
	}
	coordenador := tx.Coordenador
	estadoAtual := tx.Estado
	participantes := append([]string(nil), tx.Participantes...)
	e.mu.RUnlock()

	// 1. Tenta perguntar diretamente ao coordenador (se ele estiver recuperando
	// uma fase 2 pós-queda, pula direto para a terminação cooperativa abaixo)
	faseCoord, coordVivo := e.consultarFaseCoordenador(coordenador, txID)
	if coordVivo && faseCoord != consorcio.FaseRecuperando {
		switch faseCoord {
		case modelos.CoordConfirmada:
			log.Printf("[%s] lease de %s resolvido com coordenador %s: confirmar", e.cfg.Empresa, txID, coordenador)
			e.ConfirmarParticipante(txID)
			return
		case modelos.CoordAbortada, "":
			log.Printf("[%s] lease de %s resolvido com coordenador %s: abortar", e.cfg.Empresa, txID, coordenador)
			e.AbortarParticipante(txID)
			return
		default:
			// coordenador está vivo e ainda processando esta transação: renova o lease
			e.mu.Lock()
			if t := e.txPart[txID]; t != nil && (t.Estado == modelos.TxPreparada || t.Estado == modelos.TxPreConfirmada) {
				t.LeaseAte = time.Now().Add(e.cfg.Lease)
			}
			e.mu.Unlock()
			return
		}
	}

	// 2. Coordenador está inacessível (caiu): protocolo de terminação cooperativa!
	// Se nós ainda estamos apenas em "preparada", garantidamente nunca demos OK
	// na fase 2 (pre-confirmar), então ninguém pode ter confirmado: abortamos na hora!
	if estadoAtual == modelos.TxPreparada {
		log.Printf("[%s] coordenador %s inacessível e %s ainda em 'preparada': abortando e liberando assento",
			e.cfg.Empresa, coordenador, txID)
		e.ExpirarSePreparada(txID)
		return
	}

	// Estamos em "pre_confirmada": consulta todos os participantes (pedindo para
	// quem ainda estiver só em "preparada" expirar para "abortada").
	decisao := e.consultarDecisaoParticipantes(txID, participantes)
	switch decisao {
	case modelos.TxConfirmada:
		log.Printf("[%s] terminação cooperativa de %s: todos pré-confirmados -> CONFIRMADA", e.cfg.Empresa, txID)
		e.ConfirmarParticipante(txID)
	case modelos.TxAbortada:
		log.Printf("[%s] terminação cooperativa de %s: algum participante não pré-confirmou -> ABORTADA", e.cfg.Empresa, txID)
		e.AbortarParticipante(txID)
	}
}

// consultarFaseCoordenador pergunta via GET /transacoes/{id} qual foi a decisão
// do servidor coordenador (ou devolve false se o coordenador estiver fora do ar)
func (e *Estado) consultarFaseCoordenador(coordenador, txID string) (string, bool) {
	if coordenador == "" {
		return "", true
	}
	if coordenador == e.cfg.Empresa {
		e.mu.RLock()
		defer e.mu.RUnlock()
		return e.faseCoordenadorVisivelLocked(txID), true
	}
	ctx, cancel := context.WithTimeout(context.Background(), e.cfg.TimeoutRede)
	defer cancel()
	var resp consorcio.RespostaTx
	caminho := fmt.Sprintf("%s/transacoes/%s", consorcio.Prefixo, txID)
	cod, err := e.rede.Chamar(ctx, coordenador, http.MethodGet, caminho, nil, &resp)
	if err != nil || cod != http.StatusOK {
		return "", false
	}
	return resp.Fase, true
}

// consultarDecisaoParticipantes conversa com todos os participantes da transação:
//   - devolve TxAbortada se qualquer um estiver abortado/desconhecido (ou só em preparada)
//   - devolve TxConfirmada se algum já confirmou ou se todos responderam em pre_confirmada/confirmada
//   - devolve "" se outro participante também estiver fora do ar agora (tenta no próximo ciclo)
func (e *Estado) consultarDecisaoParticipantes(txID string, participantes []string) string {
	if len(participantes) == 0 {
		return modelos.TxAbortada
	}
	todosPreConfirmados := true
	algumInacessivel := false

	for _, emp := range participantes {
		var est string
		if emp == e.cfg.Empresa {
			_, r := e.ExpirarSePreparada(txID)
			est = r.Estado
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), e.cfg.TimeoutRede)
			var resp consorcio.RespostaTx
			caminho := fmt.Sprintf("%s/transacoes/%s/expirar-preparada", consorcio.Prefixo, txID)
			cod, err := e.rede.Chamar(ctx, emp, http.MethodPost, caminho, nil, &resp)
			cancel()
			if err != nil || cod != http.StatusOK {
				algumInacessivel = true
				todosPreConfirmados = false
				continue
			}
			est = resp.Estado
		}

		switch est {
		case modelos.TxConfirmada, modelos.TxCancelada:
			return modelos.TxConfirmada
		case modelos.TxAbortada, consorcio.EstadoDesconhecida:
			return modelos.TxAbortada
		case modelos.TxPreConfirmada:
			// segue checando os demais
		default:
			todosPreConfirmados = false
		}
	}

	if !algumInacessivel && todosPreConfirmados {
		return modelos.TxConfirmada
	}
	return ""
}

// loopOutbox tenta reenviar de tempos em tempos as mensagens pendentes até os
// servidores de destino voltarem a responder
func (e *Estado) loopOutbox(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.acordarOutbox:
			e.processarOutbox()
		case <-ticker.C:
			e.processarOutbox()
		}
	}
}

// dispararOutboxRapido dispara a entrega em background e espera um pouquinho (150ms)
// pras empresas online já receberem na hora, sem travar o cliente se tiver servidor caído
func (e *Estado) dispararOutboxRapido() {
	pronto := make(chan struct{})
	go func() {
		e.processarOutbox()
		close(pronto)
	}()
	select {
	case <-pronto:
	case <-time.After(150 * time.Millisecond):
	}
}

// processarOutbox manda as mensagens pendentes separando uma goroutine pra cada
// empresa de destino, assim um servidor fora do ar dando timeout não atrasa os outros
func (e *Estado) processarOutbox() {
	e.outboxProcMu.Lock()
	defer e.outboxProcMu.Unlock()

	e.mu.RLock()
	if len(e.outbox) == 0 {
		e.mu.RUnlock()
		return
	}
	porDestino := make(map[string][]modelos.MensagemPendente)
	for _, m := range e.outbox {
		porDestino[m.Destino] = append(porDestino[m.Destino], *m)
	}
	e.mu.RUnlock()

	var wg sync.WaitGroup
	for _, msgs := range porDestino {
		sort.Slice(msgs, func(i, j int) bool { return msgs[i].ID < msgs[j].ID })
		msgs := msgs
		wg.Add(1)
		go func() {
			defer wg.Done()
			var entregues []int64
			for _, m := range msgs {
				ctx, cancel := context.WithTimeout(context.Background(), e.cfg.TimeoutRede)
				var corpo any
				if len(m.Corpo) > 0 {
					corpo = m.Corpo
				}
				cod, err := e.rede.Chamar(ctx, m.Destino, http.MethodPost, m.Caminho, corpo, nil)
				cancel()
				if err != nil || cod >= 500 {
					// destino inacessível neste ciclo: interrompe só esta fila para
					// manter a ordem FIFO e não somar N timeouts seguidos
					break
				}
				if cod >= 200 && cod < 500 {
					entregues = append(entregues, m.ID)
				}
			}
			if len(entregues) > 0 {
				e.mu.Lock()
				for _, id := range entregues {
					delete(e.outbox, id)
				}
				e.mu.Unlock()
				e.salvar()
			}
		}()
	}
	wg.Wait()
}

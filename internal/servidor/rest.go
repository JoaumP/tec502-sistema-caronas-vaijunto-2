package servidor

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/consorcio"
)

// HandlerREST registra todas as rotas HTTP da API REST que esta empresa expõe
// pros demais servidores do consórcio.
func (e *Estado) HandlerREST() http.Handler {
	mux := http.NewServeMux()

	// rota simples pra checar se o servidor está de pé
	mux.HandleFunc("GET "+consorcio.Prefixo+"/saude", func(w http.ResponseWriter, r *http.Request) {
		e.responderJSON(w, http.StatusOK, map[string]string{
			"empresa": e.cfg.Empresa,
			"status":  "ok",
		})
	})

	// lista os trechos ativos com vagas desta empresa (chamado na busca federada)
	mux.HandleFunc("GET "+consorcio.Prefixo+"/trechos", func(w http.ResponseWriter, r *http.Request) {
		data := r.URL.Query().Get("data")
		ofertas := e.OfertasLocais(data)
		e.responderJSON(w, http.StatusOK, consorcio.RespostaTrechos{
			Empresa: e.cfg.Empresa,
			Trechos: ofertas,
		})
	})

	// consulta em que pé está uma transação aqui (usado quando um lease vence)
	mux.HandleFunc("GET "+consorcio.Prefixo+"/transacoes/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		e.responderJSON(w, http.StatusOK, e.ConsultarTransacao(id))
	})

	// Fase 1 do 3PC: tenta segurar os assentos locais usando o Ticket de Lamport
	mux.HandleFunc("POST "+consorcio.Prefixo+"/transacoes/{id}/preparar", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		var ped consorcio.PedidoPreparar
		if err := json.NewDecoder(r.Body).Decode(&ped); err != nil {
			e.responderJSON(w, http.StatusBadRequest, consorcio.RespostaTx{ID: id, Erro: "JSON inválido"})
			return
		}
		cod, resp := e.PrepararParticipante(id, ped)
		e.responderJSON(w, cod, resp)
	})

	// Fase 2 do 3PC: marca como pré-confirmada (a partir daqui não sofre mais preempção)
	mux.HandleFunc("POST "+consorcio.Prefixo+"/transacoes/{id}/pre-confirmar", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		cod, resp := e.PreConfirmarParticipante(id)
		e.responderJSON(w, cod, resp)
	})

	// Fase 3 do 3PC: confirma o assento em definitivo
	mux.HandleFunc("POST "+consorcio.Prefixo+"/transacoes/{id}/confirmar", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		cod, resp := e.ConfirmarParticipante(id)
		e.responderJSON(w, cod, resp)
	})

	// desfaz uma pré-reserva e libera as vagas locais
	mux.HandleFunc("POST "+consorcio.Prefixo+"/transacoes/{id}/abortar", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		cod, resp := e.AbortarParticipante(id)
		e.responderJSON(w, cod, resp)
	})

	// chamado na terminação cooperativa quando o coordenador caiu: se ainda estava só em preparada, aborta
	mux.HandleFunc("POST "+consorcio.Prefixo+"/transacoes/{id}/expirar-preparada", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		cod, resp := e.ExpirarSePreparada(id)
		e.responderJSON(w, cod, resp)
	})

	// libera vagas de uma reserva que já estava confirmada (aceita POST .../cancelar ou DELETE)
	cancelarHandler := func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		cod, resp := e.CancelarReservaParticipante(id)
		e.responderJSON(w, cod, resp)
	}
	mux.HandleFunc("POST "+consorcio.Prefixo+"/reservas/{id}/cancelar", cancelarHandler)
	mux.HandleFunc("DELETE "+consorcio.Prefixo+"/reservas/{id}", cancelarHandler)

	// aviso mandado pela empresa do motorista quando ele cancela uma carona com reserva ativa
	mux.HandleFunc("POST "+consorcio.Prefixo+"/reservas/{id}/cancelamento-carona", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		var ped consorcio.PedidoCancelamentoCarona
		_ = json.NewDecoder(r.Body).Decode(&ped)
		if ped.Empresa == "" {
			ped.Empresa = r.Header.Get(consorcio.HeaderEmpresa)
		}
		e.CancelarPorCaronaRemota(id, ped.Empresa)
		e.responderJSON(w, http.StatusOK, map[string]string{"id": id, "status": "cancelada"})
	})

	// rota usada pela suíte de testes pra conferir se os assentos e reservas estão batendo
	mux.HandleFunc("GET "+consorcio.Prefixo+"/admin/auditoria", func(w http.ResponseWriter, r *http.Request) {
		e.responderJSON(w, http.StatusOK, e.GerarAuditoria())
	})

	// rota exclusiva pros testes automatizados simularem queda do servidor no meio do 3PC
	mux.HandleFunc("POST "+consorcio.Prefixo+"/admin/falha", func(w http.ResponseWriter, r *http.Request) {
		if !e.cfg.PermitirFalhas {
			e.responderJSON(w, http.StatusForbidden, map[string]string{"erro": "injeção de falhas desabilitada"})
			return
		}
		var ped consorcio.PedidoFalha
		if err := json.NewDecoder(r.Body).Decode(&ped); err != nil {
			e.responderJSON(w, http.StatusBadRequest, map[string]string{"erro": "JSON inválido"})
			return
		}
		e.ArmarFalha(ped.Ponto)
		e.responderJSON(w, http.StatusOK, map[string]string{"armado": ped.Ponto})
	})

	return e.comMiddlewareRelogio(mux)
}

// comMiddlewareRelogio envolve todas as rotas REST pra atualizar o relógio de
// Lamport na chegada de cada requisição e evitar que um panic derrube o processo.
func (e *Estado) comMiddlewareRelogio(proximo http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("[%s] pânico recuperado no REST: %v", e.cfg.Empresa, rec)
				e.responderJSON(w, http.StatusInternalServerError, map[string]string{"erro": "erro interno"})
			}
		}()

		if v, err := strconv.ParseInt(r.Header.Get(consorcio.HeaderLamport), 10, 64); err == nil {
			e.relogio.Receber(v)
		} else {
			e.relogio.Tick()
		}
		proximo.ServeHTTP(w, r)
	})
}

// responderJSON escreve os headers padrão do consórcio (incluindo o X-Lamport atualizado) e o corpo JSON
func (e *Estado) responderJSON(w http.ResponseWriter, status int, corpo any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(consorcio.HeaderEmpresa, e.cfg.Empresa)
	w.Header().Set(consorcio.HeaderLamport, strconv.FormatInt(e.relogio.Tick(), 10))
	w.WriteHeader(status)
	if corpo != nil {
		_ = json.NewEncoder(w).Encode(corpo)
	}
}

// GerarAuditoria tira um retrato completo do estado interno desta empresa pra
// suíte automatizada validar que não sobrou venda dupla nem assento preso
func (e *Estado) GerarAuditoria() consorcio.Auditoria {
	e.mu.RLock()
	defer e.mu.RUnlock()

	aud := consorcio.Auditoria{
		Empresa:         e.cfg.Empresa,
		OutboxPendentes: len(e.outbox),
	}
	for _, t := range e.trechos {
		statusCarona := ""
		if c := e.caronas[t.IDCarona]; c != nil {
			statusCarona = c.Status
		}
		var ocs []consorcio.OcupacaoAuditoria
		for txID := range e.ocupacoes[t.ID] {
			est := ""
			if tx := e.txPart[txID]; tx != nil {
				est = tx.Estado
			}
			ocs = append(ocs, consorcio.OcupacaoAuditoria{TxID: txID, Estado: est})
		}
		aud.Trechos = append(aud.Trechos, consorcio.TrechoAuditoria{
			ID:             t.ID,
			IDCarona:       t.IDCarona,
			StatusCarona:   statusCarona,
			AssentosTotais: t.AssentosTotais,
			AssentosLivres: t.AssentosLivres,
			Ocupacoes:      ocs,
		})
	}
	for _, tx := range e.txPart {
		copia := *tx
		aud.TxParticipante = append(aud.TxParticipante, copia)
	}
	for _, tx := range e.txCoord {
		copia := *tx
		aud.TxCoordenador = append(aud.TxCoordenador, copia)
	}
	for _, r := range e.reservas {
		copia := *r
		aud.Reservas = append(aud.Reservas, copia)
	}
	return aud
}

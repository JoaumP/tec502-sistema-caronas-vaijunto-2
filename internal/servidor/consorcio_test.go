// testes do pacote servidor rodando com o detector de race condition (-race)
package servidor_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/consorcio"
	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/modelos"
	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/protocolo"
	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/servidor"
)

// TestConsorcioConcorrenciaComRaceDetector sobe 3 servidores em paralelo pra
// testar com o detector de corrida do Go (-race) as buscas, reservas disputadas
// entre empresas, consultas e recarga do estado.json
func TestConsorcioConcorrenciaComRaceDetector(t *testing.T) {
	nomes := []string{"vaijunto", "rotasul", "expresso"}
	dirBase := t.TempDir()

	var bridgeMu sync.RWMutex
	handlers := make(map[string]http.Handler)
	peers := make(map[string]string)

	for _, n := range nomes {
		nome := n
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			bridgeMu.RLock()
			h := handlers[nome]
			bridgeMu.RUnlock()
			h.ServeHTTP(w, r)
		}))
		t.Cleanup(srv.Close)
		peers[nome] = srv.URL
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	estados := make(map[string]*servidor.Estado)
	for _, nome := range nomes {
		peersLocal := make(map[string]string)
		for k, v := range peers {
			if k != nome {
				peersLocal[k] = v
			}
		}
		est, err := servidor.NovoEstado(servidor.Config{
			Empresa:      nome,
			Peers:        peersLocal,
			DirDados:     filepath.Join(dirBase, nome),
			Lease:        800 * time.Millisecond,
			TimeoutRede:  800 * time.Millisecond,
			TimeoutBusca: 600 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("erro criando estado %s: %v", nome, err)
		}
		estados[nome] = est
		bridgeMu.Lock()
		handlers[nome] = est.HandlerREST()
		bridgeMu.Unlock()
	}
	for _, est := range estados {
		est.IniciarWorkers(ctx)
	}

	// Cadastra 1 motorista em cada empresa e publica uma rota encadeada de 4 vagas
	const vagas = 4
	publicar(t, estados["vaijunto"], "motA", "Salvador", "Feira", "2026-11-20T08:00", "2026-11-20T09:30", vagas)
	publicar(t, estados["rotasul"], "motB", "Feira", "Conquista", "2026-11-20T10:00", "2026-11-20T14:00", vagas)
	publicar(t, estados["expresso"], "motC", "Conquista", "MontesClaros", "2026-11-20T14:30", "2026-11-20T19:00", vagas)

	// 24 passageiros simultâneos distribuídos nas 3 empresas buscando e disputando as 4 vagas
	const passageiros = 24
	var wg sync.WaitGroup
	var confirmadas int64

	for i := 0; i < passageiros; i++ {
		wg.Add(1)
		emp := nomes[i%len(nomes)]
		est := estados[emp]

		go func(idx int, e *servidor.Estado) {
			defer wg.Done()
			idPass := cadastrar(t, e, fmt.Sprintf("p_%d", idx))

			plBusca, _ := json.Marshal(protocolo.PedidoBuscarItinerario{
				Origem: "Salvador", Destino: "MontesClaros", Data: "2026-11-20",
			})
			rBusca := e.HandleBuscarItinerario(plBusca)
			if !rBusca.Sucesso {
				return
			}
			var dadosB protocolo.RespostaBuscarItinerario
			_ = json.Unmarshal(rBusca.Dados, &dadosB)
			if len(dadosB.Itinerarios) == 0 {
				return
			}

			var refs []protocolo.RefTrecho
			for _, tr := range dadosB.Itinerarios[0].Trechos {
				refs = append(refs, protocolo.RefTrecho{Empresa: tr.Empresa, ID: tr.ID})
			}
			plRes, _ := json.Marshal(protocolo.PedidoConfirmarReserva{Itinerario: refs})
			rRes := e.HandleConfirmarReserva(idPass, plRes)
			if rRes.Sucesso {
				atomic.AddInt64(&confirmadas, 1)
				_ = e.HandleConsultarReservas(idPass)
			}
		}(i, est)
	}
	wg.Wait()

	if confirmadas != vagas {
		t.Fatalf("esperava %d reservas confirmadas, obteve %d", vagas, confirmadas)
	}

	// Confere recarregamento do estado.json em disco em uma nova instância
	recarregado, err := servidor.NovoEstado(servidor.Config{
		Empresa:      "rotasul",
		Peers:        map[string]string{},
		DirDados:     filepath.Join(dirBase, "rotasul"),
		Lease:        time.Second,
		TimeoutRede:  time.Second,
		TimeoutBusca: time.Second,
	})
	if err != nil {
		t.Fatalf("erro ao recarregar estado.json da rotasul: %v", err)
	}
	aud := recarregado.GerarAuditoria()
	if len(aud.Trechos) != 1 || aud.Trechos[0].AssentosLivres != 0 {
		t.Fatalf("estado recarregado do disco inconsistente: %+v", aud.Trechos)
	}
}

// cadastrar cria um usuário de teste no servidor e devolve o ID dele
func cadastrar(t *testing.T, e *servidor.Estado, login string) int64 {
	t.Helper()
	pl, _ := json.Marshal(protocolo.PedidoLogin{Login: login, Senha: "123"})
	r := e.HandleCadastro(pl)
	if !r.Sucesso {
		t.Fatalf("falha cadastro %s: %s", login, r.Erro)
	}
	var rl protocolo.RespostaLogin
	_ = json.Unmarshal(r.Dados, &rl)
	return rl.IDUsuario
}

// publicar cadastra um motorista e já publica uma carona de trecho único pra facilitar os testes
func publicar(t *testing.T, e *servidor.Estado, login, orig, dest, saida, chegada string, vagas int) {
	t.Helper()
	idMot := cadastrar(t, e, login)
	pl, _ := json.Marshal(protocolo.PedidoPublicarCarona{
		Trechos: []protocolo.TrechoPedido{{
			Origem: orig, Destino: dest,
			HorarioSaida: saida, HorarioChegada: chegada,
			PrecoCentavos: 3000, AssentosTotais: vagas,
		}},
	})
	r := e.HandlePublicarCarona(idMot, pl)
	if !r.Sucesso {
		t.Fatalf("falha publicar carona: %s", r.Erro)
	}
}

// TestOutboxSemBloqueioECascataSobFalha confere dois cenários de falha:
//  1. que um servidor fora do ar dando timeout não trava a outbox nem o
//     cancelamento em cascata pras outras empresas que estão online
//  2. que se o motorista cancelar a carona entre a fase 2 e a chegada do confirmar
//     da fase 3, o cancelamento em cascata ainda avisa todo mundo direitinho
func TestOutboxSemBloqueioECascataSobFalha(t *testing.T) {
	nomes := []string{"vaijunto", "rotasul", "expresso"}
	dirBase := t.TempDir()

	var bridgeMu sync.RWMutex
	handlers := make(map[string]http.Handler)
	bloqueado := make(map[string]bool)
	peers := make(map[string]string)

	for _, n := range nomes {
		nome := n
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			bridgeMu.RLock()
			travar := bloqueado[nome]
			h := handlers[nome]
			bridgeMu.RUnlock()
			if travar {
				// simula máquina desligada / firewall descartando pacotes até timeout
				time.Sleep(700 * time.Millisecond)
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			h.ServeHTTP(w, r)
		}))
		t.Cleanup(srv.Close)
		peers[nome] = srv.URL
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	estados := make(map[string]*servidor.Estado)
	for _, nome := range nomes {
		peersLocal := make(map[string]string)
		for k, v := range peers {
			if k != nome {
				peersLocal[k] = v
			}
		}
		est, err := servidor.NovoEstado(servidor.Config{
			Empresa:      nome,
			Peers:        peersLocal,
			DirDados:     filepath.Join(dirBase, nome),
			Lease:        600 * time.Millisecond,
			TimeoutRede:  400 * time.Millisecond,
			TimeoutBusca: 300 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("erro criando estado %s: %v", nome, err)
		}
		estados[nome] = est
		bridgeMu.Lock()
		handlers[nome] = est.HandlerREST()
		bridgeMu.Unlock()
	}
	for _, est := range estados {
		est.IniciarWorkers(ctx)
	}

	// Publica 1 vaga em cada empresa
	idMotA := cadastrar(t, estados["vaijunto"], "motA")
	idMotB := cadastrar(t, estados["rotasul"], "motB")
	idMotC := cadastrar(t, estados["expresso"], "motC")

	plA, _ := json.Marshal(protocolo.PedidoPublicarCarona{Trechos: []protocolo.TrechoPedido{{
		Origem: "C1", Destino: "C2", HorarioSaida: "2026-11-25T08:00", HorarioChegada: "2026-11-25T09:00", PrecoCentavos: 1000, AssentosTotais: 1,
	}}})
	estados["vaijunto"].HandlePublicarCarona(idMotA, plA)

	plB, _ := json.Marshal(protocolo.PedidoPublicarCarona{Trechos: []protocolo.TrechoPedido{{
		Origem: "C2", Destino: "C3", HorarioSaida: "2026-11-25T09:30", HorarioChegada: "2026-11-25T10:30", PrecoCentavos: 1000, AssentosTotais: 1,
	}}})
	rCarB := estados["rotasul"].HandlePublicarCarona(idMotB, plB)
	var dCarB protocolo.RespostaPublicarCarona
	_ = json.Unmarshal(rCarB.Dados, &dCarB)

	plC, _ := json.Marshal(protocolo.PedidoPublicarCarona{Trechos: []protocolo.TrechoPedido{{
		Origem: "C3", Destino: "C4", HorarioSaida: "2026-11-25T11:00", HorarioChegada: "2026-11-25T12:00", PrecoCentavos: 1000, AssentosTotais: 1,
	}}})
	estados["expresso"].HandlePublicarCarona(idMotC, plC)

	// Passageiro no VaiJunto reserva [vaijunto:1, rotasul:1, expresso:1]
	idPass := cadastrar(t, estados["vaijunto"], "pass1")
	plRes, _ := json.Marshal(protocolo.PedidoConfirmarReserva{
		Itinerario: []protocolo.RefTrecho{
			{Empresa: "vaijunto", ID: 1},
			{Empresa: "rotasul", ID: 1},
			{Empresa: "expresso", ID: 1},
		},
	})
	if r := estados["vaijunto"].HandleConfirmarReserva(idPass, plRes); !r.Sucesso {
		t.Fatalf("reserva deveria confirmar: %s", r.Erro)
	}

	// Isola o VaiJunto (blackhole com timeout) e cancela a carona na RotaSul
	bridgeMu.Lock()
	bloqueado["vaijunto"] = true
	bridgeMu.Unlock()

	t0 := time.Now()
	plCanc, _ := json.Marshal(protocolo.PedidoCancelarCarona{IDCarona: dCarB.IDCarona})
	if r := estados["rotasul"].HandleCancelarCarona(idMotB, plCanc); !r.Sucesso {
		t.Fatalf("cancelamento de carona falhou: %s", r.Erro)
	}
	if dur := time.Since(t0); dur > 350*time.Millisecond {
		t.Fatalf("HandleCancelarCarona demorou demais (%v) bloqueado por peer offline", dur)
	}

	// Expresso (saudável) já deve ter recebido o cancelamento direto da RotaSul!
	audC := estados["expresso"].GerarAuditoria()
	if audC.Trechos[0].AssentosLivres != 1 {
		t.Fatalf("assento na Expresso deveria estar livre com VaiJunto offline, mas livres=%d", audC.Trechos[0].AssentosLivres)
	}

	// Religando o VaiJunto, a outbox da RotaSul entrega e libera também o trecho do VaiJunto
	bridgeMu.Lock()
	bloqueado["vaijunto"] = false
	bridgeMu.Unlock()

	prazo := time.Now().Add(2 * time.Second)
	for time.Now().Before(prazo) {
		audA := estados["vaijunto"].GerarAuditoria()
		if len(audA.Reservas) == 1 && audA.Reservas[0].Status == modelos.ReservaCancelada && audA.Trechos[0].AssentosLivres == 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	audA := estados["vaijunto"].GerarAuditoria()
	if audA.Reservas[0].Status != modelos.ReservaCancelada || audA.Trechos[0].AssentosLivres != 1 {
		t.Fatalf("VaiJunto não convergiu após religar: reservas=%+v trechos=%+v", audA.Reservas, audA.Trechos)
	}

	// Cenário 2: corrida entre cancelamento de carona após Fase 2 e chegada do ConfirmarParticipante (Fase 3)
	plB2, _ := json.Marshal(protocolo.PedidoPublicarCarona{Trechos: []protocolo.TrechoPedido{{
		Origem: "X1", Destino: "X2", HorarioSaida: "2026-11-26T09:00", HorarioChegada: "2026-11-26T10:00", PrecoCentavos: 1000, AssentosTotais: 1,
	}}})
	rCarB2 := estados["rotasul"].HandlePublicarCarona(idMotB, plB2)
	var dCarB2 protocolo.RespostaPublicarCarona
	_ = json.Unmarshal(rCarB2.Dados, &dCarB2)

	cod, _ := estados["rotasul"].PrepararParticipante("tx-corrida", consorcio.PedidoPreparar{
		Coordenador:   "vaijunto",
		Participantes: []string{"rotasul", "expresso"},
		Ticket:        modelos.Ticket{Relogio: 10, Empresa: "vaijunto", TxID: "tx-corrida"},
		Trechos:       []int64{2},
		Passageiro:    "1@vaijunto",
		LeaseSegundos: 5,
	})
	if cod != http.StatusOK {
		t.Fatalf("esperava preparar 200, obteve %d", cod)
	}
	cod, _ = estados["rotasul"].PreConfirmarParticipante("tx-corrida")
	if cod != http.StatusOK {
		t.Fatalf("esperava pre-confirmar 200, obteve %d", cod)
	}

	// Motorista cancela a carona ANTES de ConfirmarParticipante chegar
	plCanc2, _ := json.Marshal(protocolo.PedidoCancelarCarona{IDCarona: dCarB2.IDCarona})
	estados["rotasul"].HandleCancelarCarona(idMotB, plCanc2)

	// Agora chega o ConfirmarParticipante atrasado da Fase 3: deve responder 200 (cancelada) e propagar à Expresso!
	cod, respConf := estados["rotasul"].ConfirmarParticipante("tx-corrida")
	if cod != http.StatusOK || respConf.Estado != modelos.TxCancelada {
		t.Fatalf("esperava 200 cancelada na corrida Fase 3 vs cancelamento, obteve cod=%d resp=%+v", cod, respConf)
	}
}

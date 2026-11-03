// Suíte automatizada de testes distribuídos sob carga e com injeção de falhas.
//
// Sobe 3 instâncias reais e isoladas do servidor (representando 3 empresas
// distintas do consórcio, cada uma em seu próprio processo, portas e pasta de
// persistência) e verifica:
//  1. Busca federada e reserva atômica combinando as 3 empresas;
//  2. Exclusão mútua sob carga concorrente (nenhum assento vendido 2x);
//  3. Critério de precedência com relógio lógico de Lamport (Wound-Wait);
//  4. Atomicidade sob disputa parcial, validação de encadeamento e sem deadlock;
//  5. Cancelamento de carona em cascata entre 3 empresas;
//  6. Falha de participante no meio da reserva + continuidade + recuperação;
//  7. Falha do coordenador após Fase 1 (liberação sem bloqueio indefinido);
//  8. Falha do coordenador após Fase 2 (terminação cooperativa consistente);
//  9. Carga intensa com queda abrupta (SIGKILL), recuperação e auditoria global;
//  10. Cancelamento de carona em cascata com coordenador offline e reconciliação.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/consorcio"
	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/modelos"
	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/protocolo"
	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/tcp"
)

type instanciaEmpresa struct {
	Nome      string
	PortaTCP  string
	PortaHTTP string
	DirDados  string
	PeersEnv  string
	Cmd       *exec.Cmd
	SaidaLog  bytes.Buffer
}

func (inst *instanciaEmpresa) EndTCP() string  { return "127.0.0.1:" + inst.PortaTCP }
func (inst *instanciaEmpresa) URLREST() string { return "http://127.0.0.1:" + inst.PortaHTTP }

type clusterTeste struct {
	binario  string
	dirBase  string
	empresas map[string]*instanciaEmpresa
	ordem    []string
}

var totalChecks, checksOk int

func checar(nome string, condicao bool, detalhe string) {
	totalChecks++
	if condicao {
		checksOk++
		fmt.Printf("  ✔ %s\n", nome)
	} else {
		fmt.Printf("  ✘ %s — %s\n", nome, detalhe)
	}
}

func main() {
	fmt.Println("=== Suíte Automatizada: Carga e Injeção de Falhas — Consórcio VAIJUNTO ===")

	cluster, err := subirCluster([]string{"vaijunto", "rotasul", "expresso"})
	if err != nil {
		fmt.Println("erro ao preparar cluster de teste:", err)
		os.Exit(1)
	}
	defer cluster.encerrarTudo()

	teste1ItinerarioTresEmpresas(cluster)
	teste2VendaDuplaMultiEmpresa(cluster)
	teste3PrecedenciaLamport(cluster)
	teste4AtomicidadeEDeadlock(cluster)
	teste5CancelamentoCascataTresEmpresas(cluster)
	teste6QuedaParticipanteMeioDaReserva(cluster)
	teste7QuedaCoordenadorAposPreparar(cluster)
	teste8QuedaCoordenadorAposPreConfirmar(cluster)
	teste9CargaComSigkillEAuditoriaGlobal(cluster)
	teste10CancelamentoCascataComCoordenadorOffline(cluster)

	fmt.Printf("\n=== Resumo Final: %d/%d verificações passaram ===\n", checksOk, totalChecks)
	if checksOk != totalChecks {
		os.Exit(1)
	}
}

// Casos de teste distribuídos

// 1. Busca e reserva de itinerário atravessando as 3 empresas do consórcio
func teste1ItinerarioTresEmpresas(c *clusterTeste) {
	fmt.Println("\n--- Teste 1: busca federada e reserva atômica em 3 empresas ---")

	tA := publicarTrechoUnico(c.empresas["vaijunto"], "mot_t1_a", protocolo.TrechoPedido{
		Origem: "Salvador_T1", Destino: "Feira_T1",
		HorarioSaida: "2026-12-01T08:00", HorarioChegada: "2026-12-01T09:30",
		PrecoCentavos: 3000, AssentosTotais: 2,
	})
	tB := publicarTrechoUnico(c.empresas["rotasul"], "mot_t1_b", protocolo.TrechoPedido{
		Origem: "Feira_T1", Destino: "Conquista_T1",
		HorarioSaida: "2026-12-01T10:00", HorarioChegada: "2026-12-01T14:00",
		PrecoCentavos: 5500, AssentosTotais: 2,
	})
	tC := publicarTrechoUnico(c.empresas["expresso"], "mot_t1_c", protocolo.TrechoPedido{
		Origem: "Conquista_T1", Destino: "MontesClaros_T1",
		HorarioSaida: "2026-12-01T14:30", HorarioChegada: "2026-12-01T19:00",
		PrecoCentavos: 7000, AssentosTotais: 2,
	})

	leitorP, escritorP, connP := novoCliente(c.empresas["vaijunto"].EndTCP(), "pass_t1")
	defer connP.Close()

	busca := buscarItinerarios(leitorP, escritorP, "Salvador_T1", "MontesClaros_T1", "2026-12-01")
	checar("passageiro no VaiJunto descobre itinerário combinando as 3 empresas",
		len(busca.Itinerarios) == 1 && len(busca.Itinerarios[0].Trechos) == 3,
		fmt.Sprintf("encontrados: %d", len(busca.Itinerarios)))

	resp := reservar(leitorP, escritorP, []protocolo.RefTrecho{
		{Empresa: "vaijunto", ID: tA},
		{Empresa: "rotasul", ID: tB},
		{Empresa: "expresso", ID: tC},
	})
	checar("itinerário de 3 empresas confirmado atomicamente", resp.Sucesso, resp.Erro)

	// rejeita mensagens malformadas e itinerários desconexos sem derrubar nada
	respDesconexo := reservar(leitorP, escritorP, []protocolo.RefTrecho{
		{Empresa: "vaijunto", ID: tA},
		{Empresa: "expresso", ID: tC}, // pula Feira -> Conquista!
	})
	checar("itinerário desconexo entre empresas é rejeitado sem vazar assentos", !respDesconexo.Sucesso, "deveria falhar")
}

// 2. Disputa simultânea a partir das 3 empresas pelos mesmos assentos
func teste2VendaDuplaMultiEmpresa(c *clusterTeste) {
	fmt.Println("\n--- Teste 2: disputa concorrente a partir das 3 empresas (sem venda dupla) ---")

	const vagas = 5
	const disputantes = 30

	tA := publicarTrechoUnico(c.empresas["vaijunto"], "mot_t2_a", protocolo.TrechoPedido{
		Origem: "Orig_T2", Destino: "Meio1_T2",
		HorarioSaida: "2026-12-02T08:00", HorarioChegada: "2026-12-02T09:00",
		PrecoCentavos: 2000, AssentosTotais: vagas,
	})
	tB := publicarTrechoUnico(c.empresas["rotasul"], "mot_t2_b", protocolo.TrechoPedido{
		Origem: "Meio1_T2", Destino: "Meio2_T2",
		HorarioSaida: "2026-12-02T09:30", HorarioChegada: "2026-12-02T11:00",
		PrecoCentavos: 2000, AssentosTotais: vagas,
	})
	tC := publicarTrechoUnico(c.empresas["expresso"], "mot_t2_c", protocolo.TrechoPedido{
		Origem: "Meio2_T2", Destino: "Fim_T2",
		HorarioSaida: "2026-12-02T11:30", HorarioChegada: "2026-12-02T13:00",
		PrecoCentavos: 2000, AssentosTotais: vagas,
	})

	itinerario := []protocolo.RefTrecho{
		{Empresa: "vaijunto", ID: tA},
		{Empresa: "rotasul", ID: tB},
		{Empresa: "expresso", ID: tC},
	}

	var wg sync.WaitGroup
	var sucessos int64
	tempos := make([]time.Duration, disputantes)

	for i := 0; i < disputantes; i++ {
		wg.Add(1)
		nomeEmp := c.ordem[i%len(c.ordem)]
		endTCP := c.empresas[nomeEmp].EndTCP()

		go func(idx int, endereco string) {
			defer wg.Done()
			l, e, conn := novoCliente(endereco, fmt.Sprintf("pass_t2_%d", idx))
			defer conn.Close()

			t0 := time.Now()
			r := reservar(l, e, itinerario)
			tempos[idx] = time.Since(t0)
			if r.Sucesso {
				atomic.AddInt64(&sucessos, 1)
			}
		}(i, endTCP)
	}
	wg.Wait()

	checar("exatamente os 5 assentos disponíveis foram confirmados (zero venda dupla)",
		sucessos == vagas,
		fmt.Sprintf("esperado %d, obtido %d", vagas, sucessos))

	sort.Slice(tempos, func(i, j int) bool { return tempos[i] < tempos[j] })
	var soma time.Duration
	for _, d := range tempos {
		soma += d
	}
	fmt.Printf("    tempos sob disputa (30 clientes × 3 servidores) — mín: %v | méd: %v | máx: %v\n",
		tempos[0].Round(100*time.Microsecond),
		(soma / time.Duration(disputantes)).Round(100*time.Microsecond),
		tempos[len(tempos)-1].Round(100*time.Microsecond))

	okGlobal, msgGlobal := auditarConsistenciaGlobal(c)
	checar("auditoria consistente nos 3 servidores após a disputa", okGlobal, msgGlobal)
}

// 3. Critério de precedência de Lamport sob atraso de mensagens
func teste3PrecedenciaLamport(c *clusterTeste) {
	fmt.Println("\n--- Teste 3: precedência por relógio lógico de Lamport sob atraso ---")

	idTrecho := publicarTrechoUnico(c.empresas["rotasul"], "mot_t3", protocolo.TrechoPedido{
		Origem: "Orig_T3", Destino: "Dest_T3",
		HorarioSaida: "2026-12-03T08:00", HorarioChegada: "2026-12-03T09:00",
		PrecoCentavos: 4000, AssentosTotais: 1,
	})

	// Simula mensagem mais nova (relógio Lamport = 500000, empresa expresso)
	// chegando ANTES ao servidor rotasul e ocupando a última vaga em pré-reserva.
	pedMaisNova := consorcio.PedidoPreparar{
		Coordenador:   "expresso",
		Participantes: []string{"rotasul"},
		Ticket:        modelos.Ticket{Relogio: 500000, Empresa: "expresso", TxID: "tx-atraso-nova"},
		Trechos:       []int64{idTrecho},
		Passageiro:    "99@expresso",
		LeaseSegundos: 5,
	}
	cod1, _ := chamarREST(c.empresas["rotasul"].URLREST(), http.MethodPost,
		consorcio.Prefixo+"/transacoes/tx-atraso-nova/preparar", pedMaisNova, nil)
	checar("requisição mais nova pré-reserva inicialmente o último assento", cod1 == http.StatusOK, fmt.Sprintf("HTTP %d", cod1))

	// Agora chega (atrasada pela rede) uma requisição causalmente ANTERIOR
	// (relógio Lamport = 5, empresa vaijunto), que tem maior precedência.
	pedMaisAntiga := consorcio.PedidoPreparar{
		Coordenador:   "vaijunto",
		Participantes: []string{"rotasul"},
		Ticket:        modelos.Ticket{Relogio: 5, Empresa: "vaijunto", TxID: "tx-atraso-prioritaria"},
		Trechos:       []int64{idTrecho},
		Passageiro:    "1@vaijunto",
		LeaseSegundos: 5,
	}
	cod2, _ := chamarREST(c.empresas["rotasul"].URLREST(), http.MethodPost,
		consorcio.Prefixo+"/transacoes/tx-atraso-prioritaria/preparar", pedMaisAntiga, nil)
	checar("transação com menor relógio de Lamport ganha precedência sobre a mais nova", cod2 == http.StatusOK, fmt.Sprintf("HTTP %d", cod2))

	// A mais nova tenta pré-confirmar e descobre que perdeu a vaga; a prioritária confirma.
	codPerdedora, _ := chamarREST(c.empresas["rotasul"].URLREST(), http.MethodPost,
		consorcio.Prefixo+"/transacoes/tx-atraso-nova/pre-confirmar", nil, nil)
	checar("transação de menor precedência é recusada na pré-confirmação (409)", codPerdedora == http.StatusConflict, fmt.Sprintf("HTTP %d", codPerdedora))

	codPreVencedora, _ := chamarREST(c.empresas["rotasul"].URLREST(), http.MethodPost,
		consorcio.Prefixo+"/transacoes/tx-atraso-prioritaria/pre-confirmar", nil, nil)
	codConfVencedora, _ := chamarREST(c.empresas["rotasul"].URLREST(), http.MethodPost,
		consorcio.Prefixo+"/transacoes/tx-atraso-prioritaria/confirmar", nil, nil)
	checar("transação prioritária confirma o último assento com sucesso",
		codPreVencedora == http.StatusOK && codConfVencedora == http.StatusOK,
		fmt.Sprintf("pre=%d conf=%d", codPreVencedora, codConfVencedora))
}

// 4. Atomicidade sob perda parcial de disputa e ausência de deadlock cruzado
func teste4AtomicidadeEDeadlock(c *clusterTeste) {
	fmt.Println("\n--- Teste 4: atomicidade multi-empresa e ausência de deadlock ---")

	tA := publicarTrechoUnico(c.empresas["vaijunto"], "mot_t4_a", protocolo.TrechoPedido{
		Origem: "A_T4", Destino: "B_T4",
		HorarioSaida: "2026-12-04T08:00", HorarioChegada: "2026-12-04T09:00",
		PrecoCentavos: 1500, AssentosTotais: 1,
	})
	tB := publicarTrechoUnico(c.empresas["rotasul"], "mot_t4_b", protocolo.TrechoPedido{
		Origem: "B_T4", Destino: "C_T4",
		HorarioSaida: "2026-12-04T09:30", HorarioChegada: "2026-12-04T10:30",
		PrecoCentavos: 1500, AssentosTotais: 1,
	})

	// Esgota o trecho B na rotasul primeiro
	l1, e1, conn1 := novoCliente(c.empresas["rotasul"].EndTCP(), "pass_t4_ocupaB")
	defer conn1.Close()
	rOcupa := reservar(l1, e1, []protocolo.RefTrecho{{Empresa: "rotasul", ID: tB}})
	checar("trecho na RotaSul ocupado previamente", rOcupa.Sucesso, rOcupa.Erro)

	// Agora passageiro no VaiJunto tenta [vaijunto:tA, rotasul:tB]: deve falhar e liberar tA!
	l2, e2, conn2 := novoCliente(c.empresas["vaijunto"].EndTCP(), "pass_t4_completo")
	defer conn2.Close()
	rFalha := reservar(l2, e2, []protocolo.RefTrecho{
		{Empresa: "vaijunto", ID: tA},
		{Empresa: "rotasul", ID: tB},
	})
	checar("itinerário multi-empresa falha por inteiro quando um trecho está lotado", !rFalha.Sucesso, "deveria falhar")

	// Confere que tA no VaiJunto não ficou preso
	rSoA := reservar(l2, e2, []protocolo.RefTrecho{{Empresa: "vaijunto", ID: tA}})
	checar("trecho do VaiJunto permaneceu livre (nenhuma reserva pela metade)", rSoA.Sucesso, rSoA.Erro)
}

// 5. Cancelar carona na Empresa B libera assentos nas Empresas A e C do mesmo itinerário
func teste5CancelamentoCascataTresEmpresas(c *clusterTeste) {
	fmt.Println("\n--- Teste 5: cancelamento de carona propaga liberação entre 3 empresas ---")

	tA := publicarTrechoUnico(c.empresas["vaijunto"], "mot_t5_a", protocolo.TrechoPedido{
		Origem: "A_T5", Destino: "B_T5",
		HorarioSaida: "2026-12-05T08:00", HorarioChegada: "2026-12-05T09:00",
		PrecoCentavos: 2000, AssentosTotais: 1,
	})

	lMotB, eMotB, connMotB := novoCliente(c.empresas["rotasul"].EndTCP(), "mot_t5_b")
	defer connMotB.Close()
	idCaronaB, trechosB := publicarCaronaCompleta(lMotB, eMotB, []protocolo.TrechoPedido{
		{Origem: "B_T5", Destino: "C_T5", HorarioSaida: "2026-12-05T09:30", HorarioChegada: "2026-12-05T10:30", PrecoCentavos: 2000, AssentosTotais: 1},
	})
	tB := trechosB[0].ID

	tC := publicarTrechoUnico(c.empresas["expresso"], "mot_t5_c", protocolo.TrechoPedido{
		Origem: "C_T5", Destino: "D_T5",
		HorarioSaida: "2026-12-05T11:00", HorarioChegada: "2026-12-05T12:00",
		PrecoCentavos: 2000, AssentosTotais: 1,
	})

	lPass, ePass, connPass := novoCliente(c.empresas["vaijunto"].EndTCP(), "pass_t5")
	defer connPass.Close()
	rConf := reservar(lPass, ePass, []protocolo.RefTrecho{
		{Empresa: "vaijunto", ID: tA},
		{Empresa: "rotasul", ID: tB},
		{Empresa: "expresso", ID: tC},
	})
	checar("reserva de 3 empresas confirmada antes do cancelamento", rConf.Sucesso, rConf.Erro)

	// Motorista na RotaSul cancela a carona dele
	payloadCanc, _ := json.Marshal(protocolo.PedidoCancelarCarona{IDCarona: idCaronaB})
	rCanc := enviarEReceber(lMotB, eMotB, protocolo.Mensagem{Operacao: protocolo.OpCancelarCarona, Payload: payloadCanc})
	checar("cancelamento da carona na RotaSul aceito", rCanc.Sucesso, rCanc.Erro)

	// Verifica que o assento na Expresso (3ª empresa!) foi liberado automaticamente
	lPassC, ePassC, connPassC := novoCliente(c.empresas["expresso"].EndTCP(), "pass_t5_novoC")
	defer connPassC.Close()
	rNovoC := reservar(lPassC, ePassC, []protocolo.RefTrecho{{Empresa: "expresso", ID: tC}})
	checar("assento na 3ª empresa (Expresso) foi liberado pelo cancelamento na RotaSul", rNovoC.Sucesso, rNovoC.Erro)
}

// 6. Injeção de falha: participante (Expresso) cai no meio da reserva, demais
// empresas continuam operando, e ao reiniciar a Expresso nenhum assento fica preso.
func teste6QuedaParticipanteMeioDaReserva(c *clusterTeste) {
	fmt.Println("\n--- Teste 6: injeção de falha — queda de participante no meio da reserva ---")

	tA := publicarTrechoUnico(c.empresas["vaijunto"], "mot_t6_a", protocolo.TrechoPedido{
		Origem: "A_T6", Destino: "B_T6",
		HorarioSaida: "2026-12-06T08:00", HorarioChegada: "2026-12-06T09:00",
		PrecoCentavos: 2000, AssentosTotais: 1,
	})
	tB := publicarTrechoUnico(c.empresas["rotasul"], "mot_t6_b", protocolo.TrechoPedido{
		Origem: "B_T6", Destino: "C_T6",
		HorarioSaida: "2026-12-06T09:30", HorarioChegada: "2026-12-06T10:30",
		PrecoCentavos: 2000, AssentosTotais: 1,
	})
	tC := publicarTrechoUnico(c.empresas["expresso"], "mot_t6_c", protocolo.TrechoPedido{
		Origem: "C_T6", Destino: "D_T6",
		HorarioSaida: "2026-12-06T11:00", HorarioChegada: "2026-12-06T12:00",
		PrecoCentavos: 2000, AssentosTotais: 1,
	})

	// Arma falha na Expresso para cair logo após gravar a pré-reserva em disco
	armarFalha(c.empresas["expresso"], consorcio.FalhaPartAposPreparar)

	lPass, ePass, connPass := novoCliente(c.empresas["vaijunto"].EndTCP(), "pass_t6")
	defer connPass.Close()

	rFalha := reservar(lPass, ePass, []protocolo.RefTrecho{
		{Empresa: "vaijunto", ID: tA},
		{Empresa: "rotasul", ID: tB},
		{Empresa: "expresso", ID: tC},
	})
	_ = c.empresas["expresso"].Cmd.Wait()
	checar("reserva envolvendo servidor que caiu é abortada com segurança", !rFalha.Sucesso, "deveria ter abortado")

	// Enquanto a Expresso está CAÍDA, VaiJunto e RotaSul continuam operando normalmente!
	buscaSemC := buscarItinerarios(lPass, ePass, "A_T6", "C_T6", "2026-12-06")
	checar("com Expresso fora do ar, busca entre VaiJunto e RotaSul continua funcionando",
		len(buscaSemC.Itinerarios) == 1 && len(buscaSemC.EmpresasIndisponiveis) == 1 && buscaSemC.EmpresasIndisponiveis[0] == "expresso",
		fmt.Sprintf("itinerários=%d indisponíveis=%v", len(buscaSemC.Itinerarios), buscaSemC.EmpresasIndisponiveis))

	rAB := reservar(lPass, ePass, []protocolo.RefTrecho{
		{Empresa: "vaijunto", ID: tA},
		{Empresa: "rotasul", ID: tB},
	})
	checar("assentos em VaiJunto e RotaSul foram liberados e reservados normalmente com Expresso caída",
		rAB.Sucesso, rAB.Erro)

	// Agora a Expresso volta a operar (lendo seu estado.json persistido antes da queda)
	if err := c.iniciarInstancia("expresso"); err != nil {
		checar("reinício do servidor Expresso", false, err.Error())
		return
	}
	aguardarConvergencia(c)

	// O único assento de tC na Expresso (que tinha ficado pré-reservado antes do crash) deve estar livre!
	lPassC, ePassC, connPassC := novoCliente(c.empresas["expresso"].EndTCP(), "pass_t6_pos_reboot")
	defer connPassC.Close()
	rC := reservar(lPassC, ePassC, []protocolo.RefTrecho{{Empresa: "expresso", ID: tC}})
	checar("após recuperação da Expresso, assento pré-reservado antes da queda não ficou bloqueado",
		rC.Sucesso, rC.Erro)
}

// 7. Injeção de falha: coordenador (VaiJunto) cai após todos prepararem (Fase 1).
// Mesmo com VaiJunto morto, RotaSul e Expresso liberam os assentos após o lease!
func teste7QuedaCoordenadorAposPreparar(c *clusterTeste) {
	fmt.Println("\n--- Teste 7: injeção de falha — queda do coordenador após Fase 1 (sem bloqueio indefinido) ---")

	tB := publicarTrechoUnico(c.empresas["rotasul"], "mot_t7_b", protocolo.TrechoPedido{
		Origem: "B_T7", Destino: "C_T7",
		HorarioSaida: "2026-12-07T08:00", HorarioChegada: "2026-12-07T09:00",
		PrecoCentavos: 3000, AssentosTotais: 1,
	})
	tC := publicarTrechoUnico(c.empresas["expresso"], "mot_t7_c", protocolo.TrechoPedido{
		Origem: "C_T7", Destino: "D_T7",
		HorarioSaida: "2026-12-07T09:30", HorarioChegada: "2026-12-07T10:30",
		PrecoCentavos: 3000, AssentosTotais: 1,
	})

	armarFalha(c.empresas["vaijunto"], consorcio.FalhaCoordAposPreparar)

	lPassA, ePassA, connPassA := novoCliente(c.empresas["vaijunto"].EndTCP(), "pass_t7_a")
	_ = reservar(lPassA, ePassA, []protocolo.RefTrecho{
		{Empresa: "rotasul", ID: tB},
		{Empresa: "expresso", ID: tC},
	})
	connPassA.Close()
	_ = c.empresas["vaijunto"].Cmd.Wait()

	// VaiJunto continua MORTO! Aguardamos o lease vencer (~1.2s) para RotaSul e Expresso liberarem as vagas.
	time.Sleep(1500 * time.Millisecond)

	lPassB, ePassB, connPassB := novoCliente(c.empresas["rotasul"].EndTCP(), "pass_t7_b")
	defer connPassB.Close()
	rEnquantoMorto := reservar(lPassB, ePassB, []protocolo.RefTrecho{
		{Empresa: "rotasul", ID: tB},
		{Empresa: "expresso", ID: tC},
	})
	checar("RotaSul e Expresso desbloqueiam os assentos por expiração de lease mesmo com coordenador morto",
		rEnquantoMorto.Sucesso, rEnquantoMorto.Erro)

	// Reinicia o VaiJunto e verifica consistência
	if err := c.iniciarInstancia("vaijunto"); err != nil {
		checar("reinício do servidor VaiJunto", false, err.Error())
		return
	}
	aguardarConvergencia(c)

	okGlobal, msgGlobal := auditarConsistenciaGlobal(c)
	checar("consórcio permanece 100% consistente após retorno do coordenador VaiJunto", okGlobal, msgGlobal)
}

// 8. Injeção de falha: coordenador (VaiJunto) cai após todos pré-confirmarem (Fase 2).
// RotaSul e Expresso concluem o commit cooperativamente e VaiJunto reconcilia ao voltar.
func teste8QuedaCoordenadorAposPreConfirmar(c *clusterTeste) {
	fmt.Println("\n--- Teste 8: injeção de falha — queda do coordenador após Fase 2 (terminação cooperativa) ---")

	tB := publicarTrechoUnico(c.empresas["rotasul"], "mot_t8_b", protocolo.TrechoPedido{
		Origem: "B_T8", Destino: "C_T8",
		HorarioSaida: "2026-12-08T08:00", HorarioChegada: "2026-12-08T09:00",
		PrecoCentavos: 3000, AssentosTotais: 1,
	})
	tC := publicarTrechoUnico(c.empresas["expresso"], "mot_t8_c", protocolo.TrechoPedido{
		Origem: "C_T8", Destino: "D_T8",
		HorarioSaida: "2026-12-08T09:30", HorarioChegada: "2026-12-08T10:30",
		PrecoCentavos: 3000, AssentosTotais: 1,
	})

	loginPass := fmt.Sprintf("pass_t8_%d", time.Now().UnixNano())
	lPassA, ePassA, connPassA := novoClienteComLoginExato(c.empresas["vaijunto"].EndTCP(), loginPass)
	armarFalha(c.empresas["vaijunto"], consorcio.FalhaCoordAposPreConfirmar)

	_ = reservar(lPassA, ePassA, []protocolo.RefTrecho{
		{Empresa: "rotasul", ID: tB},
		{Empresa: "expresso", ID: tC},
	})
	connPassA.Close()
	_ = c.empresas["vaijunto"].Cmd.Wait()

	// Com VaiJunto morto, RotaSul e Expresso conversam após o lease e efetivam cooperativamente.
	time.Sleep(1500 * time.Millisecond)

	// Reinicia VaiJunto: no boot ele consulta os participantes e grava a reserva do passageiro como confirmada!
	if err := c.iniciarInstancia("vaijunto"); err != nil {
		checar("reinício do VaiJunto após Fase 2", false, err.Error())
		return
	}
	aguardarConvergencia(c)

	// Passageiro reconecta e faz login no VaiJunto para conferir sua reserva
	connLogin, err := net.Dial("tcp", c.empresas["vaijunto"].EndTCP())
	if err != nil {
		checar("reconexão ao VaiJunto", false, err.Error())
		return
	}
	defer connLogin.Close()
	lRec, eRec := bufio.NewReader(connLogin), bufio.NewWriter(connLogin)
	plLogin, _ := json.Marshal(protocolo.PedidoLogin{Login: loginPass, Senha: "123"})
	rLog := enviarEReceber(lRec, eRec, protocolo.Mensagem{Operacao: protocolo.OpLogin, Payload: plLogin})
	rCons := enviarEReceber(lRec, eRec, protocolo.Mensagem{Operacao: protocolo.OpConsultarReservas})
	var cons protocolo.RespostaConsultarReservas
	_ = json.Unmarshal(rCons.Dados, &cons)

	checar("após terminação cooperativa e reboot, reserva consta confirmada sem ficar pela metade",
		rLog.Sucesso && len(cons.Reservas) == 1 && cons.Reservas[0].Status == modelos.ReservaConfirmada,
		fmt.Sprintf("login=%v reservas=%+v", rLog.Sucesso, cons.Reservas))

	okGlobal, msgGlobal := auditarConsistenciaGlobal(c)
	checar("auditoria global consistente após terminação cooperativa", okGlobal, msgGlobal)
}

// 9. Carga concorrente intensa nas 3 empresas com queda abrupta (SIGKILL) no meio
func teste9CargaComSigkillEAuditoriaGlobal(c *clusterTeste) {
	fmt.Println("\n--- Teste 9: carga concorrente com queda abrupta (SIGKILL), recuperação e auditoria ---")

	publicarTrechoUnico(c.empresas["vaijunto"], "mot_t9_a", protocolo.TrechoPedido{
		Origem: "S1_T9", Destino: "S2_T9",
		HorarioSaida: "2026-12-09T08:00", HorarioChegada: "2026-12-09T09:00",
		PrecoCentavos: 2500, AssentosTotais: 8,
	})
	publicarTrechoUnico(c.empresas["rotasul"], "mot_t9_b", protocolo.TrechoPedido{
		Origem: "S2_T9", Destino: "S3_T9",
		HorarioSaida: "2026-12-09T09:30", HorarioChegada: "2026-12-09T11:00",
		PrecoCentavos: 2500, AssentosTotais: 6,
	})
	publicarTrechoUnico(c.empresas["expresso"], "mot_t9_c", protocolo.TrechoPedido{
		Origem: "S3_T9", Destino: "S4_T9",
		HorarioSaida: "2026-12-09T11:30", HorarioChegada: "2026-12-09T13:00",
		PrecoCentavos: 2500, AssentosTotais: 7,
	})
	// Rota direta VaiJunto -> Expresso (independente da RotaSul, deve funcionar mesmo com RotaSul morta!)
	publicarTrechoUnico(c.empresas["vaijunto"], "mot_t9_a2", protocolo.TrechoPedido{
		Origem: "S1_T9", Destino: "S3_T9",
		HorarioSaida: "2026-12-09T08:30", HorarioChegada: "2026-12-09T11:00",
		PrecoCentavos: 4000, AssentosTotais: 6,
	})

	var wg sync.WaitGroup
	var confirmadasDuranteCarga int64

	// Dispara 24 passageiros concorrentes nas 3 empresas fazendo buscas e reservas
	for i := 0; i < 24; i++ {
		wg.Add(1)
		nomeEmp := c.ordem[i%len(c.ordem)]
		endTCP := c.empresas[nomeEmp].EndTCP()

		go func(id int, endereco string) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", endereco, time.Second)
			if err != nil {
				return
			}
			defer conn.Close()
			l, e := bufio.NewReader(conn), bufio.NewWriter(conn)
			pl, _ := json.Marshal(protocolo.PedidoLogin{
				Login: fmt.Sprintf("carga_p%d_%d", id, time.Now().UnixNano()),
				Senha: "123",
			})
			if r := enviarEReceber(l, e, protocolo.Mensagem{Operacao: protocolo.OpCadastro, Payload: pl}); !r.Sucesso {
				return
			}

			for k := 0; k < 4; k++ {
				b := buscarItinerarios(l, e, "S1_T9", "S4_T9", "2026-12-09")
				if len(b.Itinerarios) == 0 {
					time.Sleep(25 * time.Millisecond)
					continue
				}
				it := b.Itinerarios[id%len(b.Itinerarios)]
				var refs []protocolo.RefTrecho
				for _, tr := range it.Trechos {
					refs = append(refs, protocolo.RefTrecho{Empresa: tr.Empresa, ID: tr.ID})
				}
				if r := reservar(l, e, refs); r.Sucesso {
					atomic.AddInt64(&confirmadasDuranteCarga, 1)
				}
				time.Sleep(20 * time.Millisecond)
			}
		}(i, endTCP)
	}

	// No meio da tempestade de requisições, mata abruptamente (SIGKILL) a RotaSul!
	time.Sleep(80 * time.Millisecond)
	if c.empresas["rotasul"].Cmd != nil && c.empresas["rotasul"].Cmd.Process != nil {
		_ = c.empresas["rotasul"].Cmd.Process.Kill()
		_ = c.empresas["rotasul"].Cmd.Wait()
	}

	// Deixa o consórcio operar sem a RotaSul por um instante e depois a religa
	time.Sleep(400 * time.Millisecond)
	if err := c.iniciarInstancia("rotasul"); err != nil {
		checar("recuperação da RotaSul após SIGKILL", false, err.Error())
		return
	}

	wg.Wait()
	aguardarConvergencia(c)

	checar("reservas independentes prosseguiram com sucesso durante carga e falha",
		confirmadasDuranteCarga > 0,
		fmt.Sprintf("confirmadas=%d", confirmadasDuranteCarga))

	okGlobal, msgGlobal := auditarConsistenciaGlobal(c)
	checar("invariantes globais intactas após carga + SIGKILL + recuperação (sem venda dupla, sem reserva pela metade, sem assento preso)",
		okGlobal, msgGlobal)
}

// 10. Cancelamento de carona na RotaSul enquanto o coordenador VaiJunto está fora do ar:
// Expresso libera a vaga na hora (sem esperar VaiJunto), e VaiJunto reconcilia ao voltar.
func teste10CancelamentoCascataComCoordenadorOffline(c *clusterTeste) {
	fmt.Println("\n--- Teste 10: cancelamento de carona em cascata com coordenador offline ---")

	tA := publicarTrechoUnico(c.empresas["vaijunto"], "mot_t10_a", protocolo.TrechoPedido{
		Origem: "A_T10", Destino: "B_T10",
		HorarioSaida: "2026-12-10T08:00", HorarioChegada: "2026-12-10T09:00",
		PrecoCentavos: 2000, AssentosTotais: 1,
	})

	lMotB, eMotB, connMotB := novoCliente(c.empresas["rotasul"].EndTCP(), "mot_t10_b")
	defer connMotB.Close()
	idCaronaB, trechosB := publicarCaronaCompleta(lMotB, eMotB, []protocolo.TrechoPedido{
		{Origem: "B_T10", Destino: "C_T10", HorarioSaida: "2026-12-10T09:30", HorarioChegada: "2026-12-10T10:30", PrecoCentavos: 2000, AssentosTotais: 1},
	})
	tB := trechosB[0].ID

	tC := publicarTrechoUnico(c.empresas["expresso"], "mot_t10_c", protocolo.TrechoPedido{
		Origem: "C_T10", Destino: "D_T10",
		HorarioSaida: "2026-12-10T11:00", HorarioChegada: "2026-12-10T12:00",
		PrecoCentavos: 2000, AssentosTotais: 1,
	})

	lPass, ePass, connPass := novoCliente(c.empresas["vaijunto"].EndTCP(), "pass_t10")
	rConf := reservar(lPass, ePass, []protocolo.RefTrecho{
		{Empresa: "vaijunto", ID: tA},
		{Empresa: "rotasul", ID: tB},
		{Empresa: "expresso", ID: tC},
	})
	connPass.Close()
	checar("itinerário de 3 empresas confirmado antes da queda do coordenador", rConf.Sucesso, rConf.Erro)

	// Derruba o servidor VaiJunto (coordenador do passageiro)
	if c.empresas["vaijunto"].Cmd != nil && c.empresas["vaijunto"].Cmd.Process != nil {
		_ = c.empresas["vaijunto"].Cmd.Process.Kill()
		_ = c.empresas["vaijunto"].Cmd.Wait()
	}

	// Com VaiJunto FORA DO AR, o motorista na RotaSul cancela sua carona
	plCanc, _ := json.Marshal(protocolo.PedidoCancelarCarona{IDCarona: idCaronaB})
	rCanc := enviarEReceber(lMotB, eMotB, protocolo.Mensagem{Operacao: protocolo.OpCancelarCarona, Payload: plCanc})
	checar("cancelamento de carona na RotaSul responde rápido mesmo com VaiJunto offline", rCanc.Sucesso, rCanc.Erro)

	// Mesmo com VaiJunto AINDA FORA DO AR, o assento na 3ª empresa (Expresso) já deve estar livre!
	lPassC, ePassC, connPassC := novoCliente(c.empresas["expresso"].EndTCP(), "pass_t10_c")
	defer connPassC.Close()
	rNovoC := reservar(lPassC, ePassC, []protocolo.RefTrecho{{Empresa: "expresso", ID: tC}})
	checar("Expresso liberou o assento imediatamente mesmo com o coordenador VaiJunto offline", rNovoC.Sucesso, rNovoC.Erro)

	// VaiJunto volta ao ar: outbox da RotaSul entrega o cancelamento e libera também o trecho tA
	if err := c.iniciarInstancia("vaijunto"); err != nil {
		checar("reinício do VaiJunto no Teste 10", false, err.Error())
		return
	}
	aguardarConvergencia(c)

	okGlobal, msgGlobal := auditarConsistenciaGlobal(c)
	checar("consistência global 100% convergida após retorno do VaiJunto", okGlobal, msgGlobal)
}

// Auditoria global das invariantes nas 3 empresas (sem venda dupla, sem reserva pela metade e sem assento preso)

// auditarConsistenciaGlobal consulta /admin/auditoria nas 3 empresas e cruza todos os dados
func auditarConsistenciaGlobal(c *clusterTeste) (bool, string) {
	auditorias := make(map[string]consorcio.Auditoria)
	for _, nome := range c.ordem {
		var aud consorcio.Auditoria
		cod, err := chamarREST(c.empresas[nome].URLREST(), http.MethodGet,
			consorcio.Prefixo+"/admin/auditoria", nil, &aud)
		if err != nil || cod != http.StatusOK {
			return false, fmt.Sprintf("falha ao auditar %s: cod=%d err=%v", nome, cod, err)
		}
		if aud.OutboxPendentes > 0 {
			return false, fmt.Sprintf("%s ainda tem %d mensagens na outbox", nome, aud.OutboxPendentes)
		}
		auditorias[nome] = aud
	}

	// 1. Verifica em cada empresa: zero assentos vendidos acima do total e zero pré-reservas presas
	estadoPorEmpresaETx := make(map[string]map[string]string)
	for nome, aud := range auditorias {
		estadoPorEmpresaETx[nome] = make(map[string]string)
		for _, tx := range aud.TxParticipante {
			if tx.Estado == modelos.TxPreparada || tx.Estado == modelos.TxPreConfirmada {
				return false, fmt.Sprintf("assento permaneceu bloqueado em %s pela transação %s (%s)", nome, tx.ID, tx.Estado)
			}
			estadoPorEmpresaETx[nome][tx.ID] = tx.Estado
		}

		for _, tr := range aud.Trechos {
			if len(tr.Ocupacoes) > tr.AssentosTotais {
				return false, fmt.Sprintf("venda dupla em %s trecho %d: %d ocupações para %d assentos",
					nome, tr.ID, len(tr.Ocupacoes), tr.AssentosTotais)
			}
			if tr.AssentosLivres != tr.AssentosTotais-len(tr.Ocupacoes) {
				return false, fmt.Sprintf("contagem divergente em %s trecho %d: livres=%d totais=%d ocup=%d",
					nome, tr.ID, tr.AssentosLivres, tr.AssentosTotais, len(tr.Ocupacoes))
			}
			for _, oc := range tr.Ocupacoes {
				if oc.Estado != modelos.TxConfirmada {
					return false, fmt.Sprintf("trecho %d em %s ocupado por transação não confirmada %s (%s)",
						tr.ID, nome, oc.TxID, oc.Estado)
				}
			}
		}
	}

	// 2. Verifica atomicidade entre empresas: nenhum itinerário confirmado pela metade!
	for nomeCoord, aud := range auditorias {
		reservasMap := make(map[string]modelos.Reserva)
		for _, r := range aud.Reservas {
			reservasMap[r.ID] = r
		}

		for _, coord := range aud.TxCoordenador {
			switch coord.Fase {
			case modelos.CoordConfirmada:
				res, existe := reservasMap[coord.ID]
				if !existe {
					return false, fmt.Sprintf("transação confirmada %s sem registro de Reserva em %s", coord.ID, nomeCoord)
				}
				estEsperado := modelos.TxConfirmada
				if res.Status == modelos.ReservaCancelada {
					estEsperado = modelos.TxCancelada
				}
				for _, empPart := range coord.Participantes {
					estReal := estadoPorEmpresaETx[empPart][coord.ID]
					if estReal != estEsperado {
						return false, fmt.Sprintf("itinerário inconsistente %s: coordenador %s=%s, mas participante %s=%q",
							coord.ID, nomeCoord, res.Status, empPart, estReal)
					}
				}
			case modelos.CoordAbortada:
				for _, empPart := range coord.Participantes {
					estReal := estadoPorEmpresaETx[empPart][coord.ID]
					if estReal == modelos.TxConfirmada {
						return false, fmt.Sprintf("itinerário pela metade %s: abortado em %s, mas confirmado em %s",
							coord.ID, nomeCoord, empPart)
					}
				}
			default:
				return false, fmt.Sprintf("coordenador %s ainda com transação %s em aberto (%s)", nomeCoord, coord.ID, coord.Fase)
			}
		}
	}

	return true, ""
}

// aguardarConvergencia espera até que leases vencidos e outboxes terminem de
// convergir (até um limite de 4 segundos).
func aguardarConvergencia(c *clusterTeste) {
	prazo := time.Now().Add(4 * time.Second)
	for time.Now().Before(prazo) {
		ok, _ := auditarConsistenciaGlobal(c)
		if ok {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
}

// Gerenciamento dos 3 processos reais de servidor usados durante a suíte

// subirCluster compila o binário do servidor e inicia as 3 empresas em pastas e portas isoladas
func subirCluster(nomes []string) (*clusterTeste, error) {
	dirBase, err := os.MkdirTemp("", "vaijunto-consorcio-teste-*")
	if err != nil {
		return nil, err
	}

	binario := filepath.Join(dirBase, "servidor-bin")
	build := exec.Command("go", "build", "-o", binario, "./cmd/servidor")
	if out, err := build.CombinedOutput(); err != nil {
		_ = os.RemoveAll(dirBase)
		return nil, fmt.Errorf("compilando servidor: %v\n%s", err, string(out))
	}

	c := &clusterTeste{
		binario:  binario,
		dirBase:  dirBase,
		empresas: make(map[string]*instanciaEmpresa),
		ordem:    nomes,
	}

	var partesPeers []string
	for _, nome := range nomes {
		pTCP, err1 := portaLivre()
		pHTTP, err2 := portaLivre()
		if err1 != nil || err2 != nil {
			_ = os.RemoveAll(dirBase)
			return nil, fmt.Errorf("alocando portas: %v / %v", err1, err2)
		}
		c.empresas[nome] = &instanciaEmpresa{
			Nome:      nome,
			PortaTCP:  pTCP,
			PortaHTTP: pHTTP,
			DirDados:  filepath.Join(dirBase, "dados", nome),
		}
		partesPeers = append(partesPeers, fmt.Sprintf("%s=http://127.0.0.1:%s", nome, pHTTP))
	}
	peersComum := strings.Join(partesPeers, ",")
	for _, inst := range c.empresas {
		inst.PeersEnv = peersComum
	}

	for _, nome := range nomes {
		if err := c.iniciarInstancia(nome); err != nil {
			c.encerrarTudo()
			return nil, err
		}
	}
	return c, nil
}

func (c *clusterTeste) iniciarInstancia(nome string) error {
	inst := c.empresas[nome]
	cmd := exec.Command(c.binario)
	cmd.Env = append(os.Environ(),
		"EMPRESA="+inst.Nome,
		"PORTA_TCP="+inst.PortaTCP,
		"PORTA_HTTP="+inst.PortaHTTP,
		"DIR_DADOS="+inst.DirDados,
		"PEERS="+inst.PeersEnv,
		"LEASE_MS=900",
		"TIMEOUT_REDE_MS=500",
		"TIMEOUT_BUSCA_MS=400",
		"PERMITIR_FALHAS=1",
	)
	cmd.Stdout = &inst.SaidaLog
	cmd.Stderr = &inst.SaidaLog
	if err := cmd.Start(); err != nil {
		return err
	}
	inst.Cmd = cmd

	// aguarda a API REST e a porta TCP ficarem prontas
	prazo := time.Now().Add(4 * time.Second)
	for time.Now().Before(prazo) {
		cod, err := chamarREST(inst.URLREST(), http.MethodGet, consorcio.Prefixo+"/saude", nil, nil)
		if err == nil && cod == http.StatusOK {
			if conn, errTCP := net.DialTimeout("tcp", inst.EndTCP(), 200*time.Millisecond); errTCP == nil {
				conn.Close()
				return nil
			}
		}
		time.Sleep(40 * time.Millisecond)
	}
	return fmt.Errorf("servidor %s não ficou pronto a tempo:\n%s", nome, inst.SaidaLog.String())
}

func (c *clusterTeste) encerrarTudo() {
	for _, inst := range c.empresas {
		if inst.Cmd != nil && inst.Cmd.Process != nil {
			_ = inst.Cmd.Process.Kill()
			_ = inst.Cmd.Wait()
		}
	}
	_ = os.RemoveAll(c.dirBase)
}

var proximaPortaTeste int32 = 19100

// portaLivre escolhe uma porta livre abaixo da faixa efêmera do Linux (32768+),
// evitando que o kernel entregue a porta de um servidor caído para um Dial()
// cliente enquanto o servidor está desligado durante o teste de falha.
func portaLivre() (string, error) {
	for tentativas := 0; tentativas < 500; tentativas++ {
		p := atomic.AddInt32(&proximaPortaTeste, 1)
		end := fmt.Sprintf("127.0.0.1:%d", p)
		l, err := net.Listen("tcp", end)
		if err == nil {
			_ = l.Close()
			return fmt.Sprintf("%d", p), nil
		}
	}
	return "", fmt.Errorf("nenhuma porta livre encontrada na faixa de teste")
}

// Funções auxiliares de cliente TCP e chamadas REST usadas nos testes

func enviarEReceber(leitor *bufio.Reader, escritor *bufio.Writer, msg protocolo.Mensagem) protocolo.Resposta {
	dados, _ := json.Marshal(msg)
	if err := tcp.EnviarMensagem(escritor, dados); err != nil {
		return protocolo.Resposta{Sucesso: false, Erro: "falha de envio: " + err.Error()}
	}
	respostaBytes, err := tcp.LerMensagem(leitor)
	if err != nil {
		return protocolo.Resposta{Sucesso: false, Erro: "falha de conexão: " + err.Error()}
	}
	var resposta protocolo.Resposta
	_ = json.Unmarshal(respostaBytes, &resposta)
	return resposta
}

func novoCliente(endereco, prefixo string) (*bufio.Reader, *bufio.Writer, net.Conn) {
	login := fmt.Sprintf("%s_%d", prefixo, time.Now().UnixNano())
	return novoClienteComLoginExato(endereco, login)
}

func novoClienteComLoginExato(endereco, login string) (*bufio.Reader, *bufio.Writer, net.Conn) {
	conn, err := net.DialTimeout("tcp", endereco, 2*time.Second)
	if err != nil {
		fmt.Println("erro fatal ao conectar em", endereco, ":", err)
		os.Exit(1)
	}
	l, e := bufio.NewReader(conn), bufio.NewWriter(conn)
	payload, _ := json.Marshal(protocolo.PedidoLogin{Login: login, Senha: "123"})
	r := enviarEReceber(l, e, protocolo.Mensagem{Operacao: protocolo.OpCadastro, Payload: payload})
	if !r.Sucesso {
		fmt.Println("erro ao cadastrar", login, ":", r.Erro)
		os.Exit(1)
	}
	return l, e, conn
}

func publicarTrechoUnico(inst *instanciaEmpresa, loginBase string, tp protocolo.TrechoPedido) int64 {
	l, e, conn := novoCliente(inst.EndTCP(), loginBase)
	defer conn.Close()
	_, trechos := publicarCaronaCompleta(l, e, []protocolo.TrechoPedido{tp})
	return trechos[0].ID
}

func publicarCaronaCompleta(l *bufio.Reader, e *bufio.Writer, trechos []protocolo.TrechoPedido) (int64, []protocolo.TrechoDetalhado) {
	payload, _ := json.Marshal(protocolo.PedidoPublicarCarona{Trechos: trechos})
	resp := enviarEReceber(l, e, protocolo.Mensagem{Operacao: protocolo.OpPublicarCarona, Payload: payload})
	if !resp.Sucesso {
		fmt.Println("erro ao publicar carona:", resp.Erro)
		os.Exit(1)
	}
	var pub protocolo.RespostaPublicarCarona
	_ = json.Unmarshal(resp.Dados, &pub)

	respCons := enviarEReceber(l, e, protocolo.Mensagem{Operacao: protocolo.OpConsultarCaronas})
	var cons protocolo.RespostaConsultarCaronas
	_ = json.Unmarshal(respCons.Dados, &cons)
	for _, c := range cons.Caronas {
		if c.ID == pub.IDCarona {
			return c.ID, c.Trechos
		}
	}
	os.Exit(1)
	return 0, nil
}

func buscarItinerarios(l *bufio.Reader, e *bufio.Writer, origem, destino, data string) protocolo.RespostaBuscarItinerario {
	payload, _ := json.Marshal(protocolo.PedidoBuscarItinerario{Origem: origem, Destino: destino, Data: data})
	resp := enviarEReceber(l, e, protocolo.Mensagem{Operacao: protocolo.OpBuscarItinerario, Payload: payload})
	var r protocolo.RespostaBuscarItinerario
	_ = json.Unmarshal(resp.Dados, &r)
	return r
}

func reservar(l *bufio.Reader, e *bufio.Writer, refs []protocolo.RefTrecho) protocolo.Resposta {
	payload, _ := json.Marshal(protocolo.PedidoConfirmarReserva{Itinerario: refs})
	return enviarEReceber(l, e, protocolo.Mensagem{Operacao: protocolo.OpConfirmarReserva, Payload: payload})
}

func armarFalha(inst *instanciaEmpresa, ponto string) {
	cod, err := chamarREST(inst.URLREST(), http.MethodPost,
		consorcio.Prefixo+"/admin/falha", consorcio.PedidoFalha{Ponto: ponto}, nil)
	if err != nil || cod != http.StatusOK {
		fmt.Printf("erro ao armar falha %s em %s: cod=%d err=%v\n", ponto, inst.Nome, cod, err)
		os.Exit(1)
	}
}

func chamarREST(baseURL, metodo, caminho string, entrada, saida any) (int, error) {
	var body *bytes.Reader
	if entrada != nil {
		b, _ := json.Marshal(entrada)
		body = bytes.NewReader(b)
	} else {
		body = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(metodo, baseURL+caminho, body)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	cli := &http.Client{Timeout: 2 * time.Second}
	resp, err := cli.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if saida != nil {
		_ = json.NewDecoder(resp.Body).Decode(saida)
	}
	return resp.StatusCode, nil
}

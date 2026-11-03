// simulador que abre vários passageiros ao mesmo tempo via TCP pra testar concorrência
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/protocolo"
	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/tcp"
)

type alvoEmpresa struct {
	Nome     string
	Endereco string
}

type parCidades struct {
	Origem  string
	Destino string
}

const dataSimulacao = "2026-11-15"

// pares buscados pelos passageiros virtuais (mistura trechos locais e viagens
// longas que exigem combinar caronas de 2 ou 3 empresas do consórcio)
var rotasDeBusca = []parCidades{
	{"Salvador", "Montes Claros"},
	{"Salvador", "Vitória da Conquista"},
	{"Salvador", "Belo Horizonte"},
	{"Camaçari", "Montes Claros"},
	{"Feira de Santana", "Montes Claros"},
	{"Feira de Santana", "Belo Horizonte"},
	{"Salvador", "Feira de Santana"},
	{"Vitória da Conquista", "Belo Horizonte"},
}

func main() {
	servidoresPadrao := lerEnv("SERVIDORES", "vaijunto=localhost:8080,rotasul=localhost:8082,expresso=localhost:8084")
	numPadrao := lerIntEnv("PASSAGEIROS", 45)
	opsPadrao := lerIntEnv("OPERACOES", 8)
	assentosPadrao := lerIntEnv("ASSENTOS_POR_CARONA", 25)

	flagServidores := flag.String("servidores", servidoresPadrao, "endereços TCP das empresas (nome=host:porta,...)")
	flagPassageiros := flag.Int("passageiros", numPadrao, "quantidade de passageiros simultâneos")
	flagOperacoes := flag.Int("operacoes", opsPadrao, "ciclos de busca/reserva por passageiro")
	flagAssentos := flag.Int("assentos", assentosPadrao, "assentos por carona publicada na carga inicial")
	flagPopular := flag.Bool("popular", true, "publicar caronas regionais nas empresas antes de simular")
	flag.Parse()

	empresas := interpretarServidores(*flagServidores)
	if len(empresas) == 0 {
		fmt.Println("nenhum servidor configurado em -servidores / SERVIDORES")
		os.Exit(1)
	}

	sufixo := fmt.Sprintf("%d", time.Now().UnixNano())
	fmt.Println("=== Simulador Autônomo de Passageiros — Consórcio VAIJUNTO ===")
	fmt.Printf("Empresas alvo (%d): ", len(empresas))
	for i, e := range empresas {
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Printf("%s (%s)", e.Nome, e.Endereco)
	}
	fmt.Printf("\nConfiguração: %d passageiros concorrentes × %d ciclos\n\n", *flagPassageiros, *flagOperacoes)

	if *flagPopular {
		popularMalhaRegional(empresas, *flagAssentos, sufixo)
	}

	var (
		totalBuscas           int64
		buscasComOpcao        int64
		tentativasReserva     int64
		reservasConfirmadas   int64
		reservasMultiEmpresa  int64
		reservasLotadas       int64
		errosConexaoOuEmpresa int64

		confirmadasPorEmpresa = make([]int64, len(empresas))

		latMu       sync.Mutex
		latBuscas   []time.Duration
		latReservas []time.Duration
	)

	inicioSim := time.Now()
	var wg sync.WaitGroup

	for p := 0; p < *flagPassageiros; p++ {
		wg.Add(1)
		idxEmpresa := p % len(empresas)
		alvo := empresas[idxEmpresa]

		go func(idPass, idxEmp int, emp alvoEmpresa) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(idPass)*7919))

			conn, leitor, escritor, err := abrirClienteAutenticado(emp.Endereco, fmt.Sprintf("sim_p%d_%s", idPass, sufixo))
			if err != nil {
				atomic.AddInt64(&errosConexaoOuEmpresa, 1)
				return
			}
			defer conn.Close()

			for ciclo := 0; ciclo < *flagOperacoes; ciclo++ {
				par := rotasDeBusca[rng.Intn(len(rotasDeBusca))]

				// 1. Busca itinerários a partir do servidor da empresa deste passageiro
				t0 := time.Now()
				payloadBusca, _ := json.Marshal(protocolo.PedidoBuscarItinerario{
					Origem:  par.Origem,
					Destino: par.Destino,
					Data:    dataSimulacao,
				})
				respBusca, err := enviarTCP(leitor, escritor, protocolo.Mensagem{
					Operacao: protocolo.OpBuscarItinerario,
					Payload:  payloadBusca,
				})
				durBusca := time.Since(t0)

				atomic.AddInt64(&totalBuscas, 1)
				latMu.Lock()
				latBuscas = append(latBuscas, durBusca)
				latMu.Unlock()

				if err != nil || !respBusca.Sucesso {
					atomic.AddInt64(&errosConexaoOuEmpresa, 1)
					continue
				}

				var dadosBusca protocolo.RespostaBuscarItinerario
				_ = json.Unmarshal(respBusca.Dados, &dadosBusca)
				if len(dadosBusca.Itinerarios) == 0 {
					continue
				}
				atomic.AddInt64(&buscasComOpcao, 1)

				// escolhe um dos itinerários disponíveis (priorizando opções variadas)
				escolhido := dadosBusca.Itinerarios[rng.Intn(len(dadosBusca.Itinerarios))]
				refs := make([]protocolo.RefTrecho, 0, len(escolhido.Trechos))
				empresasNoItinerario := make(map[string]bool)
				for _, tr := range escolhido.Trechos {
					refs = append(refs, protocolo.RefTrecho{Empresa: tr.Empresa, ID: tr.ID})
					empresasNoItinerario[tr.Empresa] = true
				}

				// 2. Tenta reservar atomicamente o itinerário escolhido
				t1 := time.Now()
				payloadRes, _ := json.Marshal(protocolo.PedidoConfirmarReserva{Itinerario: refs})
				respRes, err := enviarTCP(leitor, escritor, protocolo.Mensagem{
					Operacao: protocolo.OpConfirmarReserva,
					Payload:  payloadRes,
				})
				durRes := time.Since(t1)

				atomic.AddInt64(&tentativasReserva, 1)
				latMu.Lock()
				latReservas = append(latReservas, durRes)
				latMu.Unlock()

				if err != nil {
					atomic.AddInt64(&errosConexaoOuEmpresa, 1)
					return
				}
				if respRes.Sucesso {
					atomic.AddInt64(&reservasConfirmadas, 1)
					atomic.AddInt64(&confirmadasPorEmpresa[idxEmp], 1)
					if len(empresasNoItinerario) > 1 {
						atomic.AddInt64(&reservasMultiEmpresa, 1)
					}
				} else if strings.Contains(respRes.Erro, "indisponível") && strings.Contains(respRes.Erro, "empresa") {
					atomic.AddInt64(&errosConexaoOuEmpresa, 1)
				} else {
					atomic.AddInt64(&reservasLotadas, 1)
				}
			}
		}(p, idxEmpresa, alvo)
	}

	wg.Wait()
	duracaoTotal := time.Since(inicioSim)

	totalOps := totalBuscas + tentativasReserva
	vazao := float64(totalOps) / duracaoTotal.Seconds()

	fmt.Println("--- Resultados da Simulação ---")
	fmt.Printf("Tempo total:               %v\n", duracaoTotal.Round(time.Millisecond))
	fmt.Printf("Vazão agregada:            %.1f operações/s (%d operações totais)\n", vazao, totalOps)
	fmt.Printf("Buscas realizadas:         %d (com itinerários encontrados: %d)\n", totalBuscas, buscasComOpcao)
	fmt.Printf("Tentativas de reserva:     %d\n", tentativasReserva)
	fmt.Printf("  ✔ Confirmadas:           %d (sendo %d combinando múltiplas empresas)\n", reservasConfirmadas, reservasMultiEmpresa)
	fmt.Printf("  ✘ Recusadas por disputa: %d (assentos esgotados sem venda dupla)\n", reservasLotadas)
	if errosConexaoOuEmpresa > 0 {
		fmt.Printf("  ! Falhas/indisponíveis:  %d\n", errosConexaoOuEmpresa)
	}

	fmt.Println("\nReservas confirmadas por empresa de origem do passageiro:")
	for i, emp := range empresas {
		fmt.Printf("  - %-12s: %d reservas\n", emp.Nome, confirmadasPorEmpresa[i])
	}

	fmt.Println("\nLatência das operações:")
	imprimirEstatisticas("Busca federada   ", latBuscas)
	imprimirEstatisticas("Reserva atômica  ", latReservas)
}

// popularMalhaRegional cadastra motoristas em cada empresa acessível e publica
// rotas regionais complementares que se conectam nas cidades-polo.
func popularMalhaRegional(empresas []alvoEmpresa, assentos int, sufixo string) {
	malhasPorIndice := [][]protocolo.PedidoPublicarCarona{
		// empresa 0 (ex.: VaiJunto - Região Metropolitana e Recôncavo)
		{
			{Trechos: []protocolo.TrechoPedido{
				{Origem: "Camaçari", Destino: "Salvador", HorarioSaida: dataSimulacao + "T06:30", HorarioChegada: dataSimulacao + "T07:30", PrecoCentavos: 2000, AssentosTotais: assentos},
				{Origem: "Salvador", Destino: "Feira de Santana", HorarioSaida: dataSimulacao + "T08:00", HorarioChegada: dataSimulacao + "T09:30", PrecoCentavos: 3500, AssentosTotais: assentos},
			}},
			{Trechos: []protocolo.TrechoPedido{
				{Origem: "Salvador", Destino: "Feira de Santana", HorarioSaida: dataSimulacao + "T09:00", HorarioChegada: dataSimulacao + "T10:30", PrecoCentavos: 3000, AssentosTotais: assentos},
			}},
		},
		// empresa 1 (ex.: RotaSul - Portal do Sertão ao Sudoeste Baiano)
		{
			{Trechos: []protocolo.TrechoPedido{
				{Origem: "Feira de Santana", Destino: "Jequié", HorarioSaida: dataSimulacao + "T10:00", HorarioChegada: dataSimulacao + "T12:30", PrecoCentavos: 4500, AssentosTotais: assentos},
				{Origem: "Jequié", Destino: "Vitória da Conquista", HorarioSaida: dataSimulacao + "T12:45", HorarioChegada: dataSimulacao + "T14:30", PrecoCentavos: 3000, AssentosTotais: assentos},
			}},
			{Trechos: []protocolo.TrechoPedido{
				{Origem: "Feira de Santana", Destino: "Vitória da Conquista", HorarioSaida: dataSimulacao + "T11:00", HorarioChegada: dataSimulacao + "T15:00", PrecoCentavos: 6500, AssentosTotais: assentos},
			}},
		},
		// empresa 2+ (ex.: Expresso - Sudoeste Baiano ao Norte/Centro de Minas)
		{
			{Trechos: []protocolo.TrechoPedido{
				{Origem: "Vitória da Conquista", Destino: "Montes Claros", HorarioSaida: dataSimulacao + "T15:30", HorarioChegada: dataSimulacao + "T20:00", PrecoCentavos: 8000, AssentosTotais: assentos},
				{Origem: "Montes Claros", Destino: "Belo Horizonte", HorarioSaida: dataSimulacao + "T20:30", HorarioChegada: "2026-11-16T01:30", PrecoCentavos: 9000, AssentosTotais: assentos},
			}},
			{Trechos: []protocolo.TrechoPedido{
				{Origem: "Vitória da Conquista", Destino: "Montes Claros", HorarioSaida: dataSimulacao + "T16:00", HorarioChegada: dataSimulacao + "T20:30", PrecoCentavos: 7500, AssentosTotais: assentos},
			}},
		},
	}

	publicadas := 0
	for i, emp := range empresas {
		conn, leitor, escritor, err := abrirClienteAutenticado(emp.Endereco, fmt.Sprintf("sim_mot_%d_%s", i, sufixo))
		if err != nil {
			fmt.Printf("aviso: não foi possível popular caronas em %s (%s): %v\n", emp.Nome, emp.Endereco, err)
			continue
		}
		rotas := malhasPorIndice[i%len(malhasPorIndice)]
		for _, pedido := range rotas {
			payload, _ := json.Marshal(pedido)
			resp, err := enviarTCP(leitor, escritor, protocolo.Mensagem{
				Operacao: protocolo.OpPublicarCarona,
				Payload:  payload,
			})
			if err == nil && resp.Sucesso {
				publicadas++
			}
		}
		conn.Close()
	}
	fmt.Printf("Malha inicial publicada: %d caronas distribuídas entre as empresas.\n\n", publicadas)
}

// abrirClienteAutenticado abre a conexão TCP com o servidor da empresa e já cadastra o usuário virtual
func abrirClienteAutenticado(endereco, login string) (net.Conn, *bufio.Reader, *bufio.Writer, error) {
	conn, err := net.DialTimeout("tcp", endereco, 2*time.Second)
	if err != nil {
		return nil, nil, nil, err
	}
	leitor := bufio.NewReader(conn)
	escritor := bufio.NewWriter(conn)

	payload, _ := json.Marshal(protocolo.PedidoLogin{Login: login, Senha: "123"})
	resp, err := enviarTCP(leitor, escritor, protocolo.Mensagem{
		Operacao: protocolo.OpCadastro,
		Payload:  payload,
	})
	if err != nil {
		conn.Close()
		return nil, nil, nil, err
	}
	if !resp.Sucesso {
		conn.Close()
		return nil, nil, nil, fmt.Errorf("cadastro recusado: %s", resp.Erro)
	}
	return conn, leitor, escritor, nil
}

// enviarTCP manda uma mensagem JSON na conexão TCP e lê a resposta correspondente
func enviarTCP(leitor *bufio.Reader, escritor *bufio.Writer, msg protocolo.Mensagem) (protocolo.Resposta, error) {
	dados, _ := json.Marshal(msg)
	if err := tcp.EnviarMensagem(escritor, dados); err != nil {
		return protocolo.Resposta{}, err
	}
	linha, err := tcp.LerMensagem(leitor)
	if err != nil {
		return protocolo.Resposta{}, err
	}
	var resp protocolo.Resposta
	if err := json.Unmarshal(linha, &resp); err != nil {
		return protocolo.Resposta{}, err
	}
	return resp, nil
}

// imprimirEstatisticas calcula mínimo, média, percentis (p50/p95/p99) e máximo dos tempos medidos
func imprimirEstatisticas(rotulo string, tempos []time.Duration) {
	if len(tempos) == 0 {
		fmt.Printf("  %s — sem amostras\n", rotulo)
		return
	}
	sort.Slice(tempos, func(i, j int) bool { return tempos[i] < tempos[j] })
	var soma time.Duration
	for _, d := range tempos {
		soma += d
	}
	media := soma / time.Duration(len(tempos))
	p50 := tempos[len(tempos)*50/100]
	p95 := tempos[len(tempos)*95/100]
	p99 := tempos[len(tempos)*99/100]
	fmt.Printf("  %s — mín: %v | méd: %v | p50: %v | p95: %v | p99: %v | máx: %v\n",
		rotulo,
		tempos[0].Round(100*time.Microsecond),
		media.Round(100*time.Microsecond),
		p50.Round(100*time.Microsecond),
		p95.Round(100*time.Microsecond),
		p99.Round(100*time.Microsecond),
		tempos[len(tempos)-1].Round(100*time.Microsecond))
}

// interpretarServidores transforma a string "vaijunto=host:8080,rotasul=host:8082" numa lista de alvos
func interpretarServidores(bruto string) []alvoEmpresa {
	var lista []alvoEmpresa
	for i, item := range strings.Split(bruto, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if strings.Contains(item, "=") {
			partes := strings.SplitN(item, "=", 2)
			lista = append(lista, alvoEmpresa{
				Nome:     strings.TrimSpace(partes[0]),
				Endereco: strings.TrimSpace(partes[1]),
			})
		} else {
			lista = append(lista, alvoEmpresa{
				Nome:     fmt.Sprintf("empresa_%d", i+1),
				Endereco: item,
			})
		}
	}
	return lista
}

func lerEnv(chave, padrao string) string {
	if v := strings.TrimSpace(os.Getenv(chave)); v != "" {
		return v
	}
	return padrao
}

func lerIntEnv(chave string, padrao int) int {
	if v := strings.TrimSpace(os.Getenv(chave)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return padrao
}

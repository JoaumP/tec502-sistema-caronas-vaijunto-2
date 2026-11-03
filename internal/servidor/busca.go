package servidor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"

	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/consorcio"
	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/protocolo"
)

// maxSaltosItinerario limita quantos trechos uma viagem pode combinar seguida,
// pra DFS não explodir quando houver muitas rotas cadastradas
const maxSaltosItinerario = 5

// OfertasLocais lista os trechos ativos com vaga livre nesta empresa (sem dados de usuários)
func (e *Estado) OfertasLocais(dataMinima string) []consorcio.TrechoOferta {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var ofertas []consorcio.TrechoOferta
	for _, t := range e.trechos {
		if t.AssentosLivres <= 0 || !e.caronaAtiva(t.IDCarona) {
			continue
		}
		if len(t.HorarioSaida) < 10 {
			continue
		}
		if len(dataMinima) == 10 && t.HorarioSaida[:10] < dataMinima {
			continue
		}
		ofertas = append(ofertas, consorcio.TrechoOferta{
			TrechoInfo:     e.infoTrecho(t),
			AssentosLivres: t.AssentosLivres,
		})
	}
	return ofertas
}

// HandleBuscarItinerario consulta em paralelo os trechos disponíveis nas outras
// empresas via REST (com timeout curto), junta com os trechos locais e monta as
// combinações possíveis de viagem ordenadas pelo menor preço
func (e *Estado) HandleBuscarItinerario(payload json.RawMessage) protocolo.Resposta {
	var pedido protocolo.PedidoBuscarItinerario
	if err := json.Unmarshal(payload, &pedido); err != nil {
		return respostaErro("payload inválido")
	}

	// junta nossos trechos + trechos buscados em paralelo nas empresas parceiras
	todasOfertas := e.OfertasLocais(pedido.Data)
	peers := e.rede.Peers()

	var mu sync.Mutex
	var indisponiveis []string
	var wg sync.WaitGroup

	for _, emp := range peers {
		emp := emp
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), e.cfg.TimeoutBusca)
			defer cancel()

			caminho := fmt.Sprintf("%s/trechos?data=%s", consorcio.Prefixo, pedido.Data)
			var resp consorcio.RespostaTrechos
			cod, err := e.rede.Chamar(ctx, emp, http.MethodGet, caminho, nil, &resp)
			if err != nil || cod != http.StatusOK {
				log.Printf("[%s] busca: empresa %s indisponível, seguindo sem ela", e.cfg.Empresa, emp)
				mu.Lock()
				indisponiveis = append(indisponiveis, emp)
				mu.Unlock()
				return
			}
			mu.Lock()
			for _, of := range resp.Trechos {
				if of.Empresa == "" {
					of.Empresa = emp
				}
				todasOfertas = append(todasOfertas, of)
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	sort.Strings(indisponiveis)

	caminhos := montarItinerariosDFS(todasOfertas, pedido.Origem, pedido.Destino, pedido.Data)

	itinerarios := make([]protocolo.ItinerarioEncontrado, 0, len(caminhos))
	for _, cam := range caminhos {
		resumos := make([]protocolo.TrechoResumo, 0, len(cam))
		preco := 0
		for _, t := range cam {
			resumos = append(resumos, protocolo.TrechoResumo{
				Empresa:        t.Empresa,
				ID:             t.ID,
				Origem:         t.Origem,
				Destino:        t.Destino,
				HorarioSaida:   t.HorarioSaida,
				HorarioChegada: t.HorarioChegada,
				PrecoCentavos:  t.PrecoCentavos,
			})
			preco += t.PrecoCentavos
		}
		itinerarios = append(itinerarios, protocolo.ItinerarioEncontrado{
			Trechos:       resumos,
			PrecoCentavos: preco,
		})
	}

	sort.Slice(itinerarios, func(i, j int) bool {
		if itinerarios[i].PrecoCentavos != itinerarios[j].PrecoCentavos {
			return itinerarios[i].PrecoCentavos < itinerarios[j].PrecoCentavos
		}
		return len(itinerarios[i].Trechos) < len(itinerarios[j].Trechos)
	})

	dados, _ := json.Marshal(protocolo.RespostaBuscarItinerario{
		Itinerarios:           itinerarios,
		EmpresasIndisponiveis: indisponiveis,
	})
	return protocolo.Resposta{Sucesso: true, Dados: dados}
}

// montarItinerariosDFS monta um grafo temporário em memória (descartado ao fim
// da busca) e encontra caminhos válidos combinando trechos de qualquer empresa
func montarItinerariosDFS(ofertas []consorcio.TrechoOferta, origem, destino, data string) [][]consorcio.TrechoOferta {
	porOrigem := make(map[string][]consorcio.TrechoOferta)
	for _, of := range ofertas {
		if of.AssentosLivres > 0 {
			porOrigem[of.Origem] = append(porOrigem[of.Origem], of)
		}
	}

	var resultados [][]consorcio.TrechoOferta
	var caminhoAtual []consorcio.TrechoOferta
	visitadas := make(map[string]bool)

	var dfs func(cidade, chegadaAnterior string)
	dfs = func(cidade, chegadaAnterior string) {
		if cidade == destino && len(caminhoAtual) > 0 {
			copia := make([]consorcio.TrechoOferta, len(caminhoAtual))
			copy(copia, caminhoAtual)
			resultados = append(resultados, copia)
			return
		}
		if len(caminhoAtual) >= maxSaltosItinerario {
			return
		}

		visitadas[cidade] = true
		for _, t := range porOrigem[cidade] {
			if len(t.HorarioSaida) < 10 || visitadas[t.Destino] {
				continue
			}
			if chegadaAnterior == "" {
				// primeiro trecho da viagem: precisa partir na data escolhida
				if t.HorarioSaida[:10] != data {
					continue
				}
			} else if t.HorarioSaida < chegadaAnterior {
				// conexões seguintes: só pode partir após o trecho anterior chegar
				continue
			}

			caminhoAtual = append(caminhoAtual, t)
			dfs(t.Destino, t.HorarioChegada)
			caminhoAtual = caminhoAtual[:len(caminhoAtual)-1]
		}
		visitadas[cidade] = false
	}

	dfs(origem, "")
	return resultados
}


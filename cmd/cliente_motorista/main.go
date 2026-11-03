// cliente de terminal do motorista (publicar, consultar e cancelar caronas)
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/cliente"
	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/protocolo"
)

func main() {
	// pega o endereço do servidor da empresa (padrão localhost:8080 se não vier por env)
	endereco := os.Getenv("SERVIDOR")
	if endereco == "" {
		endereco = "localhost:8080"
	}
	conexao, err := net.Dial("tcp", endereco)
	if err != nil {
		fmt.Println("erro ao conectar:", err)
		return
	}
	defer conexao.Close()

	fmt.Printf("Conectado ao servidor da empresa em %s\n", endereco)

	leitor := bufio.NewReader(conexao)
	escritor := bufio.NewWriter(conexao)
	entrada := bufio.NewReader(os.Stdin)

	// ciclo da sessão: autentica -> usa o menu -> se fizer logout, volta pra tela de login
	for {
		if !cliente.Autenticar(leitor, escritor, entrada) {
			return
		}
		menuPrincipal(leitor, escritor, entrada)
	}
}

// menuPrincipal exibe as ações do motorista logado até ele escolher sair da conta
func menuPrincipal(leitor *bufio.Reader, escritor *bufio.Writer, entrada *bufio.Reader) {
	for {
		cliente.ImprimirMenu("Menu Motorista", []string{
			"Publicar carona",
			"Consultar minhas caronas",
			"Cancelar carona",
			"Sair da conta",
		})
		opcao, _ := entrada.ReadString('\n')
		opcao = strings.TrimSpace(opcao)

		switch opcao {
		case "1":
			publicarCarona(leitor, escritor, entrada)
		case "2":
			consultarCaronas(leitor, escritor)
		case "3":
			cancelarCarona(leitor, escritor, entrada)
		case "4":
			cliente.EnviarEReceber(leitor, escritor, protocolo.Mensagem{Operacao: protocolo.OpLogout})
			return
		default:
			fmt.Println("opção inválida")
		}
	}
}

// publicarCarona lê os trechos encadeados da viagem (a origem do próximo trecho
// já puxa automaticamente o destino do anterior) e envia tudo pro servidor
func publicarCarona(leitor *bufio.Reader, escritor *bufio.Writer, entrada *bufio.Reader) {
	cliente.ImprimirTitulo("Publicar Carona")
	fmt.Println("Cadastre a rota, trecho por trecho.")

	var trechos []protocolo.TrechoPedido
	origemAtual := cliente.LerTexto(entrada, "Cidade de origem (partida da rota): ")

	var dataAtual string
	var chegadaAnterior string
	numero := 1

	for {
		destino := cliente.LerTexto(entrada, "Cidade de destino deste trecho: ")

		// a data só é perguntada no 1º trecho; nos seguintes ela acompanha o relógio da viagem
		if numero == 1 {
			dataTexto := cliente.LerData(entrada, "Data (DD/MM/AAAA): ")
			dataAtual, _ = cliente.ConverterData(dataTexto)
		}

		horaSaida := cliente.LerHora(entrada, "Horário de saída (HH:MM): ")
		horaChegada := cliente.LerHora(entrada, "Horário de chegada (HH:MM): ")

		hSaida, hChegada, err := cliente.MontarHorarios(dataAtual, horaSaida, horaChegada)
		if err != nil {
			fmt.Println("erro ao montar horário:", err)
			return
		}
		// se o novo trecho sai depois da meia-noite em relação à chegada anterior, avança o dia
		if numero > 1 && hSaida < chegadaAnterior {
			if d, errDia := time.Parse("2006-01-02", dataAtual); errDia == nil {
				dataSeguinte := d.AddDate(0, 0, 1).Format("2006-01-02")
				hSaida, hChegada, err = cliente.MontarHorarios(dataSeguinte, horaSaida, horaChegada)
				if err != nil {
					fmt.Println("erro ao montar horário:", err)
					return
				}
			}
		}

		preco := cliente.LerPrecoCentavos(entrada, "Preço (R$): ")
		assentos := cliente.LerInteiroPositivo(entrada, "Assentos totais: ")

		trechos = append(trechos, protocolo.TrechoPedido{
			Origem:         origemAtual,
			Destino:        destino,
			HorarioSaida:   hSaida,
			HorarioChegada: hChegada,
			PrecoCentavos:  preco,
			AssentosTotais: assentos,
		})

		if !cliente.LerSimNao(entrada, "\nAdicionar outro trecho a partir de "+destino+"? (s/n): ") {
			break
		}

		// prepara o encadeamento pro próximo trecho da mesma carona
		origemAtual = destino
		chegadaAnterior = hChegada
		dataAtual = hChegada[:10]
		numero++
	}

	pedido := protocolo.PedidoPublicarCarona{Trechos: trechos}
	payload, _ := json.Marshal(pedido)
	msg := protocolo.Mensagem{Operacao: protocolo.OpPublicarCarona, Payload: payload}

	resposta := cliente.EnviarEReceber(leitor, escritor, msg)
	if !resposta.Sucesso {
		fmt.Println("erro:", resposta.Erro)
		return
	}

	var r protocolo.RespostaPublicarCarona
	_ = json.Unmarshal(resposta.Dados, &r)
	fmt.Printf("\n✔ Carona publicada com sucesso! ID: %d\n", r.IDCarona)
}

// consultarCaronas lista todas as caronas do motorista, mostrando vagas livres
// e os passageiros confirmados em cada trecho (ex: "2@rotasul")
func consultarCaronas(leitor *bufio.Reader, escritor *bufio.Writer) {
	msg := protocolo.Mensagem{Operacao: protocolo.OpConsultarCaronas}
	resposta := cliente.EnviarEReceber(leitor, escritor, msg)
	if !resposta.Sucesso {
		fmt.Println("erro:", resposta.Erro)
		return
	}

	var r protocolo.RespostaConsultarCaronas
	_ = json.Unmarshal(resposta.Dados, &r)

	if len(r.Caronas) == 0 {
		fmt.Println("você ainda não publicou nenhuma carona")
		return
	}

	fmt.Println()
	for _, c := range r.Caronas {
		cliente.ImprimirSeparador()
		fmt.Printf("Carona #%d — status: %s\n", c.ID, strings.ToUpper(c.Status))
		for _, t := range c.Trechos {
			fmt.Printf("   %s (%s)  →  %s (%s)  |  vagas: %d/%d  |  passageiros: %v\n",
				t.Origem, cliente.FormatarHorario(t.HorarioSaida),
				t.Destino, cliente.FormatarHorario(t.HorarioChegada),
				t.AssentosLivres, t.AssentosTotais, t.Passageiros)
		}
	}
	cliente.ImprimirSeparador()
}

// cancelarCarona pede o ID de uma carona publicada pelo motorista e solicita o cancelamento
func cancelarCarona(leitor *bufio.Reader, escritor *bufio.Writer, entrada *bufio.Reader) {
	id := cliente.LerInteiroPositivo(entrada, "ID da carona a cancelar: ")

	pedido := protocolo.PedidoCancelarCarona{IDCarona: int64(id)}
	payload, _ := json.Marshal(pedido)
	msg := protocolo.Mensagem{Operacao: protocolo.OpCancelarCarona, Payload: payload}

	resposta := cliente.EnviarEReceber(leitor, escritor, msg)
	if !resposta.Sucesso {
		fmt.Println("erro:", resposta.Erro)
		return
	}

	fmt.Println("carona cancelada com sucesso")
}

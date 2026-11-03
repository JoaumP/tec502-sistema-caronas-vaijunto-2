// Cliente de terminal do passageiro.
// O passageiro fala via TCP apenas com o servidor da própria empresa, e esse
// servidor é quem se vira pra buscar e reservar trechos nas outras empresas do consórcio.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/JoaumP/tec502-sistema-caronas-vaijunto/internal/cliente"
	"github.com/JoaumP/tec502-sistema-caronas-vaijunto/internal/protocolo"
)

func main() {
	// endereço TCP do servidor da empresa deste passageiro
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

	for {
		if !cliente.Autenticar(leitor, escritor, entrada) {
			return
		}
		menuPrincipal(leitor, escritor, entrada)
	}
}

// menuPrincipal guarda o resultado da última busca em memória pra que o
// passageiro possa escolher a opção pelo número na hora de confirmar a reserva
func menuPrincipal(leitor *bufio.Reader, escritor *bufio.Writer, entrada *bufio.Reader) {
	var ultimaBusca []protocolo.ItinerarioEncontrado

	for {
		cliente.ImprimirMenu("Menu Passageiro", []string{
			"Buscar itinerário",
			"Confirmar reserva",
			"Consultar minhas reservas",
			"Cancelar reserva",
			"Sair da conta",
		})
		opcao, _ := entrada.ReadString('\n')
		opcao = strings.TrimSpace(opcao)

		switch opcao {
		case "1":
			ultimaBusca = buscarItinerario(leitor, escritor, entrada)
		case "2":
			confirmarReserva(leitor, escritor, entrada, ultimaBusca)
		case "3":
			consultarReservas(leitor, escritor)
		case "4":
			cancelarReserva(leitor, escritor, entrada)
		case "5":
			cliente.EnviarEReceber(leitor, escritor, protocolo.Mensagem{Operacao: protocolo.OpLogout})
			return
		default:
			fmt.Println("opção inválida")
		}
	}
}

// buscarItinerario consulta as opções de viagem (diretas ou combinando trechos
// de empresas diferentes) entre origem e destino na data informada
func buscarItinerario(leitor *bufio.Reader, escritor *bufio.Writer, entrada *bufio.Reader) []protocolo.ItinerarioEncontrado {
	origem := cliente.LerTexto(entrada, "Origem: ")
	destino := cliente.LerTexto(entrada, "Destino: ")
	dataTexto := cliente.LerData(entrada, "Data (DD/MM/AAAA): ")
	data, _ := cliente.ConverterData(dataTexto)

	pedido := protocolo.PedidoBuscarItinerario{Origem: origem, Destino: destino, Data: data}
	payload, _ := json.Marshal(pedido)
	msg := protocolo.Mensagem{Operacao: protocolo.OpBuscarItinerario, Payload: payload}

	resposta := cliente.EnviarEReceber(leitor, escritor, msg)
	if !resposta.Sucesso {
		fmt.Println("erro:", resposta.Erro)
		return nil
	}

	var r protocolo.RespostaBuscarItinerario
	_ = json.Unmarshal(resposta.Dados, &r)

	// se alguma empresa parceira estava fora do ar na hora, avisa mas mostra o resto normalmente
	if len(r.EmpresasIndisponiveis) > 0 {
		fmt.Printf("aviso: empresas temporariamente indisponíveis ignoradas na busca: %s\n",
			strings.Join(r.EmpresasIndisponiveis, ", "))
	}

	if len(r.Itinerarios) == 0 {
		fmt.Println("nenhum itinerário encontrado")
		return nil
	}

	fmt.Println()
	for i, it := range r.Itinerarios {
		cliente.ImprimirSeparador()
		fmt.Printf("Opção %d — R$ %.2f — %d trecho(s)\n", i+1, float64(it.PrecoCentavos)/100, len(it.Trechos))
		for _, t := range it.Trechos {
			fmt.Printf("   [%s] %s (%s)  →  %s (%s)  |  R$ %.2f\n",
				t.Empresa, t.Origem, cliente.FormatarHorario(t.HorarioSaida),
				t.Destino, cliente.FormatarHorario(t.HorarioChegada),
				float64(t.PrecoCentavos)/100)
		}
	}
	cliente.ImprimirSeparador()

	return r.Itinerarios
}

// confirmarReserva pega uma opção da última busca, monta a lista de pares
// (empresa, id do trecho) e pede pro servidor reservar o itinerário inteiro de forma atômica
func confirmarReserva(leitor *bufio.Reader, escritor *bufio.Writer, entrada *bufio.Reader, ultimaBusca []protocolo.ItinerarioEncontrado) {
	if len(ultimaBusca) == 0 {
		fmt.Println("faça uma busca primeiro")
		return
	}

	numero := cliente.LerInteiroPositivo(entrada, "Número da opção desejada: ")
	if numero < 1 || numero > len(ultimaBusca) {
		fmt.Println("opção inválida")
		return
	}

	escolha := ultimaBusca[numero-1].Trechos
	refs := make([]protocolo.RefTrecho, 0, len(escolha))
	for _, t := range escolha {
		refs = append(refs, protocolo.RefTrecho{Empresa: t.Empresa, ID: t.ID})
	}

	pedido := protocolo.PedidoConfirmarReserva{Itinerario: refs}
	payload, _ := json.Marshal(pedido)
	msg := protocolo.Mensagem{Operacao: protocolo.OpConfirmarReserva, Payload: payload}

	resposta := cliente.EnviarEReceber(leitor, escritor, msg)
	if !resposta.Sucesso {
		fmt.Println("erro:", resposta.Erro)
		return
	}

	var r protocolo.RespostaConfirmarReserva
	_ = json.Unmarshal(resposta.Dados, &r)
	fmt.Println("✔ reserva confirmada! ID:", r.IDReserva)
}

// consultarReservas mostra todas as reservas do passageiro (confirmadas e canceladas)
func consultarReservas(leitor *bufio.Reader, escritor *bufio.Writer) {
	msg := protocolo.Mensagem{Operacao: protocolo.OpConsultarReservas}
	resposta := cliente.EnviarEReceber(leitor, escritor, msg)
	if !resposta.Sucesso {
		fmt.Println("erro:", resposta.Erro)
		return
	}

	var r protocolo.RespostaConsultarReservas
	_ = json.Unmarshal(resposta.Dados, &r)

	if len(r.Reservas) == 0 {
		fmt.Println("você ainda não tem nenhuma reserva")
		return
	}

	fmt.Println()
	for _, res := range r.Reservas {
		cliente.ImprimirSeparador()
		fmt.Printf("Reserva #%s — status: %s\n", res.ID, strings.ToUpper(res.Status))
		for _, t := range res.Trechos {
			fmt.Printf("   [%s] %s (%s)  →  %s (%s)\n",
				t.Empresa, t.Origem, cliente.FormatarHorario(t.HorarioSaida),
				t.Destino, cliente.FormatarHorario(t.HorarioChegada))
		}
	}
	cliente.ImprimirSeparador()
}

// cancelarReserva cancela uma reserva pelo ID (ex: "vaijunto-1") e libera as vagas nas empresas
func cancelarReserva(leitor *bufio.Reader, escritor *bufio.Writer, entrada *bufio.Reader) {
	id := cliente.LerTexto(entrada, "ID da reserva a cancelar: ")

	pedido := protocolo.PedidoCancelarReserva{IDReserva: id}
	payload, _ := json.Marshal(pedido)
	msg := protocolo.Mensagem{Operacao: protocolo.OpCancelarReserva, Payload: payload}

	resposta := cliente.EnviarEReceber(leitor, escritor, msg)
	if !resposta.Sucesso {
		fmt.Println("erro:", resposta.Erro)
		return
	}

	fmt.Println("reserva cancelada com sucesso")
}

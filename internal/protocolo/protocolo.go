// Pacote protocolo define as mensagens JSON trocadas via TCP entre os clientes
// (motorista e passageiro) e o servidor da empresa deles.
package protocolo

import (
	"encoding/json"

	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/modelos"
)

// Mensagem é o envelope padrão de todo pedido que o cliente manda pro servidor
type Mensagem struct {
	Operacao string          `json:"operacao"`
	Payload  json.RawMessage `json:"payload"`
}

// Resposta é o envelope padrão que o servidor devolve pro cliente
type Resposta struct {
	Sucesso bool            `json:"sucesso"`
	Erro    string          `json:"erro,omitempty"`
	Dados   json.RawMessage `json:"dados,omitempty"`
}

// Operações aceitas pelo servidor TCP
const (
	OpCadastro          = "cadastro"
	OpLogin             = "login"
	OpLogout            = "logout"
	OpPublicarCarona    = "publicar_carona"
	OpCancelarCarona    = "cancelar_carona"
	OpConsultarCaronas  = "consultar_caronas"
	OpBuscarItinerario  = "buscar_itinerario"
	OpConfirmarReserva  = "confirmar_reserva"
	OpCancelarReserva   = "cancelar_reserva"
	OpConsultarReservas = "consultar_reservas"
)

// RefTrecho identifica um trecho no consórcio inteiro (nome da empresa + ID local)
type RefTrecho = modelos.RefTrecho

// Payloads de entrada (cliente -> servidor)

// PedidoLogin serve tanto pro login quanto pro cadastro de usuário novo
type PedidoLogin struct {
	Login string `json:"login"`
	Senha string `json:"senha"`
}

// PedidoPublicarCarona traz a rota completa que o motorista quer anunciar
type PedidoPublicarCarona struct {
	Trechos []TrechoPedido `json:"trechos"`
}

// TrechoPedido representa os dados de um trecho informados na hora de publicar
type TrechoPedido struct {
	Origem         string `json:"origem"`
	Destino        string `json:"destino"`
	HorarioSaida   string `json:"horario_saida"`
	HorarioChegada string `json:"horario_chegada"`
	PrecoCentavos  int    `json:"preco_centavos"`
	AssentosTotais int    `json:"assentos_totais"`
}

// PedidoCancelarCarona indica qual carona o motorista quer cancelar
type PedidoCancelarCarona struct {
	IDCarona int64 `json:"id_carona"`
}

// PedidoConsultarCaronas não precisa de parâmetros (lista as do motorista logado)
type PedidoConsultarCaronas struct{}

// PedidoBuscarItinerario informa origem, destino e a data da partida ("AAAA-MM-DD")
type PedidoBuscarItinerario struct {
	Origem  string `json:"origem"`
	Destino string `json:"destino"`
	Data    string `json:"data"`
}

// PedidoConfirmarReserva lista os trechos escolhidos na ordem da viagem.
// Cada item traz a empresa dona, já que uma viagem pode misturar várias empresas.
type PedidoConfirmarReserva struct {
	Itinerario []RefTrecho `json:"itinerario"`
}

// PedidoCancelarReserva recebe o ID da reserva (ex: "vaijunto-3") pra cancelar
type PedidoCancelarReserva struct {
	IDReserva string `json:"id_reserva"`
}

// PedidoConsultarReservas não leva parâmetros (lista as do passageiro logado)
type PedidoConsultarReservas struct{}

// Payloads de saída (servidor -> cliente, enviados dentro de Resposta.Dados)

// RespostaLogin devolve o ID interno do usuário autenticado na empresa
type RespostaLogin struct {
	IDUsuario int64 `json:"id_usuario"`
}

// RespostaPublicarCarona devolve o ID gerado pra carona recém-criada
type RespostaPublicarCarona struct {
	IDCarona int64 `json:"id_carona"`
}

// RespostaBuscarItinerario traz as opções montadas e avisa se alguma empresa estava fora do ar
type RespostaBuscarItinerario struct {
	Itinerarios           []ItinerarioEncontrado `json:"itinerarios"`
	EmpresasIndisponiveis []string               `json:"empresas_indisponiveis,omitempty"`
}

// CaronaDetalhada mostra o estado geral de uma carona na visão do motorista
type CaronaDetalhada struct {
	ID      int64             `json:"id"`
	Status  string            `json:"status"`
	Trechos []TrechoDetalhado `json:"trechos"`
}

// TrechoDetalhado inclui ocupação e quem são os passageiros confirmados ("id@empresa")
type TrechoDetalhado struct {
	ID             int64    `json:"id"`
	Origem         string   `json:"origem"`
	Destino        string   `json:"destino"`
	HorarioSaida   string   `json:"horario_saida"`
	HorarioChegada string   `json:"horario_chegada"`
	AssentosTotais int      `json:"assentos_totais"`
	AssentosLivres int      `json:"assentos_livres"`
	Passageiros    []string `json:"passageiros"`
}

// RespostaConsultarCaronas agrupa todas as caronas publicadas pelo motorista
type RespostaConsultarCaronas struct {
	Caronas []CaronaDetalhada `json:"caronas"`
}

// ReservaDetalhada representa um itinerário reservado na visão do passageiro
type ReservaDetalhada struct {
	ID      string         `json:"id"`
	Trechos []TrechoResumo `json:"trechos"`
	Status  string         `json:"status"`
}

// RespostaConsultarReservas agrupa as reservas do passageiro logado
type RespostaConsultarReservas struct {
	Reservas []ReservaDetalhada `json:"reservas"`
}

// ItinerarioEncontrado é uma opção de viagem montada na busca, já com o preço total somado
type ItinerarioEncontrado struct {
	Trechos       []TrechoResumo `json:"trechos"`
	PrecoCentavos int            `json:"preco_centavos"`
}

// RespostaConfirmarReserva devolve o ID da transação/reserva efetivada
type RespostaConfirmarReserva struct {
	IDReserva string `json:"id_reserva"`
}

// TrechoResumo traz as informações públicas de um trecho pra mostrar pro passageiro
type TrechoResumo struct {
	Empresa        string `json:"empresa"`
	ID             int64  `json:"id"`
	Origem         string `json:"origem"`
	Destino        string `json:"destino"`
	HorarioSaida   string `json:"horario_saida"`
	HorarioChegada string `json:"horario_chegada"`
	PrecoCentavos  int    `json:"preco_centavos"`
}

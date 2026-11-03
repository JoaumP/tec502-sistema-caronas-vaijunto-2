package modelos

import "time"

// Usuario: quem usa o sistema (motorista ou passageiro)
type Usuario struct {
	ID        int64  `json:"id"`
	Login     string `json:"login"`
	SenhaHash string `json:"senha_hash"`
}

// Status possíveis de uma carona
const (
	CaronaAtiva     = "ativa"
	CaronaCancelada = "cancelada"
)

// Trecho: menor unidade de viagem, entre duas cidades
type Trecho struct {
	ID       int64 `json:"id"`
	IDCarona int64 `json:"id_carona"`

	Origem  string `json:"origem"`
	Destino string `json:"destino"`

	HorarioSaida   string `json:"horario_saida"` // formato "AAAA-MM-DDTHH:mm"
	HorarioChegada string `json:"horario_chegada"`

	PrecoCentavos int `json:"preco_centavos"`

	AssentosTotais int `json:"assentos_totais"`
	AssentosLivres int `json:"assentos_livres"`
}

// Carona: viagem anunciada pelo motorista, formada por um ou mais trechos
type Carona struct {
	ID          int64   `json:"id"`
	MotoristaID int64   `json:"motorista_id"`
	Status      string  `json:"status"`
	Trechos     []int64 `json:"trechos"`
}

// RefTrecho identifica um trecho no consórcio pelo par (empresa, id do trecho)
type RefTrecho struct {
	Empresa string `json:"empresa"`
	ID      int64  `json:"id"`
}

// TrechoInfo guarda os dados públicos de um trecho dentro da reserva
type TrechoInfo struct {
	Empresa        string `json:"empresa"`
	ID             int64  `json:"id"`
	Origem         string `json:"origem"`
	Destino        string `json:"destino"`
	HorarioSaida   string `json:"horario_saida"`
	HorarioChegada string `json:"horario_chegada"`
	PrecoCentavos  int    `json:"preco_centavos"`
}

// Ref devolve o par (empresa, id) do trecho
func (t TrechoInfo) Ref() RefTrecho { return RefTrecho{Empresa: t.Empresa, ID: t.ID} }

// Status possíveis de uma reserva
const (
	ReservaConfirmada = "confirmada"
	ReservaCancelada  = "cancelada"
)

// Reserva guarda o itinerário reservado pelo passageiro
type Reserva struct {
	ID           string       `json:"id"`
	PassageiroID int64        `json:"passageiro_id"`
	Trechos      []TrechoInfo `json:"trechos"`
	Status       string       `json:"status"`
	CriadoEm     time.Time    `json:"criado_em"`
}

// Ticket define a ordem de prioridade das reservas usando o relógio de Lamport
type Ticket struct {
	Relogio int64  `json:"relogio"`
	Empresa string `json:"empresa"`
	TxID    string `json:"tx_id"`
}

// Precede retorna true se o ticket t tem prioridade maior (é "mais antigo") que o outro
func (t Ticket) Precede(outro Ticket) bool {
	if t.Relogio != outro.Relogio {
		return t.Relogio < outro.Relogio
	}
	if t.Empresa != outro.Empresa {
		return t.Empresa < outro.Empresa
	}
	return t.TxID < outro.TxID
}

// Estados de uma transação na visão de quem é PARTICIPANTE (dono de trecho)
const (
	TxPreparada     = "preparada"      // vaga segurada na fase 1, ainda pode ser preemptada
	TxPreConfirmada = "pre_confirmada" // passou da fase 2, todos prepararam e não perde mais a vaga
	TxConfirmada    = "confirmada"     // reserva confirmada em definitivo (fase 3)
	TxAbortada      = "abortada"       // transação desfeita e vaga liberada
	TxCancelada     = "cancelada"      // já estava confirmada, mas foi cancelada depois
)

// TxParticipante guarda o controle local dos trechos desta empresa envolvidos numa transação
type TxParticipante struct {
	ID            string    `json:"id"`
	Coordenador   string    `json:"coordenador"`   // empresa onde o passageiro pediu a reserva
	Participantes []string  `json:"participantes"` // todas as empresas que têm trecho nessa viagem
	Ticket        Ticket    `json:"ticket"`
	Trechos       []int64   `json:"trechos"` // IDs dos trechos locais desta empresa reservados aqui
	Passageiro    string    `json:"passageiro"`
	Estado        string    `json:"estado"`
	LeaseAte      time.Time `json:"lease_ate"` // prazo máximo pra esperar antes de cobrar uma decisão
	AtualizadoEm  time.Time `json:"atualizado_em"`
}

// Fases da transação na visão do COORDENADOR (empresa do passageiro)
const (
	CoordIniciada       = "iniciada"        // rodando a fase 1 (preparar)
	CoordPreConfirmando = "pre_confirmando" // rodando a fase 2 (pré-confirmar)
	CoordConfirmada     = "confirmada"      // fechou commit (fase 3)
	CoordAbortada       = "abortada"        // decidiu abortar
)

// TxCoordenador registra o andamento de uma reserva coordenada por esta empresa
type TxCoordenador struct {
	ID            string       `json:"id"`
	Ticket        Ticket       `json:"ticket"`
	PassageiroID  int64        `json:"passageiro_id"`
	Participantes []string     `json:"participantes"`
	Trechos       []TrechoInfo `json:"trechos"`
	Fase          string       `json:"fase"`
	CriadoEm      time.Time    `json:"criado_em"`
}

// MensagemPendente fica na fila (outbox) salva no JSON quando precisamos avisar
// outra empresa de um commit, aborto ou cancelamento e ela estava fora do ar na hora.
type MensagemPendente struct {
	ID         int64     `json:"id"`
	Destino    string    `json:"destino"`
	Caminho    string    `json:"caminho"`
	Corpo      []byte    `json:"corpo"`
	CriadoEm   time.Time `json:"criado_em"`
	Tentativas int       `json:"tentativas"`
}

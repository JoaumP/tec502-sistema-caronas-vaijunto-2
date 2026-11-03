// Pacote consorcio define os formatos JSON e constantes da API REST usada na
// comunicação entre os servidores das empresas parceiras.
// Os detalhes completos das rotas estão documentados em API_CONSORCIO.md.
package consorcio

import "github.com/JoaumP/tec502-sistema-caronas-vaijunto/internal/modelos"

// Cabeçalhos enviados em toda chamada HTTP entre os servidores do consórcio
const (
	HeaderLamport = "X-Lamport" // relógio lógico atual de quem mandou a mensagem
	HeaderEmpresa = "X-Empresa" // nome da empresa remetente
)

// Prefixo base de todas as rotas REST entre empresas
const Prefixo = "/consorcio/v1"

// TrechoOferta é o trecho público devolvido na busca federada (sem expor usuários)
type TrechoOferta struct {
	modelos.TrechoInfo
	AssentosLivres int `json:"assentos_livres"`
}

// RespostaTrechos é o retorno de GET /trechos
type RespostaTrechos struct {
	Empresa string         `json:"empresa"`
	Trechos []TrechoOferta `json:"trechos"`
}

// PedidoPreparar é o corpo do POST /transacoes/{id}/preparar (fase 1 do 3PC)
type PedidoPreparar struct {
	Coordenador   string         `json:"coordenador"`
	Participantes []string       `json:"participantes"`
	Ticket        modelos.Ticket `json:"ticket"`
	Trechos       []int64        `json:"trechos"` // só os IDs dos trechos desta empresa
	Passageiro    string         `json:"passageiro"`
	LeaseSegundos int            `json:"lease_segundos"`
}

// RespostaTx é o formato padrão devolvido pelas rotas de transação
type RespostaTx struct {
	ID string `json:"id"`
	// estado local como participante (preparada, pre_confirmada, confirmada, abortada...)
	Estado string `json:"estado,omitempty"`
	// fase como coordenador (iniciada, pre_confirmando, recuperando, confirmada, abortada)
	Fase string `json:"fase,omitempty"`
	// dados completos dos trechos (preenchido na resposta do preparar)
	Trechos []modelos.TrechoInfo `json:"trechos,omitempty"`
	Erro    string               `json:"erro,omitempty"`
}

// EstadoDesconhecida indica que este servidor nunca recebeu essa transação
const EstadoDesconhecida = "desconhecida"

// FaseRecuperando avisa que o coordenador acabou de religar após cair na fase 2
// e ainda está consultando os participantes pra fechar a decisão
const FaseRecuperando = "recuperando"

// PedidoCancelamentoCarona avisa a empresa do passageiro que o motorista de outra
// empresa cancelou uma carona que fazia parte da viagem dele
type PedidoCancelamentoCarona struct {
	Empresa  string `json:"empresa"`
	IDCarona int64  `json:"id_carona"`
}

// Estruturas auxiliares usadas pela suíte de testes pra auditar consistência e injetar quedas

// OcupacaoAuditoria mostra qual transação está segurando vaga num trecho
type OcupacaoAuditoria struct {
	TxID   string `json:"tx_id"`
	Estado string `json:"estado"`
}

// TrechoAuditoria resume a contagem de vagas e ocupações reais de um trecho
type TrechoAuditoria struct {
	ID             int64               `json:"id"`
	IDCarona       int64               `json:"id_carona"`
	StatusCarona   string              `json:"status_carona"`
	AssentosTotais int                 `json:"assentos_totais"`
	AssentosLivres int                 `json:"assentos_livres"`
	Ocupacoes      []OcupacaoAuditoria `json:"ocupacoes"`
}

// Auditoria tira uma foto completa do estado interno da empresa pros testes conferirem
type Auditoria struct {
	Empresa         string                   `json:"empresa"`
	Trechos         []TrechoAuditoria        `json:"trechos"`
	TxParticipante  []modelos.TxParticipante `json:"tx_participante"`
	TxCoordenador   []modelos.TxCoordenador  `json:"tx_coordenador"`
	Reservas        []modelos.Reserva        `json:"reservas"`
	OutboxPendentes int                      `json:"outbox_pendentes"`
}

// PedidoFalha arma um ponto onde o servidor vai simular uma queda brusca nos testes
type PedidoFalha struct {
	Ponto string `json:"ponto"`
}

// Pontos onde dá pra simular queda do processo durante o protocolo 3PC
const (
	FalhaCoordAposPreparar     = "coordenador_apos_preparar"      // coordenador cai entre a fase 1 e a 2
	FalhaCoordAposPreConfirmar = "coordenador_apos_pre_confirmar" // coordenador cai entre a fase 2 e a 3
	FalhaPartAposPreparar      = "participante_apos_preparar"     // participante cai logo após salvar o preparar
)

package servidor

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/modelos"
	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/persistencia"
	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/relogio"
)

// Config reúne os parâmetros de execução de uma empresa do consórcio
type Config struct {
	Empresa        string            // nome desta empresa, ex: "vaijunto"
	Peers          map[string]string // outras empresas do consórcio: nome -> URL da API REST
	DirDados       string            // pasta onde fica salvo o estado.json desta empresa
	Lease          time.Duration     // tempo máximo que uma pré-reserva espera antes de cobrar decisão
	TimeoutRede    time.Duration     // timeout de cada chamada HTTP pra outra empresa
	TimeoutBusca   time.Duration     // timeout menor usado na busca federada pra não travar o passageiro
	PermitirFalhas bool              // habilita a rota /admin/falha (usado só nos testes automatizados)
}

// Estado guarda todos os dados de UMA empresa em memória.
// Um único RWMutex protege os mapas; as seções críticas mexem só em memória e
// são bem rápidas, enquanto disco e chamadas REST ficam sempre fora do lock.
type Estado struct {
	cfg     Config
	relogio *relogio.Lamport
	arquivo *persistencia.Arquivo
	rede    *Rede

	mu        sync.RWMutex
	usuarios  map[string]*modelos.Usuario // chave: login
	caronas   map[int64]*modelos.Carona
	trechos   map[int64]*modelos.Trecho
	reservas  map[string]*modelos.Reserva         // reservas dos passageiros desta empresa
	txPart    map[string]*modelos.TxParticipante  // transações em que somos donos de trecho
	txCoord   map[string]*modelos.TxCoordenador   // transações que nós coordenamos
	outbox    map[int64]*modelos.MensagemPendente // mensagens a entregar para outras empresas
	ocupacoes map[int64]map[string]bool           // trecho -> transações segurando assento (derivado)

	proximoIDUsuario int64
	proximoIDCarona  int64
	proximoIDTrecho  int64
	proximoIDTx      int64
	proximoIDMsg     int64

	// estruturas auxiliares apenas de tempo de execução (não vão pro JSON)
	txAtivas     map[string]bool // transações sendo executadas por este processo vivo
	outboxProcMu sync.Mutex      // evita duas goroutines dispararem a mesma leva da outbox

	// transações que já têm uma goroutine buscando a decisão (não é salvo)
	resolvendoMu sync.Mutex
	resolvendo   map[string]bool

	acordarOutbox chan struct{}

	falhaMu sync.Mutex
	falha   string // ponto de falha armado (testes)
}

// estadoSalvo: formato do estado.json (listas ficam mais legíveis que mapas)
type estadoSalvo struct {
	Empresa          string                      `json:"empresa"`
	Relogio          int64                       `json:"relogio_lamport"`
	ProximoIDUsuario int64                       `json:"proximo_id_usuario"`
	ProximoIDCarona  int64                       `json:"proximo_id_carona"`
	ProximoIDTrecho  int64                       `json:"proximo_id_trecho"`
	ProximoIDTx      int64                       `json:"proximo_id_tx"`
	ProximoIDMsg     int64                       `json:"proximo_id_msg"`
	Usuarios         []*modelos.Usuario          `json:"usuarios"`
	Caronas          []*modelos.Carona           `json:"caronas"`
	Trechos          []*modelos.Trecho           `json:"trechos"`
	Reservas         []*modelos.Reserva          `json:"reservas"`
	TxParticipante   []*modelos.TxParticipante   `json:"tx_participante"`
	TxCoordenador    []*modelos.TxCoordenador    `json:"tx_coordenador"`
	Outbox           []*modelos.MensagemPendente `json:"outbox"`
}

// NovoEstado carrega o estado salvo da empresa (se existir) e prepara tudo.
func NovoEstado(cfg Config) (*Estado, error) {
	e := &Estado{
		cfg:           cfg,
		usuarios:      make(map[string]*modelos.Usuario),
		caronas:       make(map[int64]*modelos.Carona),
		trechos:       make(map[int64]*modelos.Trecho),
		reservas:      make(map[string]*modelos.Reserva),
		txPart:        make(map[string]*modelos.TxParticipante),
		txCoord:       make(map[string]*modelos.TxCoordenador),
		outbox:        make(map[int64]*modelos.MensagemPendente),
		ocupacoes:     make(map[int64]map[string]bool),
		txAtivas:      make(map[string]bool),
		resolvendo:    make(map[string]bool),
		acordarOutbox: make(chan struct{}, 1),
	}

	caminho := filepath.Join(cfg.DirDados, "estado.json")
	dados, err := persistencia.Carregar(caminho)
	if err != nil {
		return nil, fmt.Errorf("lendo %s: %w", caminho, err)
	}
	var relogioSalvo int64
	if dados != nil {
		var s estadoSalvo
		if err := json.Unmarshal(dados, &s); err != nil {
			return nil, fmt.Errorf("estado.json corrompido: %w", err)
		}
		if s.Empresa != "" && s.Empresa != cfg.Empresa {
			return nil, fmt.Errorf("estado.json pertence à empresa %q, não a %q", s.Empresa, cfg.Empresa)
		}
		e.restaurar(&s)
		relogioSalvo = s.Relogio
		log.Printf("estado carregado: %d usuários, %d caronas, %d reservas, %d transações",
			len(e.usuarios), len(e.caronas), len(e.reservas), len(e.txPart)+len(e.txCoord))
	}

	e.relogio = relogio.Novo(relogioSalvo)
	e.rede = NovaRede(cfg, e.relogio)
	e.arquivo, err = persistencia.Novo(caminho, e.gerarSnapshot)
	if err != nil {
		return nil, err
	}
	return e, nil
}

// restaurar reconstrói os mapas internos e recalcula as ocupações de assentos a partir do JSON lido
func (e *Estado) restaurar(s *estadoSalvo) {
	e.proximoIDUsuario = s.ProximoIDUsuario
	e.proximoIDCarona = s.ProximoIDCarona
	e.proximoIDTrecho = s.ProximoIDTrecho
	e.proximoIDTx = s.ProximoIDTx
	e.proximoIDMsg = s.ProximoIDMsg
	for _, u := range s.Usuarios {
		e.usuarios[u.Login] = u
	}
	for _, c := range s.Caronas {
		e.caronas[c.ID] = c
	}
	for _, t := range s.Trechos {
		e.trechos[t.ID] = t
	}
	for _, r := range s.Reservas {
		e.reservas[r.ID] = r
	}
	for _, tx := range s.TxCoordenador {
		e.txCoord[tx.ID] = tx
	}
	for _, m := range s.Outbox {
		e.outbox[m.ID] = m
	}
	// as ocupações são reconstruídas a partir das transações, assim o número de
	// assentos livres sempre bate com as transações que realmente seguram vaga
	for _, tx := range s.TxParticipante {
		e.txPart[tx.ID] = tx
		if seguraAssento(tx.Estado) {
			for _, id := range tx.Trechos {
				e.ocupar(id, tx.ID)
			}
		}
	}
	for _, t := range e.trechos {
		e.recalcularLivres(t.ID)
	}
}

// gerarSnapshot monta o JSON do estado inteiro (chamado pela persistência).
func (e *Estado) gerarSnapshot() ([]byte, error) {
	e.mu.RLock()
	s := estadoSalvo{
		Empresa:          e.cfg.Empresa,
		Relogio:          e.relogio.Atual(),
		ProximoIDUsuario: e.proximoIDUsuario,
		ProximoIDCarona:  e.proximoIDCarona,
		ProximoIDTrecho:  e.proximoIDTrecho,
		ProximoIDTx:      e.proximoIDTx,
		ProximoIDMsg:     e.proximoIDMsg,
	}
	for _, u := range e.usuarios {
		s.Usuarios = append(s.Usuarios, u)
	}
	for _, c := range e.caronas {
		s.Caronas = append(s.Caronas, c)
	}
	for _, t := range e.trechos {
		s.Trechos = append(s.Trechos, t)
	}
	for _, r := range e.reservas {
		s.Reservas = append(s.Reservas, r)
	}
	for _, tx := range e.txPart {
		s.TxParticipante = append(s.TxParticipante, tx)
	}
	for _, tx := range e.txCoord {
		s.TxCoordenador = append(s.TxCoordenador, tx)
	}
	for _, m := range e.outbox {
		s.Outbox = append(s.Outbox, m)
	}
	// Marshal ainda dentro do RLock: os ponteiros apontam para dados vivos
	dados, err := json.MarshalIndent(s, "", "  ")
	e.mu.RUnlock()
	return dados, err
}

// salvar grava o estado em disco e só retorna quando estiver durável.
// Deve ser chamado SEM segurar e.mu.
func (e *Estado) salvar() {
	if err := e.arquivo.Salvar(); err != nil {
		// sem disco não dá para garantir nada do protocolo: melhor cair e
		// voltar com o último estado bom do que seguir inconsistente
		log.Fatalf("erro grave ao salvar estado: %v", err)
	}
}

// Empresa devolve o nome desta empresa
func (e *Estado) Empresa() string { return e.cfg.Empresa }

// Controle de ocupação de assentos (chamar sempre com e.mu travado pra escrita)

// seguraAssento indica se uma transação nesse estado está ocupando vaga no trecho
func seguraAssento(estado string) bool {
	return estado == modelos.TxPreparada || estado == modelos.TxPreConfirmada || estado == modelos.TxConfirmada
}

// ocupar marca que a transação txID está segurando 1 vaga no trecho e atualiza a contagem
func (e *Estado) ocupar(idTrecho int64, txID string) {
	if e.ocupacoes[idTrecho] == nil {
		e.ocupacoes[idTrecho] = make(map[string]bool)
	}
	e.ocupacoes[idTrecho][txID] = true
	e.recalcularLivres(idTrecho)
}

// desocupar tira a transação txID do mapa de ocupação do trecho e recalcula os livres
func (e *Estado) desocupar(idTrecho int64, txID string) {
	delete(e.ocupacoes[idTrecho], txID)
	e.recalcularLivres(idTrecho)
}

// recalcularLivres calcula livres = totais - ocupados. Fazer essa conta direto pelo
// tamanho do mapa evita bug de somar/subtrair duas vezes se chegar mensagem repetida.
func (e *Estado) recalcularLivres(idTrecho int64) {
	if t, ok := e.trechos[idTrecho]; ok {
		t.AssentosLivres = t.AssentosTotais - len(e.ocupacoes[idTrecho])
	}
}

// liberarAssentos solta todas as vagas locais que a transação estava segurando
func (e *Estado) liberarAssentos(tx *modelos.TxParticipante) {
	for _, id := range tx.Trechos {
		e.desocupar(id, tx.ID)
	}
}

// Fila de saída (outbox) pra mensagens que precisam chegar em outras empresas

// enfileirar coloca uma mensagem na outbox junto com a mudança de estado atual
// (com e.mu travado), assim tanto o estado quanto a mensagem vão pro disco juntos.
func (e *Estado) enfileirar(destino, caminho string, corpo any) {
	dados, _ := json.Marshal(corpo)
	e.proximoIDMsg++
	e.outbox[e.proximoIDMsg] = &modelos.MensagemPendente{
		ID:       e.proximoIDMsg,
		Destino:  destino,
		Caminho:  caminho,
		Corpo:    dados,
		CriadoEm: time.Now(),
	}
}

// avisarOutbox cutuca a goroutine da outbox pra tentar entregar logo, sem bloquear quem chamou
func (e *Estado) avisarOutbox() {
	select {
	case e.acordarOutbox <- struct{}{}:
	default:
	}
}

// Injeção de falhas controlada (usada só na suíte de testes automatizados)

// ArmarFalha configura em qual ponto do 3PC o servidor deve simular uma queda
func (e *Estado) ArmarFalha(ponto string) {
	e.falhaMu.Lock()
	e.falha = ponto
	e.falhaMu.Unlock()
	log.Printf("[falha] ponto armado: %s", ponto)
}

// pontoDeFalha mata o processo na hora se aquele ponto foi armado pelo teste (simula kill -9)
func (e *Estado) pontoDeFalha(ponto string) {
	e.falhaMu.Lock()
	armado := e.falha == ponto
	e.falhaMu.Unlock()
	if armado {
		log.Printf("[falha] encerrando o processo no ponto %s", ponto)
		os.Exit(3)
	}
}

// Auxiliares internos de leitura (chamar com e.mu travado)

// passageirosDoTrecho devolve a lista ordenada de passageiros confirmados no trecho ("id@empresa")
func (e *Estado) passageirosDoTrecho(idTrecho int64) []string {
	var lista []string
	for txID := range e.ocupacoes[idTrecho] {
		if tx := e.txPart[txID]; tx != nil && tx.Estado == modelos.TxConfirmada {
			lista = append(lista, tx.Passageiro)
		}
	}
	sort.Strings(lista)
	return lista
}

// infoTrecho monta o resumo público de um trecho local já carimbado com o nome desta empresa
func (e *Estado) infoTrecho(t *modelos.Trecho) modelos.TrechoInfo {
	return modelos.TrechoInfo{
		Empresa:        e.cfg.Empresa,
		ID:             t.ID,
		Origem:         t.Origem,
		Destino:        t.Destino,
		HorarioSaida:   t.HorarioSaida,
		HorarioChegada: t.HorarioChegada,
		PrecoCentavos:  t.PrecoCentavos,
	}
}

// caronaAtiva confere se a carona existe e ainda não foi cancelada pelo motorista
func (e *Estado) caronaAtiva(idCarona int64) bool {
	c, ok := e.caronas[idCarona]
	return ok && c.Status == modelos.CaronaAtiva
}

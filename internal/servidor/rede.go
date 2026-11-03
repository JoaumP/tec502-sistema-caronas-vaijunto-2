package servidor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/consorcio"
	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/relogio"
)

// ErrIndisponivel indica que o servidor de outra empresa não respondeu (timeout,
// conexão recusada ou erro 5xx). Nunca derruba nosso servidor: quem chamou trata
// a falha e segue operando pro resto do consórcio.
var ErrIndisponivel = errors.New("empresa indisponível")

// Rede encapsula o cliente HTTP que conversa com a API REST das outras empresas,
// cuidando automaticamente de propagar e atualizar o relógio de Lamport.
type Rede struct {
	empresa string
	peers   map[string]string
	relogio *relogio.Lamport
	cliente *http.Client
}

// NovaRede cria o cliente HTTP com pool de conexões keep-alive e timeout padrão
func NovaRede(cfg Config, rel *relogio.Lamport) *Rede {
	transporte := &http.Transport{
		// reaproveita conexões abertas entre chamadas REST pra aguentar carga alta
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     30 * time.Second,
	}
	return &Rede{
		empresa: cfg.Empresa,
		peers:   cfg.Peers,
		relogio: rel,
		cliente: &http.Client{Timeout: cfg.TimeoutRede, Transport: transporte},
	}
}

// Conhece verifica se o nome informado está na lista de empresas parceiras
func (r *Rede) Conhece(empresa string) bool {
	_, ok := r.peers[empresa]
	return ok
}

// Peers retorna a lista com os nomes das outras empresas configuradas
func (r *Rede) Peers() []string {
	var nomes []string
	for nome := range r.peers {
		nomes = append(nomes, nome)
	}
	return nomes
}

// Chamar envia uma requisição REST pra outra empresa, atualiza o relógio lógico
// na ida e na volta e decodifica o JSON de resposta em `saida`.
// Se a outra máquina estiver desligada ou estourar o timeout, retorna ErrIndisponivel.
func (r *Rede) Chamar(ctx context.Context, empresa, metodo, caminho string, entrada, saida any) (int, error) {
	base, ok := r.peers[empresa]
	if !ok {
		return 0, fmt.Errorf("empresa desconhecida: %s", empresa)
	}

	var corpo io.Reader
	if entrada != nil {
		switch v := entrada.(type) {
		case []byte: // mensagens da outbox já vêm serializadas em bytes
			corpo = bytes.NewReader(v)
		default:
			dados, err := json.Marshal(entrada)
			if err != nil {
				return 0, err
			}
			corpo = bytes.NewReader(dados)
		}
	}

	req, err := http.NewRequestWithContext(ctx, metodo, base+caminho, corpo)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(consorcio.HeaderEmpresa, r.empresa)
	// mandar mensagem conta como evento no relógio de Lamport
	req.Header.Set(consorcio.HeaderLamport, strconv.FormatInt(r.relogio.Tick(), 10))

	resp, err := r.cliente.Do(req)
	if err != nil {
		return 0, fmt.Errorf("%w: %s (%v)", ErrIndisponivel, empresa, err)
	}
	defer resp.Body.Close()

	// ao receber a resposta, sincroniza nosso relógio com o X-Lamport que voltou
	if v, err := strconv.ParseInt(resp.Header.Get(consorcio.HeaderLamport), 10, 64); err == nil {
		r.relogio.Receber(v)
	}

	if resp.StatusCode >= 500 {
		return resp.StatusCode, fmt.Errorf("%w: %s respondeu %d", ErrIndisponivel, empresa, resp.StatusCode)
	}
	if saida != nil {
		dados, _ := io.ReadAll(resp.Body)
		if len(dados) > 0 {
			json.Unmarshal(dados, saida)
		}
	}
	return resp.StatusCode, nil
}

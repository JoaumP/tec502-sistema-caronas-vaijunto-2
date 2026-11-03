package main

import (
	"bufio"
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/protocolo"
	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/servidor"
	"github.com/JoaumP/tec502-sistema-caronas-vaijunto-2/internal/tcp"
)

// maxConexoesTCP limita quantos clientes podem ficar conectados ao mesmo tempo
// nesta empresa, evitando esgotar memória/descritores sob pico extremo.
const maxConexoesTCP = 2048

func main() {
	empresa := lerEnv("EMPRESA", lerEnv("EMPRESA_ID", "vaijunto"))
	portaTCP := lerEnv("PORTA_TCP", lerEnv("PORTA", "8080"))
	portaHTTP := lerEnv("PORTA_HTTP", lerEnv("PORTA_REST", "8081"))
	dirDados := lerEnv("DIR_DADOS", filepath.Join("dados", empresa))

	cfg := servidor.Config{
		Empresa:        empresa,
		Peers:          interpretarPeers(os.Getenv("PEERS"), empresa),
		DirDados:       dirDados,
		Lease:          lerDuracaoMS("LEASE_MS", 3000),
		TimeoutRede:    lerDuracaoMS("TIMEOUT_REDE_MS", 1500),
		TimeoutBusca:   lerDuracaoMS("TIMEOUT_BUSCA_MS", 1200),
		PermitirFalhas: os.Getenv("PERMITIR_FALHAS") == "1" || os.Getenv("PERMITIR_FALHAS") == "true",
	}

	estado, err := servidor.NovoEstado(cfg)
	if err != nil {
		log.Fatalf("[%s] erro ao iniciar estado: %v", empresa, err)
	}

	ctx, cancelar := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancelar()

	estado.IniciarWorkers(ctx)

	// 1. Sobe o servidor HTTP da API REST oferecida às demais empresas do consórcio
	listenerHTTP, err := escutarTCP(":" + portaHTTP)
	if err != nil {
		log.Fatalf("[%s] erro ao abrir porta HTTP %s: %v", empresa, portaHTTP, err)
	}
	srvHTTP := &http.Server{
		Handler:      estado.HandlerREST(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	go func() {
		log.Printf("[%s] API REST do consórcio escutando na porta %s (peers: %v)",
			empresa, portaHTTP, nomesPeers(cfg.Peers))
		if err := srvHTTP.Serve(listenerHTTP); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[%s] erro na API REST (porta %s): %v", empresa, portaHTTP, err)
		}
	}()

	// 2. Sobe o servidor TCP para os clientes (motoristas e passageiros) desta empresa
	listener, err := escutarTCP(":" + portaTCP)
	if err != nil {
		log.Fatalf("[%s] erro ao abrir porta TCP %s: %v", empresa, portaTCP, err)
	}
	log.Printf("[%s] servidor TCP de clientes escutando na porta %s (dados em %s)",
		empresa, portaTCP, dirDados)

	go func() {
		<-ctx.Done()
		_ = listener.Close()
		ctxShutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srvHTTP.Shutdown(ctxShutdown)
	}()

	semaforo := make(chan struct{}, maxConexoesTCP)

	for {
		conexao, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				log.Printf("[%s] encerrando servidor", empresa)
				return
			}
			log.Printf("[%s] erro ao aceitar conexão: %v", empresa, err)
			continue
		}

		semaforo <- struct{}{}
		go func(c net.Conn) {
			defer func() { <-semaforo }()
			atenderConexao(c, estado)
		}(conexao)
	}
}

// atenderConexao cuida do ciclo de mensagens de um cliente conectado via TCP
// (uma goroutine por cliente conectado)
func atenderConexao(conexao net.Conn, estado *servidor.Estado) {
	defer conexao.Close()
	defer func() {
		// isola qualquer falha inesperada para nunca derrubar o servidor inteiro
		if rec := recover(); rec != nil {
			log.Printf("[%s] pânico isolado em conexão de cliente: %v", estado.Empresa(), rec)
		}
	}()

	leitor := bufio.NewReader(conexao)
	escritor := bufio.NewWriter(conexao)

	var idUsuarioLogado int64 // 0 = ninguém autenticado ainda nesta conexão

	for {
		dados, err := tcp.LerMensagem(leitor)
		if err != nil {
			return
		}

		var msg protocolo.Mensagem
		if err := json.Unmarshal(dados, &msg); err != nil {
			enviarResposta(escritor, protocolo.Resposta{Sucesso: false, Erro: "mensagem malformada"})
			continue
		}

		var resposta protocolo.Resposta
		switch msg.Operacao {
		case protocolo.OpLogin:
			resposta = estado.HandleLogin(msg.Payload)
			if resposta.Sucesso {
				var r protocolo.RespostaLogin
				_ = json.Unmarshal(resposta.Dados, &r)
				idUsuarioLogado = r.IDUsuario
			}
		case protocolo.OpCadastro:
			resposta = estado.HandleCadastro(msg.Payload)
			if resposta.Sucesso {
				var r protocolo.RespostaLogin
				_ = json.Unmarshal(resposta.Dados, &r)
				idUsuarioLogado = r.IDUsuario
			}
		case protocolo.OpLogout:
			idUsuarioLogado = 0
			resposta = protocolo.Resposta{Sucesso: true}
		case protocolo.OpPublicarCarona:
			resposta = estado.HandlePublicarCarona(idUsuarioLogado, msg.Payload)
		case protocolo.OpBuscarItinerario:
			resposta = estado.HandleBuscarItinerario(msg.Payload)
		case protocolo.OpConfirmarReserva:
			resposta = estado.HandleConfirmarReserva(idUsuarioLogado, msg.Payload)
		case protocolo.OpCancelarCarona:
			resposta = estado.HandleCancelarCarona(idUsuarioLogado, msg.Payload)
		case protocolo.OpConsultarCaronas:
			resposta = estado.HandleConsultarCaronas(idUsuarioLogado)
		case protocolo.OpCancelarReserva:
			resposta = estado.HandleCancelarReserva(idUsuarioLogado, msg.Payload)
		case protocolo.OpConsultarReservas:
			resposta = estado.HandleConsultarReservas(idUsuarioLogado)
		default:
			resposta = protocolo.Resposta{Sucesso: false, Erro: "operação desconhecida"}
		}

		enviarResposta(escritor, resposta)
	}
}

// enviarResposta serializa o envelope de resposta e envia de volta na conexão do cliente
func enviarResposta(escritor *bufio.Writer, resposta protocolo.Resposta) {
	dados, _ := json.Marshal(resposta)
	_ = tcp.EnviarMensagem(escritor, dados)
}

// lerEnv lê uma variável de ambiente ou devolve o valor padrão se estiver vazia
func lerEnv(chave, padrao string) string {
	if v := strings.TrimSpace(os.Getenv(chave)); v != "" {
		return v
	}
	return padrao
}

// lerDuracaoMS lê uma duração em milissegundos do ambiente (ex: LEASE_MS=3000)
func lerDuracaoMS(chave string, padraoMS int) time.Duration {
	if v := strings.TrimSpace(os.Getenv(chave)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Millisecond
		}
	}
	return time.Duration(padraoMS) * time.Millisecond
}

// interpretarPeers lê pares "empresa=http://host:porta" separados por vírgula.
// Se a própria empresa vier junto na string, ela é ignorada pra facilitar usar a mesma env em todas.
func interpretarPeers(bruto, propriaEmpresa string) map[string]string {
	peers := make(map[string]string)
	for _, item := range strings.Split(bruto, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		partes := strings.SplitN(item, "=", 2)
		if len(partes) != 2 {
			log.Printf("aviso: entrada inválida em PEERS ignorada: %q (use nome=http://host:porta)", item)
			continue
		}
		nome := strings.TrimSpace(partes[0])
		url := strings.TrimRight(strings.TrimSpace(partes[1]), "/")
		if nome == "" || url == "" || nome == propriaEmpresa {
			continue
		}
		peers[nome] = url
	}
	return peers
}

// nomesPeers extrai só as chaves do mapa de peers pra mostrar no log de inicialização
func nomesPeers(peers map[string]string) []string {
	nomes := make([]string, 0, len(peers))
	for k := range peers {
		nomes = append(nomes, k)
	}
	return nomes
}

// escutarTCP abre a porta habilitando SO_REUSEADDR/SO_REUSEPORT para que o
// servidor possa reiniciar imediatamente após uma queda abrupta sem ficar preso
// esperando sockets antigos em TIME_WAIT/FIN_WAIT fecharem no kernel.
func escutarTCP(endereco string) (net.Listener, error) {
	const soReusePort = 15 // SO_REUSEPORT no Linux
	lc := net.ListenConfig{
		Control: func(_, _ string, c syscall.RawConn) error {
			return c.Control(func(fd uintptr) {
				_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
				_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, soReusePort, 1)
			})
		},
	}
	var ultimoErr error
	for tentativa := 0; tentativa < 25; tentativa++ {
		l, err := lc.Listen(context.Background(), "tcp", endereco)
		if err == nil {
			return l, nil
		}
		ultimoErr = err
		time.Sleep(80 * time.Millisecond)
	}
	return nil, ultimoErr
}

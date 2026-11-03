# VAIJUNTO — Consórcio de Caronas Compartilhadas

Trabalho individual da disciplina de Redes e Sistemas Distribuídos (TEC502) — sistema distribuído de caronas compartilhadas em que múltiplas empresas regionais operam seus próprios servidores autônomos e cooperam via API REST para permitir a busca e a reserva atômica de itinerários entre empresas.

- Especificação do protocolo TCP (cliente ↔ servidor): [`PROTOCOLO.md`](./PROTOCOLO.md)
- Especificação da API REST (servidor ↔ servidor): [`API_CONSORCIO.md`](./API_CONSORCIO.md)

## Estrutura do projeto

```
.
├── cmd/
│   ├── servidor/               # servidor de uma empresa (TCP para clientes + REST para o consórcio)
│   ├── cliente_motorista/      # cliente de terminal para motoristas
│   ├── cliente_passageiro/     # cliente de terminal para passageiros
│   ├── simulador_passageiros/  # gerador de passageiros concorrentes via TCP
│   └── teste_carga/            # suíte automatizada de testes de carga e falhas
├── internal/
│   ├── cliente/                # leitura de entrada, formatação de data/hora, menus e sessão TCP
│   ├── consorcio/              # structs e constantes da API REST entre empresas
│   ├── modelos/                # structs de domínio (Usuario, Trecho, Carona, Reserva, Ticket)
│   ├── persistencia/           # gravação e leitura atômica do estado em JSON
│   ├── protocolo/              # envelope de mensagens e payloads do protocolo TCP
│   ├── relogio/                # relógio lógico de Lamport
│   ├── servidor/               # estado, handlers TCP, busca federada, 3PC, recuperação e REST
│   └── tcp/                    # envio e leitura de mensagens no socket TCP
├── Dockerfile.servidor
├── Dockerfile.cliente_motorista
├── Dockerfile.cliente_passageiro
├── Dockerfile.simulador
├── docker-compose.yml
├── API_CONSORCIO.md
├── PROTOCOLO.md
└── README.md
```

## Como funciona

- Cada empresa roda seu próprio servidor independente e guarda apenas seus dados locais (`estado.json` gravado de forma atômica em disco), sem banco de dados compartilhado nem servidor central.
- Clientes (motoristas e passageiros) conectam via TCP somente ao servidor da própria empresa. Quando um passageiro busca uma viagem, o servidor dele consulta em paralelo as ofertas de trechos das outras empresas via REST e monta os itinerários possíveis com busca em profundidade (DFS) sobre um grafo temporário em memória.
- Os servidores mantêm Relógios Lógicos de Lamport trocados no cabeçalho HTTP `X-Lamport`. A confirmação de um itinerário usa um protocolo de compromisso em três fases (3PC) coordenado pelo servidor do passageiro, com desempate de disputas pelo último assento baseado no relógio de Lamport (*Wound-Wait*).
- Se algum servidor cair durante uma reserva, o vencimento dos *leases* e a terminação cooperativa entre os participantes evitam que assentos fiquem bloqueados indefinidamente ou que reservas fiquem confirmadas pela metade.

## Como executar

O sistema é formado pelos **servidores das empresas** (por padrão 3 empresas: `vaijunto`, `rotasul` e `expresso`) e pelos **clientes** (`cliente_motorista`, `cliente_passageiro` ou `simulador_passageiros`) que se conectam na porta TCP de qualquer uma das empresas.

### 1. Com Docker Compose (mesma máquina)

Sobe os 3 servidores em contêineres isolados:

```bash
docker compose up --build -d
```

Portas expostas para cada empresa:
- **vaijunto**: TCP `localhost:8080` | HTTP `localhost:8081`
- **rotasul**: TCP `localhost:8082` | HTTP `localhost:8083`
- **expresso**: TCP `localhost:8084` | HTTP `localhost:8085`

Em outros terminais, rode o cliente motorista ou passageiro apontando `SERVIDOR` para a porta TCP da empresa desejada:

```bash
# Conectando na empresa vaijunto (porta 8080)
SERVIDOR=localhost:8080 go run ./cmd/cliente_motorista
SERVIDOR=localhost:8080 go run ./cmd/cliente_passageiro

# Conectando na empresa rotasul (porta 8082)
SERVIDOR=localhost:8082 go run ./cmd/cliente_motorista
```

Para encerrar os contêineres:

```bash
docker compose down
```

### 2. Localmente (sem Docker)

Requer Go 1.26 ou superior. Basta abrir um terminal para cada servidor informando o nome da empresa, as portas e a lista `PEERS`:

```bash
PEERS="vaijunto=http://localhost:8081,rotasul=http://localhost:8083,expresso=http://localhost:8085"

# Terminal 1 — servidor vaijunto
EMPRESA=vaijunto PORTA_TCP=8080 PORTA_HTTP=8081 PEERS="$PEERS" go run ./cmd/servidor

# Terminal 2 — servidor rotasul
EMPRESA=rotasul PORTA_TCP=8082 PORTA_HTTP=8083 PEERS="$PEERS" go run ./cmd/servidor

# Terminal 3 — servidor expresso
EMPRESA=expresso PORTA_TCP=8084 PORTA_HTTP=8085 PEERS="$PEERS" go run ./cmd/servidor
```

Depois, em outros terminais, execute os clientes apontando para a porta TCP da empresa escolhida (`localhost:8080`, `localhost:8082` ou `localhost:8084`):

```bash
SERVIDOR=localhost:8080 go run ./cmd/cliente_motorista
SERVIDOR=localhost:8080 go run ./cmd/cliente_passageiro
```

### 3. Com Docker (máquinas diferentes)

Build das imagens (na raiz do projeto):

```bash
docker build -f Dockerfile.servidor -t vaijunto-servidor .
docker build -f Dockerfile.cliente_motorista -t vaijunto-motorista .
docker build -f Dockerfile.cliente_passageiro -t vaijunto-passageiro .
docker build -f Dockerfile.simulador -t vaijunto-simulador .
```

Em cada máquina de servidor, defina a lista `PEERS` com os IPs reais das máquinas e suba o contêiner da respectiva empresa:

```bash
PEERS="vaijunto=http://<IP_MAQUINA_1>:8081,rotasul=http://<IP_MAQUINA_2>:8081,expresso=http://<IP_MAQUINA_3>:8081"

# Na máquina 1 (vaijunto)
docker run -d --name srv-vaijunto -p 8080:8080 -p 8081:8081 -v dados_vaijunto:/app/dados -e EMPRESA=vaijunto -e PEERS="$PEERS" vaijunto-servidor

# Na máquina 2 (rotasul)
docker run -d --name srv-rotasul -p 8080:8080 -p 8081:8081 -v dados_rotasul:/app/dados -e EMPRESA=rotasul -e PEERS="$PEERS" vaijunto-servidor

# Na máquina 3 (expresso)
docker run -d --name srv-expresso -p 8080:8080 -p 8081:8081 -v dados_expresso:/app/dados -e EMPRESA=expresso -e PEERS="$PEERS" vaijunto-servidor
```

Para rodar os clientes apontando para o IP do servidor da empresa desejada:

```bash
docker run -it --rm -e SERVIDOR=<IP_DO_SERVIDOR>:8080 vaijunto-motorista
docker run -it --rm -e SERVIDOR=<IP_DO_SERVIDOR>:8080 vaijunto-passageiro
```

### Simulador de passageiros

Com os servidores já em execução, o simulador publica caronas iniciais (com `-popular`) e dispara vários passageiros simultâneos buscando e disputando reservas entre as empresas:

```bash
go run ./cmd/simulador_passageiros \
  -servidores "vaijunto=localhost:8080,rotasul=localhost:8082,expresso=localhost:8084" \
  -popular -passageiros 30 -assentos 5
```

## Testes

O diretório `cmd/teste_carga` contém uma suíte automatizada que sobe sozinha 3 processos isolados do servidor, executa baterias de concorrência e simula quedas reais de processos durante as transações, verificando:

- busca federada e reserva atômica combinando as 3 empresas;
- exclusão mútua sob disputa concorrente pelos mesmos assentos (sem venda dupla);
- precedência pelo relógio lógico de Lamport sob atraso de rede;
- rollback atômico quando um dos trechos está lotado e ausência de deadlock;
- cancelamento de carona propagando a liberação de vagas para as outras empresas;
- queda e recuperação de servidores participantes e coordenadores nas fases do protocolo, garantindo que nenhum assento fique preso ou reservado pela metade.

Para rodar (não precisa subir servidores antes):

```bash
go run ./cmd/teste_carga
```

Para rodar os testes com o detector de condições de corrida (*data race*) do Go:

```bash
go test -race ./...
```

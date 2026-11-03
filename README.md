# VAIJUNTO — Consórcio Distribuído de Caronas Compartilhadas (Problema 2)

Trabalho individual da disciplina de Redes e Sistemas Distribuídos (TEC502) — evolução do sistema de caronas compartilhadas para uma **arquitetura distribuída em consórcio**, na qual múltiplas empresas regionais operam seus próprios servidores autônomos e cooperam via **API REST** para permitir a busca e a reserva atômica de itinerários multi-empresa.

- Especificação da **API REST entre servidores do consórcio**: [`API_CONSORCIO.md`](./API_CONSORCIO.md)
- Especificação do **protocolo TCP entre clientes e sua empresa**: [`PROTOCOLO.md`](./PROTOCOLO.md)

---

## Estrutura do Projeto

```
.
├── cmd/
│   ├── servidor/               # servidor de uma empresa (TCP para clientes + REST para o consórcio)
│   ├── cliente_motorista/      # cliente de terminal para motoristas (conecta à própria empresa)
│   ├── cliente_passageiro/     # cliente de terminal para passageiros (conecta à própria empresa)
│   ├── simulador_passageiros/  # gerador autônomo de carga (buscas e reservas simultâneas nas empresas)
│   └── teste_carga/            # suíte automatizada com carga concorrente e injeção real de falhas
├── internal/
│   ├── cliente/                # base compartilhada dos clientes CLI (sessão TCP, entrada, horários e menus)
│   ├── consorcio/              # contratos/DTOs e constantes da API REST inter-servidores
│   ├── modelos/                # entidades de domínio, tickets de Lamport e estados de transação
│   ├── persistencia/           # persistência local atômica em JSON (write tmp + fsync + rename)
│   ├── protocolo/              # envelope e payloads do protocolo TCP cliente-servidor
│   ├── relogio/                # relógio lógico de Lamport thread-safe
│   ├── servidor/               # núcleo do servidor (estado, handlers, busca, coordenador/participante 3PC, recuperação e REST)
│   └── tcp/                    # framing de mensagens delimitadas por '\n' sobre socket TCP
├── Dockerfile.servidor
├── Dockerfile.cliente_motorista
├── Dockerfile.cliente_passageiro
├── Dockerfile.simulador
├── docker-compose.yml
├── API_CONSORCIO.md
├── PROTOCOLO.md
└── README.md
```

---

## Como a Coordenação Distribuída Funciona

1. **Zero centralização e propriedade estrita dos dados**: Não há servidor central, coordenador fixo, banco compartilhado nem serviços externos (como Redis, etcd ou ZooKeeper). Cada empresa mantém apenas seus usuários e caronas em armazenamento local próprio (`estado.json` gravado atomicamente com `fsync` + `rename`).
2. **Busca federada sob demanda**: Ao receber uma busca, o servidor da empresa do passageiro consulta em paralelo (`GET /consorcio/v1/trechos`) as ofertas públicas de trechos ativos das demais empresas com timeout curto, monta um grafo temporário em memória, executa DFS com backtracking e descarta o grafo ao responder. Empresas fora do ar são detectadas por timeout e ignoradas sem interromper o serviço.
3. **Critério de precedência sem relógio sincronizado**: Os servidores propagam **Relógios Lógicos de Lamport** no cabeçalho `X-Lamport`. Cada transação recebe um `Ticket = (relogio, empresa, tx_id)` que define uma ordem total determinística em todo o consórcio. Disputas simultâneas pelo último assento na Fase 1 são arbitradas por **Wound-Wait**: transações mais antigas no relógio lógico preemptam pré-reservas não confirmadas de transações mais novas, eliminando deadlocks e respeitando precedência mesmo com atrasos de rede.
4. **Atomicidade e tolerância a falhas sem bloqueio indefinido (3PC + Leases + Terminação Cooperativa)**: A confirmação percorre três fases (`preparar` → `pre-confirmar` → `confirmar`) coordenada dinamicamente pelo servidor do passageiro. Caso o coordenador ou um participante caia no meio da reserva:
   - Se a falha ocorrer antes do fim da Fase 2, os participantes expiram o lease e **abortam/liberam os assentos automaticamente**, mesmo com o coordenador ainda desligado.
   - Se todos os participantes já tiverem atingido `pre_confirmada`, o protocolo de **terminação cooperativa** converge para a confirmação em todos os nós, evitando itinerários reservados pela metade.
   - Mensagens finais de `confirmar`, `abortar` e cancelamentos em cascata contam com **Outbox persistente** e reconciliação automática quando o servidor que falhou volta a operar.

---

## Como Executar

### 1. Com Docker em Computadores Distintos no Laboratório (LARSID / LADICA)

Suponha três máquinas no laboratório com IPs `192.168.0.10` (**VaiJunto**), `192.168.0.11` (**RotaSul**) e `192.168.0.12` (**Expresso**).

Build das imagens:
```bash
docker build -f Dockerfile.servidor -t vaijunto-servidor .
docker build -f Dockerfile.cliente_motorista -t vaijunto-motorista .
docker build -f Dockerfile.cliente_passageiro -t vaijunto-passageiro .
docker build -f Dockerfile.simulador -t vaijunto-simulador .
```

Lista comum de pares REST do consórcio (cada servidor ignora automaticamente a si mesmo na lista):
```bash
PEERS="vaijunto=http://192.168.0.10:8081,rotasul=http://192.168.0.11:8081,expresso=http://192.168.0.12:8081"
```

- **Máquina 1 (`192.168.0.10` — Empresa `vaijunto`)**:
  ```bash
  docker run -d --name srv-vaijunto \
    -p 8080:8080 -p 8081:8081 \
    -v dados_vaijunto:/app/dados \
    -e EMPRESA=vaijunto -e PEERS="$PEERS" \
    vaijunto-servidor
  ```
- **Máquina 2 (`192.168.0.11` — Empresa `rotasul`)**:
  ```bash
  docker run -d --name srv-rotasul \
    -p 8080:8080 -p 8081:8081 \
    -v dados_rotasul:/app/dados \
    -e EMPRESA=rotasul -e PEERS="$PEERS" \
    vaijunto-servidor
  ```
- **Máquina 3 (`192.168.0.12` — Empresa `expresso`)**:
  ```bash
  docker run -d --name srv-expresso \
    -p 8080:8080 -p 8081:8081 \
    -v dados_expresso:/app/dados \
    -e EMPRESA=expresso -e PEERS="$PEERS" \
    vaijunto-servidor
  ```

Durante a apresentação, qualquer contêiner pode ser interrompido e retomado a qualquer momento:
```bash
docker stop srv-rotasul
# ... demais empresas continuam operando ...
docker start srv-rotasul
```

Para conectar os clientes à sua respectiva empresa:
```bash
docker run -it --rm -e SERVIDOR=192.168.0.10:8080 vaijunto-motorista
docker run -it --rm -e SERVIDOR=192.168.0.10:8080 vaijunto-passageiro
```

---

### 2. Com Docker Compose (3 Empresas Isoladas na Mesma Máquina)

```bash
docker compose up --build -d
```

Isso sobe 3 contêineres isolados com volumes independentes:
- **VaiJunto**: TCP `localhost:8080` | REST `localhost:8081`
- **RotaSul**: TCP `localhost:8082` | REST `localhost:8083`
- **Expresso**: TCP `localhost:8084` | REST `localhost:8085`

Conectando clientes:
```bash
SERVIDOR=localhost:8080 go run ./cmd/cliente_motorista
SERVIDOR=localhost:8082 go run ./cmd/cliente_motorista
SERVIDOR=localhost:8084 go run ./cmd/cliente_motorista

SERVIDOR=localhost:8080 go run ./cmd/cliente_passageiro
```

---

### 3. Localmente sem Docker (3 Terminais)

```bash
PEERS="vaijunto=http://localhost:8081,rotasul=http://localhost:8083,expresso=http://localhost:8085"

# Terminal 1
EMPRESA=vaijunto PORTA_TCP=8080 PORTA_HTTP=8081 PEERS="$PEERS" go run ./cmd/servidor

# Terminal 2
EMPRESA=rotasul  PORTA_TCP=8082 PORTA_HTTP=8083 PEERS="$PEERS" go run ./cmd/servidor

# Terminal 3
EMPRESA=expresso PORTA_TCP=8084 PORTA_HTTP=8085 PEERS="$PEERS" go run ./cmd/servidor
```

---

## Simulador Autônomo de Passageiros

Com os 3 servidores em execução (localmente, via Docker Compose ou em máquinas distintas):

```bash
go run ./cmd/simulador_passageiros \
  -servidores "vaijunto=localhost:8080,rotasul=localhost:8082,expresso=localhost:8084" \
  -passageiros 60 \
  -operacoes 10
```

Ou via contêiner Docker apontando para os IPs do laboratório:
```bash
docker run --rm \
  -e SERVIDORES="vaijunto=192.168.0.10:8080,rotasul=192.168.0.11:8080,expresso=192.168.0.12:8080" \
  -e PASSAGEIROS=60 -e OPERACOES=10 \
  vaijunto-simulador
```

---

## Testes Automatizados (Carga, Concorrência e Injeção de Falhas)

A suíte `cmd/teste_carga` é totalmente autônoma: compila o servidor, sobe **3 processos reais isolados** (`vaijunto`, `rotasul`, `expresso`) com persistência própria em disco e executa 10 baterias verificando:
- busca federada e reserva atômica envolvendo 3 empresas;
- disputa simultânea a partir das 3 empresas pelos mesmos assentos (zero venda dupla);
- critério de precedência por relógio lógico de Lamport sob atraso de mensagens;
- atomicidade sob falha parcial, rejeição de trechos desconexos e ausência de deadlock;
- cancelamento de carona propagando liberação de vagas entre 3 empresas;
- queda de servidor participante no meio da reserva, continuidade das demais empresas e liberação após recuperação;
- queda do servidor coordenador após Fase 1 (desbloqueio de assentos por lease mesmo com coordenador desligado);
- queda do servidor coordenador após Fase 2 (terminação cooperativa sem reserva pela metade e reconciliação no reinício);
- tempestade de carga concorrente com queda abrupta (`SIGKILL`), recuperação e auditoria global de invariantes;
- cancelamento de carona em cascata com o coordenador fora do ar (liberação imediata nas empresas online e reconciliação no retorno).

Para executar:
```bash
go run ./cmd/teste_carga
```

Também é possível rodar o teste de concorrência com o detector de *data race* do Go:
```bash
go test -race ./...
```

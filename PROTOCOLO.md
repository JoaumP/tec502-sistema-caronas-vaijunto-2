# Especificação do Protocolo Cliente ↔ Servidor (TCP) — VAIJUNTO

## 1. Visão geral

Os clientes (`cliente_motorista`, `cliente_passageiro` e `simulador_passageiros`) conectam-se **exclusivamente** ao servidor da sua própria empresa sobre sockets TCP puros, sem frameworks de RPC ou mensageria. As mensagens são codificadas em **JSON** e delimitadas por quebra de linha (`\n`).

Para a comunicação **entre servidores das empresas do consórcio**, utiliza-se a API REST documentada em [`API_CONSORCIO.md`](./API_CONSORCIO.md).

## 2. Fluxo de conexão e desconexão

1. O cliente abre uma conexão TCP com o servidor da sua empresa (`net.Dial`).
2. Cliente e servidor trocam mensagens no formato requisição-resposta: o cliente envia uma `Mensagem`, o servidor responde com uma `Resposta`, e assim sucessivamente na mesma conexão.
3. A conexão permanece aberta durante toda a sessão do usuário (login, operações, logout e novo login).
4. Quando o cliente encerra o programa, o servidor detecta `EOF` no socket e libera os recursos daquela conexão sem afetar as demais.
5. Cada conexão é atendida por uma goroutine dedicada protegida por semáforo de concorrência máxima e `recover()`, de modo que falhas ou desconexões abruptas jamais derrubam o servidor.

## 3. Formato das mensagens

### 3.1 Envelope de requisição (cliente → servidor)

```json
{
  "operacao": "nome_da_operacao",
  "payload": { }
}
```

### 3.2 Envelope de resposta (servidor → cliente)

```json
{
  "sucesso": true,
  "erro": "",
  "dados": { }
}
```

### 3.3 Delimitação de mensagens

Cada mensagem JSON é transmitida em uma única linha terminada por `\n`.

## 4. Operações

| Operação | Requer autenticação | Descrição |
|---|---|---|
| `cadastro` | não | Cria um novo usuário na empresa |
| `login` | não | Autentica um usuário existente na empresa |
| `logout` | sim | Encerra a sessão do usuário na conexão atual |
| `publicar_carona` | sim (motorista) | Publica uma nova carona nesta empresa com sua rota de trechos |
| `cancelar_carona` | sim (motorista) | Cancela uma carona publicada pelo motorista (e reservas afetadas no consórcio) |
| `consultar_caronas` | sim (motorista) | Lista as caronas do motorista logado e os passageiros confirmados por trecho |
| `buscar_itinerario` | não | Busca itinerários combinando trechos de todas as empresas ativas do consórcio |
| `confirmar_reserva` | sim (passageiro) | Reserva atomicamente um itinerário (de uma ou várias empresas) |
| `cancelar_reserva` | sim (passageiro) | Cancela uma reserva confirmada, liberando os assentos em todas as empresas envolvidas |
| `consultar_reservas` | sim (passageiro) | Lista as reservas do passageiro logado |

### 4.1 `cadastro` e `login`

**Requisição**
```json
{
  "operacao": "login",
  "payload": { "login": "joao", "senha": "123456" }
}
```

**Resposta**
```json
{ "sucesso": true, "dados": { "id_usuario": 1 } }
```

### 4.2 `logout`

**Requisição**
```json
{ "operacao": "logout" }
```

**Resposta**
```json
{ "sucesso": true }
```

### 4.3 `publicar_carona`

**Requisição**
```json
{
  "operacao": "publicar_carona",
  "payload": {
    "trechos": [
      {
        "origem": "Salvador",
        "destino": "Feira de Santana",
        "horario_saida": "2026-10-30T08:00",
        "horario_chegada": "2026-10-30T09:30",
        "preco_centavos": 3000,
        "assentos_totais": 3
      }
    ]
  }
}
```

**Resposta**
```json
{ "sucesso": true, "dados": { "id_carona": 1 } }
```

### 4.4 `consultar_caronas`

Os passageiros confirmados são identificados no formato `"id@empresa"` (pois passageiros de outras empresas do consórcio podem reservar trechos desta carona):

```json
{
  "sucesso": true,
  "dados": {
    "caronas": [
      {
        "id": 1,
        "status": "ativa",
        "trechos": [
          {
            "id": 1,
            "origem": "Salvador",
            "destino": "Feira de Santana",
            "horario_saida": "2026-10-30T08:00",
            "horario_chegada": "2026-10-30T09:30",
            "assentos_totais": 3,
            "assentos_livres": 2,
            "passageiros": ["5@rotasul"]
          }
        ]
      }
    ]
  }
}
```

### 4.5 `cancelar_carona`

```json
{
  "operacao": "cancelar_carona",
  "payload": { "id_carona": 1 }
}
```

Cancela a carona localmente e propaga o cancelamento para as empresas coordenadoras dos passageiros afetados, liberando também os demais trechos dos itinerários desses passageiros em outras empresas.

### 4.6 `buscar_itinerario`

**Requisição**
```json
{
  "operacao": "buscar_itinerario",
  "payload": {
    "origem": "Salvador",
    "destino": "Montes Claros",
    "data": "2026-10-30"
  }
}
```

**Resposta**
```json
{
  "sucesso": true,
  "dados": {
    "itinerarios": [
      {
        "trechos": [
          {
            "empresa": "vaijunto",
            "id": 1,
            "origem": "Salvador",
            "destino": "Feira de Santana",
            "horario_saida": "2026-10-30T08:00",
            "horario_chegada": "2026-10-30T09:30",
            "preco_centavos": 3000
          },
          {
            "empresa": "rotasul",
            "id": 1,
            "origem": "Feira de Santana",
            "destino": "Vitória da Conquista",
            "horario_saida": "2026-10-30T10:00",
            "horario_chegada": "2026-10-30T14:00",
            "preco_centavos": 5500
          },
          {
            "empresa": "expresso",
            "id": 1,
            "origem": "Vitória da Conquista",
            "destino": "Montes Claros",
            "horario_saida": "2026-10-30T14:30",
            "horario_chegada": "2026-10-30T19:00",
            "preco_centavos": 7000
          }
        ],
        "preco_centavos": 15500
      }
    ],
    "empresas_indisponiveis": []
  }
}
```

Se algum servidor parceiro estiver fora do ar no momento da busca, a busca não falha: o nome da empresa inacessível aparece em `empresas_indisponiveis` e os itinerários possíveis com as empresas ativas são retornados normalmente.

### 4.7 `confirmar_reserva`

Cada trecho do itinerário é identificado pelo par `{ "empresa", "id" }`:

**Requisição**
```json
{
  "operacao": "confirmar_reserva",
  "payload": {
    "itinerario": [
      { "empresa": "vaijunto", "id": 1 },
      { "empresa": "rotasul", "id": 1 },
      { "empresa": "expresso", "id": 1 }
    ]
  }
}
```

**Resposta (sucesso)**
```json
{ "sucesso": true, "dados": { "id_reserva": "vaijunto-1" } }
```

### 4.8 `cancelar_reserva` e `consultar_reservas`

**Cancelar**
```json
{
  "operacao": "cancelar_reserva",
  "payload": { "id_reserva": "vaijunto-1" }
}
```

**Consultar**
```json
{ "operacao": "consultar_reservas" }
```

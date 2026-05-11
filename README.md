[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/A6uVSc3Y)

# Exercício 09 — Kafka + gRPC

Sistema mínimo que combina dois paradigmas de interação:

| Paradigma | Tecnologia | Papel |
|---|---|---|
| Pub/Sub | Apache Kafka | Transporte de eventos entre componentes |
| Cliente/Servidor | gRPC | Consulta de dados por aplicações cliente |

## Arquitetura

```
┌─────────────────────────────────────────────────────────────────────┐
│  Kafka (pub/sub)                          gRPC (cliente/servidor)   │
│                                                                     │
│  sensor_producer.py                                                 │
│       │ publica leituras brutas                                     │
│       ▼ tópico: temperature-raw                                     │
│  processor.py  ←── consome ──┘                                      │
│       │ calcula média/min/max janela 2h                             │
│       ▼ tópico: temperature-processed                               │
│  server.py  ←── consome ──┘                                         │
│       │ persiste no SQLite                                          │
│       └──► expõe TemperatureService (porta 50051) ◄── client.py    │
└─────────────────────────────────────────────────────────────────────┘
```

### Fluxo de dados

1. **sensor_producer.py** — simula três sensores (`sensor-01`, `sensor-02`, `sensor-03`) com random walk de temperatura; emite eventos para o tópico `temperature-raw` apenas quando a variação acumulada ultrapassa 1 °C (variação significativa).

2. **processor.py** — consome `temperature-raw`; mantém uma **janela deslizante de 2 horas** por sensor e calcula `avg`, `min`, `max` e contagem; publica o resultado em `temperature-processed`.

3. **server.py** — consome `temperature-processed` em thread dedicada e persiste cada registro no SQLite; expõe um **serviço gRPC** `TemperatureService` com três métodos:
   - `GetLatestStats` (unary) — última estatística de um sensor
   - `GetHistory` (server-side streaming) — histórico de estatísticas
   - `ListSensors` (unary) — lista todos os sensores conhecidos

4. **client.py** — cliente gRPC interativo (menu) ou modo demo (`--demo`).

## Pré-requisitos

- Python 3.9+
- Docker + Docker Compose
- pip

## Instalação

```bash
# 1. Instalar dependências Python
pip install -r requirements.txt

# 2. Gerar código Python a partir do .proto
python generate_proto.py

# 3. Subir Kafka (em background)
docker compose up -d

# Aguardar ~15s para o Kafka inicializar completamente
```

## Execução

Abra **quatro terminais** na raiz do projeto:

```bash
# Terminal 1 — Produtor (sensor simulado)
python sensor_producer.py

# Terminal 2 — Processador (janela deslizante 2h)
python processor.py

# Terminal 3 — Servidor gRPC (+ consumidor Kafka)
python server.py

# Terminal 4 — Cliente gRPC
python client.py          # menu interativo
python client.py --demo   # consulta automática de demonstração
```

## Tópicos Kafka

| Tópico | Produtor | Consumidor | Payload (JSON) |
|---|---|---|---|
| `temperature-raw` | sensor_producer | processor | `{sensor_id, value, timestamp}` |
| `temperature-processed` | processor | server | `{sensor_id, avg_2h, min_2h, max_2h, count_2h, timestamp}` |

## Serviço gRPC

Definido em `temperature.proto`:

```protobuf
service TemperatureService {
  rpc GetLatestStats(SensorRequest)  returns (TemperatureStats);
  rpc GetHistory(HistoryRequest)     returns (stream TemperatureStats);
  rpc ListSensors(Empty)             returns (SensorList);
}
```

## Banco de dados

SQLite (`temperature.db`), tabela `temperature_stats`:

| Coluna | Tipo | Descrição |
|---|---|---|
| sensor_id | TEXT | Identificador do sensor |
| avg_2h | REAL | Temperatura média — janela 2h |
| min_2h | REAL | Temperatura mínima — janela 2h |
| max_2h | REAL | Temperatura máxima — janela 2h |
| count_2h | INTEGER | Número de amostras na janela |
| timestamp | INTEGER | Epoch do evento (segundos) |

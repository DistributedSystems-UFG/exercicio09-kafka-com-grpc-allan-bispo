"""
(3) Consumidor/Web Service — servidor gRPC + consumidor Kafka.

Consome estatísticas de 'temperature-processed', armazena no SQLite e
expõe um serviço gRPC para consulta pelos clientes.
"""
import json
import sqlite3
import threading
import time
import signal
import sys
from concurrent import futures
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import grpc
import temperature_pb2
import temperature_pb2_grpc

KAFKA_BROKER  = 'localhost:9092'
INPUT_TOPIC   = 'temperature-processed'
GROUP_ID      = 'temperature-server'
GRPC_PORT     = '[::]:50051'
DB_PATH       = 'temperature.db'


# ---------------------------------------------------------------------------
# Banco de dados
# ---------------------------------------------------------------------------

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS temperature_stats (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id TEXT    NOT NULL,
            avg_2h    REAL    NOT NULL,
            min_2h    REAL    NOT NULL,
            max_2h    REAL    NOT NULL,
            count_2h  INTEGER NOT NULL,
            timestamp INTEGER NOT NULL
        )
    ''')
    conn.execute(
        'CREATE INDEX IF NOT EXISTS idx_sensor_ts '
        'ON temperature_stats (sensor_id, timestamp DESC)'
    )
    conn.commit()
    conn.close()


def _conn():
    return sqlite3.connect(DB_PATH)


def insert_stats(s: dict):
    with _conn() as conn:
        conn.execute(
            'INSERT INTO temperature_stats '
            '(sensor_id, avg_2h, min_2h, max_2h, count_2h, timestamp) '
            'VALUES (?,?,?,?,?,?)',
            (s['sensor_id'], s['avg_2h'], s['min_2h'],
             s['max_2h'], s['count_2h'], s['timestamp']),
        )


# ---------------------------------------------------------------------------
# Implementação do serviço gRPC
# ---------------------------------------------------------------------------

def _row_to_stats(row) -> temperature_pb2.TemperatureStats:
    return temperature_pb2.TemperatureStats(
        sensor_id=row[0],
        avg_2h=row[1],
        min_2h=row[2],
        max_2h=row[3],
        count_2h=row[4],
        timestamp=row[5],
    )


class TemperatureServiceServicer(temperature_pb2_grpc.TemperatureServiceServicer):

    def GetLatestStats(self, request, context):
        """Unary: retorna a estatística mais recente do sensor."""
        with _conn() as conn:
            row = conn.execute(
                'SELECT sensor_id, avg_2h, min_2h, max_2h, count_2h, timestamp '
                'FROM temperature_stats '
                'WHERE sensor_id = ? ORDER BY timestamp DESC LIMIT 1',
                (request.sensor_id,),
            ).fetchone()

        if row is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f'Sensor "{request.sensor_id}" não encontrado.')
            return temperature_pb2.TemperatureStats()

        return _row_to_stats(row)

    def GetHistory(self, request, context):
        """Server-side streaming: retorna histórico de estatísticas."""
        limit = request.limit if request.limit > 0 else 10
        with _conn() as conn:
            rows = conn.execute(
                'SELECT sensor_id, avg_2h, min_2h, max_2h, count_2h, timestamp '
                'FROM temperature_stats '
                'WHERE sensor_id = ? ORDER BY timestamp DESC LIMIT ?',
                (request.sensor_id, limit),
            ).fetchall()

        for row in rows:
            yield _row_to_stats(row)

    def ListSensors(self, request, context):
        """Unary: lista todos os sensores com dados armazenados."""
        with _conn() as conn:
            rows = conn.execute(
                'SELECT DISTINCT sensor_id FROM temperature_stats ORDER BY sensor_id'
            ).fetchall()
        return temperature_pb2.SensorList(sensor_ids=[r[0] for r in rows])


# ---------------------------------------------------------------------------
# Thread consumidora do Kafka
# ---------------------------------------------------------------------------

def kafka_thread(stop_event: threading.Event):
    consumer = None
    for attempt in range(1, 11):
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=GROUP_ID,
                auto_offset_reset='earliest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=1000,
            )
            print(f'[SERVER-KAFKA] Conectado ao Kafka, consumindo "{INPUT_TOPIC}"')
            break
        except NoBrokersAvailable:
            print(f'[SERVER-KAFKA] Kafka indisponível, tentativa {attempt}/10...')
            time.sleep(3)

    if consumer is None:
        print('[SERVER-KAFKA] Não foi possível conectar ao Kafka.')
        return

    while not stop_event.is_set():
        try:
            for msg in consumer:
                if stop_event.is_set():
                    break
                stats = msg.value
                insert_stats(stats)
                ts = datetime.fromtimestamp(stats['timestamp']).strftime('%H:%M:%S')
                print(f'[SERVER-KAFKA] {ts}  {stats["sensor_id"]}'
                      f'  avg={stats["avg_2h"]:6.2f}°C  armazenado.')
        except StopIteration:
            pass

    consumer.close()
    print('[SERVER-KAFKA] Encerrado.')


# ---------------------------------------------------------------------------
# Ponto de entrada
# ---------------------------------------------------------------------------

def main():
    init_db()

    stop_event = threading.Event()
    t = threading.Thread(target=kafka_thread, args=(stop_event,), daemon=True)
    t.start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    temperature_pb2_grpc.add_TemperatureServiceServicer_to_server(
        TemperatureServiceServicer(), server
    )
    server.add_insecure_port(GRPC_PORT)
    server.start()
    print(f'[SERVER-gRPC] Escutando em {GRPC_PORT}  |  Ctrl+C para encerrar')

    def stop(sig, frame):
        print('\n[SERVER] Encerrando...')
        stop_event.set()
        server.stop(2)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    server.wait_for_termination()
    t.join(timeout=5)
    print('[SERVER] Encerrado.')


if __name__ == '__main__':
    main()

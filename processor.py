"""
(2) Consumidor/Produtor — processador de eventos de temperatura.

Consome leituras brutas de 'temperature-raw', mantém uma janela deslizante
de 2 horas por sensor e publica estatísticas agregadas em 'temperature-processed'.
"""
import json
import time
import signal
import sys
from collections import defaultdict, deque
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER   = 'localhost:9092'
INPUT_TOPIC    = 'temperature-raw'
OUTPUT_TOPIC   = 'temperature-processed'
WINDOW_SECONDS = 2 * 60 * 60   # janela deslizante de 2 horas
GROUP_ID       = 'temperature-processor'


def make_clients(retries=10, delay=3):
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=GROUP_ID,
                auto_offset_reset='earliest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=1000,
            )
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            )
            print(f'[PROCESSOR] Conectado ao Kafka em {KAFKA_BROKER}')
            return consumer, producer
        except NoBrokersAvailable:
            print(f'[PROCESSOR] Kafka indisponível, tentativa {attempt}/{retries}...')
            time.sleep(delay)
    print('[PROCESSOR] Não foi possível conectar ao Kafka. Encerrando.')
    sys.exit(1)


def compute_stats(readings: list) -> dict:
    values = [r['value'] for r in readings]
    return {
        'avg_2h':   round(sum(values) / len(values), 2),
        'min_2h':   round(min(values), 2),
        'max_2h':   round(max(values), 2),
        'count_2h': len(values),
    }


def main():
    consumer, producer = make_clients()

    # Janela deslizante por sensor: deque de {'timestamp': int, 'value': float}
    windows: dict[str, deque] = defaultdict(deque)

    running = True

    def stop(sig, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    print(f'[PROCESSOR] Aguardando eventos em "{INPUT_TOPIC}"  |  Janela: 2h  |  Ctrl+C para encerrar')

    while running:
        try:
            for msg in consumer:
                if not running:
                    break

                reading = msg.value
                sensor_id = reading['sensor_id']
                ts = reading['timestamp']

                # Atualiza janela deslizante
                window = windows[sensor_id]
                window.append({'timestamp': ts, 'value': reading['value']})
                cutoff = ts - WINDOW_SECONDS
                while window and window[0]['timestamp'] < cutoff:
                    window.popleft()

                stats = compute_stats(list(window))
                event = {'sensor_id': sensor_id, 'timestamp': ts, **stats}

                producer.send(OUTPUT_TOPIC, value=event)

                label = datetime.fromtimestamp(ts).strftime('%H:%M:%S')
                print(f'[PROCESSOR] {label}  {sensor_id}'
                      f'  avg={stats["avg_2h"]:6.2f}°C'
                      f'  min={stats["min_2h"]:6.2f}°C'
                      f'  max={stats["max_2h"]:6.2f}°C'
                      f'  n={stats["count_2h"]}')

        except StopIteration:
            pass    # consumer_timeout_ms expirou, volta ao loop

    consumer.close()
    producer.flush()
    print('[PROCESSOR] Encerrado.')


if __name__ == '__main__':
    main()

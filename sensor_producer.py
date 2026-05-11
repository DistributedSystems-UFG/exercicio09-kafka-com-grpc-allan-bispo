"""
(1) Produtor — sensores simulados de temperatura.

Cada sensor executa um random walk e emite uma leitura para o tópico
'temperature-raw' somente quando a variação acumulada ultrapassa o limiar
THRESHOLD (simulando "variação significativa").
"""
import json
import time
import random
import signal
import sys
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'temperature-raw'
SENSORS = ['sensor-01', 'sensor-02', 'sensor-03']
THRESHOLD = 1.0   # °C — variação mínima para emitir evento
TICK = 0.5        # segundos entre atualizações internas do random walk


def make_producer(retries=10, delay=3):
    for attempt in range(1, retries + 1):
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            )
            print(f'[PRODUCER] Conectado ao Kafka em {KAFKA_BROKER}')
            return p
        except NoBrokersAvailable:
            print(f'[PRODUCER] Kafka indisponível, tentativa {attempt}/{retries}...')
            time.sleep(delay)
    print('[PRODUCER] Não foi possível conectar ao Kafka. Encerrando.')
    sys.exit(1)


def main():
    producer = make_producer()

    # Estado por sensor: temperatura atual e último valor emitido
    states = {
        sid: {'current': random.uniform(20.0, 25.0), 'last_emitted': None}
        for sid in SENSORS
    }

    running = True

    def stop(sig, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    print(f'[PRODUCER] Sensores: {SENSORS}  |  Limiar: {THRESHOLD}°C  |  Ctrl+C para encerrar')

    while running:
        for sensor_id in SENSORS:
            state = states[sensor_id]

            # Random walk: pequena variação a cada tick
            delta = random.uniform(-0.4, 0.4)
            state['current'] = max(10.0, min(45.0, state['current'] + delta))

            last = state['last_emitted']
            if last is None or abs(state['current'] - last) >= THRESHOLD:
                event = {
                    'sensor_id': sensor_id,
                    'value': round(state['current'], 2),
                    'timestamp': int(time.time()),
                }
                producer.send(TOPIC, value=event)
                state['last_emitted'] = state['current']

                ts = datetime.fromtimestamp(event['timestamp']).strftime('%H:%M:%S')
                print(f'[PRODUCER] {ts}  {sensor_id}  {event["value"]:6.2f}°C  '
                      f'(Δ={event["value"] - last:.2f}°C)' if last else
                      f'[PRODUCER] {ts}  {sensor_id}  {event["value"]:6.2f}°C  (inicial)')

        time.sleep(TICK)

    producer.flush()
    print('[PRODUCER] Encerrado.')


if __name__ == '__main__':
    main()

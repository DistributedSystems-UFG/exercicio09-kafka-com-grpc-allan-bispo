"""
(4) Cliente gRPC — consulta o TemperatureService.

Menu interativo para listar sensores, obter última leitura e histórico.
Use --demo para executar uma consulta automática de demonstração.
"""
import sys
import grpc
from datetime import datetime
import temperature_pb2
import temperature_pb2_grpc

GRPC_SERVER = 'localhost:50051'


def fmt_ts(epoch: int) -> str:
    return datetime.fromtimestamp(epoch).strftime('%Y-%m-%d %H:%M:%S')


# ---------------------------------------------------------------------------
# Operações
# ---------------------------------------------------------------------------

def list_sensors(stub: temperature_pb2_grpc.TemperatureServiceStub):
    resp = stub.ListSensors(temperature_pb2.Empty())
    print('\n=== Sensores disponíveis ===')
    if not resp.sensor_ids:
        print('  (nenhum sensor com dados ainda)')
    for sid in resp.sensor_ids:
        print(f'  {sid}')
    return list(resp.sensor_ids)


def get_latest(stub: temperature_pb2_grpc.TemperatureServiceStub, sensor_id: str):
    try:
        s = stub.GetLatestStats(temperature_pb2.SensorRequest(sensor_id=sensor_id))
        print(f'\n=== Última estatística: {sensor_id} ===')
        print(f'  Timestamp  : {fmt_ts(s.timestamp)}')
        print(f'  Média 2h   : {s.avg_2h:.2f} °C')
        print(f'  Mínima 2h  : {s.min_2h:.2f} °C')
        print(f'  Máxima 2h  : {s.max_2h:.2f} °C')
        print(f'  Amostras   : {s.count_2h}')
    except grpc.RpcError as e:
        print(f'  Erro ({e.code().name}): {e.details()}')


def get_history(stub: temperature_pb2_grpc.TemperatureServiceStub,
                sensor_id: str, limit: int = 10):
    print(f'\n=== Histórico: {sensor_id} (últimas {limit} entradas) ===')
    try:
        count = 0
        for s in stub.GetHistory(
            temperature_pb2.HistoryRequest(sensor_id=sensor_id, limit=limit)
        ):
            print(f'  [{fmt_ts(s.timestamp)}]'
                  f'  avg={s.avg_2h:.2f}°C'
                  f'  min={s.min_2h:.2f}°C'
                  f'  max={s.max_2h:.2f}°C'
                  f'  n={s.count_2h}')
            count += 1
        if count == 0:
            print('  (sem registros)')
    except grpc.RpcError as e:
        print(f'  Erro ({e.code().name}): {e.details()}')


# ---------------------------------------------------------------------------
# Modo demo
# ---------------------------------------------------------------------------

def run_demo(stub: temperature_pb2_grpc.TemperatureServiceStub):
    print('\n[DEMO] Listando sensores...')
    sensors = list_sensors(stub)

    for sid in sensors:
        print(f'\n[DEMO] Consultando último dado de {sid}...')
        get_latest(stub, sid)

    if sensors:
        print(f'\n[DEMO] Histórico completo de {sensors[0]}...')
        get_history(stub, sensors[0], limit=5)


# ---------------------------------------------------------------------------
# Menu interativo
# ---------------------------------------------------------------------------

def menu(stub: temperature_pb2_grpc.TemperatureServiceStub):
    while True:
        print('\n' + '─' * 40)
        print(' 1. Listar sensores')
        print(' 2. Última leitura de um sensor')
        print(' 3. Histórico de um sensor')
        print(' 0. Sair')
        print('─' * 40)
        choice = input(' > ').strip()

        if choice == '0':
            break
        elif choice == '1':
            list_sensors(stub)
        elif choice == '2':
            sid = input(' Sensor ID: ').strip()
            get_latest(stub, sid)
        elif choice == '3':
            sid = input(' Sensor ID: ').strip()
            raw = input(' Quantas entradas? [10]: ').strip()
            limit = int(raw) if raw.isdigit() else 10
            get_history(stub, sid, limit)
        else:
            print(' Opção inválida.')


# ---------------------------------------------------------------------------
# Ponto de entrada
# ---------------------------------------------------------------------------

def main():
    demo_mode = '--demo' in sys.argv

    print(f'[CLIENT] Conectando a {GRPC_SERVER}...')
    try:
        with grpc.insecure_channel(GRPC_SERVER) as channel:
            # Verifica conectividade antes de continuar
            grpc.channel_ready_future(channel).result(timeout=5)
            stub = temperature_pb2_grpc.TemperatureServiceStub(channel)
            print('[CLIENT] Conectado.')

            if demo_mode:
                run_demo(stub)
            else:
                menu(stub)

    except grpc.FutureTimeoutError:
        print(f'[CLIENT] Timeout: servidor gRPC não encontrado em {GRPC_SERVER}')
        sys.exit(1)
    except KeyboardInterrupt:
        pass

    print('[CLIENT] Encerrado.')


if __name__ == '__main__':
    main()

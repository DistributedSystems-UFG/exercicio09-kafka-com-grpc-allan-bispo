#!/usr/bin/env python3
"""Gera os arquivos Python a partir do temperature.proto."""
import sys
from grpc_tools import protoc

ret = protoc.main([
    'grpc_tools.protoc',
    '--proto_path=.',
    '--python_out=.',
    '--grpc_python_out=.',
    'temperature.proto',
])

if ret == 0:
    print('OK: temperature_pb2.py e temperature_pb2_grpc.py gerados.')
else:
    print('ERRO ao gerar protobuf.', file=sys.stderr)
    sys.exit(ret)

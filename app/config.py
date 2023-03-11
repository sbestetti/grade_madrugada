import os
import logging

http_config = {
    'api_secret': 'MjMzOTk2MDcwMDAxOTE6NDhiNWEyM2EtOGZmNy00ZWY5LTk3YzgtOGZhODU0OTAxNTVm',
    'url_registros': 'https://publica.cerc.inf.br/app/tio/transaction/arquivos/enviados?linesPerPage=2000&page=0',
    'ulr_arquivo': 'https://publica.cerc.inf.br/app/tio/transaction/arquivos/urls/download/fileControlId?received=false',
}

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'qCFFCFS1',
    'db_name': 'grade_da_madrugada',
    'chunk_size': 100000,
}

log_config = {
    'file': 'script.log',
    'encoding': 'utf-8',
    'format': '%(levelname)s;%(asctime)s;%(funcName)s;%(message)s',
    'level': logging.ERROR,
}

app_config = {
    'tmp_file': './tmp_file',
    'numero_de_threads': 16
}

import os
import logging

http_config = {
    'api_secret': os.getenv('API_SECRET'),
    'url_registros': 'https://publica.cerc.inf.br/app/tio/transaction/arquivos/enviados?linesPerPage=2000&page=0',
    'ulr_arquivo': 'https://publica.cerc.inf.br/app/tio/transaction/arquivos/urls/download/fileControlId?received=false',
}

db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWD'),
    'db_name': os.getenv('DB_NAME'),
    'chunk_size': 100000,
}

log_config = {
    'file': 'script.log',
    'encoding': 'utf-8',
    'format': '%(levelname)s;%(asctime)s;%(funcName)s;%(message)s',
    'level': logging.INFO,
}

app_config = {
    'tmp_file': './tmp_file'
}

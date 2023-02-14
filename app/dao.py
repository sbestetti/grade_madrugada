# Sistema
from datetime import date

# Ferramentas
import mysql.connector

# Internos
from log_manager import logging
import config as cfg

# Constantes de queries
INSERT_QUERY = '''
    INSERT IGNORE INTO grade_madrugada (
        cnpj,
        guid,
        codigo_erro,
        data
    )
    VALUES (
        %(cnpj)s,
        %(guid)s,
        %(codigo_erro)s,
        %(data)s
    )
    '''
GET_PARTICIPANTES_QUERY = 'SELECT cnpj FROM participantes'
INSERT_ARQUIVO_QUERY = 'INSERT INTO arquivos (cnpj, nome, data_de_processamento) VALUES (%(cnpj)s, %(nome)s, %(data)s)'

# Setup da conexão com o banco
logging.info('Conectando ao banco...')
try:
    db = mysql.connector.connect(
        host=cfg.db_config['host'],
        user=cfg.db_config['user'],
        password=cfg.db_config['password'],
        database=cfg.db_config['db_name']
    )
except mysql.connector.DatabaseError as e:
    logging.critical(f'Erro na conexão do banco: {e}')
    exit()
logging.info('Conexão estabelecida')


def get_participantes():
    # Busca a lista atual de CNPJs dos participantes
    with db.cursor() as cursor:
        cursor.execute(GET_PARTICIPANTES_QUERY)
        participantes = cursor.fetchall()
    return participantes


def save_records(registros: list):
    try:
        with db.cursor() as cursor:
            cursor.executemany(INSERT_QUERY, registros)
            db.commit()
    except Exception as e:
        logging.critical(f'Erro ao inserir registro no banco: {e}')
        exit()


def add_downloaded_file(link: list) -> None:
    arquivo_atual = {
        'cnpj': link['participante'],
        'nome': link['nome'],
        'data': date.today()
    }
    try:
        with db.cursor() as cursor:
            cursor.execute(INSERT_ARQUIVO_QUERY, arquivo_atual)
            db.commit()
    except Exception as e:
        logging.critical(f'Erro ao inserir registro no banco: {e}')
        exit()

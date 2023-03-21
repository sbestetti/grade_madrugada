# Sistema
from datetime import date
import os

# Ferramentas
import mysql.connector

# Internos
from log_manager import logging
import config as cfg

# Constantes de queries
INSERT_QUERY = '''
    INSERT IGNORE INTO registros (
        guid,
        cnpj,
        codigo_de_erro,
        data
    )
    VALUES (
        %(guid)s,
        %(cnpj)s,
        %(codigo_de_erro)s,
        %(data)s
    )
    '''
GET_PARTICIPANTES_QUERY = 'SELECT cnpj FROM participantes'
INSERT_ARQUIVO_QUERY = 'INSERT INTO arquivos (cnpj, nome, data_de_processamento) VALUES (%(cnpj)s, %(nome)s, %(data)s)'

def connect_to_db():
    # Setup da conexão com o banco
    try:
        connection = mysql.connector.connect(
            host=cfg.db_config['host'],
            user=cfg.db_config['user'],
            password=cfg.db_config['password'],
            database=cfg.db_config['db_name']
        )
    except mysql.connector.DatabaseError as e:
        logging.critical(f'Erro na conexão do banco: {e}')
        os._exit(1)
    return connection


def get_participantes():
    # Busca a lista atual de CNPJs dos participantes
    db = connect_to_db()
    cursor = db.cursor()
    cursor.execute(GET_PARTICIPANTES_QUERY)
    participantes = cursor.fetchall()
    cursor.close()
    db.close()
    return participantes


def save_records(registros: list):
    db = connect_to_db()
    cursor = db.cursor()
    try:
        cursor.executemany(INSERT_QUERY, registros)
        db.commit()
        cursor.close()
        db.close()
    except Exception as e:
        raise e


def add_downloaded_file(nome_do_arquivo, participante) -> None:
    db = connect_to_db()
    cursor = db.cursor()
    arquivo_atual = {
        'cnpj': participante,
        'nome': nome_do_arquivo,
        'data': date.today()
    }
    try:
        cursor.execute(INSERT_ARQUIVO_QUERY, arquivo_atual)
        db.commit()
        cursor.close()
        db.close()
    except Exception as e:
        logging.critical(f'Erro ao inserir registro no banco: {e}')
        os._exit(1)


def check_if_processed(link):
    db = connect_to_db()
    cursor = db.cursor()
    nome_do_arquivo = [link['nome']]
    cursor.execute('SELECT * FROM arquivos WHERE nome=%s LIMIT 1', nome_do_arquivo)
    result = cursor.fetchone()
    cursor.close()
    db.close()
    if result:
        return True
    return False

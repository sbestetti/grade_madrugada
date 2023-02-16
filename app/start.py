# Sistema
import os
import sys
from datetime import datetime, timedelta
import threading
import traceback

# Ferramentas
import requests

# Internos
from log_manager import logging
import dao
import api_handler
import file_parser

# Tratando possível argumento de data de início
if len(sys.argv) == 1:
    # Pegando arquivos do dia anterior para garantir que arquivos
    # enviados depois que o Script rodou sejam incluidos
    data_de_inicio = datetime.today() - timedelta(days=1)
else:
    data_de_inicio = datetime.strptime(sys.argv[1], '%Y-%m-%d')
data_de_inicio = data_de_inicio.date()


# Logando erros inesperados
def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logging.error(
        "Uncaught exception",
        exc_info=(exc_type, exc_value, exc_traceback)
    )


sys.excepthook = handle_exception


def main(participante, db):    
    cnpj = participante[0]    
    links = api_handler.get_links_by_cnpj(cnpj, data_de_inicio)
    qtd_arquivos = len(links)
    logging.info(f'{cnpj}: {qtd_arquivos} arquivos encontrados.')
    counter = 1
    for link in links:
        try:
            logging.info(f'{cnpj}: baixando arquivo {counter} de {qtd_arquivos}.')
            is_new_file = api_handler.get_files_by_links(link, db)
            if is_new_file:
                logging.info(f'{cnpj}: download finalizado. Iniciando processamento')
                numero_de_registros = file_parser.parse_file(cnpj, link["nome"], db)
                dao.add_downloaded_file(link, db)
                logging.info(f'{cnpj}: {numero_de_registros} registro processados')
                os.remove(link['nome'])
                counter += 1
            else:
                logging.info(f'{cnpj}: Arquivo {link["nome"]} já processado anteriormente. Pulando')
                counter += 1
                continue
        except requests.exceptions.HTTPError:
            continue
        except Exception as e:
            logging.info(f'{cnpj}: {e}\nTraceback: {traceback.print_stack()}')
            continue
    logging.info(f'{cnpj}: Processamento de arquivos finalizado.')
    return f'{cnpj}: participante finalizado'


logging.info('Iniciando a execução')
db = dao.connect_to_db()
participantes = dao.get_participantes(db)
threads = list()
for participante in participantes:
    local_db_connection = dao.connect_to_db()
    _ = threading.Thread(target=main, args=(participante, local_db_connection, ))
    threads.append(_)
    _.start()
for thread in threads:
    thread.join()

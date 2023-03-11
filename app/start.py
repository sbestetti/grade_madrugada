# Imports do sistema
import os
import sys
import threading
from datetime import datetime, timedelta
from queue import Queue

# Imports do aplicativo
import file_parser
import api_handler
import dao
import config
from log_manager import logging

# Tratando possível argumento de data de início
if len(sys.argv) == 1:
    # Pegando arquivos do dia anterior para garantir que arquivos
    # enviados depois que o Script rodou sejam incluidos
    data_de_inicio = datetime.today() - timedelta(days=1)
else:
    data_de_inicio = datetime.strptime(sys.argv[1], '%Y-%m-%d')
data_de_inicio = data_de_inicio.date()

link_jobs = Queue()
download_jobs = Queue()
process_jobs = Queue(4)

count_link_cnpj = 0
count_file_by_link = 0
count_save_file = 0
count_create = 0
count_join = 0


def print_status(nome_do_worker):
    print(f'{nome_do_worker} - {datetime.now()}: Participantes na fila: {link_jobs.qsize()} / Downloads na fila: {download_jobs.qsize()} / Arquivos na fila: {process_jobs.qsize()}          ', end='\r')

def worker_get_link_by_cnpj(counter):
    while True:
        cnpj = link_jobs.get()
        if cnpj is None:
            for i in range(config.app_config['numero_de_threads']):
                download_jobs.put(None)            
            break
        response = api_handler.get_links_by_cnpj(cnpj, data_de_inicio)
        for _ in response:
            if dao.check_if_processed(_):
                continue
            else:
                download_jobs.put(_)
        print_status('get_links')
        link_jobs.task_done()        


def worker_get_file_by_link():
    while True:
        link = download_jobs.get()
        if link is None:
            process_jobs.put(None)
            break
        try:
            file_name = api_handler.get_files_by_links(link)
            if file_name:
                process_jobs.put([link['participante'], file_name])
            else:
                continue
        except Exception as e:
            raise(e)
        print_status('downloads')
        download_jobs.task_done()


def worker_save_file_to_db():
    while True:
        current_task = process_jobs.get()
        if current_task is None:
            process_jobs.task_done()
            break
        file_parser.parse_file(current_task[0], current_task[1])
        os.remove(current_task[1])        
        print_status('processing')
        process_jobs.task_done()

logging.info('----------------------------Iniciando execucao----------------------------')
participantes = dao.get_participantes()
for participante in participantes:
    link_jobs.put(participante[0])
link_jobs.put(None)
link_fetch_thread = threading.Thread(target=worker_get_link_by_cnpj, args=[count_link_cnpj,], daemon=True)
link_fetch_thread.start()
link_fetch_thread.join()


working_threads = list()
for i in range(config.app_config['numero_de_threads']):
    working_threads.append(threading.Thread(target=worker_get_file_by_link, daemon=True))
    working_threads.append(threading.Thread(target=worker_save_file_to_db, daemon=True))
for i in working_threads:
    i.start()
for i in working_threads:
    i.join()
logging.info('----------------------------Finalizando execucao----------------------------')

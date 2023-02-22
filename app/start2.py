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

def worker_get_link_by_cnpj():
    while True:
        cnpj = link_jobs.get()
        if cnpj is None:
            download_jobs.put(None)
            print(f'{datetime.now()}: Busca de links finalizada')
            break
        response = api_handler.get_links_by_cnpj(cnpj, data_de_inicio)
        for _ in response:
            download_jobs.put(_)
        link_jobs.task_done()        


def worker_get_file_by_link():
    while True:
        link = download_jobs.get()
        if link is None:
            process_jobs.put(None)
            print(f'{datetime.now()}: Downloads finalizados')
            break
        file_name = api_handler.get_files_by_links(link)
        if file_name:
            process_jobs.put([link['participante'], file_name])
        else:
            process_jobs.put([])
        download_jobs.task_done()


def worker_save_file_to_db():
    while True:
        current_task = process_jobs.get()
        if current_task is None:
            print(f'{datetime.now()}: Processamento finalizado')
            break
        if current_task:
            registros = file_parser.parse_file(current_task[0], current_task[1])
            print(f'{registros} processados')
            os.remove(current_task[1])
        else:
            continue
        process_jobs.task_done()


for i in range(4):
    threading.Thread(target=worker_get_link_by_cnpj, daemon=True).start()
    threading.Thread(target=worker_get_file_by_link, daemon=True).start()
    threading.Thread(target=worker_save_file_to_db, daemon=True).start()

participantes = dao.get_participantes()
for participante in participantes:
    link_jobs.put(participante[0])
link_jobs.put(None)

link_jobs.join()
download_jobs.join()
process_jobs.join()

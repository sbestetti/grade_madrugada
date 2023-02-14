# Sistema
import sys
from datetime import datetime, timedelta

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


def main():
    logging.info('Iniciando a execução')
    participantes = dao.get_participantes()
    for participante in participantes:
        cnpj = participante[0]
        logging.info(f'\n\nProcessando arquivos do participante {cnpj}')
        links = api_handler.get_links_by_cnpj(cnpj, data_de_inicio)
        qtd_arquivos = len(links)
        logging.info(f'{qtd_arquivos} arquivos encontrados.')
        counter = 1
        for link in links:
            logging.info(
                f'Baixando arquivo {counter} de {qtd_arquivos}.')
            try:
                api_handler.get_files_by_links(link)
            except Exception:
                print(
                    f'Erro no arquivo {counter} de {qtd_arquivos}. Pulando'
                )
                counter += 1
                continue
            logging.info('Download finalizado. Iniciando processamento')
            numero_de_registros = file_parser.parse_file(cnpj)
            logging.info(f'{numero_de_registros} registro processados')
            counter += 1
    logging.info('Processamento de arquivos finalizado.')


main()

# Imports de sistema
import sys
from datetime import date, datetime
import os

# Imports de ferramentas
import requests
import mysql.connector
import logging
import pandas

# Tratando possível argumento de data de início
if len(sys.argv) == 1:
    DATA_DE_INICIO = datetime.today()
else:
    DATA_DE_INICIO = datetime.strptime(sys.argv[1], '%Y-%m-%d')


# Configs do banco
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWD = os.getenv('DB_PASSWD')
DB_NAME = os.getenv('DB_NAME')
CHUNK_SIZE = 100000  # Quantidade de registros a serem inseridos em cada INSERT

# Configs das requisições HTTP
API_SECRET = os.getenv('API_SECRET')
URL_REGISTROS = 'https://publica.cerc.inf.br/app/tio/transaction/arquivos/enviados?linesPerPage=2000&page=0'
URL_ARQUIVO = 'https://publica.cerc.inf.br/app/tio/transaction/arquivos/urls/download/fileControlId?received=false'

# Configs do logger
LOG_FILE = 'script.log'
LOG_ENCODING = 'utf-8'
LOG_FORMAT = '%(levelname)s;%(asctime)s;%(funcName)s;%(message)s'
LOG_LEVEL = logging.INFO

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

# Setup do logger
logging.basicConfig(
    filename=LOG_FILE,
    encoding=LOG_ENCODING,
    format=LOG_FORMAT,
    level=LOG_LEVEL
    )
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logging.error(
        "Uncaught exception",
        exc_info=(exc_type, exc_value, exc_traceback)
    )


sys.excepthook = handle_exception

# Setup da conexão com o banco
logging.info('Conectando ao banco...')
try:
    db = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWD,
        database=DB_NAME
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


def get_tio_headers() -> dict:
    # Recebe um secret e retorna um header com autenticação preenchida

    headers = {
        'Authorization': f'Basic {API_SECRET}'
    }
    form_data = {'grant_type': 'client_credentials'}
    response = requests.post(
        'https://cad-prd.cerc.inf.br/oauth/token',
        data=form_data,
        headers=headers
    )
    token = response.json()['access_token']
    header = {
        'Authorization': f'Bearer {token}'
    }

    return header


def get_links_by_cnpj(cnpj: str) -> list:
    # Recebe o CNPJ de um participante e retorna uma lista
    # de todos os arquivos recebidos na data especificada

    arquivos_recebidos = list()
    payload = {
        'companyDocument': cnpj,
        'fileLayoutId': '73e4ad69-9aa0-43d6-9931-3ef108b0fd0c',
        'finalDate': date.today().strftime('%Y-%m-%d'),
        'startDate': DATA_DE_INICIO.strftime('%Y-%m-%d')
    }

    header = get_tio_headers()

    try:
        response = requests.put(URL_REGISTROS, headers=header, json=payload)
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        logging.critical(f'Erro HTTP {response.status_code} durante busca dos arquivos do participante {cnpj}. Os dados podem estar incompletos!')
        return arquivos_recebidos

    data = response.json()
    for arquivo in data['result']['content']:
        _ = {
            'participante': str(arquivo['fileName'])[11:19],
            'id': arquivo['fileControlId'],
            'nome': arquivo['fileName'],
            'tempo_de_processamento': arquivo['processingTime']
        }
        arquivos_recebidos.append(_)

    return arquivos_recebidos


def get_files_by_links(link: list) -> None:
    # Recebe a lista de arquivos de um participante e salva todos os registros em um arquivo local único

    participante = link['participante']

    header = get_tio_headers()

    url_atual = URL_ARQUIVO.replace('fileControlId', link['id'])
    try:
        response = requests.get(url_atual, headers=header)
        response.raise_for_status()
        logging.debug(f'Status da resposta: {response.status_code}')
    except requests.exceptions.HTTPError:
        message = f'Erro HTTP {response.status_code} durante busca dos arquivos do participante {participante}. Os dados podem estar incompletos!'
        logging.warning(message)
        raise Exception(message)

    data = response.json()
    url_do_arquivo = data['result']
    arquivo = requests.get(url_do_arquivo, stream=True)
    with open('tmp_file', 'ab') as arquivo_local:
        for chunk in arquivo.iter_content(chunk_size=1024):
            arquivo_local.write(chunk)
    return


def parse_file(participante: str) -> int:
    # Move os dados do arquivo recebido para o banco

    logging.info(f'Lendo arquivos do participante {participante}')

    total_de_registros = 0

    with pandas.read_csv(
        './tmp_file',
        sep=';',
        chunksize=CHUNK_SIZE,
        names=[
            'referencia_externa',
            'guid',
            'horario',
            'codigo_erro',
            'desc_erro'
            ]
    ) as reader:
        for chunk in reader:
            registros = list()
            for line in chunk.index:
                registro = {}
                registro['cnpj'] = str(participante)
                registro['guid'] = str(chunk['guid'][line])
                if chunk['codigo_erro'][line] != 0:
                    erro = chunk['desc_erro'][line]
                    lista_erro = erro.split(';')
                    registro['codigo_erro'] = lista_erro[0]
                    registro['desc_erro'] = lista_erro[1]
                else:
                    registro['codigo_erro'] = 0
                    registro['desc_erro'] = None
                new_time = datetime.strptime(
                    chunk['horario'][line], '%Y-%m-%dT%H:%M:%S.%fZ'
                )
                registro['data'] = new_time.date()
                registros.append(registro)

            try:
                with db.cursor() as cursor:
                    cursor.executemany(INSERT_QUERY, registros)
                    db.commit()
            except Exception as e:
                logging.critical(f'Erro ao inserir registro no banco: {e}')
                print(f'Registro atual: {registro}')
                exit()

            total_de_registros = total_de_registros + len(registros)

    os.remove('./tmp_file')
    return total_de_registros


# Loop principal
logging.info('Iniciando a execução')
for participante in get_participantes():
    cnpj = participante[0]
    logging.info(f'Processando arquivos do participante {cnpj}')
    links = get_links_by_cnpj(cnpj)
    qtd_arquivos = len(links)
    logging.info(
        f'{qtd_arquivos} arquivos encontrados para o participante {cnpj}'
    )
    counter = 1
    for link in links:
        logging.info(
            f'Baixando arquivo {counter} de {qtd_arquivos} do participante {cnpj}'
        )
        try:
            get_files_by_links(link)
        except Exception:
            print(
                f'Erro ao baixar o arquivo {counter} de {qtd_arquivos}. Pulando'
            )
            counter += 1
            continue
        logging.info('Download finalizado. Iniciando processamento')
        numero_de_registros = parse_file(cnpj)
        logging.info(f'{numero_de_registros} registro processados')
        counter += 1
logging.info('Processamento de arquivos finalizado. Atualizando relatórios...')
with db.cursor() as cursor:
    cursor.callproc('update_summary_table')
logging.info('Relatórios atualizados. Processo finalizado com sucesso!')

import requests
import logging
import sys
import mysql.connector
from datetime import date, datetime
import os
import pandas


# Configs do banco
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWD = os.getenv('DB_PASSWD')
DB_NAME = os.getenv('DB_NAME')
CHUNK_SIZE = 100000 # Quantidade de registros a serem inseridos em cada INSERT

# Configs das requisições HTTP
#API_TOKEN = os.getenv('API_TOKEN')
API_TOKEN = 'eaf6a2cf-0990-4644-9e5a-e61f28a2e6dc'

URL_REGISTROS = 'https://publica.cerc.inf.br/app/tio/transaction/arquivos/enviados?linesPerPage=2000&page=0'
URL_ARQUIVO = 'https://publica.cerc.inf.br/app/tio/transaction/arquivos/urls/download/fileControlId?received=false'
HEADERS = {
    'Authorization': f'Bearer {API_TOKEN}'
    }

# Configs do logger
LOG_FILE = 'script.log'
LOG_ENCODING = 'utf-8'
LOG_FORMAT = '%(levelname)s - %(asctime)s IN %(funcName)s: %(message)s'
LOG_LEVEL = logging.INFO

# Constante dos participantes a checar
PARTICIPANTES = [
        '01181521000155',
        '01425787003898',
        '02038232000164',
        '04962772000165',
        '06167186000154',
        '10878448000166',
        '17887874000105',
        '18189547000142',
        '20520298000178',
        '08561701000101',
        '08561701014323',
        '17948578000177',
        '92934215000106',
        '14380200000121',
        '06308851000182',
    ]

# Constantes de queries
INSERT_QUERY = '''
    INSERT IGNORE INTO grade_madrugada (
        cnpj, 
        referencia_externa, 
        guid, 
        timestamp, 
        codigo_erro, 
        desc_erro
    ) 
    VALUES (
        %(cnpj)s, 
        %(referencia_externa)s, 
        %(guid)s, 
        %(date_time)s, 
        %(codigo_erro)s, 
        %(desc_erro)s
    )
    '''

# Setup do logger
logging.basicConfig(
    filename=LOG_FILE,
    encoding=LOG_ENCODING,
    format=LOG_FORMAT,
    level=LOG_LEVEL
    )
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

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

def get_links_by_cnpj(cnpj: str, data: date = date.today()) -> list:
    #Recebe o CNPJ de um participante e retorna uma lista de todos os arquivos recebidos na data especificada
    
    payload = {
    'companyDocument': cnpj,
    'fileLayoutId': '73e4ad69-9aa0-43d6-9931-3ef108b0fd0c',
    'finalDate': data.strftime('%Y-%m-%d'),
    'startDate': data.strftime('%Y-%m-%d')
    }

    try:
        response = requests.put(URL_REGISTROS, headers=HEADERS, json=payload)
        response.raise_for_status()
    except requests.exceptions.HTTPError:        
        logging.critical(f'Erro HTTP {response.status_code} durante busca dos arquivos do participante {cnpj}. Os dados podem estar incompletos!')
        return
        
    
    data = response.json()
    arquivos_recebidos = list()
    for arquivo in data['result']['content']:    
        _ = {
            'participante': str(arquivo['fileName'])[11:19], # Essa string contém parte do CNPJ do participante, extraído do nome do arquivo 
            'id': arquivo['fileControlId'],
            'nome': arquivo['fileName'],
            'tempo_de_processamento': arquivo['processingTime']
        }
        arquivos_recebidos.append(_)

    return arquivos_recebidos


def get_files_by_links(link: list) -> None:
    #Recebe a lista de arquivos de um participante e salva todos os registros em um arquivo local único
    
    participante = link['participante']
    
    url_atual = URL_ARQUIVO.replace('fileControlId', link['id'])
    try:
        response = requests.get(url_atual, headers=HEADERS)
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
    #Move os dados do arquivo recebido para o banco

    logging.info(f'Lendo arquivos do participante {participante}')

    total_de_registros = 0

    with pandas.read_csv(
    './tmp_file',
    sep=';',
    chunksize=CHUNK_SIZE,
    names=[
        'referencia_externa',
        'guid',
        'timestamp',
        'codigo_erro',
        'desc_erro'
        ]
    ) as reader:
        for chunk in reader:
            registros = list()    
            for line in chunk.index:
                new_time = datetime.strptime(chunk['timestamp'][line], '%Y-%m-%dT%H:%M:%S.%fZ')
                registro = {}
                registro['cnpj'] = participante
                registro['referencia_externa'] = str(chunk['referencia_externa'][line])
                registro['guid'] = str(chunk['guid'][line])
                registro['date_time'] = new_time
                if chunk['codigo_erro'][line] != 0:
                    erro = chunk['desc_erro'][line]
                    lista_erro = erro.split(';')
                    registro['codigo_erro'] = lista_erro[0]
                    registro['desc_erro'] = lista_erro[1]
                else:
                    registro['codigo_erro'] = 0
                    registro['desc_erro'] = None                
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
for cnpj in PARTICIPANTES:
    logging.info(f'Processando arquivos do participante {cnpj}')
    links = get_links_by_cnpj(cnpj)
    qtd_arquivos = len(links)
    logging.info(f'{qtd_arquivos} arquivos encontrados para o participante {cnpj}')
    counter = 1
    for link in links:
        logging.info(f'Baixando arquivo {counter} de {qtd_arquivos} do participante {cnpj}')
        try:
            get_files_by_links(link)
        except Exception as e:
            print(f'Erro ao baixar o arquivo {counter} de {qtd_arquivos}. Pulando')
            next
        logging.info(f'Download finalizado. Iniciando processamento')
        numero_de_registros = parse_file(cnpj)
        logging.info(f'{numero_de_registros} registro processados')
        counter += 1
logging.info('Processo finalizado')
            
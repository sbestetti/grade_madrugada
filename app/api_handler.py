# Sistema
from datetime import date, datetime

# Ferramentas
import requests
from google.cloud import storage

# Internos
import config as cfg
import dao


def get_tio_headers() -> dict:
    # Recebe um secret e retorna um header com autenticação preenchida
    headers = {
        'Authorization': f'Basic {cfg.http_config["api_secret"]}'
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


def get_links_by_cnpj(cnpj: str, data_de_inicio: datetime) -> list:
    # Recebe o CNPJ de um participante e retorna uma lista
    # de todos os arquivos recebidos na data especificada
    arquivos_recebidos = list()
    payload = {
        'companyDocument': cnpj,
        'fileLayoutId': '73e4ad69-9aa0-43d6-9931-3ef108b0fd0c',
        'finalDate': date.today().strftime('%Y-%m-%d'),
        'startDate': data_de_inicio.strftime('%Y-%m-%d'),
        'status': 3
    }
    header = get_tio_headers()
    response = requests.put(cfg.http_config['url_registros'], headers=header, json=payload)
    data = response.json()
    for arquivo in data['result']['content']:
        _ = {
            'participante': cnpj,
            'id': arquivo['fileControlId'],
            'nome': arquivo['fileName'][:-3],
            'tempo_de_processamento': arquivo['processingTime']
        }
        arquivos_recebidos.append(_)
    return arquivos_recebidos


def get_files_by_links(link) -> None:
    # Recebe a lista de arquivos de um participante
    # e salva todos os registros em um arquivo local único
    processed_file = dao.check_if_processed(link)
    if processed_file:
        return False
    
    try:
        storage_client = storage.Client()
        file_name = link['nome']
        cnpj = link['participante']
        file_name_bucket = file_name.replace(cnpj[0:8], cnpj).split('.')[0]
        blobs = storage_client.list_blobs(bucket_or_name=cfg.bucket_config['name'], prefix=f'out/{file_name_bucket}', delimiter="/")
        
        for blob in blobs:
            blob.download_to_filename(f'./{file_name}')
           

        return file_name
    except Exception as e:
        raise e

# Sistema
from datetime import date, datetime

# Ferramentas
import requests

# Internos
from log_manager import get_logger
import config as cfg

logging = get_logger()


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
        'startDate': data_de_inicio.strftime('%Y-%m-%d')
    }

    header = get_tio_headers()

    try:
        response = requests.put(cfg.http_config['url_registros'], headers=header, json=payload)
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        logging.critical(f'Erro {response.status_code} durante o download.')
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
    # Recebe a lista de arquivos de um participante
    # e salva todos os registros em um arquivo local único
    header = get_tio_headers()
    url_atual = cfg.http_config['ulr_arquivo'].replace('fileControlId', link['id'])
    try:
        response = requests.get(url_atual, headers=header)
        response.raise_for_status()
        logging.debug(f'Status da resposta: {response.status_code}')
    except requests.exceptions.HTTPError:
        message = f'Erro {response.status_code} durante o download.'
        logging.warning(message)
        raise Exception(message)
    data = response.json()
    url_do_arquivo = data['result']
    arquivo = requests.get(url_do_arquivo, stream=True)
    with open(cfg.app_config['tmp_file'], 'ab') as arquivo_local:
        for chunk in arquivo.iter_content(chunk_size=1024):
            arquivo_local.write(chunk)
    return

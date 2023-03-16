# Sistema
from datetime import date, datetime
import os
import signal

# Ferramentas
import requests

# Internos
from log_manager import logging
import config as cfg
import dao


def get_tio_headers() -> dict:
    # Recebe um secret e retorna um header com autenticação preenchida

    headers = {
        'Authorization': f'Basic {cfg.http_config["api_secret"]}'
    }
    form_data = {'grant_type': 'client_credentials'}
    try:
        response = requests.post(
            'https://cad-prd.cerc.inf.br/oauth/token',
            data=form_data,
            headers=headers
        )
        response.raise_for_status()
        token = response.json()['access_token']
        header = {
            'Authorization': f'Bearer {token}'
        }
    except requests.exceptions.HTTPError as e:
        logging.critical(f'Erro ao criar token de acesso ao portal: {e}')
        os.kill(os.getpid(), signal.SIGINT)
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
        logging.critical(f'Erro {response.status_code} durante a busca dos arquivos do participante {cnpj}.')
        return arquivos_recebidos

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
    
    header = get_tio_headers()
    url_atual = cfg.http_config['ulr_arquivo'].replace('fileControlId', link['id'])
    try:
        response = requests.get(url_atual, headers=header)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.HTTPError:
        logging.error(f'Erro {response.status_code} durante o download do arquivo {link["nome"]}')
        return None
    except requests.exceptions.JSONDecodeError as e:
        logging.error(f'Erro no arquivo {link["nome"]}: {e}')
        return None
    url_do_arquivo = data['result']
    arquivo = requests.get(url_do_arquivo, stream=True)
    with open(link['nome'], 'ab') as arquivo_local:
        for chunk in arquivo.iter_content(chunk_size=1024):
            arquivo_local.write(chunk)
    return link['nome']

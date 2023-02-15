# Sistema
from datetime import datetime

# Ferramentas
import pandas

# Internos
from log_manager import logging
import dao
import config as cfg


def parse_file(participante: str) -> int:
    # Move os dados do arquivo recebido para o banco
    total_de_registros = 0
    with pandas.read_csv(cfg.app_config['tmp_file'], sep=';', chunksize=cfg.db_config['chunk_size'], on_bad_lines='skip', names=['referencia_externa', 'guid', 'horario', 'codigo_erro', 'desc_erro']) as reader:
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
            dao.save_records(registros)
        total_de_registros = total_de_registros + len(registros)
    return total_de_registros

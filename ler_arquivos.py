import csv
import os
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

# Conexão com banco de dados
pandas_postgres = create_engine('postgresql://postgres:admin@localhost:5432/postgres')

nome_campos = (
    'codigo_estacao,data_leitura,hora_leitura,precipitacao,tempmaxima,tempminima,insolacao,evaporacao_piche,'
    'temp_comp_media,umidade_relativa_media,velocidade_do_vento_media').split(',')

# Pasta que contem os arquivos
caminho_pasta = 'C:\Projetos Python\Web_crawler-inmet.gov.br-master\Arquivo' + os.sep

nome_arquivo = os.listdir(caminho_pasta)
# começa a varrer a pasta e trazer o nome de todos os arquivos
for arquivos in nome_arquivo:
    # Monta o caminho absoluto do arquivo
    caminho_absoluto = caminho_pasta + arquivos
    abrir_arquivos = open(caminho_absoluto, 'rt')
    leitura = csv.reader(abrir_arquivos, delimiter=';')
    codigo_estacao = arquivos[9:15]
    print(arquivos, arquivos[9:15])
    completo = []
    inicio = datetime.now()
    for pular_linhas in range(48):
        next(leitura)
    for ler in leitura:
        if '</pre>' in ler:
            pass
        else:
            completo.append(ler[0:11])

    df_chuvas = pd.DataFrame(completo, columns=nome_campos)
    df_chuvas.replace('', value=0.0, inplace=True)

    df_chuvas['codigo_estacao'] = df_chuvas['codigo_estacao'].astype(int)

    df_chuvas['data_leitura'] = pd.to_datetime(df_chuvas['data_leitura'], format='%d/%m/%Y').dt.date

    df_chuvas[['precipitacao',
               'tempmaxima',
               'tempminima',
               'insolacao',
               'evaporacao_piche',
               'temp_comp_media',
               'umidade_relativa_media',
               'velocidade_do_vento_media', ]] = df_chuvas[['precipitacao',
                                                            'tempmaxima',
                                                            'tempminima',
                                                            'insolacao',
                                                            'evaporacao_piche',
                                                            'temp_comp_media',
                                                            'umidade_relativa_media',
                                                            'velocidade_do_vento_media',
                                                            ]].astype(float)

    fim_leitura = datetime.now()
    print('duração para ler:', fim_leitura - inicio, len(df_chuvas), 'linhas')
    df_chuvas.to_sql('chuvas_brasil', if_exists='append', index=False, con=pandas_postgres)
    fim_gravacao = datetime.now()
    print('duração para gravar:', fim_gravacao - fim_leitura, len(df_chuvas), 'linhas')

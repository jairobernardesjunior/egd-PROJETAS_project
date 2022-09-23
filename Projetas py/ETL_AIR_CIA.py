''' Projetas Teste:
    FAZ ETL NOS ARQUIVO DOS DIRETÓRIOS AIR_CIA
'''

import Le_arquivo_junta_df as learq
import Move_arquivo_diretorio as mad
import os
from datetime import datetime

# Carrega os arquivos .csv do diretório AIR_CIA
path= "G:\\Engenharia de Dados\\Projetas_Teste_Técnico\\"
tipo_arq= 'csv'
dir_list= os.listdir(path + 'AIR_CIA\\') 

df_air_cia= learq.Le_junta_arquivos_df(path + 'AIR_CIA\\', tipo_arq, dir_list, ';')

# Normalizando cabeçalho (snake case)
df_air_cia.columns = [x.lower() for x in df_air_cia.columns]
df_air_cia.columns = df_air_cia.columns.str.replace("[ ]", "_", regex=True)
df_air_cia.rename(columns = {'razão_social': 'razao_social',\
                             'data_decisão_operacional': 'data_decisao_operacional',\
                             'decisão_operacional': 'decisao_operacional'}, inplace = True)

df_air_cia[['icao','iata']] = df_air_cia.icao_iata.str.split(expand=True)

# Captura data e hora para compor o nome do arquivo modificado .csv
data_e_hora_atuais = datetime.now()
data_e_hora_em_texto = data_e_hora_atuais.strftime('%Y%m%d%H%M%S')

# GERA ARQUIVO CSV DE CIA E MOVE OS ARQUIVOS CRUS PARA OUTRO DIRETÓRIO
df_air_cia.to_csv(path + 'AIR_CIA_RESULT\\AIR_CIA_' + data_e_hora_em_texto + '.csv')
mad.Move_arquivo_diretorio(path + 'AIR_CIA\\', path + 'AIR_CIA_PROCESSED\\', dir_list)
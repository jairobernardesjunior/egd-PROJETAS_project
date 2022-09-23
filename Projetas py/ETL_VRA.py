''' Projetas Teste:
    FAZ ETL NOS ARQUIVO DOS DIRETÓRIOS VRA
'''

import Le_arquivo_junta_df as learq
import Move_arquivo_diretorio as mad
import os
from datetime import datetime

# Carrega os arquivos .json do diretório VRA
path= "G:\\Engenharia de Dados\\Projetas_Teste_Técnico\\"
tipo_arq= 'json'
dir_list= os.listdir(path + 'VRA\\') 

df_vra= learq.Le_junta_arquivos_df(path + 'VRA\\', tipo_arq, dir_list,'')

# Normalizando cabeçalho (snake case)
df_vra.columns = [x.lower() for x in df_vra.columns]
df_vra.columns = df_vra.columns.str.replace("[ ]", "_", regex=True)

# Captura data e hora para compor o nome do arquivo modificado .csv
data_e_hora_atuais = datetime.now()
data_e_hora_em_texto = data_e_hora_atuais.strftime('%Y%m%d%H%M%S')

# GERA ARQUIVO CSV DE CIA E MOVE OS ARQUIVOS CRUS PARA OUTRO DIRETÓRIO
df_vra.to_csv(path + 'VRA_RESULT\\VRA_'  + data_e_hora_em_texto + '.csv')
mad.Move_arquivo_diretorio(path + 'VRA\\', path + 'VRA_PROCESSED\\', dir_list)
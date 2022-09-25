''' Projetas Teste:
    LE OS AERODROMOS DO VRA E GRAVA EM ARQUIVO .CSV
'''

import Le_arquivo_junta_df as learq
import os
import pandas as pd
import http.client
from pandasql import sqldf
import json
from datetime import datetime

# Le aerodromo com API da airport-info usando o código icao e retorna um dataframe com os dados
def le_aerodromo_airport_info(icao):
    conn = http.client.HTTPSConnection("airport-info.p.rapidapi.com")
    headers = {
        'x-rapidapi-key': "3d8b9714c5msh62470001ec532a8p194090jsn4e13165beb04",
        'x-rapidapi-host': "airport-info.p.rapidapi.com"
        }
    conn.request("GET", "/airport?icao=" + icao, headers=headers)
    res = conn.getresponse()
    data = res.read()
    dataS = data.decode("utf-8") 
    dict= json.loads(dataS)
    return pd.DataFrame.from_dict(dict, orient="index")       

# Carrega os arquivos .csv do diretório VRA_RESULT
def Carrega_aerodromo():
    path= "G:\\Engenharia de Dados\\Projetas_Teste_Técnico\\"
    tipo_arq= 'csv'
    dir_list= os.listdir(path + 'VRA_RESULT\\') 

    df_vra= learq.Le_junta_arquivos_df(path + 'VRA_RESULT\\', tipo_arq, dir_list, ',')

    # ELIMINA A DUPLICIDADE DE ICAO DOS AERÓDROMOS DE ORIGEM E DESTINO
    q="""SELECT DISTINCT icaoaeródromoorigem as icao FROM df_vra;"""
    pysqldf = lambda q: sqldf(q, globals())
    df_icao_og = pysqldf(q)

    q="""SELECT DISTINCT icaoaeródromodestino as icao FROM df_vra;"""
    pysqldf = lambda q: sqldf(q, globals())
    df_icao_dt = pysqldf(q)

    frames= [df_icao_og, df_icao_dt]
    df_icao_od = pd.concat(frames)

    q="""SELECT DISTINCT icao FROM df_icao_od;"""
    pysqldf = lambda q: sqldf(q, globals())
    df_icao_od = pysqldf(q)

    # PESQUISA CADA ICAO DE AERÓDROMO E JUNTA OS DADOS EM DATAFRAME
    df = le_aerodromo_airport_info(df_icao_od.iloc[0, 0])
    df = df.transpose()

    x=1
    while x < len(df_icao_od):
        df2 = le_aerodromo_airport_info(df_icao_od.iloc[x, 0])
        df2 = df2.transpose()

        if df2.iloc[0, 0] != 'No airport found':
            frames= [df, df2]
            df = pd.concat(frames)

        x=x+1    

    # GERA ARQUIVO CSV DE AERÓDROMOS ENCONTRADOS
    if df.empty == False:
        data_e_hora_atuais = datetime.now()
        data_e_hora_em_texto = data_e_hora_atuais.strftime('%Y%m%d%H%M%S')
        df.to_csv(path + 'AERODROMOS\\AERODROMOS_' + data_e_hora_em_texto + '.csv')
''' Projetas Teste:
    APURA ROTA MAIS UTILIZADA POR EMPRESA
'''
# IMPORTA BIBLIOTECAS
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col

# SETA A PATH ONDE OS ARQUIVOS SERÃO ARMAZENADOS
def Apura_rota_mais_utilizada():
    path= "G:\\Engenharia de Dados\\Projetas_Teste_Técnico\\"

    # CRIA SESSÃO SPARK
    spark = SparkSession.builder \
        .master('local')\
        .appName('Read All CSV Files in Directory')\
        .getOrCreate()
    
    # LE TODOS OS ARQUIVOS .CSV DISPONÍVEIS, VRA, CIA, E AEROD, JUNTA OS DADOS EM 3 ARQUIVOS
    # CONFORME CATEGORIA
    df_vra_spk = spark.read.csv(path + 'VRA_RESULT\\*.csv', sep=',', inferSchema=True, header=True)
    df_cia_spk = spark.read.csv(path + 'AIR_CIA_RESULT\\*.csv', sep=',', inferSchema=True, header=True)
    df_aero_spk = spark.read.csv(path + 'AERODROMOS\\*.csv', sep=',', inferSchema=True, header=True)

    # SELECIONA E RENOMEIA OS CAMPOS QUE SERÃO UTILIZADOS
    df_vra_spk = df_vra_spk.select(col('icaoempresaaérea').alias('icao_emp_aerea'),\
                                col('icaoaeródromoorigem').alias('icao_aerod_og'),\
                                col('icaoaeródromodestino').alias('icao_aerod_dt'))

    df_cia_spk = df_cia_spk.select('razao_social', col('icao').alias('icao_empresa'))
    df_cia_spk = df_cia_spk.na.drop()

    df_aero_spk = df_aero_spk.select(col('name').alias('nome_aerod'),\
                                    col('icao').alias('icao_aerod'),\
                                    col('state').alias('estado_aerod'))

    # CONTA A QUANTIDADE DE VOOS E AGRUPA POR EMPRESA, AEROD ORIGEM E AEROD DESTINO
    df_vra_spk = df_vra_spk.groupBy("icao_emp_aerea", "icao_aerod_og", "icao_aerod_dt") \
        .agg(F.count("*").alias("qtde_voo")).sort('icao_emp_aerea')

    # SELECIONA AS ROTAS QUE FORAM MAIS REALIZADAS PARA CADA EMPRESA
    df_vra_spk = df_vra_spk.groupBy("icao_emp_aerea") \
        .agg(F.max("qtde_voo").alias('qtde_voo'),\
            F.max("icao_aerod_og").alias('icao_aerod_og'),\
            F.max("icao_aerod_dt").alias('icao_aerod_dt'))\
        .sort('icao_emp_aerea')

    # SELECIONA OS AERODROMOS DE ORIGEM E DE DESTINO QUE PARTICIPARAM DAS ROTAS
    df_vra_spk.createOrReplaceTempView("vra")
    df_aero_spk.createOrReplaceTempView("aero")

    df_aerod_og = spark.sql("select distinct(a.icao_aerod_og) as icao_aerod_og from vra as a")
    df_aerod_dt = spark.sql("select distinct(a.icao_aerod_dt) as icao_aerod_dt from vra as a")

    spark.catalog.dropTempView("vra")
    spark.catalog.dropTempView("aero")

    # INCLUI NO DATAFRAME COM JOIN, NOME E ESTADO DOS AERODROMOS DE ORIGEM E DESTINO QUE PARTICIPARAM DAS ROTAS
    taerog = df_aerod_og.alias('taerog')
    taerod = df_aerod_dt.alias('taerod')
    taero = df_aero_spk.alias('taero')

    df_aerod_og = taerog.join(taero, taerog.icao_aerod_og == taero.icao_aerod, 'full')
    df_aerod_dt = taerod.join(taero, taerod.icao_aerod_dt == taero.icao_aerod, 'full')   

    df_aerod_og = df_aerod_og.select(col('nome_aerod').alias('nome_aerod_orig'),\
                                    col('icao_aerod').alias('icao_aerod_orig'),\
                                    col('estado_aerod').alias('estado_aerod_orig'))

    df_aerod_dt = df_aerod_dt.select(col('nome_aerod').alias('nome_aerod_dest'),\
                                    col('icao_aerod').alias('icao_aerod_dest'),\
                                    col('estado_aerod').alias('estado_aerod_dest'))                                

    # FAZ JOIN NOS DATAFRAMES COMPONDO DADOS DO VRA, AERODROMO E CIA
    tvra = df_vra_spk.alias('tvra')
    tcia = df_cia_spk.alias('tcia')
    taerog = df_aerod_og.alias('taerog')
    taerod = df_aerod_dt.alias('taerod')
    taero = df_aero_spk.alias('taero')

    df_join = tvra.join(tcia, tvra.icao_emp_aerea == tcia.icao_empresa, 'left')

    tjoin = df_join.alias('tjoin')
    df_join = tjoin.join(taerog, tjoin.icao_aerod_og == taerog.icao_aerod_orig, 'left')

    tjoin = df_join.alias('tjoin')
    df_join = tjoin.join(taerod, tjoin.icao_aerod_dt == taerod.icao_aerod_dest, 'left')

    df_join=df_join.drop('icao_emp_aerea', 'qtde_voo', 'icao_aerod_og', 'icao_aerod_dt', 'icao_empresa')

    df_join.show()

    df_join.createOrReplaceTempView("vra_cia_aerod")
    spark.stop
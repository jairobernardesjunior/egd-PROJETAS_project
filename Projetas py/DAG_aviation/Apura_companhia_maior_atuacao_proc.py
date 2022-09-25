''' Projetas Teste:
    APURA ROTA MAIS UTILIZADA POR EMPRESA
'''
# IMPORTA BIBLIOTECAS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# SETA A PATH ONDE OS ARQUIVOS SERÃO ARMAZENADOS
def Apura_cia_maior_atuacao():
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
                                col('icaoaeródromodestino').alias('icao_aerod_dt'),\
                                col('partidareal').alias('dt_decolagem'))

    df_cia_spk = df_cia_spk.select('razao_social', col('icao').alias('icao_empresa'))
    df_cia_spk = df_cia_spk.na.drop()

    df_aero_spk = df_aero_spk.select(col('name').alias('nome_aerodromo'),\
                                    col('icao').alias('icao_aerod'))

    # CONTA A QUANTIDADE DE DECOLAGEM E AGRUPA POR AERODROMO, CIA E ANO (partida real)
    df_vra_spk.createOrReplaceTempView("vra")

    df_voo_og = spark.sql("select icao_aerod_og, icao_emp_aerea as icao_emp_og, year(dt_decolagem) as ano_og, count(*) as qtde_dec " +\
                        " from vra group by icao_aerod_og, icao_emp_aerea, year(dt_decolagem)")
    df_voo_og = df_voo_og.na.drop()

    # CONTA A QUANTIDADE DE ATERRIZAGEM E AGRUPA POR AERODROMO, CIA E ANO (partida real)
    df_voo_dt = spark.sql("select icao_aerod_dt, icao_emp_aerea as icao_emp_dt, year(dt_decolagem) as ano_dt, count(*) as qtde_aterr " +\
                        " from vra group by icao_aerod_dt, icao_emp_aerea, year(dt_decolagem)")
    df_voo_dt = df_voo_dt.na.drop()

    spark.catalog.dropTempView("vra")

    # JUNTA A QUANTIDADE DE DECOLAGEM E ATERRIZAGEM
    tcia = df_cia_spk.alias('tcia')
    tvoog = df_voo_og.alias('tvoog')
    tvood = df_voo_dt.alias('tvood')
    taero = df_aero_spk.alias('taero')

    df_join = tvoog.join(tvood, (tvoog.icao_aerod_og == tvood.icao_aerod_dt) & \
                                (tvoog.icao_emp_og == tvood.icao_emp_dt) &\
                                (tvoog.ano_og == tvood.ano_dt), "inner")

    tjoin = df_join.alias('tjoin')
    df_join = tjoin.join(taero, tjoin.icao_aerod_og == taero.icao_aerod, how = "inner")

    df_join.createOrReplaceTempView("ji")
    df_cia_spk.createOrReplaceTempView("cia")

    df_atuacao = spark.sql("SELECT j.nome_aerodromo, j.icao_aerod as icao_aerodromo, c.razao_social as cia_aerea, " + \
                        " j.qtde_dec as rota_origem, j.qtde_aterr as rota_destino, " + \
                        " (j.qtde_dec + j.qtde_aterr) as pousos_e_decolagens" + \
                        " FROM cia c INNER JOIN ji j ON c.icao_empresa == j.icao_emp_og")

    spark.catalog.dropTempView("ji")
    spark.catalog.dropTempView("cia")

    df_atuacao.show()

    df_atuacao.createOrReplaceTempView("atuacao_view")
    spark.stop
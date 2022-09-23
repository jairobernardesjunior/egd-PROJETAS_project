''' Projetas Teste:
    Le arquivos gerais e junta tudo em um só dataframe
'''

import pandas as pd

def Le_junta_arquivos_df(path, tipo_arq, dir_list, delimitador):
    tamanho = len(dir_list)

    x=0
    if tipo_arq=='csv':
        if delimitador == ',':
            df = pd.read_csv(path + dir_list[x], sep=',', low_memory=False)
        else:
            df = pd.read_csv(path + dir_list[x], sep=';', low_memory=False)
    else:
        df = pd.read_json(path + dir_list[x], encoding='utf-8-sig')

    x=1
    while x < tamanho:
        if tipo_arq=='csv':
            if delimitador == ',':
                df2 = pd.read_csv(path + dir_list[x], sep=',', low_memory=False)
            else:
                df2 = pd.read_csv(path + dir_list[x], sep=';', low_memory=False)
        else:
            df2 = pd.read_json(path + dir_list[x], encoding='utf-8-sig')

        frames= [df, df2]
        df = pd.concat(frames)
        x=x+1

    return df
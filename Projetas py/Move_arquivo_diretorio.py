''' Projetas Teste:
    Move arquivo processado para outro diretorio
'''

import pandas as pd
import shutil

def Move_arquivo_diretorio(pathO, pathD, dir_list):
    tamanho = len(dir_list)

    x=0
    while x < tamanho:
        shutil.move(pathO + dir_list[x], pathD + dir_list[x])
        x=x+1
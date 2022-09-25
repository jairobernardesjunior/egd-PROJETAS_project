from airflow import DAG
from datetime import datetime

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator 
from airflow.utils.edgemodifier import Label

import ETL_AIR_CIA_proc as cia
import ETL_aerodromo_proc as aero
import ETL_VRA_func as vra
import Apura_companhia_maior_atuacao_proc as apcia
import Apura_rota_mais_utilizada_proc as aprota

def carrega_cia():
    cia.Carrega_cia   

def carrega_aero():
    aero.Carrega_aerodromo  

def carrega_vra():
    carregou_vra = vra.Carrega_vra
    return carregou_vra

def ver_tem_vra(task_instance):
    carregou_vra = task_instance.xcom_pull(task_ids = 'task_vra')

    # Especifica a próxima task a ser realizada
    if carregou_vra == False:
        return 'task_sem_vra'
    else:
        return 'apura_cia'    

def apura_cia():
    apcia.Apura_cia_maior_atuacao

def apura_rota():
    aprota.Apura_rota_mais_utilizada        

with DAG('Projetas_Aviation', start_date = datetime(2022,9,1),
         schedule_interval='0 0 1 * *', catchup = False) as dag:

    # Task CARREGA CIA
    Tcia = PythonOperator(
        task_id = 'task_cia',
        python_callable = carrega_cia
    )

    # Task CARREGA AERODROMO
    Taero = PythonOperator(
        task_id = 'task_aero',
        python_callable = carrega_aero
    )

    # Task CARREGA VRA
    Tvra = PythonOperator(
        task_id = 'task_vra',
        python_callable = carrega_vra
    )

    # Task VERIFICA SE CARREGOU VRA
    Tver_tem_vra = BranchPythonOperator(
        task_id = 'task_ver_tem_vra',
        python_callable = ver_tem_vra
    )

    # Task APURA CIA
    Tapura_cia = PythonOperator(
        task_id = 'task_apura_cia',
        python_callable = apura_cia
    )

    # Task APURA ROTA
    Tapura_rota = PythonOperator(
        task_id = 'task_apura_rota',
        python_callable = apura_rota
    )  

    # Task ENCERRAMENTO SEM GERAR RESULTADO
    Tsem_vra = BashOperator(
        task_id = 'task_sem_vra',
        bash_command = 'echo "NÃO FOI ENCONTRADO NENHUM ARQUIVO DE VRA"'
    )

    # Define a ordem de execução das tasks
    Tcia >> Taero >> Tvra
    Tvra >> Label("Não encontrou arquivos VRA") >> Tsem_vra
    Tvra >> Label("Encontrou arquivos VRA") >> Tapura_cia >> Tapura_rota
import os

from datetime import datetime

from airflow import DAG

from airflow.operators.python import PythonOperator


from processed.stage_porcentaje_egresados_internacional import  main_stage_porcentaje_egresados_internacional
from processed.stage_situacion_laboral_egresados import main_stage_situacion_laboral_egresados
from processed.stage_ramas_conocimiento import main_stage_ramas_conocimiento
from processed.stage_numero_egresados_internacional import main_stage_numero_egresados_internacional
from processed.stage_egresados_niveles import main_stage_egresados_niveles
from processed.stage_egresados_universidad import main_stage_egresados_universidad
# DATA_DIRECTORY = "/tmp/data/raw/"
# FILE = '03003.xlsx'

def create_dag_situacion_laboral_egresados():
    workflow = DAG(
        "dag_descargar_archivo_situacion_laboral_egresados",
        schedule_interval=None,
        start_date=datetime(2023, 5, 20),
        tags=['dw-training']
    )

    with workflow:
                
        cargar_stage_situacion_laboral_egresados = PythonOperator(
            task_id="cargar_archivo_situacion_laboral_egresados",
            python_callable=main_stage_situacion_laboral_egresados,
        
        )

    return workflow


def create_dag_egresados_niveles():
    workflow = DAG(
        "insertar_stage_egresados_niveles",
        schedule_interval=None,
        start_date=datetime(2023, 5, 20),
        tags=['dw-training']
    )

    with workflow:
                
        insertar_stage_egresados_niveles = PythonOperator(
        task_id="insertar_stage_egresados_niveles",
        python_callable=main_stage_egresados_niveles)

    return workflow


def create_dag_stage_numero_egresados_internacional():
    workflow = DAG(
        "dag_stage_numero_egresados_internacional",
        schedule_interval=None,
        start_date=datetime(2023, 5, 20),
        tags=['dw-training']
    )

    with workflow:
                
        insertar_stage_numero_egresados_internacional = PythonOperator(
        task_id="insertar_stage_numero_egresados_internacional",
        python_callable=main_stage_numero_egresados_internacional)

    return workflow


def create_dag_stage_ramas_conocimiento():
    workflow = DAG(
        "dag_insertar_stage_ramas_conocimiento",
        schedule_interval=None,
        start_date=datetime(2023, 5, 20),
        tags=['dw-training']
    )

    with workflow:
                
         insertar_stage_ramas_conocimiento = PythonOperator(
        task_id="insertar_stage_ramas_conocimiento",
        python_callable=main_stage_ramas_conocimiento)
    return workflow


def create_dag_stage_egresados_universidad():
    workflow = DAG(
        "dag_stage_egresados_universidad",
        schedule_interval=None,
        start_date=datetime(2023, 5, 20),
        tags=['dw-training']
    )

    with workflow:
                
        insertar_stage_egresados_universidad = PythonOperator(
        task_id="insertar_stage_egresados_universidad",
        python_callable=main_stage_egresados_universidad)   

    return workflow


def create_dag_stage_porcentaje_egresados_internacional():
    workflow = DAG(
        "dag_stage_porcentaje_egresados_internacional",
        schedule_interval=None,
        start_date=datetime(2023, 5, 20),
        tags=['dw-training']
    )

    with workflow:
                
        insertar_stage_egresados_universidad = PythonOperator(
        task_id="insertar_stage_egresados_universidad",
        python_callable=main_stage_porcentaje_egresados_internacional)   

    return workflow









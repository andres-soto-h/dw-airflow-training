import os

from datetime import datetime

from airflow import DAG

from airflow.operators.python import PythonOperator

from processed.facts import * 

# DATA_DIRECTORY = "/tmp/data/raw/"
# FILE = '03003.xlsx'

def create_dag_fact_international_graduated():
    workflow = DAG(
        "dag_fact_international_graduated",
        schedule_interval=None,
        start_date=datetime(2023, 5, 20),
        tags=['dw-training']
    )

    with workflow:
                
        cargar_fact_international_graduated = PythonOperator(
            task_id="cargar_fact_international_graduated",
            python_callable=fact_international_graduated
        
        )

    return workflow


def create_dag_fact_egresados_rama_enseñanza():
    workflow = DAG(
        "insertar_fact_egresados_rama_enseñanza",
        schedule_interval=None,
        start_date=datetime(2023, 5, 20),
        tags=['dw-training']
    )

    with workflow:
                
        insertar_fact_egresados_rama_enseñanza = PythonOperator(
        task_id="insertar_fact_egresados_rama_enseñanza",
        python_callable=fact_egresados_rama_enseñanza)

    return workflow


def create_dag_fact_egresados_niveles():
    workflow = DAG(
        "dag_stage_fact_egresados_niveles",
        schedule_interval=None,
        start_date=datetime(2023, 5, 20),
        tags=['dw-training']
    )

    with workflow:
                
        insertar_fact_egresados_niveles = PythonOperator(
        task_id="insertar_fact_egresados_niveles",
        python_callable=fact_egresados_niveles)

    return workflow


def create_dag_fact_situacion_laboral_egresados():
    workflow = DAG(
        "dag_fact_situacion_laboral_egresados",
        schedule_interval=None,
        start_date=datetime(2023, 5, 20),
        tags=['dw-training']
    )

    with workflow:
                
         insertar_fact_situacion_laboral_egresados = PythonOperator(
        task_id="insertar_fact_situacion_laboral_egresados",
        python_callable=fact_situacion_laboral_egresados)
    return workflow












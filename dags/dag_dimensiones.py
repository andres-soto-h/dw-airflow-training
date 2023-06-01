import os

from datetime import datetime

from airflow import DAG

from airflow.operators.python import PythonOperator

from processed.dimensiones import *

# DATA_DIRECTORY = "/tmp/data/raw/"
# FILE = '03003.xlsx'

def create_dimension_pais():
    workflow = DAG(
        "dag_dimension_pais",
        schedule_interval=None,
        start_date=datetime(2023, 5, 20),
        tags=['dw-training']
    )

    with workflow:
                
        cargar_dimension_pais = PythonOperator(
            task_id="cargar_dimension_pais",
            python_callable=dimension_pais,
        
        )

    return workflow


def create_dag_dimension_sexo():
    workflow = DAG(
        "insertar_dimension_sexo",
        schedule_interval=None,
        start_date=datetime(2023, 5, 20),
        tags=['dw-training']
    )

    with workflow:
                
        insertar_dimension_sexo = PythonOperator(
        task_id="insertar_stage_egresados_niveles",
        python_callable=dimension_sexo)

    return workflow


def create_dag_dimm_situacion_laboral():
    workflow = DAG(
        "dag_stage_dimm_situacion_laboral",
        schedule_interval=None,
        start_date=datetime(2023, 5, 20),
        tags=['dw-training']
    )

    with workflow:
                
        insertar_dimm_situacion_laboral = PythonOperator(
        task_id="insertar_dimm_situacion_laboral",
        python_callable=dimm_situacion_laboral)

    return workflow


def create_dag_dimm_rango_edad():
    workflow = DAG(
        "dag_dimm_rango_edad",
        schedule_interval=None,
        start_date=datetime(2023, 5, 20),
        tags=['dw-training']
    )

    with workflow:
                
         insertar_dimm_rango_edad = PythonOperator(
        task_id="insertar_dimm_rango_edad",
        python_callable=dimm_rango_edad)
    return workflow


def create_dag_dimm_tipo_universidad():
    workflow = DAG(
        "dag_dimm_tipo_universidad",
        schedule_interval=None,
        start_date=datetime(2023, 5, 20),
        tags=['dw-training']
    )

    with workflow:
                
        insertar_dimm_tipo_universidad = PythonOperator(
        task_id="insertar_dimm_tipo_universidad",
        python_callable=dimm_tipo_universidad)   

    return workflow


def create_dag_dimm_universidades():
    workflow = DAG(
        "dag_dimm_universidades",
        schedule_interval=None,
        start_date=datetime(2023, 5, 20),
        tags=['dw-training']
    )

    with workflow:
                
        insertar_dimm_universidades = PythonOperator(
        task_id="insertar_dimm_universidades",
        python_callable=dimm_universidades)   

    return workflow

def create_dag_dimm_ambito_enseñanza():
    workflow = DAG(
        "dag_dimm_ambito_enseñanza",
        schedule_interval=None,
        start_date=datetime(2023, 5, 20),
        tags=['dw-training']
    )

    with workflow:
                
        insertar_dimm_ambito_enseñanza = PythonOperator(
        task_id="insertar_dimm_ambito_enseñanza",
        python_callable=dimm_ambito_enseñanza)   

    return workflow

def create_dag_dimm_rama_enseñanza():
    workflow = DAG(
        "dag_dimm_rama_enseñanza",
        schedule_interval=None,
        start_date=datetime(2023, 5, 20),
        tags=['dw-training']
    )

    with workflow:
                
        insertar_dimm_rama_enseñanza = PythonOperator(
        task_id="insertar_dimm_rama_enseñanza",
        python_callable=dimm_rama_enseñanza)   

    return workflow









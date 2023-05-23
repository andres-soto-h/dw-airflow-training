import sys
import os
myDir = os.path.dirname(os.path.abspath(__file__))
parentDir = os.path.split(myDir)[0]
sys.path.append(parentDir)

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from processed.stage_porcentaje_egresados_internacional import  main_stage_porcentaje_egresados_internacional
from processed.stage_situacion_laboral_egresados import main_stage_situacion_laboral_egresados
from processed.stage_ramas_conocimiento import main_stage_ramas_conocimiento
from processed.stage_numero_egresados_internacional import main_stage_numero_egresados_internacional
from processed.stage_egresados_niveles import main_stage_egresados_niveles
from processed.stage_egresados_universidad import main_stage_egresados_universidad
from processed.dimensiones import *


#from data.processed.stagings import *
#import data.processed.stagings as stagings

DATA_DIRECTORY = "/tmp/data/raw/"
FILE = '03003.xlsx'


workflow = DAG(
    "dag_stagings_todo",
    schedule_interval="@yearly",
    start_date=datetime(2014, 1, 1),
    tags=['dw-training'],
)

with workflow:

    insertar_stage_porcentaje_egresados_internacional = PythonOperator(
        task_id="insertar_stage_porcentaje_egresados_internacional",
        python_callable=main_stage_porcentaje_egresados_internacional)
    
    insertar_stage_egresados_niveles = PythonOperator(
        task_id="insertar_stage_egresados_niveles",
        python_callable=main_stage_egresados_niveles)
    
    insertar_stage_numero_egresados_internacional = PythonOperator(
        task_id="insertar_stage_numero_egresados_internacional",
        python_callable=main_stage_numero_egresados_internacional)
    
    insertar_stage_ramas_conocimiento = PythonOperator(
        task_id="insertar_stage_ramas_conocimiento",
        python_callable=main_stage_ramas_conocimiento)

    insertar_stage_situacion_laboral_egresados = PythonOperator(
        task_id="insertar_stage_situacion_laboral_egresados",
        python_callable=main_stage_situacion_laboral_egresados)   
    
    insertar_stage_egresados_universidad = PythonOperator(
        task_id="insertar_stage_egresados_universidad",
        python_callable=main_stage_egresados_universidad)   
    
    dim_pais = PythonOperator(
    task_id="insertar_dim_pais",
    python_callable=dimension_pais)  

    dim_sexo = PythonOperator(
    task_id="insertar_dim_sexo",
    python_callable=dimension_sexo)   

    
    dim_situacion_laboral = PythonOperator(
    task_id="insertar_dim_situacion_laboral",
    python_callable=dimm_situacion_laboral)   

    dim_rango_edad = PythonOperator(
    task_id="insertar_dim_rango_edad",
    python_callable=dimm_rango_edad)   

    dim_tipo_universidad = PythonOperator(
    task_id="insertar_dim_tipo_universidad",
    python_callable=dimm_tipo_universidad)  

    dim_universidades = PythonOperator(
    task_id="insertar_dim_universidades",
    python_callable=dimm_universidades)   

    dim_rama_enseñanza = PythonOperator(
    task_id="insertar_dim_rama_enseñanza",
    python_callable=dimm_rama_enseñanza)   

    dim_ambito_enseñanza = PythonOperator(
    task_id="insertardim_ambito_enseñanza",
    python_callable=dimm_ambito_enseñanza)    

 

    # upload_task = PythonOperator(
    #     task_id="cargar_archivo_situacion_laboral_egresados",
    #     python_callable=cargar_archivo_situacion_laboral,
        # op_kwargs=dict(
        #     user=MYSQL_USER,
        #     password=MYSQL_PASSWORD,
        #     host=MYSQL_HOST,
        #     port=MYSQL_PORT,
        #     db=MYSQL_DATABASE,
        #     table_name=TABLE_NAME_TEMPLATE,
        #     csv_file=OUTPUT_FILE_TEMPLATE
        # ),
    #)

    insertar_stage_situacion_laboral_egresados>>dim_situacion_laboral
    insertar_stage_situacion_laboral_egresados>>dim_tipo_universidad

    insertar_stage_egresados_niveles>>dim_sexo
    insertar_stage_egresados_niveles>>dim_rango_edad

    insertar_stage_porcentaje_egresados_internacional>>dim_pais
    insertar_stage_egresados_universidad>>dim_universidades
    
    insertar_stage_ramas_conocimiento>>dim_rama_enseñanza
    insertar_stage_ramas_conocimiento>>dim_ambito_enseñanza
    

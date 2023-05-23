import sys
import os
myDir = os.path.dirname(os.path.abspath(__file__))
parentDir = os.path.split(myDir)[0]
sys.path.append(parentDir)

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from processed.stage_porcentaje_egresados_internacional import  main_stage_porcentaje_egresados_internacional


#from data.processed.stagings import *
#import data.processed.stagings as stagings

DATA_DIRECTORY = "/tmp/data/raw/"
FILE = '03003.xlsx'


workflow = DAG(
    "dag_porcentaje_egresados",
    schedule_interval="@yearly",
    start_date=datetime(2014, 1, 1),
    tags=['dw-training'],
)

with workflow:

    staging_tables = PythonOperator(
        task_id="insertar_porcentaje_egresados",
        python_callable=main_stage_porcentaje_egresados_internacional)
    

 

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

    staging_tables 

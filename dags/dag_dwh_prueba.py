import sys
import os
myDir = os.path.dirname(os.path.abspath(__file__))
parentDir = os.path.split(myDir)[0]
sys.path.append(parentDir)

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from dag_stagings import *
from dag_dimensiones import * 
from dag_facts import *



#from data.processed.stagings import *
#import data.processed.stagings as stagings

# DATA_DIRECTORY = "/tmp/data/raw/"
# FILE = '03003.xlsx'


with DAG(
        dag_id="dag_load_dw_tables",
        start_date=datetime(2023, 5, 20),
        schedule_interval="@yearly",
        catchup=False,
        tags=["dw-training"]
) as dag:
    
    dag_situacion_laboral_egresados= create_dag_situacion_laboral_egresados()
    dag_egresados_niveles=create_dag_egresados_niveles()
    dag_stage_ramas_conocimiento= create_dag_stage_ramas_conocimiento()
    dag_stage_numero_egresados_internacional=create_dag_stage_numero_egresados_internacional()
    dag_stage_egresados_universidad=create_dag_stage_egresados_universidad()
    dag_stage_porcentaje_egresados_internacional=create_dag_stage_porcentaje_egresados_internacional()
    dag_dimension_pais=create_dag_dimension_sexo()
    dag_dimension_sexo=create_dag_dimension_sexo()
    dag_dimm_situacion_laboral=create_dag_dimm_situacion_laboral()
    dag_dimm_rango_edad=create_dag_dimm_rango_edad()
    dag_dimm_tipo_universidad=create_dag_dimm_tipo_universidad()
    dag_dimm_universidades=create_dag_dimm_universidades()
    dag_dimm_rama_enseñanza=create_dag_dimm_rama_enseñanza()
    dag_dimm_ambito_enseñanza=create_dag_dimm_ambito_enseñanza()
    dag_create_dag_fact_egresados_niveles=create_dag_fact_egresados_niveles()
    dag_create_dag_fact_egresados_rama_enseñanza=create_dag_fact_egresados_rama_enseñanza()
    dag_create_dag_fact_international_graduated=create_dag_fact_international_graduated()
    dag_create_dag_fact_situacion_laboral_egresados=create_dag_fact_situacion_laboral_egresados()

        
    with TaskGroup("Task_group_1", tooltip="Task group for staging tables load") as Task_group_1:

        trigger_imp_dag_stage_porcentaje_egresados_internacional = TriggerDagRunOperator(
            task_id="trigger_stage_porcentaje_egresados_internacional",
            trigger_dag_id=dag_stage_porcentaje_egresados_internacional.dag_id,
            dag=dag,
        )

        trigger_imp_dag_stage_situacion_laboral_egresados = TriggerDagRunOperator(
            task_id="trigger_stage_situacion_laboral_egresados",
            trigger_dag_id=dag_situacion_laboral_egresados.dag_id,
            dag=dag,
        )

        trigger_imp_dag_stage_ramas_conocimiento = TriggerDagRunOperator(
            task_id="trigger_stage_ramas_conocimiento",
            trigger_dag_id=dag_stage_ramas_conocimiento.dag_id,
            dag=dag,
        )

        trigger_imp_dag_stage_numero_egresados_internacional = TriggerDagRunOperator(
            task_id="trigger_stage_numero_egresados_internacional",
            trigger_dag_id=dag_stage_numero_egresados_internacional.dag_id,
            dag=dag,
        )

        trigger_imp_dag_stage_egresados_universidad = TriggerDagRunOperator(
            task_id="trigger_stage_egresados_universidad",
            trigger_dag_id=dag_stage_egresados_universidad.dag_id,
            dag=dag,
        )

        trigger_imp_stage_egresados_niveles = TriggerDagRunOperator(
            task_id="trigger_stage_egresados_niveles",
            trigger_dag_id=dag_egresados_niveles.dag_id,
            dag=dag,
        )


    with TaskGroup("Task_group_2", tooltip="Task group for dimensions tables load") as Task_group_2:

        trigger_imp_dag_dimension_pais = TriggerDagRunOperator(
            task_id="trigger_dag_dimension_pais",
            trigger_dag_id=dag_dimension_pais.dag_id,
            dag=dag,
        )

        trigger_imp_dag_dimension_sexo = TriggerDagRunOperator(
            task_id="trigger_dimension_sexo",
            trigger_dag_id=dag_dimension_sexo.dag_id,
            dag=dag,
        )

        trigger_imp_dag_dimm_situacion_laboral = TriggerDagRunOperator(
            task_id="trigger_dag_dimm_situacion_laboral",
            trigger_dag_id=dag_dimm_situacion_laboral.dag_id,
            dag=dag,
        )

        trigger_imp_dag_dimm_rango_edad = TriggerDagRunOperator(
            task_id="trigger_dimm_rango_edad",
            trigger_dag_id=dag_dimm_rango_edad.dag_id,
            dag=dag,
        )

        trigger_imp_dag_dimm_tipo_universidad = TriggerDagRunOperator(
            task_id="trigger_dimm_tipo_universidad",
            trigger_dag_id=dag_dimm_tipo_universidad.dag_id,
            dag=dag,
        )

        trigger_imp_stage_egresados_niveles = TriggerDagRunOperator(
            task_id="trigger_stage_egresados_niveles",
            trigger_dag_id=dag_egresados_niveles.dag_id,
            dag=dag,
        )


    with TaskGroup("Task_group_3", tooltip="Task group for facts tables load") as Task_group_3:

            trigger_imp_dag_fact_egresados_niveles = TriggerDagRunOperator(
                task_id="trigger_dag_create_dag_fact_egresados_niveles",
                trigger_dag_id=dag_create_dag_fact_egresados_niveles.dag_id,
                dag=dag,
            )

            trigger_imp_dag_dag_fact_egresados_rama_enseñanza = TriggerDagRunOperator(
                task_id="trigger_dag_create_dag_fact_egresados_rama_enseñanza",
                trigger_dag_id=dag_create_dag_fact_egresados_rama_enseñanza.dag_id,
                dag=dag,
            )

            trigger_imp_dag_dag_fact_international_graduated = TriggerDagRunOperator(
                task_id="trigger_dag_create_dag_fact_international_graduated",
                trigger_dag_id=dag_create_dag_fact_international_graduated.dag_id,
                dag=dag,
            )

            trigger_imp_dag_fact_situacion_laboral_egresados = TriggerDagRunOperator(
                task_id="trigger_dag_create_dag_fact_situacion_laboral_egresados",
                trigger_dag_id=dag_create_dag_fact_situacion_laboral_egresados.dag_id,
                dag=dag,
            )
Task_group_1 >> Task_group_2 >> Task_group_3
   

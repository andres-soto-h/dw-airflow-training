import dags.utils.connect_db as connect_db
#from data.processed.stage_egresados_niveles import *
import dags.processed.stage_egresados_niveles as fn_stage_egresados_niveles
import dags.processed.stage_porcentaje_egresados_internacional as fn_stage_porcentaje_egresados_internacional
import dags.processed.stage_situacion_laboral_egresados as fn_stage_situacion_laboral_egresados
import dags.processed.stage_ramas_conocimiento as fn_stage_ramas_conocimiento
import dags.processed.stage_egresados_universidad as fn_stage_egresados_universidad
import dags.processed.stage_numero_egresados_internacional as fn_numero_egresados_internacional

import os


import pandas as pd
from dags.utils.models import Pais

def main_staging():
    cwd = os.getcwd()
    files_path = cwd + '/data/raw/'
    print(files_path)
    files_path="/tmp/data/raw/"

    '''
    stage_egresados_niveles
    '''
   
    stage_egresados_niveles=pd.read_csv(files_path+'grad_5sc.csv', encoding='latin-1', delimiter=';')
    stage_egresados_niveles=fn_stage_egresados_niveles.transformation(stage_egresados_niveles)
    _, dbConnection,_=connect_db.db_connector()
    name_table="stage_egresados_niveles"

    connect_db.create_table(stage_egresados_niveles,name_table,dbConnection)

    
    '''
    stage_porcentaje_egresados_internacional
    '''
    stage_porcentaje_egresados_internacional_1=pd.read_excel(files_path+'educ_uoe_grad05.xlsx')
    stage_porcentaje_egresados_internacional_2=pd.read_excel(files_path+'educ_uoe_grad05.xlsx',sheet_name='Data2')
    stage_porcentaje_egresados_internacional_1['flag']=1
    stage_porcentaje_egresados_internacional_2['flag']=2

    stage_porcentaje_egresados_internacional_1=fn_stage_porcentaje_egresados_internacional.transformation(stage_porcentaje_egresados_internacional_1)
    stage_porcentaje_egresados_internacional_2=fn_stage_porcentaje_egresados_internacional.transformation(stage_porcentaje_egresados_internacional_2)
    stage_porcentaje_egresados_internacional=fn_stage_porcentaje_egresados_internacional.merge_df(stage_porcentaje_egresados_internacional_1,stage_porcentaje_egresados_internacional_2)
    _, dbConnection,_=connect_db.db_connector()
    name_table='stage_porcentaje_egresados_internacional'
    connect_db.create_table(stage_porcentaje_egresados_internacional,name_table,dbConnection)
    print(stage_porcentaje_egresados_internacional_1)
    print("-----------------------------------------")
    print(stage_porcentaje_egresados_internacional)
    print("finaliza")

    '''
    stage_situacion_laboral_egresados
    '''

    stage_situacion_laboral_egresados=pd.read_excel(files_path+'03003.xlsx')
    stage_situacion_laboral_egresados=fn_stage_situacion_laboral_egresados.transformation(stage_situacion_laboral_egresados)
    stage_situacion_laboral_egresados=fn_stage_situacion_laboral_egresados.pivote(stage_situacion_laboral_egresados)

    _, dbConnection,_=connect_db.db_connector()
    name_table='stage_situacion_laboral_egresados'
    connect_db.create_table(stage_situacion_laboral_egresados,name_table,dbConnection)
    print("se hizo")
    print(stage_porcentaje_egresados_internacional.columns)

    '''
    stage_ramas_conocimiento
    '''

    stage_ramas_conocimiento = pd.read_csv(files_path+'ISCED_2013.csv', encoding='latin-1', delimiter=';')
    stage_ramas_conocimiento = fn_stage_ramas_conocimiento.transformation(stage_ramas_conocimiento)

    _, dbConnection,_=connect_db.db_connector()

    name_table="stage_ramas_conocimiento"
    connect_db.create_table(stage_ramas_conocimiento, name_table, dbConnection)

    '''
    stage_egresados_universidad
    '''
    
    df_1 = pd.read_csv(files_path + 'SEGR1.csv', encoding ='latin-1', delimiter=';')
    df_2 = pd.read_csv(files_path + 'SEGR2.csv', encoding ='latin-1', delimiter=';')

    stage_egresados_universidad = fn_stage_egresados_universidad.merge_df(df_1, df_2)
    stage_egresados_universidad = fn_stage_egresados_universidad.transformation(stage_egresados_universidad)

    _, dbConnection,_=connect_db.db_connector()

    name_table="stage_egresados_universidad"
    connect_db.create_table(stage_egresados_universidad, name_table, dbConnection)

    '''
    stage_numero_egresados_internacional
    '''

    df_1 = pd.read_excel(files_path + 'educ_uoe_grad01.xlsx', sheet_name='Data', skiprows=11, nrows=13)
    df_2 = pd.read_excel(files_path + 'educ_uoe_grad01.xlsx', sheet_name='Data2', skiprows=11, nrows=13)
    df_3 = pd.read_excel(files_path + 'educ_uoe_grad01.xlsx', sheet_name='Data3', skiprows=11, nrows=13)

    dft_1, dft_2, dft_3  = fn_numero_egresados_internacional.transformation(df_1, df_2, df_3)
    
    stage_numero_egresados_internacional = fn_numero_egresados_internacional.merge_df(dft_1, dft_2, dft_3)
    print(stage_numero_egresados_internacional)

    _, dbConnection,_=connect_db.db_connector()

    name_table="stage_numero_egresados_internacional"
    connect_db.create_table(stage_numero_egresados_internacional, name_table, dbConnection)

    print(files_path)

main_staging()

# if __name__ == "__main__":

#     cwd = os.getcwd()
#     files_path = cwd + '/data/raw/'
#     main_staging(files_path)
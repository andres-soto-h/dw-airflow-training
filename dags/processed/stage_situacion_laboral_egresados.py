import pandas as pd
import mysql.connector
from datetime import datetime
import pytz
import os
import utils.connect_db as connect_db
#import connect_db
def execution_date(df) -> pd.DataFrame:
        """
        Adds a new column to a pandas DataFrame with the current execution date.

        Returns
        -------
        pd.DataFrame
            The modified DataFrame with a new column called "fecha_ejecucion" containing the execution date.
        """
        tz=pytz.timezone("America/Bogota")
        fecha_actual = datetime.now(tz)
        fecha_formateada = fecha_actual.strftime("%Y-%m-%d")
        fecha_formateada=int(fecha_formateada.replace("-",""))
        df['fecha_ejecucion'] = fecha_formateada

        return df

def transformation(df):
    df=df.iloc[8:,0:5].reset_index(drop=True)
    df.columns = ["area_estudio","cantidad_total","trabajando","desempleo","inactivo"]

    dfs = []
    start = 0
    for i in range(len(df)):
        if pd.isnull(df.loc[i, 'trabajando']):
            dfs.append(df.iloc[start:i,:])
            start = i + 1
    dfs.append(df.iloc[start:,:])

    df_lista=[]
    # Iterar a través de la lista
    for i in range(len(dfs)):
        # Comprobar la longitud del dataframe
        if len(dfs[i])> 0:
            # Eliminar el dataframe de la lista
            df_lista.append(dfs[i])



    #para el primer df
    column_general=["ambos_sexos","hombres","mujeres"]
    tipo_universidad=["total","publico","privadas"]

    for i in range(len(df_lista)):
        if i<=2:
            df_lista[i]['sexo'] = column_general[0]
        if i>=3 and i<=5:
            df_lista[i]['sexo'] = column_general[1]

        if i>=6 and i<=8:
            df_lista[i]['sexo'] = column_general[2]

        tipo = tipo_universidad[i % len(tipo_universidad)]
        df_lista[i]['tipo_universidad'] = tipo
        df_lista[i]['anio'] = 2014
        df_lista[i]['pais'] = 'spain'



    #unir los dfs

    df_concatenado = pd.concat(df_lista)

    #df = df_concatenado[(df_concatenado['tipo_universidad'] != 'total') & (df_concatenado['area_estudio'].str.strip() != 'Total')]
    df=df_concatenado[(df_concatenado['tipo_universidad']!='total') & (df_concatenado['sexo']!='ambos_sexos')]
    df=df[df['area_estudio'].str.strip()!='Total']

    df=execution_date(df)

    return df

def pivote(df_concatenado):
    # Realizamos el pivoteo
    df = pd.melt(df_concatenado, id_vars=['area_estudio', 'cantidad_total', 'sexo', 'tipo_universidad', 'anio', 'pais'],
                value_vars=['trabajando', 'desempleo', 'inactivo'],
                var_name='situacion_laboral', value_name='cantidad')

    # Ordenamos el DataFrame según nuestras especificaciones
    df = df[['anio', 'pais', 'tipo_universidad', 'area_estudio', 'sexo', 'situacion_laboral', 'cantidad']]
    df=execution_date(df)

    return df


def main_stage_situacion_laboral_egresados():
    cwd = os.getcwd()
    files_path = cwd + '/data/raw/'
    print(files_path)
    files_path="/tmp/data/raw/"

    '''
    stage_situacion_laboral_egresados
    '''
    stage_situacion_laboral_egresados=pd.read_excel(files_path+'03003.xlsx')
    stage_situacion_laboral_egresados=transformation(stage_situacion_laboral_egresados)
    stage_situacion_laboral_egresados=pivote(stage_situacion_laboral_egresados)

    _, dbConnection,_=connect_db.db_connector()
    name_table='stage_situacion_laboral_egresados'
    connect_db.create_table(stage_situacion_laboral_egresados,name_table,dbConnection)
    print("se hizo")

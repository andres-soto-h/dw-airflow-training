
import pandas as pd
import mysql.connector
import dags.utils.connect_db as connect_db
from datetime import datetime
import pytz
import os
#1

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
    flag=df['flag'][0]
    #escoger la data que necesito
    df=df.iloc[9:,:6].reset_index(drop=True)

    #coger la fila que quiero como columna
    new_header=df.iloc[0]#.apply(pd.to_numeric, errors='coerce')
    #eliminar esa fila
    df = df[1:]
    #poner esa fila como header
    df.columns=new_header

    names_columns=['GEO_TIME','2013','2014','2015','2016', '2017']
    df.columns=names_columns

    df=df[df['GEO_TIME']!=":"]
    df=df[df['GEO_TIME']!="Special value:"]
    # eliminar registros con valores NaN en la columna "x"
    df = df.dropna(subset=['GEO_TIME'])
    # reemplazar ":" por 0 en todo el DataFrame
    df = df.replace(':', 0, regex=True)
    if flag==1:
        value_name='percentage_graduated'
    else:
        value_name='percentage_youth_graduated'
    #derretir el DataFrame en una estructura más larga
    df2 = pd.melt(df, id_vars=['GEO_TIME'], value_vars=['2013', '2014', '2015', '2016', '2017'], 
                 var_name='year', value_name=value_name)

    # # # pivotear el DataFrame para obtener la estructura deseada
    # df2 = pd.pivot_table(df2, values=[value_name], index=['year', 'GEO_TIME'])

    df3 = df2.reset_index()
    df3.rename(columns={'GEO_TIME':'country'},inplace=True)
    return df3


def merge_df(df,df2):
    df_final=df.merge(df2,on=["year","country"])
    df_final=execution_date(df_final)
    df_final_1=df_final.drop(['index_x','index_y'],axis=1)
    return df_final_1


def main_stage_porcentaje_egresados_internacional():
    cwd = os.getcwd()
    files_path = cwd + '/data/raw/'
    print(files_path)
    files_path="/tmp/data/raw/"
    '''
    stage_porcentaje_egresados_internacional
    '''
    stage_porcentaje_egresados_internacional_1=pd.read_excel(files_path+'educ_uoe_grad05.xlsx')
    stage_porcentaje_egresados_internacional_2=pd.read_excel(files_path+'educ_uoe_grad05.xlsx',sheet_name='Data2')
    stage_porcentaje_egresados_internacional_1['flag']=1
    stage_porcentaje_egresados_internacional_2['flag']=2

    stage_porcentaje_egresados_internacional_1=transformation(stage_porcentaje_egresados_internacional_1)
    stage_porcentaje_egresados_internacional_2=transformation(stage_porcentaje_egresados_internacional_2)
    stage_porcentaje_egresados_internacional=merge_df(stage_porcentaje_egresados_internacional_1,stage_porcentaje_egresados_internacional_2)
    _, dbConnection,_=connect_db.db_connector()
    name_table='stage_porcentaje_egresados_internacional'
    connect_db.create_table(stage_porcentaje_egresados_internacional,name_table,dbConnection)
    print(stage_porcentaje_egresados_internacional_1)

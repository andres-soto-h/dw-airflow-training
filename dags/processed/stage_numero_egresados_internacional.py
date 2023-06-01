import pandas as pd
import utils.connect_db as connect_db
from datetime import datetime
import pytz
import os

default_df = pd.DataFrame()

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

def transformation(df_1, df_2, df_3):

    df_1.drop('Unnamed: 6', inplace=True, axis=1)

    #Consolidar información de todos los años en una sola columna
    df1_unpivot = pd.melt(df_1, id_vars=['GEO/TIME'], value_vars=['2013','2014','2015','2016','2017'], var_name='year', value_name='num_graduated')

    #Consolidar información de todos los años en una sola columna
    df2_unpivot = pd.melt(df_2, id_vars=['GEO/TIME'], value_vars=['2013','2014','2015','2016','2017'], var_name='year', value_name='num_graduated_M')

    #Consolidar información de todos los años en una sola columna
    df3_unpivot = pd.melt(df_3, id_vars=['GEO/TIME'], value_vars=['2013','2014','2015','2016','2017'], var_name='year', value_name='num_graduated_F')

    df1_unpivot.rename(columns={'GEO/TIME': 'country'}, inplace=True)
    df2_unpivot.rename(columns={'GEO/TIME': 'country'}, inplace=True)
    df3_unpivot.rename(columns={'GEO/TIME': 'country'}, inplace=True)

    df1_unpivot.replace(':', 0, inplace=True)
    df2_unpivot.replace(':', 0, inplace=True)
    df3_unpivot.replace(':', 0, inplace=True)

    return df1_unpivot, df2_unpivot, df3_unpivot


def merge_df(df_1=default_df, df_2=default_df, df_3=default_df):

    merge_df = df_1.merge(df_2, on=['country','year'] , how='left')
    merge_df = merge_df.merge(df_3, on=['country','year'], how='left')
    
    #Cambiar el orden de las columnas
    merge_df = merge_df[['year','country','num_graduated_M','num_graduated_F','num_graduated']]

    return merge_df

def main_stage_numero_egresados_internacional():
    cwd = os.getcwd()
    files_path = cwd + '/data/raw/'
    print(files_path)
    files_path="/tmp/data/raw/"

    '''
    stage_numero_egresados_internacional
    '''

    df_1 = pd.read_excel(files_path + 'educ_uoe_grad01.xlsx', sheet_name='Data', skiprows=11, nrows=13)
    df_2 = pd.read_excel(files_path + 'educ_uoe_grad01.xlsx', sheet_name='Data2', skiprows=11, nrows=13)
    df_3 = pd.read_excel(files_path + 'educ_uoe_grad01.xlsx', sheet_name='Data3', skiprows=11, nrows=13)

    dft_1, dft_2, dft_3  = transformation(df_1, df_2, df_3)
    
    stage_numero_egresados_internacional = merge_df(dft_1, dft_2, dft_3)
    print(stage_numero_egresados_internacional)

    _, dbConnection,_=connect_db.db_connector()

    name_table="stage_numero_egresados_internacional"
    connect_db.create_table(stage_numero_egresados_internacional, name_table, dbConnection)

    print(files_path)

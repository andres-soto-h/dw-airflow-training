import pandas as pd
import dags.utils.connect_db as connect_db
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

def transformation(df_union):

    data_cols = df_union.columns

    id_columns = [col for col in data_cols if not 'EGR_' in col]
    data_columns = [col for col in data_cols if 'EGR_' in col]

    #Consolidar información de todos los años en una sola columna
    df_unpivot = pd.melt(df_union, id_vars=id_columns, value_vars=data_columns, var_name='año', value_name='num_egresados')
    print(df_unpivot.head(10))
    #Actualizar columna de año usando los últimos dos digitos del encabezado
    df_unpivot['año'] = '20' + df_unpivot['año'].str[-2:]

    df_unpivot['pais'] = 'Spain'

    df_unpivot.rename(columns={'UNIVERSIDAD':'nombre_universidad', 'TIPO_UNIVERSIDAD': 'tipo_universidad', 'MODALIDAD': 'modalidad', 'RAMA_ENSEÑANZA':'rama_enseñanza'}, inplace=True)
    df_unpivot = df_unpivot[['año','pais','nombre_universidad','tipo_universidad','modalidad','rama_enseñanza','num_egresados']]

    df = df_unpivot.reset_index()

    return df


def merge_df(df_1=default_df, df_2=default_df):

    df_union = pd.concat([df_1, df_2])

    df_final=execution_date(df_union)
   

    return df_final

def main_stage_egresados_universidad():
    cwd = os.getcwd()
    files_path = cwd + '/data/raw/'
    print(files_path)
    files_path="/tmp/data/raw/"
    '''
    stage_egresados_universidad
    '''
    
    df_1 = pd.read_csv(files_path + 'SEGR1.csv', encoding ='latin-1', delimiter=';')
    df_2 = pd.read_csv(files_path + 'SEGR2.csv', encoding ='latin-1', delimiter=';')

    stage_egresados_universidad = merge_df(df_1, df_2)
    stage_egresados_universidad = transformation(stage_egresados_universidad)

    _, dbConnection,_=connect_db.db_connector()

    name_table="stage_egresados_universidad"
    connect_db.create_table(stage_egresados_universidad, name_table, dbConnection)

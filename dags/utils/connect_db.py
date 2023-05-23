import mysql.connector
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()
def db_connector():

    """CONEXION A LA BASE DE DATOS
    Returns:
        sqlEngine (sqlalchemy.engine.base.Engine) : Sirve para utilizarlo como conexion y asi, guardar informacion a la base de datos
        dbConnection (sqlalchemy.engine.base.Connection) :Sirve para utilizarlo como conexion y asi, leer informacion de la base datos
    """
    try:
        #cadena_conexion = "mysql+pymysql://root:1234@localhost:3306/pragma"
        cadena_conexion = "mysql+pymysql://admin:admin@localhost:3306/dw"
        cadena_conexion = "postgresql+psycopg2://airflow:airflow@postgres/airflow"
        cadena_conexion = os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN")
        auth_plugin='mysql_native_password'
        sqlEngine = create_engine(cadena_conexion)
        dbConnection = sqlEngine.connect()
        Session = sessionmaker(bind=sqlEngine)
        session = Session()
        print(dbConnection)
    except BaseException as e :
        print(e)
        sqlEngine=None
        dbConnection=None
        session=None
        print("No se conectò")
    return sqlEngine, dbConnection,session

def create_table(df,name_table,dbConnection):
    df.to_sql(name=name_table, con=dbConnection, if_exists='replace',index=False)
    dbConnection.close()


# Hacemos una consulta de ejemplo a la base de datos

def get_table_db_staging(session,query):
    resultados = session.execute(query)
    filas = []
    column_names = resultados.keys()  # Obtener los nombres de las columnas
    # Imprimimos los resultados
    for resultado in resultados:
        filas.append(resultado)
    df = pd.DataFrame(filas, columns=column_names)  # Pasar los nombres de las columnas al DataFrame
    # Cerramos la sesión
    session.close()
    return df


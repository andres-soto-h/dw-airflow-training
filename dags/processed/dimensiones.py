import utils.connect_db as connect_db
#from data.utils.connect_db import connect_db
import pandas as pd
import numpy as np
from utils.models import *
#from utils.models import *
from sqlalchemy import text
import logging

log = logging.getLogger(__name__)



def dimension_pais():
    sqlEngine, dbConnection, session = connect_db.db_connector()
    # read dimension_pais
    query = session.query(Pais.nombre_pais)
    df_dim_pais_db = pd.read_sql_query(query.statement, sqlEngine)
    df_dim_pais_db = df_dim_pais_db.rename(
        columns={'nombre_pais': 'nombre_pais_db'})

    # read stage_porcentaje_egresados_internacional
    query = text('SELECT distinct country as country FROM stage_porcentaje_egresados_internacional')
    stage_porcentaje_egresados_internacional = connect_db.get_table_db_staging(
        session, query)
    log.info("Contenido del DataFrame:")
    log.info(stage_porcentaje_egresados_internacional)
    log.info(stage_porcentaje_egresados_internacional.columns)
    print("----------------------------------- 1")

    df_dim_pais = stage_porcentaje_egresados_internacional[['country']].drop_duplicates().reset_index(drop=True)
    df_dim_pais = df_dim_pais.rename(columns={'country': 'nombre_pais'})
    print("----------------------------------- 2")

    nuevos_paises = pd.merge(df_dim_pais, df_dim_pais_db,
                             left_on='nombre_pais', right_on='nombre_pais_db', how='left')
    nuevos_paises = nuevos_paises[nuevos_paises['nombre_pais_db'].isnull()].drop([
        'nombre_pais_db'], axis=1)
    print(len(nuevos_paises))
    # pensarlo mejor
    ec_model = []
    try:
        for index, row in nuevos_paises.iterrows():
            print(row)
            pais = (Pais(
                nombre_pais=row['nombre_pais']))
            ec_model.append(pais)
            print("pasa por aca")

        # Insertar los objetos Project en la base de datos utilizando bulk_save_objects()
        session.bulk_save_objects(ec_model)
        # Confirmar la transacción
        session.commit()
        print('se guarda los datos')
    except Exception as e:
        print(e)
        session.rollback()

    finally:
        session.close()


def dimension_sexo():
    sqlEngine, dbConnection, session = connect_db.db_connector()
    # read dimension_pais
    query = session.query(Sexo.desc_sexo)
    df_dim_sexo_db = pd.read_sql_query(query.statement, sqlEngine)
    df_dim_sexo_db = df_dim_sexo_db.rename(
        columns={'desc_sexo': 'desc_sexo_dim'})

    # read stage_porcentaje_egresados_internacional
    query = text('SELECT "SEXO" as desc_sexo_staging FROM stage_egresados_niveles')
    stage_porcentaje_egresados_internacional = connect_db.get_table_db_staging(
        session, query)

    df_dim_sexo = stage_porcentaje_egresados_internacional[[
        'desc_sexo_staging']].drop_duplicates().reset_index(drop=True)

    nuevos_sexos = pd.merge(df_dim_sexo, df_dim_sexo_db,
                            left_on='desc_sexo_staging', right_on='desc_sexo_dim', how='left')
    nuevos_sexos = nuevos_sexos[nuevos_sexos['desc_sexo_dim'].isnull()].drop([
        'desc_sexo_dim'], axis=1)

    ec_model = []
    try:
        for index, row in nuevos_sexos.iterrows():

            sexo = Sexo(
                desc_sexo=row['desc_sexo_staging'])
            ec_model.append(sexo)
        print("pasa por aca")

        # Insertar los objetos Project en la base de datos utilizando bulk_save_objects()
        session.bulk_save_objects(ec_model)
        # Confirmar la transacción
        session.commit()
    except Exception as e:
        print(e)
        session.rollback()

    finally:
        session.close()


def dimm_situacion_laboral():
    sqlEngine, dbConnection, session = connect_db.db_connector()
    # read dimension_situacion_laboral
    query = session.query(Situacion_laboral.desc_situacion_laboral)
    df_dim_situacion_laboral = pd.read_sql_query(query.statement, sqlEngine)
    df_dim_situacion_laboral_db = df_dim_situacion_laboral.rename(
        columns={'desc_situacion_laboral': 'desc_situacion_laboral_dim'})

    # read stage_porcentaje_egresados_internacional
    query = text('SELECT situacion_laboral as desc_situacion_laboral_staging FROM stage_situacion_laboral_egresados')
    stage_situacion_laboral_egresados = connect_db.get_table_db_staging(
        session, query)

    stage_situacion_laboral_egresados = stage_situacion_laboral_egresados[[
        'desc_situacion_laboral_staging']].drop_duplicates().reset_index(drop=True)

    nuevos_situacion_laboral = pd.merge(stage_situacion_laboral_egresados, df_dim_situacion_laboral_db,
                                        left_on='desc_situacion_laboral_staging', right_on='desc_situacion_laboral_dim', how='left')
    nuevos_situacion_laboral = nuevos_situacion_laboral[nuevos_situacion_laboral['desc_situacion_laboral_dim'].isnull()].drop([
        'desc_situacion_laboral_dim'], axis=1)
    print(nuevos_situacion_laboral)
    ec_model = []
    try:
        for index, row in nuevos_situacion_laboral.iterrows():

            situacion_laboral = Situacion_laboral(
                desc_situacion_laboral=row['desc_situacion_laboral_staging'])
            ec_model.append(situacion_laboral)
        print("pasa por aca")

        # Insertar los objetos Project en la base de datos utilizando bulk_save_objects()
        session.bulk_save_objects(ec_model)
        # Confirmar la transacción
        session.commit()
    except Exception as e:
        print(e)
        session.rollback()

    finally:
        session.close()


def dimm_rango_edad():
    sqlEngine, dbConnection, session = connect_db.db_connector()
    # read dimension_rango_edad
    query = session.query(Rango_edad.desc_rango_edad)
    df_dim_rango_edad = pd.read_sql_query(query.statement, sqlEngine)
    df_dim_rango_edad_db = df_dim_rango_edad.rename(
        columns={'desc_rango_edad': 'desc_rango_edad_dim'})

    # read stage_porcentaje_egresados_internacional
    query = text('SELECT distinct "EDAD" as edad_staging FROM stage_egresados_niveles')
    stage_rango_edad = connect_db.get_table_db_staging(session, query)

    stage_rango_edad = stage_rango_edad[[
        'edad_staging']].drop_duplicates().reset_index(drop=True)

    nuevas_edades = pd.merge(stage_rango_edad, df_dim_rango_edad_db,
                             left_on='edad_staging', right_on='desc_rango_edad_dim', how='left')
    nuevas_edades = nuevas_edades[nuevas_edades['desc_rango_edad_dim'].isnull()].drop(
        ['desc_rango_edad_dim'], axis=1)
    print(nuevas_edades)
    ec_model = []
    try:
        for index, row in nuevas_edades.iterrows():

            situacion_laboral = Rango_edad(
                desc_rango_edad=row['edad_staging'])
            ec_model.append(situacion_laboral)
        print("pasa por aca")

        # Insertar los objetos Project en la base de datos utilizando bulk_save_objects()
        session.bulk_save_objects(ec_model)
        # Confirmar la transacción
        session.commit()
    except Exception as e:
        print(e)
        session.rollback()

    finally:
        session.close()


def dimm_tipo_universidad():
    sqlEngine, dbConnection, session = connect_db.db_connector()

    query = session.query(Tipo_universidad.desc_tipo_universidad)

    df_dim_tipo_uni_db = pd.read_sql_query(query.statement, sqlEngine)
    df_dim_tipo_uni_db = df_dim_tipo_uni_db.rename(
        columns={'desc_tipo_universidad': 'desc_tipo_universidad_dim'})

    # read  data from staging table
    query = text('SELECT tipo_universidad as desc_tipo_universidad_staging FROM stage_situacion_laboral_egresados')
    stage_desc_tipo_universidad = connect_db.get_table_db_staging(
        session, query)

    df_dim_tipo_uni = stage_desc_tipo_universidad[[
        'desc_tipo_universidad_staging']].drop_duplicates().reset_index(drop=True)

    nuevos_tipos = pd.merge(df_dim_tipo_uni, df_dim_tipo_uni_db,
                            left_on='desc_tipo_universidad_staging', right_on='desc_tipo_universidad_dim', how='left')
    nuevos_tipos = nuevos_tipos[nuevos_tipos['desc_tipo_universidad_dim'].isnull()].drop(
        ['desc_tipo_universidad_dim'], axis=1)

    ec_model = []
    try:
        for index, row in nuevos_tipos.iterrows():

            tipo_uni = Tipo_universidad(
                desc_tipo_universidad=row['desc_tipo_universidad_staging'])

            ec_model.append(tipo_uni)

        # Insertar los objetos en la base de datos utilizando bulk_save_objects()
        session.bulk_save_objects(ec_model)

        # Confirmar la transacción
        session.commit()
    except Exception as e:
        print(e)
        session.rollback()

    finally:
        session.close()


def dimm_universidades():
    sqlEngine, dbConnection, session = connect_db.db_connector()

    query = session.query(Universidades.nombre_universidad)

    df_dim_uni_db = pd.read_sql_query(query.statement, sqlEngine)
    df_dim_uni_db = df_dim_uni_db.rename(
        columns={'nombre_universidad': 'nombre_universidad_dim'})
    
    # read  data from staging table
    query = text('SELECT nombre_universidad as nombre_universidad_staging, tipo_universidad, modalidad  FROM stage_egresados_universidad')
    stage_desc_tipo_universidad = connect_db.get_table_db_staging(
        session, query)
    

    df_dim_tipo_uni = stage_desc_tipo_universidad[[
        'nombre_universidad_staging', 'tipo_universidad', 'modalidad' ]].drop_duplicates().reset_index(drop=True)


    nuevos_uni = pd.merge(df_dim_tipo_uni, df_dim_uni_db,
                            left_on='nombre_universidad_staging', right_on='nombre_universidad_dim', how='left')
    

    nuevos_uni = nuevos_uni[nuevos_uni['nombre_universidad_dim'].isnull()].drop(
        ['nombre_universidad_dim'], axis=1)
    
    
    ec_model = []
    try:

        for index, row in nuevos_uni.iterrows():

            universidad = Universidades(
                nombre_universidad = row['nombre_universidad_staging'],
                tipo_universidad = row['tipo_universidad'],
                modalidad = row['modalidad']
            )
 
            ec_model.append(universidad)

        # Insertar los objetos en la base de datos utilizando bulk_save_objects()
        session.bulk_save_objects(ec_model)

        # Confirmar la transacción
        session.commit()
    except Exception as e:
        print(e)
        session.rollback()

    finally:
        session.close()

def dimm_rama_enseñanza():
    sqlEngine, dbConnection, session = connect_db.db_connector()

    query = session.query(Rama_enseñanza.nombre_rama)

    df_dim_rama_db = pd.read_sql_query(query.statement, sqlEngine)
    df_dim_rama_db = df_dim_rama_db.rename(
        columns={'nombre_rama': 'nombre_rama_dim'})

    # read data from staging table
    query = text('SELECT nombre_rama_1 as nombre_rama_staging FROM stage_ramas_conocimiento')
    stage_nombre_rama = connect_db.get_table_db_staging(
        session, query)

    df_dim_nombre_rama = stage_nombre_rama[[
        'nombre_rama_staging']].drop_duplicates().reset_index(drop=True)

    nuevos_ramas = pd.merge(df_dim_nombre_rama, df_dim_rama_db,
                            left_on='nombre_rama_staging', right_on='nombre_rama_dim', how='left')
    nuevos_ramas = nuevos_ramas[nuevos_ramas['nombre_rama_dim'].isnull()].drop(
        ['nombre_rama_dim'], axis=1)

    ec_model = []
    try:
        for index, row in nuevos_ramas.iterrows():

            rama_con = Rama_enseñanza(
                id = (index + 1) ,  nombre_rama=row['nombre_rama_staging'])

            ec_model.append(rama_con)

        # Insertar los objetos en la base de datos utilizando bulk_save_objects()
        session.bulk_save_objects(ec_model)

        # Confirmar la transacción
        session.commit()
    except Exception as e:
        print(e)
        session.rollback()

    finally:
        session.close()



def dimm_ambito_enseñanza():
    sqlEngine, dbConnection, session = connect_db.db_connector()

    query = session.query(Ambito_enseñanza.desc_ambito)

    df_dim_rama_db = pd.read_sql_query(query.statement, sqlEngine)
    df_dim_rama_db = df_dim_rama_db.rename(
        columns={'desc_ambito': 'desc_ambito_dim'})

    # read data from staging table
    query = text('SELECT codigo_rama_1, nombre_rama_1, codigo_rama_4, nombre_rama_4 as nombre_ambito_staging FROM stage_ramas_conocimiento')
    stage_nombre_rama = connect_db.get_table_db_staging(
        session, query)

    df_dim_nombre_rama = stage_nombre_rama[['codigo_rama_1','nombre_rama_1','codigo_rama_4','nombre_ambito_staging']].drop_duplicates().reset_index(drop=True)

    nuevos_ambitos = pd.merge(df_dim_nombre_rama, df_dim_rama_db,
                            left_on='nombre_ambito_staging', right_on='desc_ambito_dim', how='left')
    
    nuevos_ambitos = nuevos_ambitos[nuevos_ambitos['desc_ambito_dim'].isnull()].drop(
        ['desc_ambito_dim'], axis=1)

    ec_model = []
    try:
        for index, row in nuevos_ambitos.iterrows():

            rama_con = Ambito_enseñanza(
                id = '0' + str(row['codigo_rama_4']) if len (str(row['codigo_rama_4'])) <=3 else str(row['codigo_rama_4']),
                desc_ambito = row['nombre_ambito_staging'],
                id_rama = row['codigo_rama_1'],
                nombre_rama = row['nombre_rama_1']
            )

            ec_model.append(rama_con)

        # Insertar los objetos en la base de datos utilizando bulk_save_objects()
        session.bulk_save_objects(ec_model)

        # Confirmar la transacción
        session.commit()
    except Exception as e:
        print(e)
        session.rollback()

    finally:
        session.close()


def main():
    dimension_pais()
    # dimension_sexo()
    # dimm_situacion_laboral()
    # dimm_rango_edad()
    # dimm_tipo_universidad()
    # dimm_universidades()
    # dimm_rama_enseñanza()
    # dimm_ambito_enseñanza()



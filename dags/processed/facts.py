import utils.connect_db as connect_db
import pandas as pd
from utils.models import Fact_situacion_laboral_egresados,Fact_egresados_niveles,Pais,Fact_international_graduated,Universidades,Rama_enseñanza,Fact_egresados_rama_enseñanza,Rango_edad,Sexo,Tipo_universidad,Ambito_enseñanza,Situacion_laboral
from sqlalchemy import text

def fact_international_graduated():

    sqlEngine, _,session=connect_db.db_connector()
    #read stage_porcentaje_egresados_internacional
    query='SELECT * FROM stage_porcentaje_egresados_internacional'
    stage_porcentaje_egresados_internacional=connect_db.get_table_db_staging(session,query)

    query='SELECT * from  stage_numero_egresados_internacional'
    stage_numero_egresados_internacional=connect_db.get_table_db_staging(session,query)

    query = session.query(Pais)
    df_dim_pais_db= pd.read_sql_query(query.statement, sqlEngine)
    df_dim_pais_db=df_dim_pais_db.rename(columns={'nombre_pais':'country','id':'id_country'})

    column_order=['year',
    'id_country',
    'num_graduated_M',
    'num_graduated_F',
    'num_graduated',
    'percentage_graduated',
    'percentage_youth_graduated',
    'fecha_ejecucion']

    df=pd.merge(stage_numero_egresados_internacional,stage_porcentaje_egresados_internacional,on=['year','country'],how='left')
    fact_international_graduated=pd.merge(df_dim_pais_db,df,on=['country'],how='inner').drop('country',axis=1)
    fact_international_graduated = fact_international_graduated.reindex(columns=column_order)

    ec_model=[]
    try:
        for index,row in fact_international_graduated.iterrows():
            international_graduated=(Fact_international_graduated(
                year=row['year'],
                id_country=row['id_country'],
                num_graduated_male=row['num_graduated_M'],
                num_graduated_female=row['num_graduated_F'],
                num_graduated=row['num_graduated'],
                percentage_graduated=row['percentage_graduated'],
                percentage_youth_graduated=row['percentage_youth_graduated'])
                )
            ec_model.append(international_graduated)

        # Insertar los objetos Project en la base de datos utilizando bulk_save_objects()
        session.bulk_save_objects(ec_model)
        # Confirmar la transacción
        session.commit()
        print('se guarda los datos')
    except Exception as e:
        print(e)
        session.rollback()
        raise  

    finally:
        session.close()


def fact_egresados_rama_enseñanza():
# fact_egresados_rama_enseñanza
    sqlEngine, dbConnection,session=connect_db.db_connector()
    query = session.query(Pais)
    df_dim_pais_db= pd.read_sql_query(query.statement, sqlEngine)
    df_dim_pais_db=df_dim_pais_db.rename(columns={'nombre_pais':'country','id':'id_country'})


    query = session.query(Universidades.id, Universidades.nombre_universidad)
    df_dim_universidad_db= pd.read_sql_query(query.statement, sqlEngine)
    df_dim_universidad_db=df_dim_universidad_db.rename(columns={'id':'id_universidad'})

    query = session.query(Rama_enseñanza.id, Rama_enseñanza.nombre_rama)
    df_dim_rama_enseñanza_db= pd.read_sql_query(query.statement, sqlEngine)
    df_dim_rama_enseñanza_db=df_dim_rama_enseñanza_db.rename(columns={'id':'id_rama_enseñanza'})

    sqlEngine, dbConnection,session=connect_db.db_connector()
    #read stage_porcentaje_egresados_internacional
    query='SELECT * FROM stage_egresados_universidad'
    stage_egresados_universidad=connect_db.get_table_db_staging(session,query)

    df=pd.merge(stage_egresados_universidad,df_dim_pais_db,left_on='pais',right_on='country').drop(['pais','country','index'],axis=1)
    df=pd.merge(df,df_dim_universidad_db, on=['nombre_universidad'],how='left').drop(['nombre_universidad','tipo_universidad','modalidad'],axis=1)
    df=pd.merge(df,df_dim_rama_enseñanza_db, left_on=['rama_enseñanza'],right_on=['nombre_rama'],how='inner').drop(['rama_enseñanza','nombre_rama'],axis=1)

    column_order=['año','id_country','id_universidad','id_rama_enseñanza','num_egresados']
    df = df.reindex(columns=column_order)

    ec_model=[]
    try:
        for index,row in df.iterrows():
            international_graduated=(Fact_egresados_rama_enseñanza(
                año=row['año'],
                id_pais=row['id_country'],
                id_universidad=row['id_universidad'],
                id_rama_enseñanza=row['id_rama_enseñanza'],
                num_egresados=row['num_egresados'])
                )
            ec_model.append(international_graduated)

                # Insertar los objetos Project en la base de datos utilizando bulk_save_objects()
        session.bulk_save_objects(ec_model)
            # Confirmar la transacción
        session.commit()
        print('se guarda los datos')
    except Exception as e:
        print(e)
        session.rollback()
        raise  

    finally:
        session.close()


def fact_egresados_niveles():
    sqlEngine, dbConnection,session=connect_db.db_connector()
    
    data_pais = session.query(Pais)
    df_pais = pd.read_sql_query(data_pais.statement, sqlEngine)
    df_pais.rename(columns={'id': 'id_pais'}, inplace=True)
    
    data_edad = session.query(Rango_edad)
    df_edad = pd.read_sql_query(data_edad.statement, sqlEngine)
    df_edad.rename(columns={'desc_rango_edad': 'edad', 'id': 'id_rango_edad'}, inplace=True)

    data_sexo = session.query(Sexo)
    df_sexo = pd.read_sql_query(data_sexo.statement, sqlEngine)
    df_sexo.rename(columns={'desc_sexo': 'sexo', 'id': 'id_sexo'}, inplace=True)

    sql_query=text('SELECT * FROM stage_egresados_niveles')

    df_stage_egresados_niveles=connect_db.get_table_db_staging(session,sql_query)
    df_stage_egresados_niveles.rename(columns={'COD_AMBITO': 'id_ambito', 'NUM_EGR_NV1': 'num_egresados_nivel_1', 'NUM_EGR_NV2': 'num_egresados_nivel_2', 'EDAD': 'edad', 'SEXO':'sexo'}, inplace=True)
    df_stage_egresados_niveles['num_egresados'] = df_stage_egresados_niveles['num_egresados_nivel_1'] + df_stage_egresados_niveles['num_egresados_nivel_2']
    df_stage_egresados_niveles['nombre_pais'] = 'Spain'
    
    df_stage_egresados_niveles['año'] = df_stage_egresados_niveles['ANIO'].str.split('-').str[1]

    df_merged = pd.merge(df_stage_egresados_niveles, df_edad, on=['edad'], how='left')
    df_merged = pd.merge(df_merged, df_sexo, on=['sexo'], how='left')
    df_merged = pd.merge(df_merged, df_pais, on=['nombre_pais'], how='left')

    df_merged = df_merged[['año', 'id_pais', 'id_ambito', 'id_sexo', 'id_rango_edad', 'num_egresados_nivel_1', 'num_egresados_nivel_2', 'num_egresados']]

    ec_model = []
    try:
        for index, row in df_merged.iterrows():

            datos = (Fact_egresados_niveles(
                 year = row['año'],
                 id_pais = row['id_pais'],
                 id_ambito = row['id_ambito'],
                 id_sexo = row['id_sexo'],
                 id_rango_edad = row['id_rango_edad'],
                 num_egresados_nivel_1 = row['num_egresados_nivel_1'],
                 num_egresados_nivel_2 = row['num_egresados_nivel_2'],
                 num_egresados = row['num_egresados']
            ))

            ec_model.append(datos)

        session.bulk_save_objects(ec_model)
        session.commit()

    except Exception as e:
        print(e)
        session.rollback()
        raise  

    finally:
        session.close()

def fact_situacion_laboral_egresados():
    sqlEngine, dbConnection,session=connect_db.db_connector()

    data_pais = session.query(Pais)
    df_pais = pd.read_sql_query(data_pais.statement, sqlEngine)
    df_pais.rename(columns={'id': 'id_pais', 'nombre_pais': 'pais'}, inplace=True)

    data_universidad = session.query(Tipo_universidad)
    df_univer = pd.read_sql_query(data_universidad.statement, sqlEngine)
    df_univer.rename(columns={'id': 'id_tipo_universidad', 'desc_tipo_universidad':'tipo_universidad'}, inplace=True)

    data_sexo = session.query(Sexo)
    df_sexo = pd.read_sql_query(data_sexo.statement, sqlEngine)
    df_sexo.rename(columns={'desc_sexo': 'sexo', 'id': 'id_sexo'}, inplace=True)
    df_sexo['sexo'] = df_sexo['sexo'].str.lower()

    data_rama = session.query(Ambito_enseñanza)
    df_rama = pd.read_sql_query(data_rama.statement, sqlEngine)
    df_rama['nombre_rama'] = df_rama['nombre_rama'].str.lower()
    df_rama['nombre_rama'] = df_rama['nombre_rama'].str.strip()
    df_rama.rename(columns={'id': 'id_ambito', 'nombre_rama':'area_estudio'}, inplace=True)

    data_situa = session.query(Situacion_laboral)
    df_situa = pd.read_sql_query(data_situa.statement, sqlEngine)
    df_situa.rename(columns={'id': 'id_situacion_laboral', 'desc_situacion_laboral':'situacion_laboral'}, inplace=True)

    sql_query=text('SELECT * FROM stage_situacion_laboral_egresados;')
    df_stage_egresados_niveles=connect_db.get_table_db_staging(session,sql_query)
    
    df_stage_egresados_niveles['area_estudio'] = df_stage_egresados_niveles['area_estudio'].str.lower()
    df_stage_egresados_niveles['area_estudio'] = df_stage_egresados_niveles['area_estudio'].str.strip()
    df_stage_egresados_niveles['pais'] = 'Spain'

    df_merged = pd.merge(df_stage_egresados_niveles, df_sexo, on=['sexo'], how='left')
    df_merged = pd.merge(df_merged, df_pais, on=['pais'], how='left')
    df_merged = pd.merge(df_merged, df_univer, on=['tipo_universidad'], how='left')
    df_merged = pd.merge(df_merged, df_rama, on=['area_estudio'], how='left')
    df_merged = pd.merge(df_merged, df_situa, on=['situacion_laboral'], how='left')
    
    df_merged.rename(columns={'id_rama': 'id_area_estudio', 'anio':'year'}, inplace=True)

    df_merged = df_merged[['year', 'id_pais', 'id_tipo_universidad', 'id_area_estudio', 'id_sexo', 'id_situacion_laboral', 'cantidad']]

    df_merged.drop_duplicates(keep='first', inplace=True)

    ec_model = []
    try:
        for index, row in df_merged.iterrows():

            datos = (Fact_situacion_laboral_egresados(
                year = row['year'],
                id_pais = row['id_pais'],
                id_tipo_universidad = row['id_tipo_universidad'],
                id_area_estudio = row['id_area_estudio'],
                id_sexo = row['id_sexo'],
                id_situacion_laboral = row['id_situacion_laboral'],
                cantidad = row['cantidad']
            ))

            ec_model.append(datos)

        session.bulk_save_objects(ec_model)
        session.commit()

    except Exception as e:
        print(e)
        session.rollback()
        raise  

    finally:
        session.close()





-- Staging Tables

DROP TABLE IF EXISTS dw.stage_numero_egresados_internacional;
CREATE TABLE IF NOT EXISTS dw.stage_numero_egresados_internacional (
    year INT,
    country VARCHAR(255),
    num_graduated_M INT,
    num_graduated_F INT,
    num_graduated INT
);

DROP TABLE IF EXISTS dw.stage_porcentaje_egresados_internacional;
CREATE TABLE IF NOT EXISTS dw.stage_porcentaje_egresados_internacional (
    year INT,
    country VARCHAR(255),
    percentage_graduated FLOAT,
    percentage_youth_graduated FLOAT
);

DROP TABLE IF EXISTS dw.stage_situacion_laboral_egresados;
CREATE TABLE IF NOT EXISTS dw.stage_situacion_laboral_egresados (
	año INT,
	pais VARCHAR(255),
    tipo_universidad VARCHAR(255),
    area_estudio VARCHAR(255),
    sexo VARCHAR(255),
    situacion_laboral VARCHAR(255),
	cantidad INT
);

DROP TABLE IF EXISTS dw.stage_egresados_niveles;
CREATE TABLE IF NOT EXISTS dw.stage_egresados_niveles (
	año INT,
	pais VARCHAR(255),
	cod_ambito VARCHAR(255),
	ambito VARCHAR(255),
    sexo VARCHAR(255),
    edad VARCHAR(255),
    num_egresados_nivel_1 INT,
    num_egresados_nivel_2 INT
);


DROP TABLE IF EXISTS dw.stage_egresados_universidad;
CREATE TABLE IF NOT EXISTS dw.stage_egresados_universidad (
	año INT,
	pais VARCHAR(255),
    nombre_universidad VARCHAR(255),
    tipo_universidad VARCHAR(255),
    modalidad VARCHAR(255),
    rama_enseñanza VARCHAR(255),
    num_egresados INT
);


DROP TABLE IF EXISTS dw.stage_ramas_conocimiento;
CREATE TABLE IF NOT EXISTS dw.stage_ramas_conocimiento (
	codigo_rama_1 VARCHAR(255),
    nombre_rama_1 VARCHAR(255),
    codigo_rama_2 VARCHAR(255),
    nombre_rama_2 VARCHAR(255),
    codigo_rama_3 VARCHAR(255),
    nombre_rama_3 VARCHAR(255),
    codigo_rama_4 VARCHAR(255),
    nombre_rama_4 VARCHAR(255),
    codigo_rama_5 VARCHAR(255),
    nombre_rama_5 VARCHAR(255)
);

-- Dimmensional Tables

DROP TABLE IF EXISTS dw.dimm_pais;   
CREATE TABLE IF NOT EXISTS dw.dimm_pais (
	id INTEGER AUTO_INCREMENT PRIMARY KEY,
	nombre_pais VARCHAR(255) NOT NULL
);

DROP TABLE IF EXISTS dw.dimm_sexo;  
CREATE TABLE IF NOT EXISTS dw.dimm_sexo (
	id INTEGER AUTO_INCREMENT PRIMARY KEY,
	desc_sexo VARCHAR(255) NOT NULL
);

DROP TABLE IF EXISTS dw.dimm_tipo_universidad;  
CREATE TABLE IF NOT EXISTS dw.dimm_tipo_universidad (
	id INTEGER AUTO_INCREMENT PRIMARY KEY,
	desc_tipo_universidad VARCHAR(255)
);

DROP TABLE IF EXISTS dw.dimm_universidades;
CREATE TABLE IF NOT EXISTS dw.dimm_universidades (
	id INTEGER AUTO_INCREMENT PRIMARY KEY,
	nombre_universidad VARCHAR(255),
	tipo_universidad VARCHAR(255),
	modalidad VARCHAR(255)
);

DROP TABLE IF EXISTS dw.dimm_rama_enseñanza;
CREATE TABLE IF NOT EXISTS dw.dimm_rama_enseñanza (
	id INTEGER PRIMARY KEY,
	nombre_rama VARCHAR(255)
);

DROP TABLE IF EXISTS dw.dimm_ambito_enseñanza;
CREATE TABLE IF NOT EXISTS dw.dimm_ambito_enseñanza (
	id VARCHAR(255),
    desc_ambito VARCHAR(255),
    id_rama VARCHAR(255),
	nombre_rama VARCHAR(255)
);

DROP TABLE IF EXISTS dw.dimm_situacion_laboral;
CREATE TABLE IF NOT EXISTS dw.dimm_situacion_laboral (
	id INTEGER AUTO_INCREMENT PRIMARY KEY,
	desc_situacion_laboral VARCHAR(255)
);

DROP TABLE IF EXISTS ;
CREATE TABLE IF NOT EXISTS dimm_rango_edad (
	id INTEGER AUTO_INCREMENT PRIMARY KEY,
	desc_rango_edad VARCHAR(255)
);

-- Fact Tables

DROP TABLE IF EXISTS dw.fact_international_graduated;
CREATE TABLE IF NOT EXISTS dw.fact_international_graduated (
	year INT NOT NULL,
	id_country INT,
	num_graduated_male INT,
	num_graduated_female INT,
	num_graduated INT,
	percentage_graduated FLOAT,
	percentage_youth_graduated FLOAT
);


DROP TABLE IF EXISTS dw.fact_egresados_rama_enseñanza;
CREATE TABLE IF NOT EXISTS dw.fact_egresados_rama_enseñanza (
	año INT NOT NULL,
	id_pais INT,
    id_universidad INT,
    id_rama_enseñanza INT,
    num_egresados INT
);

DROP TABLE IF EXISTS dw.fact_egresados_niveles;
CREATE TABLE IF NOT EXISTS dw.fact_egresados_niveles (
	year INT NOT NULL,
	id_pais INT,
    id_ambito VARCHAR(255),
    id_sexo INT,
    id_rango_edad INT,
    num_egresados_nivel_1 INT,
    num_egresados_nivel_2 INT,
    num_egresados INT
);

DROP TABLE IF EXISTS dw.fact_situacion_laboral_egresados;
CREATE TABLE IF NOT EXISTS dw.fact_situacion_laboral_egresados (
	year INT NOT NULL,
	id_pais INT,
    id_tipo_universidad INT,
    id_area_estudio VARCHAR(255),
    id_sexo INT,
    id_situacion_laboral INT,
	cantidad INT
);
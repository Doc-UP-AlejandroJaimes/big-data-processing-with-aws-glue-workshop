-- Validacion en bronze de los campos a realizar transformaciones

SELECT
    matricula,
    fecha_matricula,
    fecha_renovacion,
    fecha_actualizacion,
    estado_matricula,
    clase_identificacion,
    numero_identificacion,
    razon_social
FROM "db_rues"."bronze_rues_data_bronze_parquet"
WHERE matricula = '21590'
AND estado_matricula = 'ACTIVA'
ORDER BY fecha_actualizacion DESC
LIMIT 1;

-- #	matricula	fecha_matricula	fecha_renovacion	fecha_actualizacion	estado_matricula	clase_identificacion	numero_identificacion	razon_social
-- 1	21590	20061108	20250506	2025/05/13 14:51:55.923000000	ACTIVA	NIT	900118485	OPTICA CRISTAL MAGANGUE LTDA.



-- Validacion en silver, de uno de esos campos
-- matricula:  22924
SUPERSERVICIOS LIMITADA EN LIQUIDACION


SELECT
    matricula,
    fecha_matricula,
    fecha_renovacion,
    fecha_actualizacion,
    antiguedad_empresa,
    clase_identificacion,
    codigo_clase_identificacion,
    numero_identificacion,
    tipo_persona,
    razon_social,
    id_unico
FROM "db_rues"."silver_rues_data_silver_parquet"
WHERE matricula = '21590'
AND estado_matricula = 'ACTIVA'
ORDER BY fecha_actualizacion DESC
LIMIT 1;


-- #	matricula	fecha_matricula	fecha_renovacion	fecha_actualizacion	antiguedad_empresa	clase_identificacion	codigo_clase_identificacion	numero_identificacion	tipo_persona	razon_social	id_unico
-- 1	21590	2006-11-08	2025-05-06	2025-05-13	19	NIT	02	900118485	2	OPTICA CRISTAL MAGANGUE LTDA.	19_21590_OPTICA CRISTAL MAGANGUE LTDA.


--- Validacion en gold del modelo final
SELECT
    d.matricula,
    f.fecha_matricula,
    f.fecha_renovacion,
    f.fecha_actualizacion,
    d.antiguedad_empresa,
    d.clase_identificacion,
    d.codigo_identificacion AS codigo_clase_identificacion,
    d.numero_identificacion,
    d.tipo_persona,
    d.razon_social,
    CONCAT(d.codigo_camara, '_', d.matricula, '_', d.razon_social) AS id_unico
FROM "db_rues"."gold_dim_empresa" d
LEFT JOIN "db_rues"."gold_fact_renovacion" f
    ON d.matricula = f.matricula
WHERE d.matricula = '21590'
AND f.estado_matricula = 'ACTIVA'
AND d.numero_identificacion = '900118485'
ORDER BY f.fecha_actualizacion DESC;

-- #	matricula	fecha_matricula	fecha_renovacion	fecha_actualizacion	antiguedad_empresa	clase_identificacion	codigo_clase_identificacion	numero_identificacion	tipo_persona	razon_social	id_unico
-- 1	21590	2006-11-08	2025-05-06	2025-05-13	19	NIT	11	900118485	2	OPTICA CRISTAL MAGANGUE LTDA.	19_21590_OPTICA CRISTAL MAGANGUE LTDA.
-- 2	21590	2006-11-08	2025-05-06	2025-05-13	19	NIT	11	900118485	2	OPTICA CRISTAL MAGANGUE LTDA.	19_21590_OPTICA CRISTAL MAGANGUE LTDA.
-- 3	21590	2010-10-14	2025-03-31	2025-05-09	19	NIT	11	900118485	2	OPTICA CRISTAL MAGANGUE LTDA.	19_21590_OPTICA CRISTAL MAGANGUE LTDA.
-- 4	21590	2010-10-14	2025-03-31	2025-05-09	19	NIT	11	900118485	2	OPTICA CRISTAL MAGANGUE LTDA.	19_21590_OPTICA CRISTAL MAGANGUE LTDA.
-- 5	21590	2019-03-08	2025-02-20	2025-02-20	19	NIT	11	900118485	2	OPTICA CRISTAL MAGANGUE LTDA.	19_21590_OPTICA CRISTAL MAGANGUE LTDA.
-- 6	21590	2019-03-08	2025-02-20	2025-02-20	19	NIT	11	900118485	2	OPTICA CRISTAL MAGANGUE LTDA.	19_21590_OPTICA CRISTAL MAGANGUE LTDA.
-- 7	21590	2021-09-22	2021-09-22	2025-01-15	19	NIT	11	900118485	2	OPTICA CRISTAL MAGANGUE LTDA.	19_21590_OPTICA CRISTAL MAGANGUE LTDA.
-- 8	21590	2021-09-22	2021-09-22	2025-01-15	19	NIT	11	900118485	2	OPTICA CRISTAL MAGANGUE LTDA.	19_21590_OPTICA CRISTAL MAGANGUE LTDA.
-- 9	21590	1991-02-27	1991-02-27	2025-01-15	19	NIT	11	900118485	2	OPTICA CRISTAL MAGANGUE LTDA.	19_21590_OPTICA CRISTAL MAGANGUE LTDA.
-- 10	21590	1991-02-27	1991-02-27	2025-01-15	19	NIT	11	900118485	2	OPTICA CRISTAL MAGANGUE LTDA.	19_21590_OPTICA CRISTAL MAGANGUE LTDA.
-- 11	21590	2015-03-10	2024-06-26	2024-06-26	19	NIT	11	900118485	2	OPTICA CRISTAL MAGANGUE LTDA.	19_21590_OPTICA CRISTAL MAGANGUE LTDA.
-- 12	21590	2015-03-10	2024-06-26	2024-06-26	19	NIT	11	900118485	2	OPTICA CRISTAL MAGANGUE LTDA.	19_21590_OPTICA CRISTAL MAGANGUE LTDA.
-- 13	21590	2000-03-31	2024-04-09	2024-04-09	19	NIT	11	900118485	2	OPTICA CRISTAL MAGANGUE LTDA.	19_21590_OPTICA CRISTAL MAGANGUE LTDA.
-- 14	21590	2000-03-31	2024-04-09	2024-04-09	19	NIT	11	900118485	2	OPTICA CRISTAL MAGANGUE LTDA.	19_21590_OPTICA CRISTAL MAGANGUE LTDA.


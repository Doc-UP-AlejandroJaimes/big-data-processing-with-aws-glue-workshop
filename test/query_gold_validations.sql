-- 1. Empresas activas por tipo de sociedad
-- Propósito: identificar la distribución de empresas activas según su tipo de sociedad registrada.

-- 1. Empresas activas por tipo de sociedad
SELECT
    d.tipo_sociedad,
    COUNT(DISTINCT d.matricula) AS total_empresas_activas
FROM "db_rues"."gold_dim_empresa" AS d
JOIN "db_rues"."gold_fact_renovacion" AS f
    ON d.matricula = f.matricula
WHERE UPPER(f.estado_matricula) = 'ACTIVA'
GROUP BY d.tipo_sociedad
ORDER BY total_empresas_activas DESC;


--2. Antigüedad promedio por actividad económica
--Propósito: analizar la antigüedad promedio de las empresas activas agrupadas por su principal actividad económica.

-- 2. Antigüedad promedio por actividad económica
SELECT
    d.actividad_economica,
    ROUND(AVG(d.antiguedad_empresa), 2) AS antiguedad_promedio,
    COUNT(DISTINCT d.matricula) AS total_empresas
FROM "db_rues"."gold_dim_empresa" AS d
JOIN "db_rues"."gold_fact_renovacion" AS f
    ON d.matricula = f.matricula
WHERE UPPER(f.estado_matricula) = 'ACTIVA'
GROUP BY d.actividad_economica
HAVING COUNT(DISTINCT d.matricula) > 5 -- opcional: evita ruido en categorías con pocos datos
ORDER BY antiguedad_promedio DESC
LIMIT 5;


-- 3. Tasa de renovación por cámara de comercio
--Propósito: calcular la tasa de renovación empresarial por cámara de comercio, considerando la proporción de empresas con registro actualizado.
SELECT
    d.camara_comercio,
    COUNT(DISTINCT CASE WHEN UPPER(f.estado_matricula) = 'ACTIVA' THEN d.matricula END) AS empresas_activas,
    COUNT(DISTINCT d.matricula) AS total_empresas,
    ROUND(
        COUNT(DISTINCT CASE WHEN UPPER(f.estado_matricula) = 'ACTIVA' THEN d.matricula END) * 100.0
        / COUNT(DISTINCT d.matricula),
        2
    ) AS tasa_renovacion_pct
FROM "db_rues"."gold_dim_empresa" AS d
JOIN "db_rues"."gold_fact_renovacion" AS f
    ON d.matricula = f.matricula
GROUP BY d.camara_comercio
ORDER BY tasa_renovacion_pct DESC
LIMIT 5;


-- 4. Dataset para Predicción de Renovación Empresarial

-- Propósito: generar el conjunto de datos base para aplicar un modelo de Machine Learning supervisado que prediga la probabilidad de renovación empresarial.
-- Este dataset se genera a partir de los datos ya procesados en la capa Gold y contiene las variables relevantes para modelado predictivo.

-- Columnas del dataset de predicción:

-- codigo_camara: Código de la cámara de comercio.
-- camara_comercio: Nombre de la cámara de comercio.
-- tipo_sociedad: Tipo de sociedad registrada.
-- organizacion_juridica: Tipo de organización jurídica.
-- categoria_matricula: Categoría de matrícula asignada.
-- actividad_economica: Descripción del sector económico (derivada del código CIIU).
-- tipo_persona: Tipo de persona (1 = natural, 2 = jurídica).
-- antiguedad_empresa: Años de existencia de la empresa desde su fecha de matrícula.
-- ultimo_ano_renovado: Último año en que se renovó la matrícula.
-- renovo: Variable binaria objetivo (1 si renovó en el año actual, 0 en caso contrario).

-- Generación del dataset de predicción de renovación empresarial
-- Dataset para Predicción de Renovación Empresarial
-- Propósito: Predecir la probabilidad de renovación de matrícula empresarial

WITH base_join AS (
    SELECT
        d.matricula,
        d.codigo_camara,
        d.camara_comercio,
        d.tipo_sociedad,
        d.organizacion_juridica,
        d.categoria_matricula,
        d.actividad_economica,
        d.tipo_persona,
        f.estado_matricula,
        d.antiguedad_empresa,
        CAST(f.ultimo_ano_renovado AS bigint) AS ultimo_ano_renovado,
        f.fecha_vigencia,
        f.fecha_renovacion,
        f.fecha_actualizacion
    FROM "db_rues"."gold_dim_empresa" AS d
    INNER JOIN "db_rues"."gold_fact_renovacion" AS f
        ON d.matricula = f.matricula
    WHERE
        f.estado_matricula IN ('ACTIVA', 'RENOVADA', 'CANCELADA')
        AND d.antiguedad_empresa IS NOT NULL
        AND f.ultimo_ano_renovado IS NOT NULL
        AND d.tipo_sociedad IS NOT NULL
        AND d.actividad_economica IS NOT NULL
),

deduplicados AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY matricula ORDER BY fecha_actualizacion DESC) AS rn
    FROM base_join
),

datos_limpios AS (
    SELECT
        matricula,
        codigo_camara,
        camara_comercio,
        tipo_sociedad,
        organizacion_juridica,
        categoria_matricula,
        actividad_economica,
        tipo_persona,
        estado_matricula,
        CAST(antiguedad_empresa AS double) AS antiguedad_empresa,
        ultimo_ano_renovado,
        YEAR(fecha_vigencia) AS ano_vigencia,
        YEAR(fecha_renovacion) AS ano_ultima_renovacion,
        YEAR(CURRENT_DATE) AS ano_actual
    FROM deduplicados
    WHERE rn = 1
),

dataset_ml AS (
    SELECT
        codigo_camara,
        camara_comercio,
        tipo_sociedad,
        organizacion_juridica,
        categoria_matricula,
        actividad_economica,
        tipo_persona,
        antiguedad_empresa,
        ultimo_ano_renovado,

        CASE 
            WHEN ultimo_ano_renovado = CAST(YEAR(CURRENT_DATE) AS bigint) THEN 1
            WHEN ultimo_ano_renovado = CAST(YEAR(CURRENT_DATE) - 1 AS bigint) THEN 1
            ELSE 0
        END AS renovo,

        -- Segmento de antigüedad
        CASE 
            WHEN antiguedad_empresa < 2 THEN 'Nueva'
            WHEN antiguedad_empresa BETWEEN 2 AND 5 THEN 'Joven'
            WHEN antiguedad_empresa BETWEEN 6 AND 10 THEN 'Establecida'
            ELSE 'Madura'
        END AS segmento_antiguedad,

        -- Años sin renovación
        CAST(YEAR(CURRENT_DATE) AS bigint) - ultimo_ano_renovado AS anos_sin_renovar

    FROM datos_limpios
)

SELECT *
FROM dataset_ml
WHERE renovo IS NOT NULL
ORDER BY RAND()
LIMIT 500000;

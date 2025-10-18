-- 1. Empresas activas por tipo de sociedad
-- Propósito: identificar la distribución de empresas activas según su tipo de sociedad registrada.

SELECT 
    tipo_sociedad,
    COUNT(*) AS total_empresas_activas
FROM "db_rues"."gold_rues_data_gold_parquet"
WHERE estado_matricula = 'ACTIVA'
GROUP BY tipo_sociedad
ORDER BY total_empresas_activas DESC;

--2. Antigüedad promedio por actividad económica
--Propósito: analizar la antigüedad promedio de las empresas activas agrupadas por su principal actividad económica.

SELECT 
    actividad_economica,
    ROUND(AVG(year(current_date) - year(fecha_matricula)), 1) AS antiguedad_promedio,
    COUNT(*) AS total_empresas
FROM "db_rues"."gold_rues_data_gold_parquet"
WHERE estado_matricula = 'ACTIVA'
GROUP BY actividad_economica
ORDER BY antiguedad_promedio DESC;

-- 3. Tasa de renovación por cámara de comercio
--Propósito: calcular la tasa de renovación empresarial por cámara de comercio, considerando la proporción de empresas con registro actualizado.

SELECT 
    camara_comercio,
    COUNT(CASE WHEN fecha_renovacion IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) AS tasa_renovacion,
    COUNT(*) AS total_empresas
FROM "db_rues"."gold_rues_data_gold_parquet"
GROUP BY camara_comercio
ORDER BY tasa_renovacion DESC;

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

WITH datos_actuales AS (
    SELECT 
        -- Identificadores
        matricula,
        
        -- Variables categóricas
        codigo_camara,
        camara_comercio,
        tipo_sociedad,
        organizacion_juridica,
        categoria_matricula,
        actividad_economica,
        tipo_persona,
        estado_matricula,
        
        -- Variables numéricas
        antiguedad_empresa,
        ultimo_ano_renovado,
        
        -- Fechas para cálculo
        YEAR(fecha_vigencia) AS ano_vigencia,
        YEAR(fecha_renovacion) AS ano_ultima_renovacion,
        YEAR(CURRENT_DATE) AS ano_actual,
        
        -- Calcular si renovó (variable objetivo)
        CASE 
            WHEN ultimo_ano_renovado = YEAR(CURRENT_DATE) THEN 1
            WHEN ultimo_ano_renovado = YEAR(CURRENT_DATE) - 1 THEN 1
            ELSE 0
        END AS renovo
        
    FROM "db_rues"."gold_rues_data_gold_parquet"
    
    WHERE 
        -- Filtrar solo registros activos o con información reciente
        estado_matricula IN ('ACTIVA', 'RENOVADA')
        AND antiguedad_empresa IS NOT NULL
        AND ultimo_ano_renovado IS NOT NULL
        AND tipo_sociedad IS NOT NULL
)

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
    renovo,
    
    -- Variables derivadas adicionales para mejorar el modelo
    CASE 
        WHEN antiguedad_empresa < 2 THEN 'Nueva'
        WHEN antiguedad_empresa BETWEEN 2 AND 5 THEN 'Joven'
        WHEN antiguedad_empresa BETWEEN 6 AND 10 THEN 'Establecida'
        ELSE 'Madura'
    END AS segmento_antiguedad,
    
    ano_actual - ultimo_ano_renovado AS anos_sin_renovar

FROM datos_actuales

-- Opcional: Balancear clases si es necesario
-- WHERE renovo = 1 OR RAND() < 0.3  -- Ajustar según el desbalance

ORDER BY RAND()
LIMIT 500000;  -- Ajustar según necesidad
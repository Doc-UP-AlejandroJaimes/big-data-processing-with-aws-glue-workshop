import sys
import os
import logging
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import re

# ============================================
# CONFIGURACIÓN DE LOGGING
# ============================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================
# INICIALIZACIÓN Y LECTURA DE PARÁMETROS
# ============================================
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'BUCKET',
    'BRONZE_PATH',
    'SILVER_PATH',
    'CATALOGOS_PATH',
    'LOGS_PATH'
])

# Inicializar contexto de Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configurar manejo de fechas antiguas
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")

# Obtener parámetros
BUCKET = args['BUCKET']
BRONZE_PATH = args['BRONZE_PATH']
SILVER_PATH = args['SILVER_PATH']
CATALOGOS_PATH = args['CATALOGOS_PATH']
LOGS_PATH = args['LOGS_PATH']

# Lista global para almacenar errores
errores = []

logger.info("=" * 60)
logger.info("JOB SILVER - APLICACION DE REGLAS DE NEGOCIO")
logger.info("=" * 60)
logger.info(f"Bucket: {BUCKET}")
logger.info(f"Bronze Path: {BRONZE_PATH}")
logger.info(f"Silver Path: {SILVER_PATH}")
logger.info(f"Catalogos Path: {CATALOGOS_PATH}")
logger.info(f"Logs Path: {LOGS_PATH}")
logger.info("=" * 60)

# ============================================
# FUNCIONES DE CARGA
# ============================================

def load_bronze_data(path):
    """Carga los datos procesados desde la capa Bronze en formato Parquet"""
    try:
        logger.info("Cargando datos desde Bronze (Parquet particionado)")
        
        df = spark.read.parquet(path)
        
        if 'year_partition' in df.columns:
            df = df.drop('year_partition')
        
        logger.info(f"Datos cargados: {len(df.columns)} columnas")
        return df
        
    except Exception as e:
        logger.error(f"Error al cargar datos de Bronze: {str(e)}")
        raise e

def load_catalog(path, catalog_name):
    """Carga un catálogo desde CSV"""
    try:
        logger.info(f"Cargando catalogo: {catalog_name}")
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("encoding", "UTF-8") \
            .load(path)
        logger.info(f"Catalogo cargado: {df.count()} registros")
        return df
    except Exception as e:
        logger.error(f"Error al cargar catalogo {catalog_name}: {str(e)}")
        raise e

# ============================================
# FUNCIONES DE REGISTRO DE ERRORES
# ============================================

def registrar_error(columna, mensaje, valor):
    """Registra un error en la lista global de errores"""
    errores.append({
        'columna': columna,
        'mensaje_error': mensaje,
        'valor': str(valor)
    })

# ============================================
# RN-010: ESTANDARIZACIÓN DE NOMBRES DE COLUMNAS
# ============================================

def to_snake_case(col_name):
    """Convierte un nombre de columna a formato snake_case"""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', col_name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    s3 = re.sub(r'[\s\-\.]+', '_', s2)
    return s3.lower()

def aplicar_rn010_estandarizacion_columnas(df):
    """RN-010: Estandarización de nombres de columnas a snake_case"""
    logger.info("Aplicando RN-010: Estandarizacion de nombres de columnas")
    
    columnas_renombradas = 0
    for old_col in df.columns:
        new_col = to_snake_case(old_col)
        if old_col != new_col:
            df = df.withColumnRenamed(old_col, new_col)
            columnas_renombradas += 1
    
    logger.info(f"Columnas renombradas a snake_case: {columnas_renombradas}")
    return df

# ============================================
# RN-001: ESTANDARIZACIÓN DE FECHAS
# ============================================

def aplicar_rn001_estandarizacion_fechas(df):
    """RN-001: Convertir todas las fechas al formato YYYY-MM-DD (ISO 8601)"""
    logger.info("Aplicando RN-001: Estandarizacion de fechas a formato ISO 8601")
    
    fechas_columnas = ['fecha_matricula', 'fecha_renovacion', 'fecha_actualizacion', 
                       'fecha_vigencia', 'fecha_cancelacion']
    
    fechas_convertidas = 0
    for col_fecha in fechas_columnas:
        if col_fecha in df.columns:
            # Intentar convertir en múltiples formatos
            df = df.withColumn(
                col_fecha + '_temp',
                F.coalesce(
                    # Formato 1: yyyyMMdd (números de 8 dígitos)
                    F.when(
                        (F.length(F.trim(F.col(col_fecha))) == 8) & 
                        (F.col(col_fecha).rlike('^[0-9]{8}$')),
                        F.to_date(F.col(col_fecha).cast('string'), 'yyyyMMdd')
                    ),
                    # Formato 2: yyyy/MM/dd HH:mm:ss.SSSSSSSSS (timestamp completo)
                    F.when(
                        F.col(col_fecha).contains('/'),
                        F.to_date(F.to_timestamp(F.col(col_fecha), 'yyyy/MM/dd HH:mm:ss.SSSSSSSSS'))
                    ),
                    # Si no coincide con ningún formato, dejar como NULL
                    F.lit(None).cast('date')
                )
            )
            
            # Reemplazar la columna original
            df = df.drop(col_fecha).withColumnRenamed(col_fecha + '_temp', col_fecha)
            fechas_convertidas += 1
    
    logger.info(f"Columnas de fecha estandarizadas: {fechas_convertidas}")
    return df

# ============================================
# RN-003: NORMALIZACIÓN DE ESTADOS
# ============================================

def aplicar_rn003_normalizacion_estados(df):
    """RN-003: Unificar valores inconsistentes en estado_matricula"""
    logger.info("Aplicando RN-003: Normalizacion de estados")
    
    if 'estado_matricula' in df.columns:
        df = df.withColumn(
            'estado_matricula',
            F.upper(F.trim(F.col('estado_matricula')))
        )
        logger.info("Estado normalizado a mayusculas sin espacios")
    else:
        logger.warning("Columna 'estado_matricula' no encontrada")
    
    return df

# ============================================
# RN-002: LIMPIEZA DE REGISTROS DUPLICADOS
# ============================================

def aplicar_rn002_limpieza_duplicados(df):
    """RN-002: Identificar duplicados basados en matricula y nit, conservando el más reciente"""
    logger.info("Aplicando RN-002: Limpieza de registros duplicados")
    
    window_spec = Window.partitionBy('matricula', 'nit').orderBy(F.col('fecha_actualizacion').desc_nulls_last())
    
    df = df.withColumn('row_num', F.row_number().over(window_spec))
    df = df.filter(F.col('row_num') == 1).drop('row_num')
    
    logger.info("Duplicados eliminados")
    return df

# ============================================
# RN-008: MAPEO DE CLASE DE IDENTIFICACIÓN
# ============================================

def aplicar_rn008_mapeo_identificacion(df, df_cat_identificacion):
    """RN-008: Asignar código numérico según clase_identificacion"""
    logger.info("Aplicando RN-008: Mapeo de clase de identificacion")
    
    if 'clase_identificacion' in df.columns:
        df = df.withColumn(
            'clase_identificacion',
            F.upper(F.trim(F.col('clase_identificacion')))
        )
        
        df_cat_identificacion = df_cat_identificacion.withColumn(
            'clase_identificacion',
            F.upper(F.trim(F.col('clase_identificacion')))
        )
        
        df = df.join(
            df_cat_identificacion.select(
                'clase_identificacion', 
                F.col('codigo').alias('codigo_identificacion')
            ),
            on='clase_identificacion',
            how='left'
        )
        
        logger.info("Codigos de identificacion mapeados")
    else:
        logger.warning("Columna 'clase_identificacion' no encontrada")
    
    return df

# ============================================
# RN-009: DETERMINACIÓN DE TIPO DE PERSONA
# ============================================

def aplicar_rn009_tipo_persona(df):
    """RN-009: Crear campo tipo_persona según el tipo de identificación"""
    logger.info("Aplicando RN-009: Determinacion de tipo de persona")
    
    df = df.withColumn(
        'tipo_persona',
        F.when(F.upper(F.col('clase_identificacion')) == 'NIT', 2).otherwise(1)
    )
    
    logger.info("Tipo de persona creado")
    return df

# ============================================
# RN-005: ENRIQUECIMIENTO DE ACTIVIDAD ECONÓMICA
# ============================================

def aplicar_rn005_enriquecimiento_ciiu(df, df_cat_ciiu):
    """RN-005: Crear columna actividad_economica mapeando cod_ciiu_act_econ_pri con el catálogo CIIU"""
    logger.info("Aplicando RN-005: Enriquecimiento de actividad economica")
    
    if 'cod_ciiu_act_econ_pri' in df.columns:
        df = df.withColumn(
            'cod_ciiu_act_econ_pri',
            F.trim(F.col('cod_ciiu_act_econ_pri').cast('string'))
        )
        
        df_cat_ciiu = df_cat_ciiu.withColumn(
            'codigo',
            F.trim(F.col('codigo').cast('string'))
        )
        
        df = df.join(
            df_cat_ciiu.select(
                F.col('codigo').alias('cod_ciiu_join'), 
                F.col('actividad').alias('actividad_economica')
            ),
            df.cod_ciiu_act_econ_pri == F.col('cod_ciiu_join'),
            how='left'
        ).drop('cod_ciiu_join')
        
        logger.info("Actividades economicas enriquecidas")
    else:
        logger.warning("Columna 'cod_ciiu_act_econ_pri' no encontrada")
    
    return df

# ============================================
# RN-004: CREACIÓN DE VARIABLES DERIVADAS
# ============================================

def aplicar_rn004_variables_derivadas(df):
    """RN-004: Calcular antiguedad_empresa basado en la fecha de matrícula"""
    logger.info("Aplicando RN-004: Creacion de variables derivadas")
    
    if 'fecha_matricula' in df.columns:
        anio_actual = datetime.now().year
        
        df = df.withColumn(
            'antiguedad_empresa',
            F.lit(anio_actual) - F.year(F.col('fecha_matricula'))
        )
        
        logger.info(f"Antiguedad de empresa calculada (año base: {anio_actual})")
    else:
        logger.warning("Columna 'fecha_matricula' no encontrada")
    
    return df

# ============================================
# RN-006: CREACIÓN DE LLAVE ÚNICA
# ============================================

def aplicar_rn006_llave_unica(df):
    """RN-006: Generar campo id_unico para identificación inequívoca"""
    logger.info("Aplicando RN-006: Creacion de llave unica")
    
    columnas_requeridas = ['codigo_camara', 'matricula', 'razon_social']
    columnas_presentes = [col for col in columnas_requeridas if col in df.columns]
    
    if len(columnas_presentes) == len(columnas_requeridas):
        df = df.withColumn(
            'id_unico',
            F.concat_ws('_', 
                        F.col('codigo_camara'), 
                        F.col('matricula'), 
                        F.col('razon_social'))
        )
        
        logger.info("Llave unica (id_unico) creada correctamente")
    else:
        logger.warning(f"No se pudo crear llave unica. Faltan columnas: {set(columnas_requeridas) - set(columnas_presentes)}")
        registrar_error('id_unico', 
                       'No se pudieron encontrar todas las columnas necesarias', 
                       ', '.join(columnas_requeridas))
    
    return df

# ============================================
# VALIDACIONES CONSOLIDADAS (OPTIMIZADO)
# ============================================

def validaciones_consolidadas(df):
    """Realizar todas las validaciones en una sola pasada"""
    logger.info("Ejecutando validaciones consolidadas en una sola pasada")
    
    validaciones = df.select(
        # Fechas nulas
        F.count(F.when(F.col('fecha_matricula').isNull(), 1)).alias('fecha_matricula_null'),
        F.count(F.when(F.col('fecha_renovacion').isNull(), 1)).alias('fecha_renovacion_null'),
        F.count(F.when(F.col('fecha_actualizacion').isNull(), 1)).alias('fecha_actualizacion_null'),
        F.count(F.when(F.col('fecha_vigencia').isNull(), 1)).alias('fecha_vigencia_null'),
        F.count(F.when(F.col('fecha_cancelacion').isNull(), 1)).alias('fecha_cancelacion_null'),
        
        # Código identificación nulo
        F.count(F.when(F.col('codigo_identificacion').isNull(), 1)).alias('codigo_identificacion_null'),
        
        # Actividad económica nula
        F.count(F.when(F.col('actividad_economica').isNull(), 1)).alias('actividad_economica_null'),
        
        # Antigüedad negativa
        F.count(F.when(F.col('antiguedad_empresa') < 0, 1)).alias('antiguedad_negativa'),
        
        # Llave única con componentes nulos
        F.count(F.when(
            F.col('codigo_camara').isNull() | 
            F.col('matricula').isNull() | 
            F.col('razon_social').isNull(), 1
        )).alias('llave_unica_invalida'),
        
        # Campos obligatorios nulos
        F.count(F.when(F.col('matricula').isNull(), 1)).alias('matricula_null'),
        F.count(F.when(F.col('codigo_camara').isNull(), 1)).alias('codigo_camara_null'),
        F.count(F.when(F.col('clase_identificacion').isNull(), 1)).alias('clase_identificacion_null'),
        
        # Conteos de tipos de persona
        F.count(F.when(F.col('tipo_persona') == 2, 1)).alias('personas_juridicas'),
        F.count(F.when(F.col('tipo_persona') == 1, 1)).alias('personas_naturales')
    ).collect()[0].asDict()
    
    # Registrar errores según los resultados
    if validaciones['fecha_matricula_null'] > 0:
        registrar_error('fecha_matricula', 'Formato de fecha invalido', f"{validaciones['fecha_matricula_null']} registros")
    
    if validaciones['fecha_renovacion_null'] > 0:
        registrar_error('fecha_renovacion', 'Formato de fecha invalido', f"{validaciones['fecha_renovacion_null']} registros")
    
    if validaciones['fecha_actualizacion_null'] > 0:
        registrar_error('fecha_actualizacion', 'Formato de fecha invalido', f"{validaciones['fecha_actualizacion_null']} registros")
    
    if validaciones['fecha_vigencia_null'] > 0:
        registrar_error('fecha_vigencia', 'Formato de fecha invalido', f"{validaciones['fecha_vigencia_null']} registros")
    
    if validaciones['fecha_cancelacion_null'] > 0:
        registrar_error('fecha_cancelacion', 'Formato de fecha invalido', f"{validaciones['fecha_cancelacion_null']} registros")
    
    if validaciones['codigo_identificacion_null'] > 0:
        logger.warning(f"Registros sin codigo de identificacion: {validaciones['codigo_identificacion_null']}")
        registrar_error('clase_identificacion', 'Clase no encontrada en catalogo', f"{validaciones['codigo_identificacion_null']} registros")
    
    if validaciones['actividad_economica_null'] > 0:
        logger.warning(f"Registros sin actividad economica: {validaciones['actividad_economica_null']}")
        registrar_error('cod_ciiu_act_econ_pri', 'Codigo CIIU no encontrado', f"{validaciones['actividad_economica_null']} registros")
    
    if validaciones['antiguedad_negativa'] > 0:
        logger.warning(f"Registros con antiguedad negativa: {validaciones['antiguedad_negativa']}")
        registrar_error('fecha_matricula', 'Antiguedad calculada es negativa', f"{validaciones['antiguedad_negativa']} registros")
    
    if validaciones['llave_unica_invalida'] > 0:
        logger.warning(f"Registros sin llave unica valida: {validaciones['llave_unica_invalida']}")
        registrar_error('id_unico', 'Falta componente de llave unica', f"{validaciones['llave_unica_invalida']} registros")
    
    if validaciones['matricula_null'] > 0:
        logger.warning(f"Campo 'matricula' tiene {validaciones['matricula_null']} valores NULL")
        registrar_error('matricula', 'Campo obligatorio NULL', f"{validaciones['matricula_null']} registros")
    
    if validaciones['codigo_camara_null'] > 0:
        logger.warning(f"Campo 'codigo_camara' tiene {validaciones['codigo_camara_null']} valores NULL")
        registrar_error('codigo_camara', 'Campo obligatorio NULL', f"{validaciones['codigo_camara_null']} registros")
    
    if validaciones['clase_identificacion_null'] > 0:
        logger.warning(f"Campo 'clase_identificacion' tiene {validaciones['clase_identificacion_null']} valores NULL")
        registrar_error('clase_identificacion', 'Campo obligatorio NULL', f"{validaciones['clase_identificacion_null']} registros")
    
    logger.info(f"Personas juridicas: {validaciones['personas_juridicas']}")
    logger.info(f"Personas naturales: {validaciones['personas_naturales']}")
    logger.info("Validaciones consolidadas completadas")
    
    return df

# ============================================
# RN-007: EXPORTAR LOG DE ERRORES
# ============================================

def exportar_log_errores(logs_path):
    """RN-007: Exportar errores a CSV"""
    logger.info("Aplicando RN-007: Generando log de errores")
    
    if errores:
        df_errores = spark.createDataFrame(errores)
        
        fecha_actual = datetime.now().strftime('%Y%m%d_%H%M%S')
        ruta_errores = f"{logs_path}errors_{fecha_actual}.csv"
        
        df_errores.coalesce(1).write.mode('overwrite').option('header', 'true').csv(ruta_errores)
        
        logger.info(f"Log de errores exportado: {ruta_errores}")
        logger.info(f"Total de errores registrados: {len(errores)}")
    else:
        logger.info("No se registraron errores durante el procesamiento")

# ============================================
# GUARDAR EN CAPA SILVER
# ============================================

def save_to_silver(df, path):
    """Guardar datos procesados en la capa Silver"""
    logger.info("Guardando datos en capa Silver")
    
    try:
        df_with_partition = df.withColumn(
            'year_partition',
            F.year(F.to_timestamp(F.col('fecha_actualizacion'), 'yyyy/MM/dd HH:mm:ss.SSSSSSSSS'))
        )
        
        df_repartitioned = df_with_partition.repartition(30, 'year_partition')
        
        dynamic_frame = DynamicFrame.fromDF(df_repartitioned, glueContext, "dynamic_frame_silver")
        
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            connection_options={
                "path": path,
                "partitionKeys": ["year_partition"]
            },
            format="parquet",
            format_options={
                "compression": "snappy"
            }
        )
        
        logger.info(f"Datos guardados exitosamente en Silver: {path}")
        logger.info("Datos particionados por year_partition")
        
    except Exception as e:
        logger.error(f"Error al guardar datos en Silver: {str(e)}")
        raise e

# ============================================
# FUNCIÓN PRINCIPAL
# ============================================

def main():
    """Función principal que orquesta el procesamiento"""
    
    # 01. Cargar datos desde Bronze
    bronze_parquet_path = os.path.join(BRONZE_PATH, "RUES_DATA_BRONZE_PARQUET")
    df = load_bronze_data(bronze_parquet_path)
    
    # 02. Cargar catálogos
    cat_identificacion_path = os.path.join(CATALOGOS_PATH, "catalogo_clases_identificaciones.csv")
    df_cat_identificacion = load_catalog(cat_identificacion_path, "Identificaciones")
    
    cat_ciiu_path = os.path.join(CATALOGOS_PATH, "catalogo_codigos_ciuu.csv")
    df_cat_ciiu = load_catalog(cat_ciiu_path, "CIIU")
    
    # 03. Aplicar reglas de negocio en orden
    df = aplicar_rn010_estandarizacion_columnas(df)
    df = aplicar_rn001_estandarizacion_fechas(df)
    df = aplicar_rn003_normalizacion_estados(df)
    df = aplicar_rn002_limpieza_duplicados(df)
    df = aplicar_rn008_mapeo_identificacion(df, df_cat_identificacion)
    df = aplicar_rn009_tipo_persona(df)
    df = aplicar_rn005_enriquecimiento_ciiu(df, df_cat_ciiu)
    df = aplicar_rn004_variables_derivadas(df)
    df = aplicar_rn006_llave_unica(df)
    
    # 04. Validaciones consolidadas (una sola pasada)
    df = validaciones_consolidadas(df)
    
    # 05. Exportar log de errores
    exportar_log_errores(LOGS_PATH)
    
    # 06. Guardar en Silver
    silver_output_path = os.path.join(SILVER_PATH, "RUES_DATA_SILVER_PARQUET")
    save_to_silver(df, silver_output_path)
    
    # 07. Resumen final
    logger.info("=" * 60)
    logger.info("RESUMEN DE PROCESAMIENTO - CAPA SILVER")
    logger.info("=" * 60)
    logger.info(f"Total de columnas: {len(df.columns)}")
    logger.info(f"Total de errores detectados: {len(errores)}")
    logger.info("Estado: DATOS PROCESADOS EN SILVER")
    logger.info("=" * 60)
    
    # 08. Finalizar Job
    job.commit()
    logger.info("Job Silver completado exitosamente")
    logger.info("Los datos estan listos para ser procesados en la capa Gold")

# ============================================
# PUNTO DE ENTRADA
# ============================================

if __name__ == '__main__':
    main()
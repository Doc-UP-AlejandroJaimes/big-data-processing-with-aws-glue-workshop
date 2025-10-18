import sys
import os
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import functions as F

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
    'BRONZE_PATH'
])

# Inicializar contexto de Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Obtener parámetros
BUCKET = args['BUCKET']
BRONZE_PATH = args['BRONZE_PATH']

logger.info("=" * 60)
logger.info("JOB BRONZE - CARGA DE DATOS EMPRESARIALES")
logger.info("=" * 60)
logger.info(f"Bucket: {BUCKET}")
logger.info(f"Raw Data Path: {BRONZE_PATH}")
logger.info("=" * 60)

# ============================================
# LEER DATOS CRUDOS
# ============================================

def load_data(path:str):
    try:
        # Leer datos empresariales desde CSV
        df_empresas = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .option("encoding", "UTF-8") \
            .option("sep", ",") \
            .option("quote", "\"") \
            .option("escape", "\"") \
            .option("multiLine", "true") \
            .option("ignoreLeadingWhiteSpace", "true") \
            .option("ignoreTrailingWhiteSpace", "true") \
            .option("mode", "PERMISSIVE") \
            .option("maxColumns", "50") \
            .load(path)
        
        logger.info("Datos empresariales cargados exitosamente desde CSV")
        return df_empresas
    except Exception as e:
        logger.error(f"Error al leer datos empresariales: {str(e)}")
        raise e

# ============================================
# VALIDACIONES BÁSICAS
# ============================================

def validate_dataframe(df):
    
    logger.info("Iniciando validaciones basicas de calidad de datos (muestra)")
    
    # Tomar una muestra del 1% para todas las validaciones
    df_sample = df.sample(False, 0.01, seed=42)
    registros_sample = df_sample.count()
    
    logger.info(f"Validando con muestra de {registros_sample} registros (1% del total)")

    # Contar registros con valores nulos por columna en la muestra
    logger.info("Conteo de valores NULL por columna (muestra 1%):")
    
    null_counts = df_sample.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c) 
        for c in df_sample.columns
    ]).collect()[0].asDict()
    
    columnas_con_nulos = 0
    for col_name, null_count in null_counts.items():
        if null_count > 0:
            porcentaje = (null_count / registros_sample) * 100
            logger.warning(f"Columna '{col_name}': ~{porcentaje:.2f}% valores NULL (estimado)")
            columnas_con_nulos += 1

    if columnas_con_nulos == 0:
        logger.info("No se detectaron valores NULL significativos")
    else:
        logger.warning(f"Total de columnas con valores NULL: {columnas_con_nulos}")

    # Análisis de duplicados en la misma muestra
    logger.info("Analizando duplicados (muestra 1%)")
    registros_unicos_sample = df_sample.dropDuplicates().count()
    duplicados_sample = registros_sample - registros_unicos_sample

    if duplicados_sample > 0:
        porcentaje_dup = (duplicados_sample / registros_sample) * 100
        logger.warning(f"Duplicados en muestra: {duplicados_sample} (~{porcentaje_dup:.2f}%)")
    else:
        logger.info("No se detectaron duplicados en la muestra")
    
    return columnas_con_nulos, duplicados_sample

# ============================================
# GUARDAR EN FORMATO PARQUET
# ============================================
def save_dataframe(df, path):
    logger.info("Guardando datos en formato Parquet")

    try:
        # Extraer año de fecha_actualizacion para particionar
        df_with_partition = df.withColumn(
            'year_partition',
            F.year(F.to_timestamp(F.col('fecha_actualizacion'), 'yyyy/MM/dd HH:mm:ss.SSSSSSSSS'))
        )
        
        # Reparticionar para mejor paralelismo (con 10 workers, usa 20-40 particiones)
        df_repartitioned = df_with_partition.repartition(30, 'year_partition')
        
        # Convertir a DynamicFrame
        dynamic_frame = DynamicFrame.fromDF(df_repartitioned, glueContext, "dynamic_frame_empresas")
        
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
        
        logger.info("Datos guardados exitosamente en formato Parquet")
        logger.info(f"Ubicacion: {path}")
        logger.info("Datos particionados por year_partition (año de fecha_actualizacion)")
        
    except Exception as e:
        logger.error(f"Error al guardar datos en Parquet: {str(e)}")
        raise e



if __name__ == '__main__':
    # 01. Load File 
    FILE_FULL_PATH = os.path.join(BRONZE_PATH, "rues_empresas_colombia_2025.csv")
    df = load_data(FILE_FULL_PATH)
    # 02. Validate dataframe
    validate_dataframe(df)
    # 03. save in parquet format
    FILE_PATH_SAVE = os.path.join(BRONZE_PATH, "RUES_DATA_BRONZE_PARQUET")
    save_dataframe(df, FILE_PATH_SAVE)
    # 04. End Job
    job.commit()
    logger.info("Job Bronze completado exitosamente")
    logger.info("Los datos estan listos para ser procesados en la capa Silver")
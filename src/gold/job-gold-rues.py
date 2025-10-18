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
    'SILVER_PATH',
    'GOLD_PATH'
])

# Inicializar contexto de Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# CONFIGURAR MANEJO DE FECHAS ANTIGUAS
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Obtener parámetros
BUCKET = args['BUCKET']
SILVER_PATH = args['SILVER_PATH']
GOLD_PATH = args['GOLD_PATH']

logger.info("=" * 60)
logger.info("JOB GOLD - CREACION DEL MODELO DIMENSIONAL")
logger.info("=" * 60)
logger.info(f"Bucket: {BUCKET}")
logger.info(f"Silver Path: {SILVER_PATH}")
logger.info(f"Gold Path: {GOLD_PATH}")
logger.info("=" * 60)

# ============================================
# FUNCIÓN DE CARGA
# ============================================

def load_silver_data(database_name="db_rues", table_name="silver_rues_data_silver_parquet"):
    """Carga los datos procesados desde la capa Silver usando Glue Catalog"""
    try:
        logger.info("Cargando datos desde Silver usando Glue Data Catalog")
        
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=database_name,
            table_name=table_name,
            transformation_ctx="dynamic_frame_silver"
        )
        
        df = dynamic_frame.toDF()
        
        if 'year_partition' in df.columns:
            df = df.drop('year_partition')
        
        logger.info(f"Datos cargados desde catalog: {len(df.columns)} columnas")
        return df
        
    except Exception as e:
        logger.error(f"Error al cargar datos desde catalog: {str(e)}")
        raise e

# ============================================
# CREAR DIMENSIÓN EMPRESA
# ============================================

def crear_dim_empresa(df):
    """Crear la dimensión empresa con atributos descriptivos y estáticos"""
    logger.info("Creando dimension: dim_empresa")
    
    dim_empresa = df.select(
        F.col('matricula').alias('matricula'),
        F.col('numero_identificacion'),
        F.col('nit'),
        F.col('digito_verificacion'),
        F.col('clase_identificacion'),
        F.col('codigo_identificacion'),
        F.col('razon_social'),
        F.col('primer_nombre'),
        F.col('segundo_nombre'),
        F.col('primer_apellido'),
        F.col('segundo_apellido'),
        F.col('sigla'),
        F.col('tipo_sociedad'),
        F.col('codigo_tipo_sociedad'),
        F.col('organizacion_juridica'),
        F.col('codigo_organizacion_juridica'),
        F.col('categoria_matricula'),
        F.col('codigo_categoria_matricula'),
        F.col('cod_ciiu_act_econ_pri'),
        F.col('cod_ciiu_act_econ_sec'),
        F.col('actividad_economica'),
        F.col('camara_comercio'),
        F.col('codigo_camara'),
        F.col('tipo_persona'),
        F.col('antiguedad_empresa'),
        F.col('fecha_actualizacion')
    ).distinct()
    
    registros_dim = dim_empresa.count()
    logger.info(f"Dimension dim_empresa creada: {registros_dim} registros unicos")
    
    return dim_empresa

# ============================================
# CREAR TABLA DE HECHOS RENOVACIÓN
# ============================================

def crear_fact_renovacion(df):
    """Crear la tabla de hechos con eventos de renovación y cambios de estado"""
    logger.info("Creando tabla de hechos: fact_renovacion")
    
    fecha_actual = F.current_date()
    
    fact_renovacion = df.select(
        F.col('matricula'),
        F.col('fecha_matricula'),
        F.col('fecha_renovacion'),
        F.col('fecha_vigencia'),
        F.col('fecha_cancelacion'),
        F.col('fecha_actualizacion'),
        F.col('estado_matricula'),
        F.col('codigo_estado_matricula'),
        F.col('ultimo_ano_renovado'),
        F.datediff(F.col('fecha_vigencia'), fecha_actual).alias('dias_vigencia'),
        F.when(F.col('fecha_vigencia') < fecha_actual, 1).otherwise(0).alias('flag_vencido')
    )
    
    registros_fact = fact_renovacion.count()
    logger.info(f"Tabla de hechos fact_renovacion creada: {registros_fact} registros")
    
    return fact_renovacion

# ============================================
# GUARDAR TABLAS EN GOLD
# ============================================

def save_table_to_gold(df, path, table_name, partition_col=None):
    """Guardar una tabla en la capa Gold"""
    logger.info(f"Guardando tabla: {table_name}")
    
    try:
        if partition_col:
            df_repartitioned = df.repartition(30, partition_col)
            
            dynamic_frame = DynamicFrame.fromDF(df_repartitioned, glueContext, f"dynamic_frame_{table_name}")
            
            glueContext.write_dynamic_frame.from_options(
                frame=dynamic_frame,
                connection_type="s3",
                connection_options={
                    "path": path,
                    "partitionKeys": [partition_col]
                },
                format="parquet",
                format_options={
                    "compression": "snappy"
                }
            )
            logger.info(f"Tabla {table_name} guardada con particion por {partition_col}")
        else:
            df_repartitioned = df.repartition(20)
            
            dynamic_frame = DynamicFrame.fromDF(df_repartitioned, glueContext, f"dynamic_frame_{table_name}")
            
            glueContext.write_dynamic_frame.from_options(
                frame=dynamic_frame,
                connection_type="s3",
                connection_options={
                    "path": path,
                    "partitionKeys": []
                },
                format="parquet",
                format_options={
                    "compression": "snappy"
                }
            )
            logger.info(f"Tabla {table_name} guardada sin particion")
        
        logger.info(f"Ubicacion: {path}")
        
    except Exception as e:
        logger.error(f"Error al guardar tabla {table_name}: {str(e)}")
        raise e

# ============================================
# VALIDAR INTEGRIDAD REFERENCIAL
# ============================================

def validar_integridad_referencial(dim_empresa, fact_renovacion):
    """Validar que todas las matrículas en hechos existan en dimensión"""
    logger.info("Validando integridad referencial entre hechos y dimension")
    
    matriculas_dim = dim_empresa.select('matricula').distinct()
    matriculas_fact = fact_renovacion.select('matricula').distinct()
    
    matriculas_huerfanas = matriculas_fact.join(
        matriculas_dim,
        on='matricula',
        how='left_anti'
    )
    
    count_huerfanas = matriculas_huerfanas.count()
    
    if count_huerfanas > 0:
        logger.warning(f"Se encontraron {count_huerfanas} matriculas en hechos sin dimension correspondiente")
    else:
        logger.info("Integridad referencial validada: todas las matriculas tienen dimension")
    
    return count_huerfanas

# ============================================
# FUNCIÓN PRINCIPAL
# ============================================

def main():
    """Función principal que orquesta la creación del modelo dimensional"""
    
    # 01. Cargar datos desde Silver
    df_silver = load_silver_data(database_name="db_rues", table_name="silver_rues_data_silver_parquet")
    
    # 02. Crear dimensión empresa
    dim_empresa = crear_dim_empresa(df_silver)
    
    # 03. Crear tabla de hechos renovación
    fact_renovacion = crear_fact_renovacion(df_silver)
    
    # 04. Validar integridad referencial
    validar_integridad_referencial(dim_empresa, fact_renovacion)
    
    # 05. Guardar dimensión empresa
    dim_empresa_path = os.path.join(GOLD_PATH, "dim_empresa")
    save_table_to_gold(dim_empresa, dim_empresa_path, "dim_empresa", partition_col=None)
    
    # 06. Guardar tabla de hechos
    fact_renovacion_path = os.path.join(GOLD_PATH, "fact_renovacion")
    save_table_to_gold(fact_renovacion, fact_renovacion_path, "fact_renovacion", partition_col="estado_matricula")
    
    # 07. Resumen final
    logger.info("=" * 60)
    logger.info("RESUMEN MODELO DIMENSIONAL - CAPA GOLD")
    logger.info("=" * 60)
    logger.info("Dimension dim_empresa: Guardada exitosamente")
    logger.info("Tabla hechos fact_renovacion: Guardada exitosamente")
    logger.info("Esquema: Star Schema")
    logger.info("Particionamiento hechos: estado_matricula")
    logger.info("Estado: MODELO DIMENSIONAL CREADO")
    logger.info("=" * 60)
    
    # 08. Finalizar Job
    job.commit()
    logger.info("Job Gold completado exitosamente")
    logger.info("Modelo dimensional listo para consultas analiticas")

# ============================================
# PUNTO DE ENTRADA
# ============================================

if __name__ == '__main__':
    main()
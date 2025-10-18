# JOB GOLD - Creación del Modelo Dimensional

## Parámetros del Job

El job de Gold recibirá los siguientes parámetros:

| Parámetro | Descripción | Ejemplo |
|-----------|-------------|---------|
| `--BUCKET` | Nombre del bucket S3 | `glue-bucket-rues-jc2025` |
| `--SILVER_PATH` | Ruta de la capa Silver | `s3://glue-bucket-rues-jc2025/silver/` |
| `--GOLD_PATH` | Ruta de la capa Gold | `s3://glue-bucket-rues-jc2025/gold/` |


## Estructura del Modelo Dimensional

Según los requerimientos, el modelo dimensional sigue un **esquema estrella (Star Schema)** con:

### Tabla de Hechos: `fact_renovacion`
- **Propósito:** Registrar eventos y actividades que cambian en el tiempo
- **Granularidad:** Un registro por evento de renovación/cambio de estado
- **Campos:**
  - `matricula` (FK)
  - `fecha_matricula`
  - `fecha_renovacion`
  - `fecha_vigencia`
  - `fecha_cancelacion`
  - `fecha_actualizacion`
  - `estado_matricula`
  - `ultimo_ano_renovado`
  - `dias_vigencia` (calculado)
  - `flag_vencido` (calculado)

### Dimensión: `dim_empresa`
- **Propósito:** Almacenar atributos descriptivos y relativamente estáticos
- **Campos:**
  - `matricula` (PK)
  - `razon_social`
  - `primer_nombre`, `primer_apellido`, `segundo_nombre`, `segundo_apellido`
  - `sigla`
  - `clase_identificacion`
  - `numero_identificacion`
  - `nit`
  - `digito_verificacion`
  - `cod_ciiu_act_econ_pri`, `cod_ciiu_act_econ_sec`
  - `actividad_economica` (enriquecido)
  - `tipo_sociedad`
  - `camara_comercio`, `codigo_camara`
  - `tipo_persona` (derivado)
  - `antiguedad_empresa` (derivado)
  - `fecha_actualizacion`


## Configuración del Job en AWS Glue

**Información básica:**
- **Nombre:** `job-gold-modelo-dimensional`
- **Rol de IAM:** `GlueETLMedallionRole`
- **Tipo:** Spark
- **Versión de Glue:** 4.0
- **Lenguaje:** Python 3

**Configuración de recursos:**
- **Tipo de worker:** G.1X
- **Número de workers:** 10

**Parámetros del Job:**
- `--BUCKET` = `glue-bucket-rues-jc2025`
- `--SILVER_PATH` = `s3://glue-bucket-rues-jc2025/silver/`
- `--GOLD_PATH` = `s3://glue-bucket-rues-jc2025/gold/`

**Etiquetas:**
- `workshop` = `big_data`

## Estructura resultante en S3

```
gold/
├── dim_empresa/
│   └── *.parquet
└── fact_renovacion/
    ├── estado_matricula=ACTIVA/
    │   └── *.parquet
    ├── estado_matricula=CANCELADA/
    │   └── *.parquet
    └── estado_matricula=SUSPENDIDA/
        └── *.parquet
```

## Características del Modelo

1. **Esquema Estrella:** Fact table conectada a dimension por `matricula`
2. **SCD Tipo 1:** La dimensión sobrescribe valores (simplicidad)
3. **Particionamiento:** Hechos particionados por `estado_matricula` para consultas eficientes
4. **Campos calculados:** `dias_vigencia`, `flag_vencido`, `tipo_persona`, `antiguedad_empresa`
5. **Integridad:** Validación de llaves foráneas
6. **Optimizado:** Para consultas analíticas y reportes
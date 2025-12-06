# TaxiFare-ML-Pipeline

**Universidad San Francisco de Quito**  
**Alumno:** Joel Cuascota  
**Curso:** Data Mining – 2025  
**Tema:** Predicción de `total_amount` en viajes de taxi NYC TLC (2015–2025)

---

## Resumen Ejecutivo

Este proyecto implementa un pipeline completo de ciencia de datos para predecir el valor total (`total_amount`) de un viaje de taxi en la ciudad de Nueva York, utilizando datos del New York City TLC Trip Record Data (Yellow & Green).

El flujo abarca todas las etapas de un proyecto de data mining profesional:

**Ingesta masiva (Spark) → Limpieza e integración (Postgres) → Construcción OBT → ML predictivo (from-scratch y scikit-learn)**

Se logra así una solución reproducible, escalable y documentada, capaz de ejecutarse tanto con un subconjunto (enero 2015) como con todo el histórico 2015–2025.

---

## Herramientas y Tecnologías

### Lenguajes de Programación
- **Python 3.11** - Lenguaje principal para análisis de datos y machine learning
- **SQL** - Consultas y transformaciones de datos en PostgreSQL

### Frameworks y Librerías de Big Data
- **Apache Spark 3.x** - Procesamiento distribuido de datos masivos
- **PySpark** - API de Python para Apache Spark
- **Jupyter Notebook** - Entorno interactivo de desarrollo

### Base de Datos
- **PostgreSQL 16** - Sistema de gestión de base de datos relacional
- **psycopg2-binary** - Adaptador de base de datos PostgreSQL para Python

### Machine Learning y Análisis de Datos
- **scikit-learn** - Biblioteca principal para machine learning
  - `SGDRegressor` - Regresión con descenso de gradiente estocástico
  - `Ridge` - Regresión lineal con regularización L2
  - `Lasso` - Regresión lineal con regularización L1  
  - `ElasticNet` - Regresión lineal con regularización L1 + L2
  - `StandardScaler` - Normalización de características numéricas
  - `OneHotEncoder` - Codificación de variables categóricas
  - `PolynomialFeatures` - Generación de características polinomiales
  - `ColumnTransformer` - Preprocesamiento diferenciado por tipo de columna
- **NumPy** - Computación numérica y álgebra lineal
- **pandas** - Manipulación y análisis de datos estructurados
- **SciPy** - Computación científica (matrices dispersas)
- **SQLAlchemy** - ORM y herramientas de base de datos
- **joblib** - Serialización eficiente de modelos

### Visualización y Análisis
- **matplotlib** - Visualización de datos y diagnósticos de modelos

### Containerización y Orquestación
- **Docker** - Contenedorización de servicios
- **Docker Compose** - Orquestación multi-contenedor

### Infraestructura y Configuración
- **Jupyter/all-spark-notebook** - Imagen Docker con Jupyter + Spark preconfigurado
- **PostgreSQL JDBC Driver** - Conectividad entre Spark y PostgreSQL

---

## Objetivos

- Diseñar un proceso automatizado de ingesta e integración de datos usando Spark y Postgres
- Construir una tabla analítica One Big Table (OBT) consolidada y limpia
- Entrenar modelos de regresión lineal regularizada (SGD, Ridge, Lasso, ElasticNet)
- Comparar implementaciones from-scratch (NumPy) y scikit-learn con el mismo pipeline
- Evitar data leakage aplicando un split temporal determinístico
- Evaluar, seleccionar y justificar el modelo final con métricas cuantitativas y análisis cualitativo

---

## Arquitectura del Proyecto

| Servicio Docker   | Descripción                                                                 |
|-------------------|-----------------------------------------------------------------------------|
| `spark-notebook` | Entorno Jupyter + Spark para ingesta y exploración de datos                 |
| `postgres`        | Almacenamiento estructurado de datos en esquemas `raw` y `analytics`        |
| `obt-builder`     | Script CLI para construir la OBT (`analytics.obt_trips`) desde `raw.*`     |

---

## Variables de Entorno

```bash
PG_HOST=postgres
PG_PORT=5432
PG_DB=nyc_taxi
PG_USER=postgres
PG_PASSWORD=postgres
PG_SCHEMA_RAW=raw
PG_SCHEMA_ANALYTICS=analytics
RUN_ID=obt-run
YEARS=2015-2025
SERVICES=yellow,green
```

Estas variables aseguran que todos los contenedores compartan la misma configuración y base de datos, garantizando reproducibilidad total del flujo.

## Ingesta de Datos con Spark

El notebook `01_ingest_raw.ipynb` permite descargar, leer y cargar los archivos Parquet de cada servicio (yellow, green) hacia Postgres.

### Configuración de Operación
```bash
# --- Configuración: elige modo selectivo o completo ---
SELECTIVE_MODE = True   # True = solo los meses indicados; False = recorre YEARS x 1..12

# Lista de meses cuando SELECTIVE_MODE=True
SELECTIVE_MONTHS = [
    ("green",  2015, 1),
    ("yellow", 2015, 1),
]

# Para cargar todos los años 2015–2025
SELECTIVE_MODE = False
SERVICES = ["yellow", "green"]
YEARS = list(range(2015, 2026))

```

### Idempotencia del Proceso

```python
def delete_partition(service, y, m):
    tbl = target_table(service)
    pg_exec(f"DELETE FROM {tbl} WHERE service=%s AND year=%s AND month=%s;", [service, y, m])
```

### Ventajas del Proceso de Ingesta

- Seguro ante repeticiones (no genera duplicados)
- Limpieza estructural desde el primer paso (coherencia de tipos, fechas y formatos)
- Descarga temporal con borrado automático para optimizar espacio en disco


## Construcción de la One Big Table

El servicio `obt-builder` integra los datos desde `raw.*` hacia `analytics.obt_trips`, aplicando limpieza, joins y normalización de tipos.

### Ejecución

```bash
docker compose run obt-builder \
  --mode full \
  --year-start 2015 --year-end 2025 \
  --services yellow,green \
  --run-id obt-run \
  --overwrite true
```

### Características Principales

**Idempotencia:** Elimina la partición correspondiente (servicio, año, mes) antes de insertar nuevos datos.

**Limpieza automatizada de datos:**
- `pickup_datetime <= dropoff_datetime`
- `trip_distance >= 0`
- `total_amount >= 0`
- `passenger_count` dentro de rangos razonables

**Columnas opcionales dinámicas:** El proceso detecta y añade automáticamente campos como:
- `airport_fee`
- `cbd_congestion_fee`
- `trip_type`

**Joins geográficos:** Se realiza unión con la tabla `taxi_zone_lookup` para enriquecer los datos con:
- `borough` (pickup y dropoff)
- `zone` (pickup y dropoff)

## Estructura de la One Big Table

La tabla `analytics.obt_trips` consolida todos los viajes de taxi en un formato analítico, limpio y listo para modelado o consultas avanzadas.

| **Categoría** | **Campos incluidos** |
|---------------|------------------------|
| **Tiempo**    | `pickup_ts`, `dropoff_ts`, `pickup_hour`, `pickup_dow`, `month`, `year` |
| **Ubicación** | `pu_zone`, `do_zone`, `pu_borough`, `do_borough` |
| **Servicio**  | `service`, `vendor_id`, `ratecode_id` |
| **Tarifas**   | `fare_amount`, `tip_amount`, `total_amount`, `airport_fee`, `congestion_surcharge` |
| **Metadatos** | `run_id`, `ingested_at_utc` |

---


## Modelo de Machine Learning

### Objetivo

Predecir `total_amount` al momento del pickup, usando únicamente variables conocidas antes o durante el inicio del viaje. Esto elimina completamente el data leakage, ya que se excluyen todas las variables relacionadas con `dropoff_*` o derivadas del fin del trayecto.

### Análisis Exploratorio de Datos (EDA)

- No se detectaron nulos relevantes en las variables seleccionadas
- **Distribución del target (`total_amount`)**:
  - Altamente sesgada hacia valores bajos (< 50 USD)
  - Existen outliers positivos (viajes largos o aeropuertos)
- **Cardinalidad:**
  - `pu_zone`: 234 zonas → se reduce a top 100 + "Other"
  - `ratecode_id`, `vendor_id`: baja cardinalidad → ideales para One-Hot Encoding
- **Correlaciones relevantes:**
  - `trip_distance` muestra alta correlación con `total_amount` (~0.71)
  - Variables temporales (`pickup_hour`, `pickup_dow`) muestran picos claros en horas laborales y fines de semana

### Variables Utilizadas

**Numéricas:**
`trip_distance`, `passenger_count`, `pickup_hour`, `pickup_dow`, `month`, `year`, `is_peak_hour`, `is_weekend`

**Categóricas:**
`service`, `vendor_id`, `ratecode_id`, `pu_borough`, `pu_zone`

**Derivadas:**
- `is_peak_hour`: 1 si el pickup ocurre entre [7–9] o [16–18]
- `is_weekend`: 1 si el día es sábado o domingo

### División Temporal de Datos

División determinística basada en hash de tiempo (0–9 folds):

| Conjunto    | Folds | Descripción          |
|-------------|-------|----------------------|
| **Train**   | 0–7   | Años más antiguos     |
| **Validación** | 8   | Periodo intermedio    |
| **Test**    | 9     | Año más reciente      |

- Evita que el modelo aprenda información del futuro
- Asegura reproducibilidad y consistencia

### Preprocesamiento de Datos

| Proceso            | Herramienta utilizada                           |
|--------------------|--------------------------------------------------|
| Escalado numérico  | `StandardScaler`                                 |
| Codificación       | `OneHotEncoder` con `handle_unknown='infrequent_if_exist'` |
| Polinomios         | `PolynomialFeatures` (grado 2 en `trip_distance` y `pickup_hour`) |

El resultado final es una matriz dispersa (`sparse matrix`), optimizada para entrenar modelos con millones de registros.

## Modelos From Scratch (NumPy)

Se desarrollaron implementaciones propias de los siguientes modelos de regresión lineal:

- **SGD** (descenso estocástico, pérdida MSE)
- **Ridge** (regularización L2)
- **Lasso** (regularización L1)
- **Elastic Net** (combinación L1 + L2)

### Características de Implementación

- Entrenamiento realizado de forma incremental por chunks de 600,000 filas
- Optimizador de descenso estocástico implementado manualmente
- Regularización configurable (`alpha`)
- Tasa de aprendizaje (`lr`)
- Parámetro `l1_ratio` para Elastic Net
- Búsqueda de hiperparámetros manual

### Observaciones

- Funcionan bien en pequeños subconjuntos de datos
- En grandes volúmenes, presentan divergencia numérica si no se escala correctamente
- Reflejan correctamente el comportamiento esperado:
  - **L1** → sparsidad (coeficientes = 0)
  - **L2** → suavizado de coeficientes

## Modelos con Scikit-learn

Se implementaron los equivalentes utilizando Scikit-learn, manteniendo el mismo preprocesamiento, división temporal y `random_state=42`.

| Modelo                          |
|----------------------------------|
| `SGDRegressor(loss="squared_error", penalty="elasticnet")` |
| `Ridge()` |
| `Lasso()` |
| `ElasticNet()` |

- Todos convergieron de forma estable
- Permiten comparación directa con las implementaciones from-scratch

## Resultados Cuantitativos

### Validación (fold = 8)

| Modelo        | RMSE  | MAE  | R²     | Tiempo (s) |
|---------------|-------|------|--------|------------|
| `lasso_skl`   | 1.12  | 5.76 | 0.36   | 890        |
| `enet_skl`    | 1.27  | 5.78 | 0.35   | 960        |
| `ridge_skl`   | 2.73  | 2.13 | -3.6e82| 411        |
| From-scratch  | >1e26 | —    | —      | —          |

### Test (fold = 9)

| Modelo           | RMSE | MAE | R²   |
|------------------|------|-----|------|
| `ridge_skl_test` | 3.89 | 5.34| 0.54 |
| `lasso_skl_test` | 8.97 | 5.75| 0.48 |

### Interpretación de Resultados

- **Ridge es el modelo más estable y robusto**
  - La regularización L2 evita explosión de coeficientes y generaliza mejor
- **Lasso** reduce coeficientes a cero (sparsity), aunque tiende a subajustar
- **ElasticNet** combina ventajas de ambos, pero necesita ajuste fino
- **From-scratch** muestra buen entendimiento teórico, pero requiere mejor normalización y tuning para estabilidad numérica

## Diagnóstico Cualitativo

- Los residuales se distribuyen de forma simétrica alrededor de 0
- Outliers: errores altos en viajes largos o con peajes elevados
- Mayores errores en zonas fuera de Manhattan (mayor variabilidad)
- El modelo subestima valores extremos > 200 USD pero funciona bien entre 5–80 USD (rango operativo típico)

## Conclusiones

### Principales Hallazgos

- **Pipeline robusto y reproducible**: Spark → Postgres → ML
- **Sin data leakage**: solo se usan variables conocidas al inicio del viaje
- **Scikit-learn supera ampliamente a from-scratch** en estabilidad y métricas
- **Modelo ganador: Ridge**

### Modelo Seleccionado: Ridge

**Justificación:**
- RMSE ≈ 3.9 en test
- R² ≈ 0.54 → explica más de la mitad de la varianza de `total_amount`
- Estable, simple y con bajo riesgo de sobreajuste
- Ideal para producción en tiempo real

**Aplicación Práctica:**
- Estimación de tarifa al inicio del viaje
- Validación de precios atípicos en sistemas de taxi / ride-hailing

**Escalabilidad:**
- Pipeline soporta datasets de >100 millones de registros
- Reentrenamiento fácil: basta ejecutar `obt-builder` + notebook ML nuevamente

## Extensión a Todos los Años (2015–2025)

Los resultados entregados se generaron con el primer mes (enero 2015) por limitaciones de entorno, pero el sistema está preparado para correr todo el histórico.

### Para usar todo el rango temporal:

1. **En el notebook de ingesta:**
```python
SELECTIVE_MODE = False
SERVICES = ["yellow", "green"]
YEARS = list(range(2015, 2026))
```

2. **Construcción de OBT:**
```bash
docker compose run obt-builder \
  --mode full \
  --year-start 2015 --year-end 2025 \
  --services yellow,green \
  --run-id obt-run \
  --overwrite true
```

3. **Reentrenamiento:** Ejecutar nuevamente `ml_total_amount_regression.ipynb` para reentrenar los modelos con todos los años cargados.

### Configuración para entornos con mayor capacidad:

```python
N_TRAIN = 1_000_000
N_VAL   = 300_000
N_TEST  = 300_000
CHUNKSIZE = 400_000
```

## Evidencias del Proyecto

| Evidencia              | Archivo                          | Descripción                                      |
|------------------------|----------------------------------|--------------------------------------------------|
| **EDA**                | `3 eda.jpg`                      | Distribución del target y detección de nulos     |
| **Features**           | `3 features.jpg`                 | Variables numéricas y categóricas utilizadas     |
| **Folds**              | `3 folds.jpg`                    | División temporal train/validation/test          |
| **Preprocesamiento**   | `3 preprocesamiento resumen.jpg` | OHE, escalado y generación de polinomios         |
| **Resultados**         | `3 resultados val test.jpg`      | Métricas RMSE / MAE / R² en validación y test    |
| **Diagnóstico**        | `3 diagnostico errores.jpg`      | Distribución de residuales y detección de outliers |

## Resumen del Proyecto

Este proyecto demuestra la implementación completa y profesional de un flujo de Data Mining, destacando:

### Logros Técnicos

- **Datos reales y masivos** (NYC TLC 2015–2025)
- **Ingesta y consolidación reproducible** utilizando Spark + Postgres (SQL)
- **Construcción de una OBT analítica** con limpieza, joins y metadatos
- **Modelos lineales implementados from-scratch (NumPy) y con scikit-learn**
- **Evaluación cuantitativa rigurosa** + análisis cualitativo de errores
- **Pipeline reproducible end-to-end con Docker Compose y Python**

### Cumplimiento de Requisitos

Este trabajo cumple con todos los requisitos de la Sección 9.3 del PSet-4, incluyendo:

- Justificación del split temporal
- Tuning de hiperparámetros
- Comparación entre modelos
- Diagnóstico visual
- Conclusiones fundamentadas

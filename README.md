# Proyecto 3 –Automatización del análisis de datos climáticos con PySpark en AWS

Este proyecto tiene como objetivo automatizar un pipeline completo de ingeniería de datos en una arquitectura batch. La solución abarca desde la captura automática de datos históricos del clima hasta su almacenamiento, transformación y análisis mediante PySpark sobre Amazon EMR. Finalmente, los resultados se publican para su consulta desde Athena o mediante APIs.

### Integrantes
* Felipe Uribe CorreaAdd commentMore actions
* Laura Danniela Zárate Guerrero
* Victor Daniela Arango Sohm

## 1. Descripción General

El proyecto implementa una arquitectura escalable que integra:

- API pública (`Open-Meteo`) para captura de datos meteorológicos históricos.
- Motor de procesamiento distribuido con PySpark en Amazon EMR.
- Bucket simulado con zonas: `raw`, `trusted`, `refined` y `code`.

## 2. Captura e Ingesta de Datos

El script `api_data_extractor.py` automatiza la descarga de datos climáticos históricos:

- Fuente: API Open-Meteo (sin autenticación)
- Variables: temperatura máxima diaria y precipitación
- Ubicación ejemplo: Medellín (lat: 6.25, lon: -75.56)
- Periodo: año completo 2022
- Salida: archivos CSV en la carpeta `raw/`

## 3. Almacenamiento

Los archivos descargados se almacenan en la carpeta `raw/` en formato CSV con nombres organizados por ciudad y fecha. Ejemplo:
```
raw/medellin_2022.csv
```

## 4. Procesamiento ETL en Spark

El script `etl_pipeline.py` realiza las siguientes tareas:

- Lectura de datos crudos desde `raw/`
- Conversión de tipos y limpieza
- Eliminación de valores nulos
- Agregado de columnas derivadas (mes, año, categorías)
- Escritura de datos limpios en formato Parquet en `trusted/`

Este proceso puede ejecutarse como `Step` en Amazon EMR.

## 5. Análisis y Aprendizaje Automático

El script `weather_model.py` aplica:

- Estadísticas descriptivas con SparkSQL
- Modelo de regresión lineal con SparkML
- Evaluación con RMSE y R²
- Escritura de predicciones en `refined/` (CSV/Parquet)

## 6. Consulta de Resultados

Los resultados pueden ser consultados mediante:

- **Athena**: configurando una tabla externa sobre los archivos en `refined/`
- **API Gateway + Lambda (opcional)**: prototipo REST de acceso a resultados

## 7. Automatización

Todo el pipeline está automatizado:

- Captura programada desde la API
- Pasos ETL y análisis definidos como `Steps` de EMR
- Sin intervención manual

## 8. Requisitos Técnicos Cumplidos

- API real y carga automatizada
- Ingesta en S3 (`raw`)
- Transformación con Spark (`trusted`)
- Análisis y ML (`refined`)
- Consulta vía Athena
- Automatización con pasos en EMR

## 9. Herramientas

- AWS S3, EMR, Athena
- Apache Spark (PySpark)
- Python 3.10+
- Open-Meteo API
- Parquet / CSV

## 10. Estructura del Repositorio

```
PySpark-wether-prediction-main/
├── code/
│   ├── api_data_extractor.py
│   ├── etl_pipeline.py
│   └── weather_model.py
├── raw/
├── trusted/
├── refined/
└── unsaved/

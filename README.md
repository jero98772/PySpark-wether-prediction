# Proyecto 3 –Automatización del análisis de datos climáticos con PySpark en AWS

Este proyecto tiene como objetivo automatizar un pipeline completo de ingeniería de datos en una arquitectura batch. La solución implementada abarca desde la captura e ingesta automática de datos históricos del clima desde una API pública, hasta su almacenamiento, transformación y análisis mediante PySpark sobre Amazon EMR, simulando un flujo de datos empresarial real. Finalmente, los resultados se publican para su consulta desde Athena o mediante APIs.

### Integrantes
* Felipe Uribe CorreaAdd commentMore actions
* Laura Danniela Zárate Guerrero
* Victor Daniela Arango Sohm

## 1. Descripción General

El proyecto implementa una arquitectura de datos escalable y automatizada:

- API pública (`Open-Meteo`) para captura de datos meteorológicos históricos.
- Motor de procesamiento distribuido con PySpark en Amazon EMR.
- Bucket simulado con zonas:  `unsaved`, `raw`, `trusted` y `code`.

Toda la estructura está organizada en cuatro zonas del bucket S3:
- raw/: zona donde se almacenan los datos originales sin procesar, descargados desde la fuente.
- trusted/: datos que han sido transformados y limpiados mediante procesos ETL.
- unsaved/: zona temporal o de descartes durante pruebas de desarrollo.
- code/: scripts utilizados para todas las etapas del pipeline automatizado.

## 2. Captura e Ingesta de Datos

El script `.sh` automatiza la descarga de datos climáticos históricos:

- Fuente: API Open-Meteo (sin autenticación)
- Variables: temperatura máxima diaria y precipitación
- Ubicación ejemplo: Medellín (lat: 6.25, lon: -75.56)
- Periodo: año completo 2022
- Salida: Archivo csv en la zona '/raw/api/'

## 3. Almacenamiento

Los archivos descargados se almacenan en la carpeta `/raw/api/` en formato CSV con nombres organizados por ciudad y fecha. Ejemplo:
```
raw/api/medellin_2022.csv
```
La carga es directa desde el script sin intervención manual. El mismo script puede ejecutarse periódicamente para mantener los datos actualizados.

## 4. Procesamiento ETL en Spark

Se utiliza PySpark en Amazon EMR para realizar la transformación y preparación de los datos. El script principal de esta etapa es etl_pipeline.py, ubicado en la carpeta code/.

Funciones Clave implementadas:
- Lectura de datos crudos desde `raw/`
- Conversión de tipos y limpieza
- Eliminación de valores nulos o duplicados
- Agregado de columnas derivadas (mes, año, categorías)
- Escritura de datos limpios en formato Parquet en `trusted/`

Este proceso puede ejecutarse como `Step`, dentro del clúster en Amazon EMR.


## 5. Consulta de Resultados

La segunda parte del procesamiento automatizado se realiza mediante el script weather_model.py, que contiene lógica de SparkML.

Funcionalidades del análisis:
- Lectura de los datos desde la zona trusted/.
- Análisis estadístico básico: media, varianza, correlaciones.
- Generación de visualizaciones descriptivas (aunque limitadas al entorno notebook local).
- Aplicación de un modelo de regresión lineal para predecir la temperatura máxima en función de variables como precipitaciones y fecha.
- Evaluación del modelo usando RMSE y R2.
Escritura de los resultados y predicciones en la zona refined/ en formato CSV o Parquet.

Los resultados están diseñados para ser compatibles con Athena y API Gateway.

## 6. Automatización

Los resultados de los análisis se almacenan en la carpeta refined/ con formato estructurado (Parquet o CSV) para su consulta con herramientas como:

- Athena, donde se puede crear una base de datos externa que apunte al bucket y ejecutar consultas SQL.
- API Gateway + Lambda (opcional): para exponer los resultados como endpoints RESTful de consulta programática.

Todos los procesos han sido diseñados para correr automáticamente sin intervención humana. La ejecución completa puede orquestarse mediante:
- Scripts bash o python invocados por cron.
- Pasos programados en Amazon EMR para el clúster Spark.
- Automatización de la descarga diaria desde la API.
- Generación y cierre del clúster tras cada ejecución (opcional).

## 7. Requisitos Técnicos Cumplidos

- Ingesta de datos desde API real (Open-Meteo)
- Almacenamiento en S3 (raw/, trusted/, refined/)
- Procesamiento en Spark EMR con Steps
- Limpieza, unión, y transformación de datos (ETL)
- Análisis descriptivo (SparkSQL + DataFrames)
- Modelo de ML supervisado (regresión lineal con SparkML)
- Resultados accesibles por Athena
- Código y lógica completamente automatizada

## 8. Herramientas

- AWS S3, EMR, Athena
- Apache Spark (PySpark)
- Python 3.10+
- Open-Meteo API
- Parquet / CSV

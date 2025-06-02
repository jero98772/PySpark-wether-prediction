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
- Bucket simulado con zonas: `raw`, `trusted` y `code`.

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

## 4. Procesamiento ETL en Spark

El script `.py` realiza las siguientes tareas:

- Lectura de datos crudos desde `raw/`
- Conversión de tipos y limpieza
- Eliminación de valores nulos
- Agregado de columnas derivadas (mes, año, categorías)
- Escritura de datos limpios en formato Parquet en `trusted/`

Este proceso puede ejecutarse como `Step` en Amazon EMR.

(HAY UNO QUE LEE EL ARCHIVO DESDE S3 Y OTRO QUE LO VUELVE A DESCARGAR DESDE LA API)

## 5. Consulta de Resultados

Los resultados pueden ser consultados mediante:

- **Athena**: configurando una tabla externa sobre los archivos en `refined/`

(LO DE ATHENA ES OTRO PROCESO DIFERENTE, SI SE PUEDEN COGER LOS ANALISIS PERO NO ES NECESARIAMENTE ESO, ES OTRO PROCESO DIFERENTE PARA HACER ANALISIS DIRECTO DEL ARCHIVO A TRAVEZ DE CONSULTAS SQL)

## 6. Automatización

Todo el pipeline está automatizado:

- Captura programada desde la API
- Pasos ETL y análisis definidos como `Steps` de EMR
- Sin intervención manual

## 7. Requisitos Técnicos Cumplidos

- API real y carga automatizada
- Ingesta en S3 (`raw`)
- Transformación con Spark (`trusted`)
- Consulta vía Athena
- Automatización con pasos en EMR

## 8. Herramientas

- AWS S3, EMR, Athena
- Apache Spark (PySpark)
- Python 3.10+
- Open-Meteo API
- Parquet / CSV

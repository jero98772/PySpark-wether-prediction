# Automatización del análisis de datos climáticos con PySpark en AWS

Este proyecto implementa una arquitectura batch de big data que automatiza la recolección, procesamiento y análisis de datos meteorológicos utilizando herramientas del ecosistema AWS y PySpark. El objetivo es replicar un flujo real de ingeniería de datos donde se integran distintas fuentes y se habilita el consumo final de resultados procesados.

## Objetivo del proyecto

Automatizar el ciclo completo de captura, ingesta, almacenamiento y análisis de datos climáticos provenientes de una API pública (Open-Meteo), utilizando AWS S3 como sistema de almacenamiento distribuido, y Amazon EMR como entorno de ejecución para PySpark. Los resultados generados deben poder ser consultados fácilmente desde servicios como Athena o API Gateway.

## Arquitectura general

La solución está dividida en etapas claramente definidas:

1. **Captura de datos**: Se extrae información meteorológica desde la API de Open-Meteo.
2. **Almacenamiento en zona Raw**: Los datos crudos se guardan en un bucket de Amazon S3.
3. **Procesamiento ETL con PySpark**: Se utiliza Spark en EMR para limpiar, transformar y enriquecer los datos.
4. **Análisis descriptivo**: Se ejecuta un segundo job en PySpark que genera estadísticas agregadas.
5. **Almacenamiento en zona Refined**: Los resultados se almacenan en formato Parquet listos para consulta.
6. **Consulta de resultados**: La salida es accesible desde Amazon Athena o una API desplegada.

## Detalles de implementación

El proyecto se encuentra dividido en carpetas funcionales. A continuación se describen los archivos más relevantes.

### Extracción de datos desde Open-Meteo

La carpeta `API-Extraction` contiene el script `apiExtractor.py`, encargado de conectar con la API pública de Open-Meteo, enviar los parámetros adecuados (coordenadas geográficas, fechas y variables meteorológicas) y guardar el resultado en un archivo CSV.

Este script puede ejecutarse periódicamente o integrarse en un flujo más amplio de automatización. El formato de salida es compatible con Spark y listo para su almacenamiento en S3.

### Scripts de procesamiento en Spark

Dentro de la carpeta `Spark-Jobs` se encuentran los scripts `spark_etl.py` y `spark_analysis.py`.

- `spark_etl.py`: carga los datos crudos desde S3, realiza limpieza de nulos, normalización de columnas, transformación de tipos, y almacena la salida intermedia en la zona `/trusted/` del mismo bucket.
- `spark_analysis.py`: lee los datos ya procesados, aplica agregaciones estadísticas (promedios, máximos, mínimos) agrupadas por ciudad y periodo, y escribe el resultado final en la zona `/refined/`.

Ambos scripts están diseñados para ejecutarse como steps en un clúster EMR. Pueden utilizarse de forma independiente o secuencial.

### Automatización del clúster EMR

En la carpeta `EMR-Launch` se encuentra el script `cluster_launcher.py`, que crea un clúster EMR con Spark ya instalado, y permite lanzar steps de manera automática. Es posible ajustar los parámetros del clúster como número de nodos, tipo de instancia o versión de Spark.

Este módulo permite que todo el procesamiento se realice sin intervención manual, cumpliendo con los requisitos del proyecto.

### Estructura de almacenamiento en S3

El bucket de Amazon S3 utilizado en el proyecto está organizado en zonas que representan distintas fases del flujo de datos:

- `/raw/`: contiene los archivos originales descargados desde la API.
- `/trusted/`: almacena los resultados del procesamiento ETL.
- `/refined/`: guarda los datos finales listos para análisis y consulta.

Esta segmentación facilita la trazabilidad, control de versiones y reproducibilidad del proceso.

## Consulta de resultados

Los archivos almacenados en la zona `/refined/` son compatibles con Amazon Athena, lo que permite ejecutar consultas SQL directamente desde la consola web de AWS. También es posible crear una vista sobre estas tablas y exponer los datos a través de una API (opcional, no incluida en esta versión básica del proyecto).

## Cómo ejecutar el proyecto

1. Ejecutar el script `apiExtractor.py` para descargar datos históricos de Open-Meteo.
2. Subir manual o automáticamente los archivos generados a la carpeta `/raw/` de un bucket S3.
3. Crear un clúster EMR usando `cluster_launcher.py`.
4. Añadir y lanzar los steps con los scripts `spark_etl.py` y `spark_analysis.py`.
5. Verificar que los resultados han sido almacenados en `/refined/`.
6. Consultar desde Athena o mediante cualquier otra herramienta compatible con Parquet.

## Requisitos técnicos

Es necesario contar con una cuenta en AWS con permisos para crear clústeres EMR, usar buckets S3 y acceder a servicios como Athena. Además, se debe tener configurado `aws-cli` con las credenciales necesarias y acceso a Python 3.x con las librerías `boto3`, `pyspark` y `requests`.

## Conclusión

Este proyecto demuestra la viabilidad de implementar una arquitectura de datos escalable y automatizada en un contexto académico, con herramientas reales utilizadas en la industria. La modularidad de los scripts permite extender el flujo fácilmente a otros dominios o fuentes de datos.

El diseño está centrado en la simplicidad, claridad y separación de responsabilidades para facilitar la prueba, depuración y escalamiento del sistema.



# PySpark-wether-prediction

# Proyecto 3 — Automatización de Análisis Climático con PySpark y AWS

### Integrantes
* Felipe Uribe Correa
* Laura Danniela Zárate Guerrero
* Victor Daniela Arango Sohm

Este proyecto tiene como objetivo demostrar un flujo completo de ingeniería de datos orientado a escenarios reales de Big Data, utilizando como fuente principal la API pública de Open-Meteo para la captura de datos meteorológicos. La solución abarca desde la ingesta automática de datos, su almacenamiento y procesamiento sobre un clúster EMR de AWS, hasta la publicación de resultados en formatos accesibles para análisis a través de Athena y API Gateway.

## Descripción general

El proyecto simula un caso de uso empresarial en el que una organización desea automatizar la recolección y análisis de variables climáticas históricas (temperatura máxima, precipitación diaria) para una ciudad específica, en este caso Medellín, Colombia. Los datos son capturados desde la API de Open-Meteo, almacenados en formato CSV en una zona raw (AWS S3), y posteriormente transformados y analizados mediante PySpark sobre un clúster EMR. Finalmente, los resultados del análisis se almacenan en una zona refined en formato Parquet y pueden ser consultados vía Amazon Athena o una API HTTP expuesta con AWS API Gateway.

## Captura e Ingesta de Datos

Se desarrolló un script en Python que permite la extracción automática de información climática desde la API de Open-Meteo. Este script genera archivos CSV diarios con los registros obtenidos para el periodo seleccionado. Posteriormente, dichos archivos son enviados automáticamente a un bucket de Amazon S3 en la carpeta correspondiente a la zona raw.

Para mantener la modularidad y automatización, este proceso puede ser ejecutado manualmente, o agendado utilizando herramientas como cronjobs o scripts orquestadores.

## Procesamiento ETL con PySpark en EMR

Una vez los datos se encuentran en la zona raw, se lanza un clúster de Amazon EMR configurado para ejecutar varios steps de procesamiento, incluyendo limpieza de datos, normalización, conversión de formatos y agregación por periodos (mes, año).

El procesamiento se realiza utilizando PySpark con dataframes y SparkSQL, y los resultados intermedios se almacenan en la zona trusted, también en Amazon S3. Posteriormente, se ejecuta un segundo step con enfoque analítico para obtener estadísticas descriptivas como medias, máximos y tendencias de temperatura y precipitación por ciudad y periodo.

La arquitectura del clúster EMR es mínima, pensada para ambientes académicos: un nodo master y dos nodos core. Todo el proceso de creación del clúster y la adición de steps ha sido automatizado mediante scripts en Python usando `boto3`.

## Resultados y Analítica

El resultado del análisis es exportado a una carpeta final (`/refined`) dentro del bucket S3 en formato Parquet. Estos resultados pueden ser consultados directamente desde Amazon Athena utilizando SQL estándar, o bien mediante un endpoint HTTP que accede a los datos refinados a través de una función Lambda conectada a API Gateway.

En la práctica, esto permite consultar desde una interfaz web las estadísticas climáticas analizadas, sin necesidad de interactuar directamente con la infraestructura subyacente.

## Componentes del repositorio

Este repositorio está organizado en varios módulos, cada uno enfocado en una etapa del flujo de datos. La carpeta `api_captura` contiene el script para la extracción de datos desde Open-Meteo. `s3_upload` maneja el envío de datos a S3. En `emr_scripts` se encuentran los scripts para la creación de clústeres EMR y la ejecución de los pasos de procesamiento. `spark_jobs` contiene los jobs de Spark utilizados para la etapa ETL y de análisis. Finalmente, la carpeta `api_gateway` incluye un ejemplo de función Lambda para exponer los resultados a través de una API HTTP.

## Requisitos

Para ejecutar el proyecto se requiere una cuenta de AWS con permisos suficientes para crear buckets S3, instancias EC2, clústeres EMR, funciones Lambda, y configurar API Gateway. También es necesario tener configurado `aws-cli` en el entorno local y haber generado las credenciales correspondientes.

Además, es necesario tener Python 3.x, junto con las siguientes librerías instaladas: `boto3`, `requests`, `pandas`, `pyspark`.

## Cómo ejecutar el proyecto

1. Iniciar descargando los datos con el script `api_captura/openmeteo_extract.py`, configurando los parámetros deseados (fecha, ciudad, variables meteorológicas).
2. Subir los archivos resultantes al bucket S3 definido, ejecutando el script `s3_upload/upload_raw.py`.
3. Crear el clúster EMR ejecutando `emr_scripts/create_cluster.py`. Asegurarse de guardar el ID del clúster para pasos posteriores.
4. Añadir los pasos de procesamiento ejecutando `emr_scripts/add_steps.py`, indicando el ID del clúster y el path al job de Spark.
5. Esperar la ejecución de los steps. Verificar que los resultados han sido almacenados en la carpeta `/refined` del bucket S3.
6. (Opcional) Consultar los datos refinados desde la consola de Amazon Athena o desplegar la API Gateway utilizando el código en `api_gateway`.

## Notas finales

Este proyecto busca replicar el ciclo completo de un flujo de datos empresarial, con énfasis en la automatización y buenas prácticas de ingeniería. Se ha procurado mantener una separación clara entre etapas y diseñar scripts fácilmente adaptables a otros dominios o fuentes de datos.

En una entrega real, estos scripts podrían integrarse en flujos orquestados por Apache Airflow, Step Functions o herramientas similares. Para este ejercicio académico, se simula dicha orquestación mediante la secuenciación manual de scripts.

Cualquier duda o sugerencia puede ser enviada como issue en este repositorio.


# Tarea 3 de Big Data con Apache Spark y Kafka

Autora: Yiseth Patricia Gualteros Jaime. Grupo 202016911_18. UNAD.

## Problema y fuente

El análisis batch identifica días, horas y zonas de origen frecuentes en los viajes de taxis amarillos de Nueva York de enero de 2024. Se procesa el archivo completo de NYC TLC con 2.964.624 filas originales. Los sensores del componente streaming son simulados y constituyen la práctica independiente del anexo 3.

Fuente oficial: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Requisitos

Linux, Java 17, Python 3.10 u 3.11 recomendado, Spark 3.5.3 y Kafka 3.9.2 con ZooKeeper. La guía adjunta explica la preparación de Ubuntu con WSL o la reutilización de la máquina virtual del curso. Mantener productor, broker y consumidor en el mismo Linux.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
export SPARK_HOME="$(python -c 'import os,pyspark; print(os.path.dirname(pyspark.__file__))')"
export PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python"
export SPARK_LOCAL_IP=127.0.0.1
```

## Análisis batch

```bash
python descargar_datos.py
spark-submit --master local[2] batch_taxis.py
python graficar_batch.py
spark-submit --master local[2] validar_codigo.py
```

La limpieza elimina duplicados exactos y asigna una causa primaria de exclusión a cada registro. Se exige inicio en enero de 2024, valores requeridos no nulos y finitos, distancia mayor que cero y hasta 100 millas, tarifa e importe total positivos, duración mayor que cero y hasta 180 minutos y zona de origen entre 1 y 265. El límite de distancia delimita un estudio urbano y puede excluir viajes legítimos de larga distancia. Los campos opcionales no se imputan.

Spark guarda CSV y JSON en `resultados/batch`. Matplotlib genera las imágenes a partir de agregados pequeños. Las horas conservan el valor civil registrado en la fuente. El total de importes no representa utilidad de una empresa.

Resultados de validación incluidos: 2.867.591 viajes aceptados, 97.033 exclusiones, cero duplicados exactos, 206.134 viajes entre las 18:00 y las 18:59. La huella del archivo está en `resultados/fuente.json`.

## Kafka y Structured Streaming

Descargar y extraer Kafka 3.9.2:

```bash
cd ~
wget https://archive.apache.org/dist/kafka/3.9.2/kafka_2.12-3.9.2.tgz
tar -xzf kafka_2.12-3.9.2.tgz
```

Terminal 1:

```bash
cd ~/kafka_2.12-3.9.2
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Terminal 2:

```bash
cd ~/kafka_2.12-3.9.2
bin/kafka-server-start.sh config/server.properties
```

Terminal 3, crear el topic y ejecutar productor:

```bash
cd ~/kafka_2.12-3.9.2
bin/kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sensor_data
cd ~/proyecto_bigdata
source .venv/bin/activate
python kafka_producer.py
```

Terminal 4, activar el mismo entorno y ejecutar consumidor:

```bash
cd ~/proyecto_bigdata
source .venv/bin/activate
export SPARK_HOME="$(python -c 'import os,pyspark; print(os.path.dirname(pyspark.__file__))')"
export PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python"
export SPARK_LOCAL_IP=127.0.0.1
spark-submit --master local[2] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
```

El conector se descarga con sus dependencias durante el primer arranque. El productor emite segundos Unix; el consumidor convierte LongType mediante timestamp_seconds. La consulta usa ventanas de un minuto, watermark de dos minutos y modo update. Cada microbatch muestra y guarda las actualizaciones por sensor y ventana.

```bash
python graficar_streaming.py
```

La gráfica conserva la última actualización de cada pareja ventana y sensor antes de contar. No se deben sumar todos los JSON directamente. Para otra prueba utiliza tanto un checkpoint como una salida nuevos, por ejemplo `--checkpoint checkpoints/prueba2 --output resultados/prueba2`; después grafica con `--input resultados/prueba2`. Una consulta nueva con startingOffsets=earliest puede releer mensajes que sigan retenidos en Kafka.

Ctrl+C detiene cada proceso. Detener productor y consumidor antes del broker y ZooKeeper. La interfaz Spark suele estar en http://localhost:4040 mientras se ejecuta la consulta.

## Validación incluida y límites

El análisis batch se ejecutó con el archivo completo. La integración envió 60 eventos antes de iniciar el consumidor y 60 durante su ejecución; Spark contabilizó los 120 sobre diez sensores. Los archivos de `resultados/streaming` son evidencia de esa prueba acotada. Una nueva ejecución tendrá otras marcas de tiempo y puede dar otra distribución por ventana. Las pruebas de reglas de calidad y conversión de fechas están en `validar_codigo.py`.

La demostración local no mide escalamiento de clúster ni alta disponibilidad. Un solo broker y replicación uno no toleran su caída. foreachBatch y un generador sin identificador único no demuestran entrega exactamente una vez ante cualquier fallo.

## Archivos

- `descargar_datos.py`: descarga original y huella SHA256.
- `batch_taxis.py`: limpieza y EDA con DataFrames.
- `graficar_batch.py`: tres gráficas de agregados.
- `kafka_producer.py`: sensores simulados con envío confirmado.
- `spark_streaming_consumer.py`: ventanas y promedios con Kafka.
- `graficar_streaming.py`: gráfica sin duplicar actualizaciones.
- `validar_codigo.py`: pruebas de reglas y timestamp Unix.
- `requirements.txt`: dependencias de Python.
- `resultados`: salidas y gráficas de validación.

## Publicación

Crear el repositorio `bigdata-tarea3-spark-kafka` y cargar estos archivos extraídos. Conservar el README en la raíz. No cargar el dataset, los instaladores, `.venv` ni checkpoints. Agregar aquí los enlaces reales de presentación y videos después de publicarlos. La entrega escrita utiliza el archivo `Tarea3_grupo.pdf` actualizado con esos enlaces.

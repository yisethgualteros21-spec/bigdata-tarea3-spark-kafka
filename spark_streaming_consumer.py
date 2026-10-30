"""Kafka y Structured Streaming: estadísticas en ventanas de un minuto."""
import argparse
from pyspark.sql import SparkSession, functions as F, types as T


def transformar(df):
    # El productor emite segundos Unix. Leer como long antes de convertir a timestamp.
    schema=T.StructType([T.StructField('sensor_id',T.IntegerType()),
        T.StructField('temperature',T.DoubleType()),T.StructField('humidity',T.DoubleType()),
        T.StructField('timestamp',T.LongType())])
    d=df.select(F.from_json(F.col('value').cast('string'),schema).alias('d')).select('d.*')
    return (d.withColumn('event_time',F.timestamp_seconds('timestamp'))
        .filter(F.col('sensor_id').between(1,10)&F.col('temperature').between(20,30)&
                F.col('humidity').between(30,70)&F.col('event_time').isNotNull()))


def main():
    a=argparse.ArgumentParser()
    a.add_argument('--bootstrap',default='localhost:9092')
    a.add_argument('--topic',default='sensor_data')
    a.add_argument('--checkpoint',default='checkpoints/sensores_v1')
    a.add_argument('--output',default='resultados/streaming')
    a.add_argument('--duracion',type=int,default=0,help='0 ejecuta hasta Ctrl+C')
    args=a.parse_args()
    spark=(SparkSession.builder.appName('Tarea3SensoresKafka')
        .config('spark.sql.shuffle.partitions','2').config('spark.sql.session.timeZone','UTC').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    raw=(spark.readStream.format('kafka').option('kafka.bootstrap.servers',args.bootstrap)
        .option('subscribe',args.topic).option('startingOffsets','earliest').load())
    d=transformar(raw)
    stats=(d.withWatermark('event_time','2 minutes')
        .groupBy(F.window('event_time','1 minute'),'sensor_id')
        .agg(F.count('*').alias('eventos'),F.round(F.avg('temperature'),2).alias('temperatura_media'),
             F.round(F.avg('humidity'),2).alias('humedad_media')))
    from pathlib import Path
    import json
    out=Path(args.output); out.mkdir(parents=True,exist_ok=True)
    def mostrar_y_guardar(batch,batch_id):
        # Volumen pequeño: una fila por sensor y ventana modificada en este microbatch.
        batch.persist()
        try:
            filas=batch.select(F.col('window.start').cast('string').alias('inicio_utc'),
                F.col('window.end').cast('string').alias('fin_utc'),'sensor_id','eventos',
                'temperatura_media','humedad_media').orderBy('inicio_utc','sensor_id')
            filas.show(100,False)
            (out/f'batch_{batch_id:05d}.json').write_text(
                json.dumps([r.asDict() for r in filas.collect()],indent=2),encoding='utf-8')
        finally: batch.unpersist()
    query=(stats.writeStream.outputMode('update').foreachBatch(mostrar_y_guardar)
        .option('checkpointLocation',args.checkpoint).trigger(processingTime='5 seconds').start())
    try:
        if args.duracion: query.awaitTermination(args.duracion)
        else: query.awaitTermination()
    except KeyboardInterrupt: pass
    finally:
        query.stop(); spark.stop()

if __name__=='__main__': main()

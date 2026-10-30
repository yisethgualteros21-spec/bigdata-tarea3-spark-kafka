"""Pruebas pequeñas de reglas de limpieza y conversión de segundos Unix."""
from pyspark.sql import SparkSession, functions as F
from batch_taxis import preparar
from spark_streaming_consumer import transformar

spark=(SparkSession.builder.master('local[2]').appName('PruebasTarea3')
       .config('spark.sql.shuffle.partitions','2').config('spark.sql.session.timeZone','UTC').getOrCreate())
spark.sparkContext.setLogLevel('WARN')
try:
    # Incluye un duplicado exacto, una distancia absurda, fecha fuera del mes,
    # duración cero, zona inválida y un dato requerido nulo.
    raw=spark.sql("""SELECT cast(inicio AS timestamp_ntz) tpep_pickup_datetime,
        cast(fin AS timestamp_ntz) tpep_dropoff_datetime,dist trip_distance,
        10.0 fare_amount,15.0 total_amount,zona PULocationID,
        1 payment_type,1 passenger_count FROM VALUES
        ('2024-01-01 10:00:00','2024-01-01 10:15:00',2.0,161),
        ('2024-01-01 10:00:00','2024-01-01 10:15:00',2.0,161),
        ('2024-01-01 10:00:00','2024-01-01 10:15:00',300000.0,161),
        ('2024-02-01 10:00:00','2024-02-01 10:15:00',2.0,161),
        ('2024-01-01 10:00:00','2024-01-01 10:00:00',2.0,161),
        ('2024-01-01 10:00:00','2024-01-01 10:15:00',2.0,999),
        ('2024-01-01 10:00:00','2024-01-01 10:15:00',NULL,161)
        AS t(inicio,fin,dist,zona)""")
    auditado,limpio=preparar(raw)
    estados={r['estado']:r['count'] for r in auditado.groupBy('estado').count().collect()}
    assert raw.count()==7 and auditado.count()==6 and limpio.count()==1
    assert len(estados)==6,estados
    assert limpio.first()['duracion_min']==15
    entrada=spark.sql('''SELECT '{"sensor_id":1,"temperature":25.5,"humidity":50.0,"timestamp":1704067200}' AS value''')
    salida=transformar(entrada)
    assert salida.select(F.col('event_time').cast('string')).first()[0]=='2024-01-01 00:00:00'
    assert transformar(spark.sql('''SELECT '{"sensor_id":99,"temperature":25,"humidity":50,"timestamp":1704067200}' AS value''')).count()==0
    print('OK: duplicados, seis categorías de calidad, duración y timestamp Unix.')
finally:
    spark.stop()

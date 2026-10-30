"""Análisis batch reproducible con DataFrames de Apache Spark 3.5.3."""
import argparse
import csv
import json
from pathlib import Path
from pyspark.sql import SparkSession, functions as F


def guardar_csv(df, ruta):
    # Solo se recolectan resultados agregados pequeños, nunca los millones de viajes.
    filas = df.collect()
    with ruta.open('w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(df.columns)
        w.writerows(tuple(r) for r in filas)
    return [r.asDict() for r in filas]


def preparar(raw):
    """Reglas de calidad explícitas. La causa primaria permite conciliar los conteos."""
    unicos = raw.dropDuplicates()  # Igualdad en todas las columnas de la fuente.
    d = unicos.select(
        F.col('tpep_pickup_datetime').cast('timestamp').alias('inicio'),
        F.col('tpep_dropoff_datetime').cast('timestamp').alias('fin'),
        F.col('trip_distance').cast('double').alias('distancia_millas'),
        F.col('fare_amount').cast('double').alias('tarifa_usd'),
        F.col('total_amount').cast('double').alias('total_usd'),
        F.col('PULocationID').alias('zona_origen'),
        F.col('payment_type').alias('tipo_pago'),
        F.col('passenger_count').alias('pasajeros'))
    d = d.withColumn('duracion_min',
                     (F.col('fin').cast('long') - F.col('inicio').cast('long')) / 60.0)
    requerido = ['inicio', 'fin', 'distancia_millas', 'tarifa_usd', 'total_usd', 'zona_origen']
    nulo = F.lit(False)
    for c in requerido:
        nulo = nulo | F.col(c).isNull()
    for c in ['distancia_millas', 'tarifa_usd', 'total_usd']:
        nulo = nulo | F.isnan(c) | (F.abs(F.col(c)) == float('inf'))
    causa = (F.when(nulo, '01_nulos_o_no_finitos')
        .when((F.col('inicio') < F.lit('2024-01-01').cast('timestamp')) |
              (F.col('inicio') >= F.lit('2024-02-01').cast('timestamp')), '02_fuera_de_enero')
        .when((F.col('distancia_millas') <= 0) | (F.col('distancia_millas') > 100) | (F.col('tarifa_usd') <= 0) |
              (F.col('total_usd') <= 0), '03_distancia_o_importe_fuera_de_criterio')
        .when((F.col('duracion_min') <= 0) | (F.col('duracion_min') > 180), '04_duracion_fuera_de_rango')
        .when(~F.col('zona_origen').between(1, 265), '05_zona_fuera_de_catalogo')
        .otherwise('valido'))
    auditado = d.withColumn('estado', causa)
    limpio = (auditado.filter(F.col('estado') == 'valido').drop('estado')
              .withColumn('fecha', F.to_date('inicio'))
              .withColumn('hora', F.hour('inicio')))
    return auditado, limpio


def main():
    a = argparse.ArgumentParser()
    a.add_argument('--input', default='datos/yellow_tripdata_2024-01.parquet')
    a.add_argument('--output', default='resultados/batch')
    a.add_argument('--master', default='local[2]')
    args = a.parse_args()
    out = Path(args.output); out.mkdir(parents=True, exist_ok=True)
    spark = (SparkSession.builder.appName('Tarea3AnalisisTaxis')
             .master(args.master).config('spark.sql.shuffle.partitions', '4')
             .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        raw = spark.read.parquet(args.input).cache()
        raw.printSchema()
        n_original = raw.count()
        nulos = raw.agg(*[F.sum(F.col(c).isNull().cast('long')).alias(c) for c in raw.columns])
        guardar_csv(nulos, out / 'nulos_originales.csv')
        auditado, limpio = preparar(raw)
        auditado.cache(); limpio.cache()
        calidad = guardar_csv(auditado.groupBy('estado').count().orderBy('estado'), out/'calidad.csv')
        n_unicos = sum(x['count'] for x in calidad)
        n_validos = limpio.count()
        guardar_csv(limpio.select('distancia_millas','duracion_min','tarifa_usd','total_usd')
                    .describe(), out/'estadisticas_descriptivas.csv')
        diario = (limpio.groupBy('fecha').agg(F.count('*').alias('viajes'),
                    F.round(F.sum('total_usd'),2).alias('importe_total_usd'),
                    F.round(F.avg('duracion_min'),2).alias('duracion_media_min')).orderBy('fecha'))
        horario = limpio.groupBy('hora').agg(F.count('*').alias('viajes')).orderBy('hora')
        zonas = (limpio.groupBy('zona_origen').agg(F.count('*').alias('viajes'))
                 .orderBy(F.desc('viajes'), 'zona_origen').limit(10))
        pagos = limpio.groupBy('tipo_pago').count().orderBy(F.desc('count'))
        guardar_csv(diario, out/'viajes_diarios.csv')
        h = guardar_csv(horario, out/'viajes_por_hora.csv')
        z = guardar_csv(zonas, out/'top_zonas.csv')
        guardar_csv(pagos, out/'tipos_pago.csv')
        medias = limpio.agg(F.avg('distancia_millas').alias('distancia_media_millas'),
            F.avg('duracion_min').alias('duracion_media_min'),
            F.avg('total_usd').alias('importe_medio_usd'),
            F.sum('total_usd').alias('importe_total_usd')).first().asDict()
        resumen = {'registros_originales':n_original,'duplicados_exactos':n_original-n_unicos,
            'registros_unicos':n_unicos,'registros_validos':n_validos,
            'registros_excluidos':n_original-n_validos,
            'porcentaje_conservado':round(100*n_validos/n_original,3) if n_original else 0,
            **medias,'hora_con_mas_viajes':max(h,key=lambda x:x['viajes']) if h else None,
            'zona_con_mas_viajes':z[0] if z else None,'version_spark':spark.version}
        (out/'resumen.json').write_text(json.dumps(resumen,indent=2,ensure_ascii=False),encoding='utf-8')
        print(json.dumps(resumen,indent=2,ensure_ascii=False))
        diario.show(31,False); horario.show(24,False); zonas.show(10,False)
        limpio.unpersist(); auditado.unpersist(); raw.unpersist()
    finally:
        spark.stop()

if __name__ == '__main__':
    main()

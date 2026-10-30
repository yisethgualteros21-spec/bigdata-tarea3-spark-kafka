"""Generador de sensores del anexo 3. Los datos son simulados."""
import argparse
import json
import random
import time
from kafka import KafkaProducer


def main():
    a=argparse.ArgumentParser()
    a.add_argument('--bootstrap',default='localhost:9092')
    a.add_argument('--topic',default='sensor_data')
    a.add_argument('--eventos',type=int,default=0,help='0 genera hasta Ctrl+C')
    a.add_argument('--intervalo',type=float,default=1)
    a.add_argument('--semilla',type=int,default=42)
    args=a.parse_args(); rng=random.Random(args.semilla)
    producer=KafkaProducer(bootstrap_servers=[args.bootstrap],acks='all',retries=3,
        value_serializer=lambda v:json.dumps(v).encode('utf-8'))
    enviados=0
    try:
        while args.eventos==0 or enviados<args.eventos:
            dato={'sensor_id':rng.randint(1,10),'temperature':round(rng.uniform(20,30),2),
                  'humidity':round(rng.uniform(30,70),2),'timestamp':int(time.time())}
            producer.send(args.topic,key=str(dato['sensor_id']).encode(),value=dato).get(timeout=15)
            print('Sent:',json.dumps(dato),flush=True); enviados+=1
            time.sleep(args.intervalo)
    except KeyboardInterrupt:
        print('Generación detenida por el usuario.')
    finally:
        producer.flush(); producer.close()
        print('Eventos confirmados:',enviados)

if __name__=='__main__': main()

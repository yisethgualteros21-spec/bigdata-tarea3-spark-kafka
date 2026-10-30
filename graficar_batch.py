"""Grafica los agregados que Spark guardó como CSV."""
import argparse
import csv
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def leer(p):
    with p.open(encoding='utf-8') as f: return list(csv.DictReader(f))


def main():
    a=argparse.ArgumentParser(); a.add_argument('--input',default='resultados/batch'); args=a.parse_args()
    p=Path(args.input)
    plt.rcParams.update({'font.size':11,'axes.spines.top':False,'axes.spines.right':False})
    for archivo,x,y,nombre,xlabel in [
        ('viajes_diarios.csv','fecha','viajes','viajes_diarios.png','Día de enero de 2024'),
        ('viajes_por_hora.csv','hora','viajes','viajes_por_hora.png','Hora de inicio del viaje'),
        ('top_zonas.csv','zona_origen','viajes','top_zonas.png','Identificador de zona de origen')]:
        datos=leer(p/archivo); xx=[r[x][-2:] if x=='fecha' else r[x] for r in datos]
        yy=[int(r[y]) for r in datos]
        fig,ax=plt.subplots(figsize=(9,4.2),layout='constrained')
        ax.bar(xx,yy,color='#156580'); ax.set_xlabel(xlabel); ax.set_ylabel('Número de viajes válidos')
        if x=='fecha': ax.tick_params(axis='x',labelsize=8)
        ax.grid(axis='y',alpha=.2); ax.set_axisbelow(True)
        fig.savefig(p/nombre,dpi=180); plt.close(fig)
        print(p/nombre)

if __name__ == '__main__': main()

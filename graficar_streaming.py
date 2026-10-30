"""Cuenta eventos por sensor sin sumar de nuevo las actualizaciones de una ventana."""
import argparse,json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def main():
    a=argparse.ArgumentParser();a.add_argument('--input',default='resultados/streaming');args=a.parse_args()
    p=Path(args.input);ultimas={}
    for f in sorted(p.glob('batch_*.json')):
        for r in json.loads(f.read_text(encoding='utf-8')):
            ultimas[(r['inicio_utc'],r['sensor_id'])]=r
    conteos={}
    for r in ultimas.values():conteos[r['sensor_id']]=conteos.get(r['sensor_id'],0)+r['eventos']
    if not conteos: raise SystemExit('Todavía no hay eventos procesados.')
    sensores=sorted(conteos)
    fig,ax=plt.subplots(figsize=(9,4),layout='constrained')
    ax.bar([str(s) for s in sensores],[conteos[s] for s in sensores],color='#156580')
    ax.set_xlabel('Sensor simulado');ax.set_ylabel('Eventos procesados')
    ax.spines[['top','right']].set_visible(False)
    fig.savefig(p/'eventos_por_sensor.png',dpi=180);plt.close(fig)
    print('Total:',sum(conteos.values()))

if __name__=='__main__':main()

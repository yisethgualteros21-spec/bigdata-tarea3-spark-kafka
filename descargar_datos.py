"""Descarga el archivo original de NYC TLC sin cargarlo completo en memoria."""
from pathlib import Path
import hashlib
import urllib.request
import json

URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
DESTINO = Path('datos/yellow_tripdata_2024-01.parquet')

def main():
    DESTINO.parent.mkdir(parents=True, exist_ok=True)
    if not DESTINO.exists():
        temporal = DESTINO.with_suffix('.part')
        print('Descargando enero de 2024 desde NYC TLC...')
        urllib.request.urlretrieve(URL, temporal)
        temporal.replace(DESTINO)
    h = hashlib.sha256()
    with DESTINO.open('rb') as f:
        for bloque in iter(lambda: f.read(1024 * 1024), b''):
            h.update(bloque)
    metadatos = {'url': URL, 'archivo': str(DESTINO),
                 'bytes': DESTINO.stat().st_size, 'sha256': h.hexdigest()}
    Path('datos/fuente.json').write_text(json.dumps(metadatos, indent=2), encoding='utf-8')
    print(json.dumps(metadatos, indent=2))

if __name__ == '__main__':
    main()

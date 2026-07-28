"""lib_celda.py — (Paso 2) localizador/recortador determinista de filas del censo.

Dado (page, n_orden[, localidad]) recorta la BANDA de esa fila desde la pagina
completa pages/v{page}.png y la guarda en pages/checks/. Funcion pura: el recorte
depende solo del orden de la fila dentro de su pagina (segun poblados_1960.csv) y
de la geometria fija de la tabla. Reutilizada por 2.0 / 2.1 / 2.3.

Por que por indice y no por OCR: el crudo preserva el orden de lectura de la pagina
(h1 arriba, h2 abajo, costura contada una vez). Las mitades son contiguas y el paso
entre filas es ~uniforme, asi que la posicion vertical de la fila i es
  y_centro = DATA_TOP + (i+0.5) * (DATA_BOTTOM-DATA_TOP)/N
La banda se recorta con margen (+-1.2 pasos) para absorber el error de calibracion:
la fila objetivo SIEMPRE queda dentro y se identifica por su n_orden (1a columna).
"""
import os, csv
from PIL import Image

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PAGES = os.path.join(BASE, "pages")
CHECKS = os.path.join(PAGES, "checks")
CRUDO = os.path.join(BASE, "poblados_1960.csv")

# Geometria fija de la tabla en la pagina completa (calibrada con make_halves.py:
# h1 crop y0=430, h2 crop y1=2120; el header de columnas ocupa ~430-470).
DATA_TOP = 515       # y del centro-aprox de la 1a fila de datos (calibrado)
DATA_BOTTOM = 2110   # y del centro-aprox de la ultima fila de datos (calibrado)
MARGEN_PASOS = 1.3   # cuantos pasos de fila de margen arriba/abajo de la banda


def _orden_por_pagina(crudo=CRUDO):
    """page -> lista ordenada de (n_orden, localidad) tal como aparecen en el crudo."""
    orden = {}
    with open(crudo, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            orden.setdefault(r["page"], []).append((r["n_orden"], r["localidad"]))
    return orden


def localizar(page, n_orden, localidad=None, orden=None):
    """Devuelve (idx_en_pagina, N_filas_pagina). Desambigua por localidad si el
    n_orden se repite dentro de la pagina (raro). Lanza si no encuentra."""
    orden = orden or _orden_por_pagina()
    filas = orden[page]
    n_orden = str(n_orden)
    cands = [i for i, (no, lo) in enumerate(filas)
             if no == n_orden and (localidad is None or lo == localidad)]
    if not cands:
        raise KeyError(f"No encontre (page={page}, n_orden={n_orden}, localidad={localidad})")
    return cands[0], len(filas)


def recortar_fila(page, n_orden, localidad=None, orden=None, sufijo="row"):
    """Recorta la banda de la fila desde pages/v{page}.png a pages/checks/.
    Devuelve la ruta del recorte."""
    os.makedirs(CHECKS, exist_ok=True)
    idx, N = localizar(page, n_orden, localidad, orden)
    src = os.path.join(PAGES, f"{page}.png")
    im = Image.open(src)
    W, H = im.size
    paso = (DATA_BOTTOM - DATA_TOP) / max(1, N - 1)
    centro = DATA_TOP + idx * paso
    y0 = max(0, int(centro - MARGEN_PASOS * paso))
    y1 = min(H, int(centro + MARGEN_PASOS * paso))
    c = im.crop((0, y0, W, y1))
    if c.width < 1600:                       # ampliar un poco si la pagina es chica
        f = 1600 / c.width
        c = c.resize((int(c.width * f), int(c.height * f)))
    out = os.path.join(CHECKS, f"{page}_{n_orden}_{sufijo}.png")
    c.save(out)
    return out


if __name__ == "__main__":
    # Test: primera y ultima fila de v3_p01, y un caso conocido.
    for pg, no, lo in [("v3_p01", "359", "Abbott"),
                       ("v3_p01", "574", "Araujo"),
                       ("v3_p22", "262", "Villa Gessell")]:
        try:
            print(recortar_fila(pg, no, lo))
        except Exception as e:
            print("ERROR", pg, no, e)

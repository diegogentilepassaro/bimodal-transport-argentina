"""Paso 3.3 — arma la hoja de revision para Jose (SOLO lo que necesita ojo humano) + la muestra del Paso 2.
NO decide nada: prepara. Lee coordenadas_1960.csv (provisional, con columnas de auditoria).
Salidas:
  revision_3.3.csv  — Grupo A (humano_individual) + B (humano_lote) + C (sin_coordenada), ordenados,
                      con evidencia + columna 'veredicto' vacia para que Jose complete.
  muestra_paso2.csv — 30 filas al azar (semilla fija) de auto_muestreo, para el chequeo de muestra.
"""
import csv, os, random

BASE = r"C:\Users\josem\repos\bimodal-transport-argentina\_local_geocoding_1960"

def main():
    rows = list(csv.DictReader(open(os.path.join(BASE, "coordenadas_1960.csv"), encoding="utf-8")))
    pob = lambda r: int(r["total"] or 0)

    grupo = {"humano_individual": "A", "humano_lote": "B", "sin_coordenada": "C"}
    rev = []
    for r in rows:
        g = grupo.get(r["criterio_aceptacion"])
        if not g:
            continue
        rev.append({"grupo": g, "veredicto": "", "page": r["page"], "n_orden": r["n_orden"],
                    "localidad_1960": r["localidad_canon"], "provincia": r["provincia_canon"],
                    "depto_1960": r["departamento_canon"], "poblacion": r["total"],
                    "propuesta": r["nombre_oficial"], "depto_propuesto": r["georef_depto"],
                    "lat": r["lat"], "lon": r["lon"], "criterio_deteccion": r["criterio_deteccion"],
                    "verificado_geo": r["verificado_geo"], "fuente": r["fuente"],
                    "fuente_url": r["fuente_url"], "nota": r["nota"]})
    # orden: A primero (por poblacion desc), luego B (por poblacion), luego C
    rev.sort(key=lambda x: ({"A": 0, "B": 1, "C": 2}[x["grupo"]], -int(x["poblacion"] or 0)))
    fields = list(rev[0].keys())
    with open(os.path.join(BASE, "revision_3.3.csv"), "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(rev)

    # muestra del Paso 2 (auto_muestreo) — semilla fija, reproducible
    pool = [r for r in rows if r["criterio_aceptacion"] == "auto_muestreo"]
    rng = random.Random(1960)
    muestra = rng.sample(pool, min(30, len(pool)))
    muestra.sort(key=lambda r: -pob(r))
    mf = ["page", "n_orden", "localidad_canon", "provincia_canon", "departamento_canon", "total",
          "nombre_oficial", "georef_depto", "lat", "lon", "criterio_deteccion", "fuente_url", "veredicto_muestra"]
    with open(os.path.join(BASE, "muestra_paso2.csv"), "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=mf); w.writeheader()
        for r in muestra:
            w.writerow({**{k: r.get(k, "") for k in mf if k != "veredicto_muestra"}, "veredicto_muestra": ""})

    from collections import Counter
    print(f"revision_3.3.csv: {len(rev)} filas | por grupo: {dict(Counter(x['grupo'] for x in rev))}")
    print(f"muestra_paso2.csv: {len(muestra)} filas (de {len(pool)} auto_muestreo, semilla 1960)")
    print("\n=== GRUPO A (uno por uno) — todos ===")
    for x in [r for r in rev if r["grupo"] == "A"]:
        print(f"  {int(x['poblacion'] or 0):>6,} {x['localidad_1960']:<26} [{x['depto_1960']:<16}] -> "
              f"{x['propuesta']!r}/{x['depto_propuesto']} guard={x['verificado_geo']} ({x['criterio_deteccion']})")

if __name__ == "__main__":
    main()

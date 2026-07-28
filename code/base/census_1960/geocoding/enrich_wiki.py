"""enrich_wiki.py — completa wiki_cache.json con opensearch para los items que
quedaron `sin_sugerencia` en validacion_nombres.csv (Paso 2.2). Ritmo educado +
backoff (evita el 429). Tras correr, re-ejecutar validar_nombres_wiki.py para que
las sugerencias entren en la cola.
"""
import os, csv, json, time
import validar_nombres_wiki as V

BASE = V.BASE
VNOM = os.path.join(BASE, "validacion_nombres.csv")
CRUDO = os.path.join(BASE, "poblados_1960.csv")

dep_de = {}
for r in csv.DictReader(open(CRUDO, encoding="utf-8")):
    dep_de[(r["page"], r["localidad"])] = r["departamento"]

cache = json.load(open(V.CACHE, encoding="utf-8")) if os.path.exists(V.CACHE) else {}
import requests
sess = requests.Session()

pend = [r for r in csv.DictReader(open(VNOM, encoding="utf-8"))
        if r["tipo_match"] in ("sin_sugerencia", "sin_match")]
print(f"enriquecer: {len(pend)} items")
hechos = 0
for i, r in enumerate(pend, 1):
    dep = dep_de.get((r["page"], r["localidad"]), r["departamento"])
    key = f'{r["localidad"]}|{r["provincia"]}|{dep}'
    if key in cache:
        continue
    try:
        cache[key] = V._opensearch(r["localidad"], sess)
        hechos += 1
    except Exception as e:
        print("  err", r["localidad"], str(e)[:40])
    time.sleep(1.2)
    if i % 25 == 0:
        json.dump(cache, open(V.CACHE, "w", encoding="utf-8"), ensure_ascii=False, indent=0)
        print(f"  {i}/{len(pend)} (nuevos {hechos})")
json.dump(cache, open(V.CACHE, "w", encoding="utf-8"), ensure_ascii=False, indent=0)
print(f"listo: {hechos} nuevas entradas en cache")

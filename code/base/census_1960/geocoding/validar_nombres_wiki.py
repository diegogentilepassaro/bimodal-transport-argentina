"""validar_nombres_wiki.py — (Paso 2.2) valida nombres contra Wikipedia ES.

Determinista (con cache). NO corrige nombres (regla B): solo clasifica y parte filas
en `validado` (match exacto = pasa-limpio) o `pendiente-humano`. Wikipedia es
SUGERENCIA, no autoridad. El crudo NO se toca.

Metodo (robusto al rate-limit de Wikipedia):
- EXACTO: API batcheada `query&titles&redirects` (50 titulos/request, ~19 requests).
  Una localidad es `exacto` si existe un articulo cuyo titulo (resuelto por
  normalizacion + redirects de Wikipedia) coincide con norm(localidad).
- NO-exacto: se enriquece con una SUGERENCIA via opensearch reutilizando wiki_cache.json
  (y consultando en vivo solo un puñado faltante, p.ej. las dudas). aproximado
  (Levenshtein<=2) / sin_match / sin_sugerencia. Todo no-exacto -> revision humana.

Cache: wiki_cache.json (opensearch por `localidad|provincia|departamento`).
Salida: validacion_nombres.csv
(page, localidad, provincia, departamento, wiki_titulo, wiki_url, tipo_match, estado).
"""
import os, csv, json, time, unicodedata, re
from urllib.parse import quote
import requests

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CRUDO = os.path.join(BASE, "poblados_1960.csv")
DUDAS = os.path.join(BASE, "dudas_transcripcion.csv")
CACHE = os.path.join(BASE, "wiki_cache.json")
OUT = os.path.join(BASE, "validacion_nombres.csv")
API = "https://es.wikipedia.org/w/api.php"
UA = "geocoding1960/1.0 (academic research; contacto: jose.belmar)"
DIST_APROX = 2
MAX_LIVE_OPENSEARCH = 30   # tope de consultas opensearch en vivo (solo faltantes clave)


def norm(s):
    s = re.sub(r"\s*\([^)]*\)\s*$", "", s or "")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", s.casefold()).strip()


def lev(a, b):
    if a == b: return 0
    if not a: return len(b)
    if not b: return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]


def _get(sess, params, reintentos=6):
    espera = 2.0
    for _ in range(reintentos):
        r = sess.get(API, params=params, timeout=30, headers={"User-Agent": UA})
        if r.status_code == 429:
            ra = r.headers.get("Retry-After")
            time.sleep(float(ra) if ra and ra.isdigit() else espera)
            espera = min(espera * 2, 30)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError("429 persistente")


def existencia_exacta(localidades, sess):
    """set de localidades (string crudo) que tienen articulo exacto en Wikipedia ES."""
    uniq = sorted({l for l in localidades})
    existe = set()
    for i in range(0, len(uniq), 50):
        lote = uniq[i:i + 50]
        data = _get(sess, {"action": "query", "titles": "|".join(lote),
                           "redirects": 1, "format": "json", "formatversion": "2"}).get("query", {})
        nmap = {d["from"]: d["to"] for d in data.get("normalized", [])}
        rmap = {d["from"]: d["to"] for d in data.get("redirects", [])}
        vivos = {p["title"] for p in data.get("pages", []) if not p.get("missing")}
        for t in lote:
            f = rmap.get(nmap.get(t, t), nmap.get(t, t))
            if f in vivos and norm(f) == norm(t):
                existe.add(t)
        time.sleep(0.4)
    return existe


def _opensearch(term, sess):
    data = _get(sess, {"action": "opensearch", "search": term, "limit": 5,
                       "namespace": 0, "format": "json"})
    return list(zip(data[1], data[3]))


def run():
    cache = json.load(open(CACHE, encoding="utf-8")) if os.path.exists(CACHE) else {}
    sess = requests.Session()
    with open(CRUDO, encoding="utf-8") as f:
        crudo = list(csv.DictReader(f))
    dudas_keys = set()
    if os.path.exists(DUDAS):
        with open(DUDAS, encoding="utf-8") as f:
            for d in csv.DictReader(f):
                dudas_keys.add((d["page"], d["n_orden"], d["localidad"]))

    # --- EXACTO (batcheado) ---
    print("2.2: detectando exactos (batched query&titles)...")
    existe = existencia_exacta([r["localidad"] for r in crudo], sess)
    print(f"   exactos detectados: {len(existe)} titulos unicos")

    # --- enriquecer NO-exactos con sugerencia (cache + pocas en vivo) ---
    live = 0
    rows_out = []
    for r in crudo:
        loc, prov, dep = r["localidad"], r["provincia"], r["departamento"]
        if loc in existe:
            tit = loc
            url = "https://es.wikipedia.org/wiki/" + quote(loc.replace(" ", "_"))
            rows_out.append({"page": r["page"], "localidad": loc, "provincia": prov,
                             "departamento": dep, "wiki_titulo": tit, "wiki_url": url,
                             "tipo_match": "exacto", "estado": "validado"})
            continue
        # no-exacto: sugerencia desde cache, o en vivo si es duda y queda presupuesto
        key = f"{loc}|{prov}|{dep}"
        cands = cache.get(key)
        es_duda = (r["page"], r["n_orden"], loc) in dudas_keys
        if cands is None and es_duda and live < MAX_LIVE_OPENSEARCH:
            try:
                cands = _opensearch(loc, sess); cache[key] = cands; live += 1; time.sleep(0.5)
            except Exception:
                cands = None
        if not cands:
            tipo, tit, url = ("sin_match" if cands == [] else "sin_sugerencia"), "", ""
        else:
            best = min(cands, key=lambda c: lev(norm(loc), norm(c[0])))
            d = lev(norm(loc), norm(best[0]))
            tipo = "aproximado" if 0 < d <= DIST_APROX else "sin_match"
            tit, url = best
        rows_out.append({"page": r["page"], "localidad": loc, "provincia": prov,
                         "departamento": dep, "wiki_titulo": tit, "wiki_url": url,
                         "tipo_match": tipo, "estado": "pendiente-humano"})

    json.dump(cache, open(CACHE, "w", encoding="utf-8"), ensure_ascii=False, indent=0)
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["page", "localidad", "provincia", "departamento",
                                          "wiki_titulo", "wiki_url", "tipo_match", "estado"])
        w.writeheader(); w.writerows(rows_out)
    from collections import Counter
    c = Counter(r["tipo_match"] for r in rows_out)
    print(f"2.2 nombres: {len(rows_out)} filas | {dict(c)}")
    print(f"   validado(exacto)={c['exacto']} | a revision humana={len(rows_out)-c['exacto']} "
          f"| opensearch en vivo={live}")
    return rows_out


if __name__ == "__main__":
    run()

"""Paso 3 — motor de geocoding contra Georef/INDEC (API publica apis.datos.gob.ar/georef).

Capacidades (Paso 3.2):
- match por 3 campos: nombre + provincia + departamento, con exacto=true (sin fuzzy).
- max>1 para detectar homonimos (ambiguedad) dentro de depto/prov.
- CASCADA de capas: localidades-censales -> localidades -> asentamientos (BAHRA), para rescatar parajes.
- captura de TODOS los candidatos (no solo top-1).
- cache reproducible `georef_cache.json` con CLAVE = FIRMA COMPLETA de la consulta
  (`capa|nombre|provincia|departamento|exacto|max`) -> seguro de conservar entre versiones del algoritmo.

API:
- query(nombre, provincia, departamento=None, capa=CENSAL, exacto=True, mx=5) -> [cand, ...]
- query_batch(items, capa=CENSAL, exacto=True, mx=5) -> [[cand,...], ...]  (items = list de dicts)
- cascade(nombre, provincia, departamento=None, exacto=True, mx=5) -> (capa_hit, [cand,...])
- geocode(pairs) -> dict compat viejo {(loc,prov): {lon,lat,georef_*} or None}
Cada `cand` = {id, nombre, lat, lon, depto, prov, capa}.
"""
import requests, json, os, time, unicodedata

# ---- normalizacion de nombres (FUENTE UNICA DE VERDAD) ----------------------
# POR QUE EXISTE: NO se usa `exacto=true` de Georef para decidir la igualdad de nombres. Los datos de
# Georef traen un mojibake sistematico -> un U+00AD (SOFT HYPHEN, invisible) incrustado despues de la
# letra 'i' acentuada ('Juni­n', 'Olavarri­a', 'Fri­as'...). Para el servidor "Junin" != "Juni­n" -> 0 hits
# -> la localidad caia a BAHRA/sin_match EN SILENCIO (18 casos, 125.463 hab, incl. Junin y Olavarria).
# La igualdad la decidimos NOSOTROS acá, de forma documentada y determinista.
_QUOTES = {"’": "'", "‘": "'", "`": "'", "´": "'", "“": '"', "”": '"'}
_DASHES = {"‐": "-", "‑": "-", "‒": "-", "–": "-", "—": "-", "−": "-"}

def norm_name(s):
    """Igualdad de nombres robusta. Inmune a:
    - caracteres INVISIBLES: categoria Unicode `Cf` (soft hyphen U+00AD, zero-width U+200B-200D, BOM) —
      generico, no una lista negra de un caracter;
    - forma NFC/NFD y acentos (acento-insensible, igual que la norma del README 3.2(E));
    - comillas y guiones variantes; NBSP y espacios repetidos.
    """
    s = unicodedata.normalize("NFKD", s or "")
    out = []
    for ch in s:
        if unicodedata.combining(ch):            # marcas de acento (tras NFKD)
            continue
        if unicodedata.category(ch) == "Cf":     # invisibles / formato
            continue
        out.append(_QUOTES.get(ch, _DASHES.get(ch, ch)))
    s = "".join(out).casefold()
    for ch in "-.,'\"":
        s = s.replace(ch, " ")
    return " ".join(s.split())                   # split() ya colapsa NBSP y multiples espacios

BASE_URL = "https://apis.datos.gob.ar/georef/api"
CAMPOS = "id,nombre,centroide.lat,centroide.lon,departamento.nombre,provincia.nombre"
CACHE = r"C:\Users\josem\repos\bimodal-transport-argentina\_local_geocoding_1960\georef_cache.json"

CENSAL = "localidades-censales"
CASCADA = [CENSAL, "localidades", "asentamientos"]     # orden de la cascada (fino -> BAHRA)
# clave plural del recurso en el body/resultado del batch, por capa
PLURAL = {"localidades-censales": "localidades_censales",
          "localidades": "localidades", "asentamientos": "asentamientos"}

# ---- cache -----------------------------------------------------------------
def _load_cache():
    if os.path.exists(CACHE):
        return json.load(open(CACHE, encoding="utf-8"))
    return {}

def _save_cache(cache):
    json.dump(cache, open(CACHE, "w", encoding="utf-8"), ensure_ascii=False)

def _key(capa, nombre, provincia, departamento, exacto, mx):
    """FIRMA COMPLETA de la consulta -> clave de cache (algoritmo-independiente)."""
    return "|".join([capa, nombre or "", provincia or "", departamento or "",
                     "1" if exacto else "0", str(mx)])

def _parse(hit, capa):
    return {"id": hit.get("id"), "nombre": hit.get("nombre"),
            "lat": hit.get("centroide", {}).get("lat"),
            "lon": hit.get("centroide", {}).get("lon"),
            "depto": (hit.get("departamento") or {}).get("nombre"),
            "prov": (hit.get("provincia") or {}).get("nombre"),
            "capa": capa}

# ---- consultas -------------------------------------------------------------
def query_batch(items, capa=CENSAL, exacto=True, mx=5, batch=200, sleep=0.3, cache=None):
    """items = [{'nombre':..,'provincia':..,'departamento':..(opcional)}, ...].
    Devuelve lista alineada de listas de candidatos. Cachea por firma completa."""
    own = cache is None
    if own:
        cache = _load_cache()
    plural = PLURAL[capa]
    keys = [_key(capa, it["nombre"], it.get("provincia"), it.get("departamento"), exacto, mx)
            for it in items]
    todo_idx = [i for i, k in enumerate(keys) if k not in cache]
    for s in range(0, len(todo_idx), batch):
        chunk_idx = todo_idx[s:s+batch]
        body = {plural: []}
        for i in chunk_idx:
            it = items[i]
            q = {"nombre": it["nombre"], "campos": CAMPOS, "max": mx, "orden": "id"}
            if it.get("provincia"):
                q["provincia"] = it["provincia"]
            if it.get("departamento"):
                q["departamento"] = it["departamento"]
            if exacto:
                q["exacto"] = True
            body[plural].append(q)
        r = requests.post(f"{BASE_URL}/{capa}", json=body, timeout=90)
        r.raise_for_status()
        results = r.json().get("resultados", [])
        for i, res in zip(chunk_idx, results):
            hits = res.get(plural, [])
            cache[keys[i]] = [_parse(h, capa) for h in hits]
        _save_cache(cache)
        time.sleep(sleep)
    if own:
        _save_cache(cache)
    return [cache[k] for k in keys]

def query(nombre, provincia=None, departamento=None, capa=CENSAL, exacto=True, mx=5, cache=None):
    """Consulta unica (GET). Devuelve lista de candidatos. Cachea por firma completa."""
    own = cache is None
    if own:
        cache = _load_cache()
    k = _key(capa, nombre, provincia, departamento, exacto, mx)
    if k not in cache:
        params = {"nombre": nombre, "campos": CAMPOS, "max": mx, "orden": "id"}
        if provincia:
            params["provincia"] = provincia
        if departamento:
            params["departamento"] = departamento
        if exacto:
            params["exacto"] = "true"
        r = requests.get(f"{BASE_URL}/{capa}", params=params, timeout=60)
        r.raise_for_status()
        hits = r.json().get(PLURAL[capa], [])
        cache[k] = [_parse(h, capa) for h in hits]
        if own:
            _save_cache(cache)
    return cache[k]

def cascade(nombre, provincia=None, departamento=None, exacto=True, mx=5, cache=None):
    """Prueba las capas en orden; devuelve (capa_hit, candidatos) en la primera con resultados.
    Si ninguna devuelve nada -> (None, [])."""
    own = cache is None
    if own:
        cache = _load_cache()
    for capa in CASCADA:
        cands = query(nombre, provincia, departamento, capa=capa, exacto=exacto, mx=mx, cache=cache)
        if cands:
            if own:
                _save_cache(cache)
            return capa, cands
    if own:
        _save_cache(cache)
    return None, []

# ---- compat viejo ----------------------------------------------------------
def geocode(pairs, batch=200, sleep=0.3):
    """Compat: pairs=[(loc,prov),...] -> {(loc,prov): {lon,lat,georef_nombre,georef_depto,georef_prov} or None}.
    (censal, sin depto, top-1). Nuevo codigo del Paso 3.2 usa query/query_batch/cascade."""
    items = [{"nombre": loc, "provincia": prov} for loc, prov in pairs]
    res = query_batch(items, capa=CENSAL, exacto=False, mx=1, batch=batch, sleep=sleep)
    out = {}
    for (loc, prov), cands in zip(pairs, res):
        if cands:
            c = cands[0]
            out[(loc, prov)] = {"lon": c["lon"], "lat": c["lat"], "georef_nombre": c["nombre"],
                                "georef_depto": c["depto"], "georef_prov": c["prov"]}
        else:
            out[(loc, prov)] = None
    return out

if __name__ == "__main__":
    print("exacto 3 campos - Abbott/Monte/Buenos Aires:")
    print(" ", query("Abbott", "Buenos Aires", "Monte"))
    print("cascada - Aldea Romana/Buenos Aires (esperado: sin match en ninguna capa):")
    print(" ", cascade("Aldea Romana", "Buenos Aires"))
    print("cascada - Azcuenaga (con acento) via nombre exacto:")
    print(" ", cascade("Azcuénaga", "Buenos Aires", "San Andrés de Giles"))

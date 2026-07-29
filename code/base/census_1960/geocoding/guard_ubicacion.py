"""Paso 3.3a-C5 — GUARD geografico determinista. Verifica que una coordenada propuesta caiga en el
CONJUNTO PERMITIDO de departamentos (crosswalk 3.1), via Georef /ubicacion (reverse-geocode).

Hace la FUENTE irrelevante para la seguridad: venga de Georef, Wikipedia u OSM, la coord se verifica
de forma independiente contra la geografia esperada.

Uso:
  ubicacion(lat, lon) -> {'depto':..., 'prov':...} | None   (cachea en ubicacion_cache.json)
  verificar(lat, lon, expected_deptos) -> ('verde'|'rojo'|'sin_depto', depto_del_punto)
"""
import requests, json, os, time, unicodedata

URL = "https://apis.datos.gob.ar/georef/api/ubicacion"
CACHE = r"C:\Users\josem\repos\bimodal-transport-argentina\_local_geocoding_1960\ubicacion_cache.json"

def _norm(s):
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c) and unicodedata.category(c) != "Cf")
    return " ".join(s.casefold().replace("-", " ").replace(".", " ").split())

def _load():
    return json.load(open(CACHE, encoding="utf-8")) if os.path.exists(CACHE) else {}

def _save(c):
    json.dump(c, open(CACHE, "w", encoding="utf-8"), ensure_ascii=False)

def ubicacion(lat, lon, cache=None, sleep=0.2):
    own = cache is None
    if own: cache = _load()
    key = f"{round(float(lat),5)},{round(float(lon),5)}"
    if key not in cache:
        r = requests.get(URL, params={"lat": lat, "lon": lon,
                                       "campos": "departamento.nombre,provincia.nombre"}, timeout=30)
        r.raise_for_status()
        u = r.json().get("ubicacion", {})
        cache[key] = {"depto": (u.get("departamento") or {}).get("nombre"),
                      "prov": (u.get("provincia") or {}).get("nombre")}
        if own: _save(cache)
        time.sleep(sleep)
    return cache[key]

def verificar(lat, lon, expected_deptos, cache=None):
    if lat in (None, "") or lon in (None, ""):
        return ("sin_coord", None)
    u = ubicacion(lat, lon, cache=cache)
    dep = u.get("depto")
    if not dep:
        return ("sin_depto", None)          # mar / exterior -> coord rechazada
    expn = {_norm(e) for e in expected_deptos}
    return ("verde" if _norm(dep) in expn else "rojo", dep)

if __name__ == "__main__":
    # test: Quequen (-38.57,-58.70) deberia ser 'verde' si Necochea esta en el conjunto
    print("Necochea point vs {Necochea}:", verificar(-38.57, -58.70, ["Necochea"]))
    print("Necochea point vs {Lobería}:", verificar(-38.57, -58.70, ["Lobería"]))
    print("mar:", verificar(-40, -55, ["Necochea"]))

"""DIAGNOSTICO (read-only sobre resultados; solo consulta Georef) — responde: cuantos flag_bahra /
flag_variante son en realidad localidades CENSALES que `exacto=true` no encontro por caracteres
INVISIBLES en los datos de Georef (soft hyphen U+00AD, zero-width...).

Metodo: por cada flag, consultar censal SIN `exacto` (fuzzy) y comparar nosotros mismos el nombre
limpiando invisibles. Si hay match exacto (limpio) y el depto esta en el conjunto permitido -> el item
deberia ser auto_ok y hoy esta mal clasificado.
Salida: reporte + diag_bahra.csv
"""
import csv, os, re, sys, unicodedata
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import geocode_georef as G

BASE = r"C:\Users\josem\repos\bimodal-transport-argentina\_local_geocoding_1960"
INVIS = re.compile(r"[­​‌‍﻿]")   # soft hyphen + zero-width + BOM

def clean(s): return INVIS.sub("", s or "")
def norm_exact(s):
    """igualdad de nombre robusta a invisibles y espacios (NO afloja acentos ni palabras)."""
    return " ".join(unicodedata.normalize("NFC", clean(s)).split()).casefold()

def main():
    xw = {}
    for r in csv.DictReader(open(os.path.join(BASE, "crosswalk_indec.csv"), encoding="utf-8")):
        if r["depto_moderno"]:
            xw.setdefault((r["provincia_canon"], r["departamento_canon"]), set()).add(r["depto_moderno"])
    va = {(r["page"], r["n_orden"]): r for r in csv.DictReader(open(os.path.join(BASE, "vista_ancha.csv"), encoding="utf-8"))}
    rows = list(csv.DictReader(open(os.path.join(BASE, "geo_match_simple.csv"), encoding="utf-8")))
    target = [r for r in rows if r["estado"] in ("flag_bahra", "flag_variante", "flag_sin_match")]

    cache = G._load_cache()
    out, n = [], 0
    for r in target:
        n += 1
        if n % 100 == 0:
            G._save_cache(cache); print(f"  ... {n}/{len(target)}", flush=True)
        v = va.get((r["page"], r["n_orden"]))
        if not v: continue
        exp = xw.get((v["provincia_canon"], v["departamento_canon"]), set())
        cands = G.query(r["localidad_canon"], r["provincia_georef"], None,
                        capa=G.CENSAL, exacto=False, mx=10, cache=cache)
        hit = [h for h in cands
               if norm_exact(h["nombre"]) == norm_exact(r["localidad_canon"]) and h["depto"] in exp]
        if hit:
            h = hit[0]
            try: pob = int(v.get("total_canon") or 0)
            except: pob = 0
            out.append({"estado_actual": r["estado"], "localidad": r["localidad_canon"],
                        "provincia": r["provincia_georef"], "depto_1960": v["departamento_canon"],
                        "georef_nombre": h["nombre"], "georef_depto": h["depto"], "georef_id": h["id"],
                        "lat": h["lat"], "lon": h["lon"], "poblacion_1960": pob,
                        "tenia_invisible": bool(INVIS.search(h["nombre"]))})
    G._save_cache(cache)

    with open(os.path.join(BASE, "diag_bahra.csv"), "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out[0].keys()) if out else ["estado_actual"])
        w.writeheader(); w.writerows(out)

    from collections import Counter
    print(f"\nAnalizados: {len(target)} flags (bahra/variante/sin_match)")
    print(f"MAL CLASIFICADOS (son censal exacto tras limpiar invisibles, depto en conjunto): {len(out)}")
    print("  por estado actual:", dict(Counter(o["estado_actual"] for o in out)))
    print(f"  de esos, con caracter invisible en el nombre Georef: {sum(1 for o in out if o['tenia_invisible'])}")
    print(f"  poblacion 1960 involucrada: {sum(o['poblacion_1960'] for o in out):,}")
    print("\n  top 12 por poblacion:")
    for o in sorted(out, key=lambda x: -x["poblacion_1960"])[:12]:
        inv = " [INVISIBLE]" if o["tenia_invisible"] else ""
        print(f"    {o['poblacion_1960']:>7,}  {o['estado_actual']:<14} {o['localidad']:<24} == {o['georef_nombre']!r}/{o['georef_depto']}{inv}")

if __name__ == "__main__":
    main()

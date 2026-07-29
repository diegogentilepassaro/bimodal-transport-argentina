"""Paso 3.3a-C5 — ensambla los result_N.csv de los agentes, aplica el GUARD geografico a cada coordenada
propuesta, y produce c5_investigacion.csv. NO decide: propone con fuente + verificado_geo para 3.3.

Verificaciones incorporadas: cobertura exacta de los 188 (clave (page,n_orden,localidad)); toda coord
con fuente_url; toda coord con verificado_geo calculado.
"""
import csv, os, glob, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from guard_ubicacion import verificar, _load, _save

BASE = r"C:\Users\josem\repos\bimodal-transport-argentina\_local_geocoding_1960"
LOTES = os.path.join(BASE, "_c5_lotes")

def main():
    # esperado: la lista de entrada (188) con su conjunto permitido
    import json
    inp = {(r["page"], r["n_orden"], r["localidad"]): r
           for r in json.load(open(os.path.join(BASE, "_c5_input.json"), encoding="utf-8"))}

    # leer resultados de los agentes
    got = {}
    for f in sorted(glob.glob(os.path.join(LOTES, "result_*.csv"))):
        for r in csv.DictReader(open(f, encoding="utf-8")):
            got[(r["page"], r["n_orden"], r["localidad"])] = r

    faltan = [k for k in inp if k not in got]
    sobran = [k for k in got if k not in inp]
    print(f"entrada: {len(inp)} | resultados: {len(got)} | faltan: {len(faltan)} | sobran: {len(sobran)}")
    if faltan:
        print("  FALTAN (agente no los devolvio):")
        for k in faltan[:20]: print("   ", k)

    cache = _load()
    out = []
    for k, meta in inp.items():
        g = got.get(k, {})
        tipo = (g.get("tipo_resolucion") or "sin_resultado").strip()
        lat, lon = (g.get("lat") or "").strip(), (g.get("lon") or "").strip()
        url = (g.get("fuente_url") or "").strip()
        expected = [d.strip() for d in meta["deptos_modernos"].split("|") if d.strip()]
        # GUARD
        if lat and lon:
            try:
                vg, depto_pt = verificar(float(lat), float(lon), expected, cache=cache)
            except Exception as e:
                vg, depto_pt = ("error", str(e)[:30])
        else:
            vg, depto_pt = ("sin_coord", "")
        # coord sin fuente -> se descarta la coord (regla dura)
        if (lat and lon) and not url:
            vg = "SIN_FUENTE_descartada"; lat = lon = ""
        out.append({"page": k[0], "n_orden": k[1], "localidad": k[2], "provincia": meta["provincia"],
                    "depto_1960": meta["depto_1960"], "poblacion": meta["poblacion"],
                    "tipo_resolucion": tipo, "propuesta": (g.get("propuesta") or "").strip(),
                    "lat": lat, "lon": lon, "fuente": (g.get("fuente") or "").strip(),
                    "fuente_url": url, "verificado_geo": vg, "depto_del_punto": depto_pt or "",
                    "nota": (g.get("nota") or "").strip()})
    _save(cache)

    out.sort(key=lambda r: -int(r["poblacion"] or 0))
    fields = ["page","n_orden","localidad","provincia","depto_1960","poblacion","tipo_resolucion",
              "propuesta","lat","lon","fuente","fuente_url","verificado_geo","depto_del_punto","nota"]
    with open(os.path.join(BASE, "c5_investigacion.csv"), "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(out)

    from collections import Counter
    tc, vc = Counter(r["tipo_resolucion"] for r in out), Counter(r["verificado_geo"] for r in out)
    con_coord = [r for r in out if r["lat"]]
    pob = lambda rs: sum(int(r["poblacion"] or 0) for r in rs)
    print(f"\nc5_investigacion.csv: {len(out)} filas")
    print("por tipo_resolucion:", dict(tc))
    print("por verificado_geo :", dict(vc))
    print(f"con coordenada: {len(con_coord)} ({pob(con_coord):,} hab) | sin coord: {len(out)-len(con_coord)}")
    # verificaciones
    sinf = [r for r in con_coord if not r["fuente_url"]]
    print(f"VERIF coords sin fuente_url: {len(sinf)} (debe ser 0)")
    rojos = [r for r in out if r["verificado_geo"] == "rojo"]
    print(f"\nROJOS (coord fuera del depto esperado -> mas ojo) = {len(rojos)}:")
    for r in sorted(rojos, key=lambda x: -int(x["poblacion"] or 0))[:12]:
        print(f"   {int(r['poblacion'] or 0):>6,}  {r['localidad']:<26} propuesto {r['propuesta']!r} cae en {r['depto_del_punto']!r} (esperado {r['depto_1960']})")

if __name__ == "__main__":
    main()

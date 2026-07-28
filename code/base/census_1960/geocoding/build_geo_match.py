"""Paso 3.2 — match de localidad contra Georef -> geo_match_simple.csv (o salida de piloto).
Reglas IF/THEN (ver README 'PASO 3'):
  single (identidad/rename/especial): query nombre+prov+depto exacto.
    1 hit -> auto_ok | >1 -> flag_ambiguo | 0 -> ver sin depto:
      hay hit sin depto -> flag_depto | cascada BAHRA hit -> flag_bahra | nada -> flag_sin_match(fuzzy).
  split (N destinos): query nombre+prov SIN depto exacto; hits con depto in destinos:
    1 -> auto_ok | >1 -> flag_ambiguo | 0 pero hay hits -> flag_depto | 0 -> cascada/flag_sin_match.
  sin_equivalente: flag_sin_match (revisar imagen).
Uso: python build_geo_match.py --provincia "Cordoba" [--departamento "San Alberto"] --out geo_match_pilotoA.csv
Determinista; cachea en georef_cache.json (firma completa). NO toca el crudo.
"""
import csv, os, sys, argparse
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import geocode_georef as G

BASE = r"C:\Users\josem\repos\bimodal-transport-argentina\_local_geocoding_1960"
VISTA = os.path.join(BASE, "vista_ancha.csv")
XW = os.path.join(BASE, "crosswalk_indec.csv")
PROV = os.path.join(BASE, "autoridad_prov.csv")

FIELDS = ["page","n_orden","localidad_canon","provincia_georef","departamento_canon",
          "depto_moderno_esperado","estado","georef_capa","georef_id","georef_nombre",
          "georef_depto","lat","lon","dist_km","candidatos"]

# --- centroides de depto (evidencia dura para flag_depto: cerca=transferencia/split, lejos=homonimo) ---
_CENT = {}
def centroides(prov_georef):
    import json, urllib.request, urllib.parse
    if prov_georef not in _CENT:
        q = urllib.parse.urlencode({"provincia": prov_georef, "max": 300,
                                    "campos": "nombre,centroide.lat,centroide.lon"})
        with urllib.request.urlopen(f"https://apis.datos.gob.ar/georef/api/departamentos?{q}", timeout=60) as r:
            _CENT[prov_georef] = {d["nombre"]: (d["centroide"]["lat"], d["centroide"]["lon"])
                                  for d in json.load(r)["departamentos"]}
    return _CENT[prov_georef]

def km(a, b):
    import math
    R, p = 6371.0, math.pi / 180
    dlat, dlon = (b[0]-a[0])*p, (b[1]-a[1])*p
    h = math.sin(dlat/2)**2 + math.cos(a[0]*p)*math.cos(b[0]*p)*math.sin(dlon/2)**2
    return 2 * R * math.asin(math.sqrt(h))

def dist_esperado_candidato(prov_g, esperados, cand_depto):
    """Distancia minima entre el centroide del/los depto(s) esperado(s) y el del candidato."""
    try:
        c = centroides(prov_g)
        if cand_depto not in c: return ""
        ds = [km(c[e], c[cand_depto]) for e in esperados if e in c]
        return f"{min(ds):.0f}" if ds else ""
    except Exception:
        return ""

def _cand_str(cands):
    return "; ".join(f"{c['nombre']}/{c['depto']} [{c['id']}]" for c in cands)

def match_one(loc, prov_g, expected, tipo, cache):
    """REGLA DE CONJUNTO (README 'PASO 3' 3.2) + igualdad de nombres DECIDIDA POR NOSOTROS.
    NO se usa `exacto=true` de Georef (sus datos traen U+00AD invisible -> falsos 0 hits, en silencio).
    Se consulta SIN `exacto` (mas recall) y se compara client-side con G.norm_name().
    El `depto ∈ conjunto` tambien se compara normalizado (mismo bug latente)."""
    expn = {G.norm_name(e) for e in expected}
    locn = G.norm_name(loc)
    def in_set(h): return G.norm_name(h["depto"]) in expn
    def is_exact(h): return G.norm_name(h["nombre"]) == locn
    def ok(h, capa=G.CENSAL):
        return dict(estado="auto_ok", georef_capa=capa, georef_id=h["id"], georef_nombre=h["nombre"],
                    georef_depto=h["depto"], lat=h["lat"], lon=h["lon"], dist_km="", candidatos="")
    def flag(estado, cands, capa="", dist=""):
        # grabar la coord del candidato PROPUESTO (cands[0]) para que las filas flag lleven su punto
        # provisional. Excepcion: flag_sin_match -> los 'cands' son solo una PISTA (no propuesta) -> sin coord.
        top = cands[0] if (cands and estado != "flag_sin_match") else None
        return dict(estado=estado, georef_capa=capa,
                    georef_id=top["id"] if top else "", georef_nombre=top["nombre"] if top else "",
                    georef_depto=top["depto"] if top else "", lat=top["lat"] if top else "",
                    lon=top["lon"] if top else "", dist_km=dist, candidatos=_cand_str(cands))

    if tipo == "sin_equivalente" or not expected:
        return flag("flag_sin_match", [])
    lower = [c for c in G.CASCADA if c != G.CENSAL]   # /localidades, /asentamientos (BAHRA)

    def cands_por_depto(capa):
        """candidatos laxos dentro de los deptos del conjunto (universo chico => el exacto siempre entra)"""
        out = []
        for dep in expected:
            out += G.query(loc, prov_g, dep, capa=capa, exacto=False, mx=10, cache=cache)
        return out

    # --- tier 1: censal, dentro del conjunto, igualdad NUESTRA ---
    cen = cands_por_depto(G.CENSAL)
    ex = [h for h in cen if is_exact(h) and in_set(h)]
    ex = list({h["id"]: h for h in ex}.values())
    if len(ex) == 1: return ok(ex[0])
    if len(ex) > 1:  return flag("flag_ambiguo", ex)

    # --- tier 2: existe exacto en la provincia pero FUERA del conjunto -> transferencia u homonimo ---
    prov_hits = [h for h in G.query(loc, prov_g, None, capa=G.CENSAL, exacto=False, mx=20, cache=cache)
                 if is_exact(h)]
    fuera = [h for h in prov_hits if not in_set(h)]
    if fuera:
        d = dist_esperado_candidato(prov_g, expected, fuera[0]["depto"])
        return flag("flag_depto", fuera, dist=d)

    # --- tier 3: cascada BAHRA, exacto (nuestro) dentro del conjunto ---
    for capa in lower:
        chits = [h for h in cands_por_depto(capa) if is_exact(h) and in_set(h)]
        if chits: return flag("flag_bahra", chits, capa=capa)

    # --- tier 4: variante de nombre DENTRO del conjunto, capa CENSAL (no exacto) ---
    var = [h for h in cen if in_set(h)]
    if var: return flag("flag_variante", var)

    # --- tier 5: variante de nombre DENTRO del conjunto, capa BAHRA (no exacto) ---
    # POR QUE EXISTE: sin esto el algoritmo era ASIMETRICO -> en censal haciamos exacto Y fuzzy, pero en
    # BAHRA solo exacto; un paraje presente en BAHRA con el nombre apenas distinto caia a sin_match sin
    # que nadie lo mirara (67 casos, 42.473 hab). Va DESPUES del censal-fuzzy: un candidato censal pesa
    # mas que uno BAHRA. OJO: el fuzzy es LAXO -> es una PROPUESTA a revisar en 3.3, no un match.
    for capa in lower:
        vb = [h for h in cands_por_depto(capa) if in_set(h)]
        if vb: return flag("flag_variante_bahra", vb, capa=capa)

    # --- miss real: fuzzy de provincia (fuera del conjunto) como pista ---
    hint = G.query(loc, prov_g, None, capa=G.CENSAL, exacto=False, mx=3, cache=cache)
    return flag("flag_sin_match", hint)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--provincia", default=None)     # provincia_canon; omitir = TODAS (corrida masiva)
    ap.add_argument("--departamento", default=None)  # departamento_canon (opcional)
    ap.add_argument("--out", default="geo_match_simple.csv")
    args = ap.parse_args()

    prov_map = {r["provincia_canonica"]: r["provincia_georef"]
                for r in csv.DictReader(open(PROV, encoding="utf-8"))}

    # crosswalk: (prov,depto) -> (tipo, [deptos_modernos])
    xw = {}
    for r in csv.DictReader(open(XW, encoding="utf-8")):
        k = (r["provincia_canon"], r["departamento_canon"])
        xw.setdefault(k, {"tipos": set(), "mod": []})
        xw[k]["tipos"].add(r["tipo"])
        if r["depto_moderno"]:
            xw[k]["mod"].append(r["depto_moderno"])

    cache = G._load_cache()
    rows_out, seen = [], 0
    for r in csv.DictReader(open(VISTA, encoding="utf-8")):
        if args.provincia and r["provincia_canon"] != args.provincia: continue
        if args.departamento and r["departamento_canon"] != args.departamento: continue
        prov_g = prov_map[r["provincia_canon"]]
        seen += 1
        k = (r["provincia_canon"], r["departamento_canon"])
        info = xw.get(k, {"tipos": {"?"}, "mod": []})
        # CONJUNTO PERMITIDO = todos los depto_moderno del crosswalk para ese depto 1960
        expected = info["mod"]
        tipo = "sin_equivalente" if not expected else "set"
        res = match_one(r["localidad_canon"], prov_g, expected, tipo, cache)
        rows_out.append({"page": r["page"], "n_orden": r["n_orden"], "localidad_canon": r["localidad_canon"],
                         "provincia_georef": prov_g, "departamento_canon": r["departamento_canon"],
                         "depto_moderno_esperado": " | ".join(expected), **res})
        if seen % 200 == 0:
            G._save_cache(cache); print(f"  ... {seen} procesadas", flush=True)
    G._save_cache(cache)

    outpath = os.path.join(BASE, args.out)
    with open(outpath, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS); w.writeheader(); w.writerows(rows_out)

    from collections import Counter
    tc = Counter(r["estado"] for r in rows_out)
    print(f"{args.out}: {seen} localidades -> {dict(tc)}")
    if args.provincia:   # modo piloto: detalle por fila
        for r in rows_out:
            mark = "" if r["estado"] == "auto_ok" else "   <<<"
            print(f"  {r['localidad_canon']:<28} [{r['departamento_canon']}] -> {r['estado']:<14} "
                  f"{r['georef_nombre'] or r['candidatos'][:60]}{mark}")

if __name__ == "__main__":
    main()

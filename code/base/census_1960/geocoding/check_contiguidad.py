# -*- coding: utf-8 -*-
"""
Paso 3.3b-2 — CHEQUEO DE CONTIGÜIDAD (evidencia geográfica dura, determinística).

Una transferencia de límite REAL exige que el depto de 1960 y el depto moderno sean CONTIGUOS
(comparten borde). Si NO lo son, es un homónimo, no una transferencia → no se acepta como
`transferencia_limite_declarado`. Complementa la unicidad-de-nombre con una 2ª verificación
independiente, así el bloque `limite_no_documentada` no descansa solo en el nombre.

Fuente de geometría: ref/deptos_argentina.geojson (polígonos de departamentos; provincia+departamento).
Salida: contiguidad_transfer.csv (page, n_orden, localidad, provincia, depto_1960, depto_moderno,
contiguo, dist_borde_km).
"""
import csv, os, sys, json, unicodedata
from shapely.geometry import shape

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GEO = os.path.join(BASE, "ref", "deptos_argentina.geojson")
REV = os.path.join(BASE, "revision_3.3_web.csv")
OUT = os.path.join(BASE, "contiguidad_transfer.csv")

TOL_KM = 0.2  # < 0.2 km de distancia entre polígonos = comparten borde (tolerancia de precisión)

ABREV = {"lib": "libertador", "gral": "general", "cnel": "coronel", "cptan": "capitan",
         "pte": "presidente", "gob": "gobernador", "cmte": "comandante", "grl": "general"}


def norm(s):
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    toks = s.casefold().replace("-", " ").replace(".", " ").split()
    toks = [ABREV.get(t, t) for t in toks]
    return " ".join(toks)


def load_index():
    gj = json.load(open(GEO, encoding="utf-8"))
    idx = {}
    for f in gj["features"]:
        p = f["properties"]
        prov = p["provincia"] if isinstance(p["provincia"], str) else p["provincia"].get("nombre", "")
        idx[(norm(prov), norm(p["departamento"]))] = shape(f["geometry"])
    return idx


def lookup(idx, prov, depto):
    g = idx.get((prov, depto))
    if g is not None:
        return g
    # fallback: contención de tokens dentro de la provincia
    cands = [k for k in idx if k[0] == prov and (depto in k[1] or k[1] in depto)]
    return idx[cands[0]] if len(cands) == 1 else None


def main():
    idx = load_index()
    rows = list(csv.DictReader(open(REV, encoding="utf-8")))
    blk = [r for r in rows if r["tipo_doc"] == "limite_no_documentada"]
    out = []
    for r in blk:
        prov, d1, d2 = norm(r["provincia"]), norm(r["depto_1960"]), norm(r["depto_del_punto"])
        g1, g2 = lookup(idx, prov, d1), lookup(idx, prov, d2)
        if g1 is None or g2 is None:
            cont, dist = "sin_poligono", ""
        else:
            dist = round(g1.distance(g2) * 111, 2)
            cont = "contiguo" if dist < TOL_KM else "no_contiguo"
        out.append({"page": r["page"], "n_orden": r["n_orden"], "localidad": r["localidad"],
                    "provincia": r["provincia"], "depto_1960": r["depto_1960"],
                    "depto_moderno": r["depto_del_punto"], "contiguo": cont, "dist_borde_km": dist})
    cols = ["page", "n_orden", "localidad", "provincia", "depto_1960", "depto_moderno",
            "contiguo", "dist_borde_km"]
    out.sort(key=lambda x: (x["page"], int(x["n_orden"])))
    with open(OUT, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(out)
    from collections import Counter
    print(f"contiguidad_transfer.csv: {len(out)} pares", file=sys.stderr)
    print("contiguo:", dict(Counter(r["contiguo"] for r in out)), file=sys.stderr)
    noc = [r["localidad"] for r in out if r["contiguo"] != "contiguo"]
    if noc:
        print("NO contiguos / sin poligono:", noc, file=sys.stderr)


if __name__ == "__main__":
    main()

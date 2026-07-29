# -*- coding: utf-8 -*-
"""
Paso 3.3b — Input de la pasada web sobre `flag_depto` (tier B revisitado).

Determinístico, NO decide nada: extrae los ítems del Grupo A de `revision_3.3.csv`
que necesitan investigación web (todos salvo los ya resueltos en Paso 0 =
`correccion_fuente_imagen`) y les arrastra el contexto de la flag previa (estado 3.2,
candidato Georef, dist_km) desde `geo_match_simple.csv`.

Salida: web33_input.csv  (insumo del fan-out de agentes).
"""
import csv, os, sys, unicodedata

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REV = os.path.join(BASE, "revision_3.3.csv")
GEO = os.path.join(BASE, "geo_match_simple.csv")
OUT = os.path.join(BASE, "web33_input.csv")

# Paso 0 ya resuelto en decisiones.csv -> fuera de la pasada web
EXCLUIR_CRITERIO = {"correccion_fuente_imagen"}


def norm(s):
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.casefold().strip()


def load_geo_index():
    """(page, n_orden) -> lista de filas 3.2 (para el tiebreak por localidad)."""
    idx = {}
    with open(GEO, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            idx.setdefault((r["page"], r["n_orden"]), []).append(r)
    return idx


def pick_geo(idx, page, n_orden, localidad):
    rows = idx.get((page, n_orden), [])
    if not rows:
        return None
    if len(rows) == 1:
        return rows[0]
    # tiebreak por localidad (footnote(1) del conurbano comparte (page,n_orden))
    for r in rows:
        if norm(r["localidad_canon"]) == norm(localidad):
            return r
    return rows[0]


def main():
    geo = load_geo_index()
    out_rows = []
    with open(REV, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["grupo"] != "A":
                continue
            if r["criterio_deteccion"] in EXCLUIR_CRITERIO:
                continue
            g = pick_geo(geo, r["page"], r["n_orden"], r["localidad_1960"]) or {}
            out_rows.append({
                "page": r["page"],
                "n_orden": r["n_orden"],
                "localidad_1960": r["localidad_1960"],
                "provincia": r["provincia"],
                "depto_1960": r["depto_1960"],
                "poblacion": r["poblacion"],
                "estado_flag_previo": g.get("estado", ""),
                "criterio_deteccion": r["criterio_deteccion"],
                "depto_moderno_esperado": g.get("depto_moderno_esperado", ""),
                "candidato_georef_id": g.get("georef_id", ""),
                "candidato_georef_nombre": g.get("georef_nombre", ""),
                "candidato_georef_depto": g.get("georef_depto", ""),
                "dist_km": g.get("dist_km", ""),
                "cand_lat": g.get("lat", ""),
                "cand_lon": g.get("lon", ""),
                "verificado_geo_previo": r["verificado_geo"],
                "propuesta_previa": r["propuesta"],
                "fuente_previa": r["fuente"],
                "fuente_url_previa": r["fuente_url"],
                "nota_previa": r["nota"],
            })

    cols = ["page", "n_orden", "localidad_1960", "provincia", "depto_1960", "poblacion",
            "estado_flag_previo", "criterio_deteccion", "depto_moderno_esperado",
            "candidato_georef_id", "candidato_georef_nombre", "candidato_georef_depto",
            "dist_km", "cand_lat", "cand_lon", "verificado_geo_previo",
            "propuesta_previa", "fuente_previa", "fuente_url_previa", "nota_previa"]
    # orden determinístico: por poblacion desc, luego (page, n_orden)
    out_rows.sort(key=lambda x: (-int((x["poblacion"] or "0").replace(".", "") or 0),
                                 x["page"], int(x["n_orden"])))
    with open(OUT, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(out_rows)

    # resumen a stderr
    from collections import Counter
    c = Counter(r["criterio_deteccion"] for r in out_rows)
    print(f"web33_input.csv: {len(out_rows)} itemes", file=sys.stderr)
    for k, v in c.most_common():
        print(f"  {k}: {v}", file=sys.stderr)


if __name__ == "__main__":
    main()

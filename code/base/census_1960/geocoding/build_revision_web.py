# -*- coding: utf-8 -*-
"""
Paso 3.3b — Log de auditoría de la pasada web sobre `flag_depto`.

Toma:
  - web33_input.csv         (contexto de la flag previa, determinístico)
  - web33_out_*.json        (salidas del fan-out de agentes: propuesta + coord + fuente_url)
  - crosswalk_indec.csv     (conjunto permitido por depto 1960, para el guard)
Corre el GUARD geográfico (guard_ubicacion.verificar) sobre TODA coord devuelta y aplica la
REGLA DE AUTO-ACEPTACIÓN (§3.3b del README) → `veredicto`.

Salida: revision_3.3_web.csv  (arrastra la flag previa + capa web + guard + veredicto).
NADA se pisa: este es un log nuevo. Determinístico (el guard cachea en ubicacion_cache.json).
"""
import csv, os, sys, glob, json, unicodedata

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import guard_ubicacion as G

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INP = os.path.join(BASE, "web33_input.csv")
XW = os.path.join(BASE, "crosswalk_indec.csv")
TD = os.path.join(BASE, "transfer_doc.csv")
HV = os.path.join(BASE, "veredictos_3.3b.csv")
OUT = os.path.join(BASE, "revision_3.3_web.csv")
SP = os.path.join(BASE, "web33_agent_out")  # JSONs crudos de los agentes (permanentes, reproducible)


def norm(s):
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c) and unicodedata.category(c) != "Cf")
    return " ".join(s.casefold().replace("-", " ").replace(".", " ").split())


def load_conjunto():
    """(norm prov, norm depto_1960) -> set de deptos modernos permitidos."""
    d = {}
    with open(XW, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            k = (norm(r["provincia_canon"]), norm(r["departamento_canon"]))
            d.setdefault(k, set()).add(r["depto_moderno"])
    return d


def load_transfer_doc():
    """(page, n_orden, norm localidad) -> dict de transfer_doc.csv (§3.3b-2)."""
    if not os.path.exists(TD):
        return {}
    d = {}
    with open(TD, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            d[(r["page"], str(r["n_orden"]), norm(r["localidad"]))] = r
    return d


def load_veredictos_humanos():
    """(page, n_orden, norm localidad) -> decisión humana de José (ledger append-only §3.3b)."""
    if not os.path.exists(HV):
        return {}
    d = {}
    with open(HV, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            d[(r["page"], str(r["n_orden"]), norm(r["localidad"]))] = r
    return d


def load_agent_outputs():
    """(page, n_orden, norm localidad) -> dict de salida del agente."""
    out = {}
    files = sorted(glob.glob(os.path.join(SP, "web33_out_*.json")))
    for fp in files:
        with open(fp, encoding="utf-8") as f:
            arr = json.load(f)
        for o in arr:
            k = (o["page"], str(o["n_orden"]), norm(o["localidad_1960"]))
            out[k] = o
    return out, files


def has_coord(o):
    lat, lon = str(o.get("lat", "")).strip(), str(o.get("lon", "")).strip()
    return lat not in ("", "None") and lon not in ("", "None")


def decidir_veredicto(tipo, guard_color, o):
    """Regla de auto-aceptación §3.3b. Devuelve (veredicto, motivo)."""
    coord = has_coord(o)
    url = str(o.get("fuente_url", "")).strip()
    if tipo == "residuo_humano":
        # el agente mismo pidió ojo humano -> nunca auto-aceptar (aunque el guard sea verde)
        return "residuo_humano", "el agente lo marcó residuo_humano (necesita ojo humano)"
    if coord and not url:
        return "residuo_humano", "coord SIN fuente_url (prohibido aceptar)"
    if coord:
        if guard_color == "sin_depto":
            return "sin_coordenada", "guard=sin_depto (mar/exterior) -> coord RECHAZADA"
        if guard_color == "verde":
            return "aceptar", "guard=verde (coord en conjunto permitido)"
        if guard_color == "rojo":
            if tipo == "transferencia_confirmada":
                return "aceptar", "guard=rojo esperado; transferencia de limite confirmada con fuente"
            return "residuo_humano", "guard=rojo + tipo!=transferencia -> escrutinio humano"
        return "residuo_humano", "guard indeterminado"
    # sin coord
    if tipo in ("sin_coordenada", "homonimo"):
        return "sin_coordenada", f"tipo={tipo} (documentado, sin coord)"
    return "residuo_humano", "sin coord y sin resolucion clara"


def overlay_transfer_doc(row, td):
    """§3.3b-2: solo `documentada` auto-acepta; `limite_no_documentada` -> residuo (bloque
    declarado); `artefacto_fuente_depto` -> corrección de fuente. Devuelve row modificado."""
    row["tipo_doc"] = td.get("tipo_doc", "")
    row["ley_o_anio"] = td.get("ley_o_anio", "")
    row["fuente_url_cambio"] = td.get("fuente_url_cambio", "")
    tipo_doc = td.get("tipo_doc", "")
    if tipo_doc == "documentada":
        row["veredicto"] = "aceptar"
        row["motivo_veredicto"] = f"transferencia DOCUMENTADA: {td.get('ley_o_anio','')}"
        row["confianza"] = "alta"
    elif tipo_doc == "limite_no_documentada":
        row["veredicto"] = "residuo_humano"
        row["motivo_veredicto"] = ("limite declarado (README 3.1b): reasignacion entre deptos "
                                    "preexistentes, sin acto legal citable; identidad solida -> ratificar bloque")
    elif tipo_doc == "artefacto_fuente_depto":
        row["veredicto"] = "aceptar"
        row["tipo_resolucion"] = "correccion_fuente_depto"
        row["motivo_veredicto"] = "artefacto: depto mal impreso en la fuente 1960 (documentado)"
        row["confianza"] = td.get("confianza", "") or row["confianza"]
    if tipo_doc:
        row["nota"] = (row["nota"] + " || [3.3b-2] " + td.get("nota", "")).strip(" |")
    return row


def main():
    conjunto = load_conjunto()
    agentout, files = load_agent_outputs()
    transdoc = load_transfer_doc()
    humanos = load_veredictos_humanos()
    cache = G._load()

    rows = []
    faltan = []
    with open(INP, encoding="utf-8") as f:
        inp = list(csv.DictReader(f))

    for r in inp:
        k = (r["page"], r["n_orden"], norm(r["localidad_1960"]))
        o = agentout.get(k)
        if o is None:
            faltan.append((r["page"], r["n_orden"], r["localidad_1960"]))
            o = {"tipo_resolucion": "residuo_humano", "propuesta": "", "georef_id": "",
                 "lat": "", "lon": "", "fuente": "", "fuente_url": "", "confianza": "",
                 "nota": "SIN salida de agente"}
        tipo = o.get("tipo_resolucion", "residuo_humano")
        expected = conjunto.get((norm(r["provincia"]), norm(r["depto_1960"])), set())
        if has_coord(o):
            color, depto_punto = G.verificar(o["lat"], o["lon"], expected, cache=cache)
        else:
            color, depto_punto = "sin_coord", None
        veredicto, motivo = decidir_veredicto(tipo, color, o)
        row = {
            "page": r["page"], "n_orden": r["n_orden"], "localidad": r["localidad_1960"],
            "provincia": r["provincia"], "depto_1960": r["depto_1960"], "poblacion": r["poblacion"],
            "estado_flag_previo": r["estado_flag_previo"], "criterio_deteccion": r["criterio_deteccion"],
            "dist_km": r["dist_km"], "candidato_georef": r["candidato_georef_nombre"],
            "verificado_geo_previo": r["verificado_geo_previo"],
            "tipo_resolucion": tipo, "propuesta": o.get("propuesta", ""), "georef_id": o.get("georef_id", ""),
            "lat": o.get("lat", ""), "lon": o.get("lon", ""), "fuente": o.get("fuente", ""),
            "fuente_url": o.get("fuente_url", ""), "verificado_geo": color,
            "depto_del_punto": depto_punto or "", "veredicto": veredicto, "motivo_veredicto": motivo,
            "confianza": o.get("confianza", ""), "nota": o.get("nota", ""),
            "tipo_doc": "", "ley_o_anio": "", "fuente_url_cambio": "",
            "decidido_por": "auto", "estado_final": "",
        }
        td = transdoc.get(k)
        if td:
            row = overlay_transfer_doc(row, td)
        hv = humanos.get(k)
        if hv:
            # decisión humana de José (§3.3b) GANA sobre lo automático; queda registrada
            row["veredicto"] = hv["veredicto_humano"]
            row["estado_final"] = hv.get("estado_final", "")
            row["decidido_por"] = "humano (Jose)"
            row["motivo_veredicto"] = hv.get("nota_humano", "") or row["motivo_veredicto"]
            row["nota"] = (row["nota"] + " || [humano] " + hv.get("nota_humano", "")).strip(" |")
        rows.append(row)

    G._save(cache)

    cols = ["page", "n_orden", "localidad", "provincia", "depto_1960", "poblacion",
            "estado_flag_previo", "criterio_deteccion", "dist_km", "candidato_georef",
            "verificado_geo_previo", "tipo_resolucion", "propuesta", "georef_id", "lat", "lon",
            "fuente", "fuente_url", "verificado_geo", "depto_del_punto", "veredicto",
            "motivo_veredicto", "confianza", "tipo_doc", "ley_o_anio", "fuente_url_cambio",
            "decidido_por", "estado_final", "nota"]
    rows.sort(key=lambda x: (-int((x["poblacion"] or "0").replace(".", "") or 0), x["page"], int(x["n_orden"])))
    with open(OUT, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)

    # resumen
    from collections import Counter
    print(f"revision_3.3_web.csv: {len(rows)} filas (de {len(files)} archivos de agente)", file=sys.stderr)
    print("veredicto:", dict(Counter(r["veredicto"] for r in rows)), file=sys.stderr)
    print("tipo_resolucion:", dict(Counter(r["tipo_resolucion"] for r in rows)), file=sys.stderr)
    print("guard:", dict(Counter(r["verificado_geo"] for r in rows)), file=sys.stderr)
    if faltan:
        print(f"FALTAN salidas de agente ({len(faltan)}):", faltan, file=sys.stderr)


if __name__ == "__main__":
    main()

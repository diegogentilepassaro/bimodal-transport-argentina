"""build_cola_humana.py — (Paso 2) consolida TODO lo `flag` de 2.0/2.1/2.2/2.4 en
una sola cola_humana.csv para la revision humana (2.3), con recorte de imagen +
crudo + evidencia. Es el unico punto donde se decide (en 2.3); aca solo se arma.

DISEÑO EN DOS CAPAS (determinístico, documentado, replicable):
  - Capa 1 (automatica, stateless): check_numeros / validar_nombres_wiki / etc.
    recomputan flags desde el crudo inmutable. NO leen ledgers. Solo marcan.
  - Capa 2 (humana, trazable, append-only): decisiones.csv, revision_aproximados.csv,
    dudas_resueltas.csv. El estado "resuelto" vive en estos ledgers, no en los scripts.
Esta cola = f(flags − ledgers): a cada item le calcula `estado` (open|resuelto) y
`resuelto_por` (que ledger/regla lo cerro). No descarta nada en silencio; el "limpio"
es `estado==open`. Sigue siendo determinística: mismos archivos de entrada → misma cola.

Regla de defer (2.2): los nombres `sin_match`/`sin_sugerencia` (sin articulo de Wikipedia
= sin señal de typo) se difieren en masa a Paso 3 (Georef canoniza). Se materializan en
`nombres_diferidos_paso3.csv` (insumo para Paso 3, no decision humana) y se marcan resuelto.

Salida: cola_humana.csv (origen, page, n_orden, localidad, campo, valor_crudo,
evidencia, recorte, estado, resuelto_por) + nombres_diferidos_paso3.csv.
Para 2.2 sin_match (lote grande) NO se recorta; si para dudas/sumas/aproximados-open/depto.
"""
import os, csv
import lib_celda

BASE = lib_celda.BASE
CRUDO = os.path.join(BASE, "poblados_1960.csv")
DUDAS = os.path.join(BASE, "dudas_transcripcion.csv")
V_NUM = os.path.join(BASE, "validacion_numeros.csv")
V_NOM = os.path.join(BASE, "validacion_nombres.csv")
V_DEP = os.path.join(BASE, "validacion_depto.csv")
# --- Capa 2: ledgers de estado "resuelto" ---
DECIS = os.path.join(BASE, "decisiones.csv")
REV_APROX = os.path.join(BASE, "revision_aproximados.csv")
DUDAS_RES = os.path.join(BASE, "dudas_resueltas.csv")
OUT = os.path.join(BASE, "cola_humana.csv")
DIFERIDOS = os.path.join(BASE, "nombres_diferidos_paso3.csv")

HEADER = ["origen", "page", "n_orden", "localidad", "campo", "valor_crudo",
          "evidencia", "recorte", "estado", "resuelto_por"]
RECORTAR = {"2.0-duda", "2.1-suma", "2.1-incompleto", "2.2-aproximado", "2.4-depto"}
NUMERICOS = {"total", "varones", "mujeres"}
DEFER_TIPOS = {"sin_match", "sin_sugerencia"}


def _read(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _crudo_idx():
    idx = {}
    pl2n = {}   # (page, localidad) -> n_orden  (primero gana; desambigua 2.2 sin n_orden)
    with open(CRUDO, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            idx[(r["page"], r["n_orden"], r["localidad"])] = r
            pl2n.setdefault((r["page"], r["localidad"]), r["n_orden"])
    return idx, pl2n


def _ledgers():
    """Construye los indices de estado 'resuelto' desde la Capa 2 (append-only)."""
    dec = _read(DECIS)
    dec_prc = set()      # (page,row,campo) de cualquier decision fila (2.0/generico)
    dec_num_pr = set()   # (page,row) con decision numerica (resuelve 2.1)
    dec_names = set()    # (page, valor_original) de decisiones campo=localidad (typos/verbatim)
    for d in dec:
        if d.get("scope") != "fila":
            continue
        p, row, campo = d.get("page", ""), d.get("row", ""), d.get("campo", "")
        if p and row:
            dec_prc.add((p, row, campo))
            if campo in NUMERICOS:
                dec_num_pr.add((p, row))
        if campo == "localidad" and p:
            dec_names.add((p, d.get("valor_original", "")))

    rev_pl = {}          # (page, localidad) -> veredicto (revision 2.2-aproximado)
    for r in _read(REV_APROX):
        v = (r.get("veredicto") or "").strip()
        if v:
            rev_pl[(r["page"], r["localidad"])] = v

    dudas_res = set()    # (page,n_orden,campo) de dudas cerradas
    for r in _read(DUDAS_RES):
        dudas_res.add((r["page"], r["n_orden"], r["campo"]))

    return dec_prc, dec_num_pr, dec_names, rev_pl, dudas_res


def _resolver(it, dec_prc, dec_num_pr, dec_names, rev_pl, dudas_res):
    """Devuelve (estado, resuelto_por) para un item de la cola. Determinístico."""
    o, p, n = it["origen"], it["page"], it["n_orden"]
    loc, campo = it["localidad"], it["campo"]
    if o == "2.0-duda":
        if (p, n, campo) in dudas_res:
            return "resuelto", "dudas_resueltas.csv"
        if (p, n, campo) in dec_prc:
            return "resuelto", "decisiones.csv"
        return "open", ""
    if o in ("2.1-suma", "2.1-incompleto"):
        if (p, n) in dec_num_pr:
            return "resuelto", "decisiones.csv:2.3"
        return "open", ""
    if o == "2.2-aproximado":
        v = rev_pl.get((p, loc))
        if v:
            return "resuelto", f"revision_aproximados.csv:{v}"
        if (p, loc) in dec_names:
            return "resuelto", "decisiones.csv"
        return "open", ""
    if o in ("2.2-sin_match", "2.2-sin_sugerencia"):
        return "resuelto", "regla:diferido-paso3"
    if o == "2.4-depto":
        return "open", ""   # 2.4 diferido (faltan xlsx)
    return "open", ""


def _materializar_diferidos(v_nom, pl2n):
    """Regla de defer: sin_match/sin_sugerencia (todas las zonas) -> ledger para Paso 3.
    Función pura de validacion_nombres.csv; re-generable e idéntica en cada corrida."""
    filas = []
    for r in v_nom:
        if r.get("estado") == "pendiente-humano" and r.get("tipo_match") in DEFER_TIPOS:
            filas.append({
                "page": r["page"],
                "n_orden": pl2n.get((r["page"], r["localidad"]), ""),
                "localidad": r["localidad"], "provincia": r.get("provincia", ""),
                "departamento": r.get("departamento", ""), "tipo_match": r["tipo_match"],
                "motivo": "sin articulo Wikipedia -> canonizacion en Paso 3 (Georef)",
            })
    filas.sort(key=lambda x: (x["page"], x["localidad"]))
    cols = ["page", "n_orden", "localidad", "provincia", "departamento", "tipo_match", "motivo"]
    with open(DIFERIDOS, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader(); w.writerows(filas)
    return filas


def run():
    orden = lib_celda._orden_por_pagina()
    crudo, pl2n = _crudo_idx()
    v_nom = _read(V_NOM)
    items = []

    # 2.0 — dudas
    for d in _read(DUDAS):
        cr = crudo.get((d["page"], d["n_orden"], d["localidad"]), {})
        items.append({"origen": "2.0-duda", "page": d["page"], "n_orden": d["n_orden"],
                      "localidad": d["localidad"], "campo": d["campo"],
                      "valor_crudo": cr.get(d["campo"], ""), "evidencia": d["nota"]})

    # 2.1 — sumas
    for r in _read(V_NUM):
        if r["estado"] == "flag":
            items.append({"origen": "2.1-suma", "page": r["page"], "n_orden": r["n_orden"],
                          "localidad": r["localidad"], "campo": "total/varones/mujeres",
                          "valor_crudo": f"{r['total']}/{r['varones']}/{r['mujeres']}",
                          "evidencia": f"v+m={r['suma_vm']} delta={r['delta']}"})
        elif r["estado"] == "incompleto":
            items.append({"origen": "2.1-incompleto", "page": r["page"], "n_orden": r["n_orden"],
                          "localidad": r["localidad"], "campo": "numerico",
                          "valor_crudo": f"{r['total']}/{r['varones']}/{r['mujeres']}",
                          "evidencia": "celda numerica vacia (confirmar en imagen)"})

    # 2.2 — nombres (todo lo no exacto). n_orden se recupera del crudo por (page, localidad).
    for r in v_nom:
        if r["estado"] != "pendiente-humano":
            continue
        origen = "2.2-aproximado" if r["tipo_match"] == "aproximado" else "2.2-" + r["tipo_match"]
        items.append({"origen": origen, "page": r["page"],
                      "n_orden": pl2n.get((r["page"], r["localidad"]), ""),
                      "localidad": r["localidad"], "campo": "localidad",
                      "valor_crudo": r["localidad"],
                      "evidencia": f"wiki={r['tipo_match']}: {r['wiki_titulo']} {r['wiki_url']}".strip()})

    # 2.4 — depto
    for r in _read(V_DEP):
        if r.get("estado") == "flag":
            items.append({"origen": "2.4-depto", "page": "", "n_orden": "",
                          "localidad": f"{r['provincia']}/{r['departamento']}", "campo": "depto-total",
                          "valor_crudo": r["suma_transcripcion"],
                          "evidencia": f"xlsx={r['suma_xlsx']} delta={r['delta']}"})

    # --- Capa 2: marcar estado/resuelto_por por join a ledgers ---
    dec_prc, dec_num_pr, dec_names, rev_pl, dudas_res = _ledgers()
    for it in items:
        it["estado"], it["resuelto_por"] = _resolver(
            it, dec_prc, dec_num_pr, dec_names, rev_pl, dudas_res)

    # ledger materializado de diferidos a Paso 3 (regla sin_match/sin_sugerencia)
    diferidos = _materializar_diferidos(v_nom, pl2n)

    # recortes SOLO para lo que sigue open y lo amerita (evita recortar resueltos/sin_match)
    for it in items:
        it["recorte"] = ""
        if it["estado"] == "open" and it["origen"] in RECORTAR and it["page"] and it["n_orden"]:
            try:
                p = lib_celda.recortar_fila(it["page"], it["n_orden"], it["localidad"],
                                            orden=orden, sufijo="cola")
                it["recorte"] = os.path.relpath(p, BASE).replace("\\", "/")
            except Exception as e:
                it["recorte"] = f"ERROR:{e}"

    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HEADER)
        w.writeheader(); w.writerows(items)

    # resumen: open vs resuelto por origen
    from collections import Counter
    tot = Counter(it["origen"] for it in items)
    opn = Counter(it["origen"] for it in items if it["estado"] == "open")
    n_open = sum(opn.values())
    print(f"cola_humana: {len(items)} items ({n_open} open / {len(items)-n_open} resuelto) -> {OUT}")
    for k in sorted(tot):
        print(f"   {k:18} total={tot[k]:4}  open={opn.get(k,0):4}  resuelto={tot[k]-opn.get(k,0):4}")
    print(f"nombres_diferidos_paso3: {len(diferidos)} nombres -> {DIFERIDOS}")
    return items


if __name__ == "__main__":
    run()

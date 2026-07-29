"""build_vista_ancha.py — (Paso 2) ensambla la vista de inspeccion humana.

1) flags_filas.csv — banderas determinISTICAS por fila. Hoy: footnote=="1" =>
   tipo=total_partido (18 partidos del conurbano = una fila con el total del partido;
   geocoding/uso se decide en el Paso 3). El crudo NO se toca.
2) vista_ancha.csv — crudo + columna de flag + valores canonicos aplicando decisiones.csv
   (valor_final por (page, n_orden, campo)). Mientras decisiones.csv este vacio, el
   canon == crudo. Re-correr tras la revision humana 2.3 para reflejar correcciones.
"""
import os, csv

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CRUDO = os.path.join(BASE, "poblados_1960.csv")
DEC = os.path.join(BASE, "decisiones.csv")
FLAGS = os.path.join(BASE, "flags_filas.csv")
VISTA = os.path.join(BASE, "vista_ancha.csv")
CAMPOS = ["provincia", "departamento", "localidad", "footnote", "total", "varones", "mujeres"]


def _decisiones():
    """(page, n_orden, campo) -> valor_final, segun decisiones.csv (si existe).
    Solo entradas a NIVEL FILA (page y row no vacios); las de vocabulario (1B, scope=depto)
    se aplican via autoridad_depto."""
    d = {}
    if not os.path.exists(DEC):
        return d
    with open(DEC, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r.get("valor_final", "") != "" and r.get("page", "") and r.get("row", ""):
                d[(r["page"], r["row"], r["campo"])] = r["valor_final"]
    return d


def _autoridad():
    """Mapeos canonicos del Paso 1B (si existen)."""
    prov, depto = {}, {}
    ap = os.path.join(BASE, "autoridad_prov.csv")
    ad = os.path.join(BASE, "autoridad_depto.csv")
    if os.path.exists(ap):
        for r in csv.DictReader(open(ap, encoding="utf-8")):
            prov[r["provincia_cruda"]] = r["provincia_canonica"]
    if os.path.exists(ad):
        for r in csv.DictReader(open(ad, encoding="utf-8")):
            depto[(r["provincia"], r["departamento_crudo"])] = r["departamento_canonico"]
    return prov, depto


def run():
    with open(CRUDO, encoding="utf-8") as f:
        crudo = list(csv.DictReader(f))

    # 1) flags_filas.csv
    flags = [{"page": r["page"], "n_orden": r["n_orden"], "localidad": r["localidad"],
              "tipo": "total_partido",
              "motivo": "footnote (1): corresponde al total del Partido (conurbano, sin localidades internas)"}
             for r in crudo if r["footnote"] == "1"]
    with open(FLAGS, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["page", "n_orden", "localidad", "tipo", "motivo"])
        w.writeheader(); w.writerows(flags)

    # 2) vista_ancha.csv
    dec = _decisiones()
    aut_prov, aut_depto = _autoridad()
    flagset = {(r["page"], r["n_orden"]) for r in flags}
    out_cols = (["page", "n_orden"] + CAMPOS +
                [f"{c}_canon" for c in CAMPOS] + ["tipo_flag", "tiene_correccion"])
    with open(VISTA, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=out_cols)
        w.writeheader()
        for r in crudo:
            row = {"page": r["page"], "n_orden": r["n_orden"]}
            corr = False
            for c in CAMPOS:
                row[c] = r[c]
                canon = dec.get((r["page"], r["n_orden"], c), r[c])  # correcciones de fila (2.x)
                row[f"{c}_canon"] = canon
            # provincia/departamento canonico desde autoridad 1B (vocabulario)
            row["provincia_canon"] = aut_prov.get(r["provincia"], row["provincia_canon"])
            dep_canon = aut_depto.get((r["provincia"], r["departamento"]))
            if dep_canon == "" and r["departamento"] == "":   # footnote-2: depto = localidad
                dep_canon = r["localidad"]
            row["departamento_canon"] = dep_canon if dep_canon is not None else row["departamento_canon"]
            for c in CAMPOS:
                if row[f"{c}_canon"] != r[c]:
                    corr = True
            row["tipo_flag"] = "total_partido" if (r["page"], r["n_orden"]) in flagset else ""
            row["tiene_correccion"] = "si" if corr else ""
            w.writerow(row)

    print(f"flags_filas.csv: {len(flags)} filas (footnote 1 = total_partido)")
    print(f"vista_ancha.csv: {len(crudo)} filas | decisiones aplicadas={len(dec)}")


if __name__ == "__main__":
    run()

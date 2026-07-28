# -*- coding: utf-8 -*-
"""
Paso 3.3b — VOLCADO trazable de los veredictos del Grupo A.
1. Respalda revision_3.3.csv -> revision_3.3.prev.csv (nada se pisa sin backup).
2. Rellena veredicto/propuesta/coord/fuente/nota de las filas del Grupo A en revision_3.3.csv,
   desde revision_3.3_web.csv (50 web) + Paso 0 (2 ya en decisiones.csv).
3. Append en decisiones.csv una fila por CADA aceptación web (paso=3.3b; nada en silencio).
"""
import csv, os, sys, shutil, unicodedata

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REV = os.path.join(BASE, "revision_3.3.csv")
PREV = os.path.join(BASE, "revision_3.3.prev.csv")
WEB = os.path.join(BASE, "revision_3.3_web.csv")
DEC = os.path.join(BASE, "decisiones.csv")


def norm(s):
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.casefold().strip()


def main():
    web = {}
    with open(WEB, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            web[(r["page"], r["n_orden"], norm(r["localidad"]))] = r

    with open(REV, encoding="utf-8") as f:
        rev = list(csv.DictReader(f))
        revcols = rev[0].keys() if rev else []

    shutil.copy(REV, PREV)  # backup

    dec_rows = []
    updated = 0
    for r in rev:
        if r["grupo"] != "A":
            continue
        k = (r["page"], r["n_orden"], norm(r["localidad_1960"]))
        w = web.get(k)
        if w is None:
            # Paso 0 (correccion_fuente_imagen): ya resuelto en decisiones.csv
            if r["criterio_deteccion"] == "correccion_fuente_imagen":
                r["veredicto"] = "aceptar"
                r["nota"] = (r["nota"] + " | Paso 0: ratificado en decisiones.csv").strip(" |")
                updated += 1
            continue
        # actualizar la fila de revision_3.3 con la resolucion web
        r["veredicto"] = w["veredicto"]
        r["propuesta"] = w["propuesta"] or r["propuesta"]
        r["lat"], r["lon"] = w["lat"], w["lon"]
        r["verificado_geo"] = w["verificado_geo"]
        r["fuente"] = w["fuente"]
        r["fuente_url"] = w["fuente_url"]
        tipo = w["estado_final"] or w["tipo_resolucion"]
        r["nota"] = (f"[3.3b {tipo}] " + (w["motivo_veredicto"] or "") +
                     ((" | ley: " + w["ley_o_anio"]) if w["ley_o_anio"] else "")).strip()
        updated += 1
        # fila de decisiones.csv (una por aceptacion web)
        fuente = w["decidido_por"] + " + " + (w["fuente"] or "web")
        url = w["fuente_url_cambio"] or w["fuente_url"]
        dec_rows.append({
            "paso": "3.3b", "tipo": tipo, "scope": "fila", "page": r["page"], "row": r["n_orden"],
            "campo": "coordenada",
            "valor_original": w["estado_flag_previo"] + " (" + (w["candidato_georef"] or "") + ")",
            "valor_final": f"{w['lat']},{w['lon']}" + (f" [{w['georef_id']}]" if w["georef_id"] else ""),
            "motivo": (w["motivo_veredicto"] or "")[:300],
            "fuente": (fuente + " " + url).strip()[:400],
        })

    # escribir revision_3.3.csv actualizado
    with open(REV, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(revcols))
        w.writeheader()
        w.writerows(rev)

    # append a decisiones.csv
    with open(DEC, encoding="utf-8") as f:
        deccols = next(csv.reader(f))
    with open(DEC, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=deccols)
        for d in dec_rows:
            w.writerow(d)

    print(f"revision_3.3.csv: {updated} filas de Grupo A actualizadas (backup en revision_3.3.prev.csv)",
          file=sys.stderr)
    print(f"decisiones.csv: +{len(dec_rows)} filas (paso 3.3b)", file=sys.stderr)


if __name__ == "__main__":
    main()

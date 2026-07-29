"""check_numeros.py — (Paso 2.1) check determinista total == varones + mujeres.

Funcion pura de comparacion. NO corrige nada: solo parte filas en `ok` / `flag`.
Toda fila `flag` va a la cola humana 2.3 (y se le recorta la fila como evidencia).
El crudo NO se toca.

Salida: validacion_numeros.csv (page, n_orden, localidad, total, varones, mujeres,
suma_vm, delta, estado). Recortes de los flags en pages/checks/.
"""
import os, csv
import lib_celda

BASE = lib_celda.BASE
CRUDO = os.path.join(BASE, "poblados_1960.csv")
OUT = os.path.join(BASE, "validacion_numeros.csv")


def run(recortar=True):
    orden = lib_celda._orden_por_pagina()
    rows_out, flags = [], []
    with open(CRUDO, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            t, v, m = r["total"], r["varones"], r["mujeres"]
            if t == "" or v == "" or m == "":
                estado, suma, delta = "incompleto", "", ""   # ya tratado en 2.0
            else:
                suma = int(v) + int(m)
                delta = int(t) - suma
                estado = "ok" if delta == 0 else "flag"
            rows_out.append({
                "page": r["page"], "n_orden": r["n_orden"], "localidad": r["localidad"],
                "total": t, "varones": v, "mujeres": m,
                "suma_vm": suma, "delta": delta, "estado": estado,
            })
            if estado == "flag":
                flags.append(r)

    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["page", "n_orden", "localidad", "total",
                                          "varones", "mujeres", "suma_vm", "delta", "estado"])
        w.writeheader()
        w.writerows(rows_out)

    if recortar:
        for r in flags:
            try:
                lib_celda.recortar_fila(r["page"], r["n_orden"], r["localidad"],
                                        orden=orden, sufijo="num")
            except Exception as e:
                print("  recorte ERROR", r["page"], r["n_orden"], e)

    ok = sum(1 for r in rows_out if r["estado"] == "ok")
    inc = sum(1 for r in rows_out if r["estado"] == "incompleto")
    print(f"2.1 numeros: {len(rows_out)} filas | ok={ok} | flag={len(flags)} | incompleto={inc}")
    for r in flags:
        print(f"   flag {r['page']} {r['n_orden']:>3} {r['localidad']:<22} "
              f"total={r['total']} vs v+m={int(r['varones'])+int(r['mujeres'])}")
    return flags


if __name__ == "__main__":
    run()

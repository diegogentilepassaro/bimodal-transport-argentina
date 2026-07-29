# -*- coding: utf-8 -*-
"""
Paso 3.3b-2 — Volcado del fan-out de documentación de transferencia a transfer_doc.csv.
Lee scratchpad/transfer_doc_out_*.json y produce transfer_doc.csv (append-only en espíritu:
es la evidencia por-ítem del acto administrativo que movió el límite, o su ausencia declarada).
"""
import csv, os, sys, glob, json

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(BASE, "transfer_doc.csv")
SP = os.path.join(BASE, "web33_agent_out")  # JSONs crudos de los agentes (permanentes, reproducible)

COLS = ["page", "n_orden", "localidad", "tipo_doc", "fuente_cambio", "fuente_url_cambio",
        "ley_o_anio", "confianza", "nota"]


def main():
    rows = []
    for fp in sorted(glob.glob(os.path.join(SP, "transfer_doc_out_*.json"))):
        for o in json.load(open(fp, encoding="utf-8")):
            rows.append({c: str(o.get(c, "")).strip() for c in COLS})
    rows.sort(key=lambda x: (x["page"], int(x["n_orden"])))
    with open(OUT, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLS)
        w.writeheader()
        w.writerows(rows)
    from collections import Counter
    print(f"transfer_doc.csv: {len(rows)} filas", file=sys.stderr)
    print("tipo_doc:", dict(Counter(r["tipo_doc"] for r in rows)), file=sys.stderr)


if __name__ == "__main__":
    main()

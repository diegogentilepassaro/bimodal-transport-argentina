"""Paso 3.1 — Crosswalk de departamentos 1960 -> INDEC moderno.
Auto-match determinista: norm(departamento_canon) == norm(depto_moderno) UNICO dentro de la provincia
-> tipo=identidad (auto_ok). Todo lo demas -> flag (resolucion humana: rename/split/merge/sin_equivalente
con fuente_url; PROHIBIDO asignar depto moderno sin fuente).

Entrada: vista_ancha.csv (cols *_canon), autoridad_prov.csv (provincia_georef, id_georef).
Salida: crosswalk_indec.csv (cols: provincia_canon, departamento_canon, tipo, depto_moderno, id_indec,
        fuente, fuente_url, nota) — las filas identidad quedan resueltas; las flag quedan para Jose con
        candidatos en 'nota'.
Solo GET a Georef. Todo output en el sandbox _local_geocoding_1960/.
"""
import csv, json, os, time, unicodedata, urllib.request, urllib.parse, difflib

BASE = r"C:\Users\josem\repos\bimodal-transport-argentina\_local_geocoding_1960"
VISTA = os.path.join(BASE, "vista_ancha.csv")
PROV = os.path.join(BASE, "autoridad_prov.csv")
OUT = os.path.join(BASE, "crosswalk_indec.csv")
DEPTO_URL = "https://apis.datos.gob.ar/georef/api/departamentos"

def norm(s):
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.casefold()
    for ch in "-.,":
        s = s.replace(ch, " ")
    return " ".join(s.split())

def fetch_departamentos(prov_georef):
    q = urllib.parse.urlencode({"provincia": prov_georef, "campos": "id,nombre",
                                "max": 300, "orden": "nombre"})
    with urllib.request.urlopen(f"{DEPTO_URL}?{q}", timeout=60) as r:
        data = json.load(r)
    return [(d["id"], d["nombre"]) for d in data["departamentos"]]

def main():
    # provincia_canonica -> provincia_georef
    prov_rows = list(csv.DictReader(open(PROV, encoding="utf-8")))
    canon2georef = {p["provincia_canonica"]: p["provincia_georef"] for p in prov_rows}

    # deptos modernos por provincia (Georef)
    modern = {}   # provincia_canonica -> [(id, nombre_moderno), ...]
    for p in prov_rows:
        modern[p["provincia_canonica"]] = fetch_departamentos(p["provincia_georef"])
        time.sleep(0.2)

    # pares canon distintos del censo 1960
    pares = {}    # (provincia_canon, departamento_canon) -> count localidades
    for r in csv.DictReader(open(VISTA, encoding="utf-8")):
        key = (r["provincia_canon"], r["departamento_canon"])
        pares[key] = pares.get(key, 0) + 1

    out_rows, flags = [], []
    per_prov = {}
    for (prov_c, depto_c), n in sorted(pares.items()):
        mods = modern.get(prov_c, [])
        idx = {}
        for mid, mnom in mods:
            idx.setdefault(norm(mnom), []).append((mid, mnom))
        nc = norm(depto_c)
        hit = idx.get(nc, [])
        per_prov.setdefault(prov_c, {"canon": 0, "identidad": 0, "flag": 0})
        per_prov[prov_c]["canon"] += 1
        if len(hit) == 1:                                  # match exacto unico -> identidad
            mid, mnom = hit[0]
            out_rows.append({"provincia_canon": prov_c, "departamento_canon": depto_c,
                             "tipo": "identidad", "depto_moderno": mnom, "id_indec": mid,
                             "fuente": "georef", "fuente_url": DEPTO_URL, "nota": ""})
            per_prov[prov_c]["identidad"] += 1
        else:                                              # flag -> humano (con candidatos)
            cand = difflib.get_close_matches(nc, list(idx.keys()), n=3, cutoff=0.6)
            cand_disp = [f"{idx[c][0][1]} [{idx[c][0][0]}]" for c in cand]
            nota = f"n_localidades={n}; candidatos_georef={cand_disp}"
            if len(hit) > 1:
                nota = f"n_localidades={n}; AMBIGUO exacto={[h[1] for h in hit]}"
            out_rows.append({"provincia_canon": prov_c, "departamento_canon": depto_c,
                             "tipo": "flag", "depto_moderno": "", "id_indec": "",
                             "fuente": "", "fuente_url": "", "nota": nota})
            flags.append((prov_c, depto_c, n, cand_disp if len(hit) <= 1 else [h[1] for h in hit]))
            per_prov[prov_c]["flag"] += 1

    fields = ["provincia_canon", "departamento_canon", "tipo", "depto_moderno", "id_indec",
              "fuente", "fuente_url", "nota"]
    with open(OUT, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader(); w.writerows(out_rows)

    # reporte
    tot_canon = sum(v["canon"] for v in per_prov.values())
    tot_id = sum(v["identidad"] for v in per_prov.values())
    tot_flag = sum(v["flag"] for v in per_prov.values())
    print(f"Pares canon (provincia,departamento): {tot_canon} | identidad={tot_id} | flag={tot_flag}")
    print(f"Georef deptos por provincia:")
    for p in prov_rows:
        pc = p["provincia_canonica"]
        v = per_prov.get(pc, {"canon": 0, "identidad": 0, "flag": 0})
        print(f"  {pc:<22} canon={v['canon']:>3}  identidad={v['identidad']:>3}  flag={v['flag']:>2}  (georef={len(modern.get(pc,[]))})")
    print(f"\n--- FLAGS a resolver por Jose ({len(flags)}) ---")
    for prov_c, depto_c, n, cand in flags:
        print(f"  [{prov_c}] {depto_c!r} (n={n}) -> candidatos: {cand}")

if __name__ == "__main__":
    main()

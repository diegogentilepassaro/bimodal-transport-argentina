"""Paso 3.0 — Provincias: mapear provincia_canonica (23) -> nombre/id EXACTO de Georef.
Auto: norm(canon)==norm(georef) unico -> ok. Residual -> flag (humano). Unico flag esperado:
Tierra del Fuego (Georef usa nombre largo; en 1960 Territorio Nacional, provincializada 1990).
Tambien verifica que el crudo NO tenga filas CABA.
Salida: reescribe autoridad_prov.csv agregando cols provincia_georef, id_georef, estado_georef, nota_georef.
Solo lectura de red (GET). Todo output en _local_geocoding_1960/ (sandbox).
"""
import csv, json, os, unicodedata, urllib.request

BASE = r"C:\Users\josem\repos\bimodal-transport-argentina\_local_geocoding_1960"
PROV_CSV = os.path.join(BASE, "autoridad_prov.csv")
CRUDO = os.path.join(BASE, "poblados_1960.csv")
URL = "https://apis.datos.gob.ar/georef/api/provincias?campos=id,nombre&max=30&orden=nombre"

def norm(s):
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.casefold()
    for ch in "-.,":
        s = s.replace(ch, " ")
    return " ".join(s.split())

def fetch_georef_provincias():
    with urllib.request.urlopen(URL, timeout=60) as r:
        data = json.load(r)
    return [(p["id"], p["nombre"]) for p in data["provincias"]]

def main():
    georef = fetch_georef_provincias()               # [(id, nombre), ...] set moderno (incluye CABA)
    gmap = {norm(n): (i, n) for i, n in georef}

    rows = list(csv.DictReader(open(PROV_CSV, encoding="utf-8")))
    out = []
    for r in rows:
        canon = r["provincia_canonica"]
        nc = norm(canon)
        if nc in gmap:                                # match exacto normalizado
            gid, gname = gmap[nc]
            estado, nota = "ok", ""
        else:                                         # residual -> candidato por prefijo (flag humano)
            cand = [(gid, gn) for gnorm, (gid, gn) in gmap.items() if gnorm.startswith(nc)]
            if len(cand) == 1:
                gid, gname = cand[0]
                estado = "confirmado_humano"
                nota = "Georef usa nombre largo; 1960 Territorio Nacional -> provincia 1990; conf. Jose (plan 2026-07-15)"
            else:
                gid, gname, estado, nota = "", "", "flag_sin_resolver", f"candidatos={cand}"
        out.append({**r, "provincia_georef": gname, "id_georef": gid,
                    "estado_georef": estado, "nota_georef": nota})

    # verificar ausencia de CABA en el crudo
    caba_norms = {"ciudad autonoma de buenos aires", "capital federal", "ciudad de buenos aires"}
    crudo_provs = {norm(x["provincia"]) for x in csv.DictReader(open(CRUDO, encoding="utf-8"))}
    caba_hit = sorted(crudo_provs & caba_norms)

    fields = list(rows[0].keys()) + ["provincia_georef", "id_georef", "estado_georef", "nota_georef"]
    with open(PROV_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader(); w.writerows(out)

    # reporte
    print(f"Georef /provincias: {len(georef)} (moderno, incluye CABA)")
    print(f"autoridad_prov.csv: {len(out)} filas canon")
    for o in out:
        marca = "" if o["estado_georef"] == "ok" else f"  <-- {o['estado_georef']}"
        print(f"  {o['provincia_canonica']:<22} -> {o['provincia_georef']:<45} [{o['id_georef']}]{marca}")
    print(f"CABA en el crudo: {'SI -> ' + str(caba_hit) if caba_hit else 'no (correcto)'}")

if __name__ == "__main__":
    main()

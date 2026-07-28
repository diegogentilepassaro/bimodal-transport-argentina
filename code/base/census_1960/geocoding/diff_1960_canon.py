"""Paso 3.1b (QC) — diff por NOMBRE entre la fuente primaria 1960 (ref/deptos_1960_oficial.csv,
OCR-eada) y nuestro vocabulario canonico 1B (vista_ancha.csv *_canon), por provincia.
El PDF tiene ruido OCR -> se usa matching difuso para alinear; lo que no alinea se reporta.
NO decide nada: solo expone diferencias para revision humana.
"""
import csv, os, re, unicodedata, difflib
from collections import defaultdict

BASE = r"C:\Users\josem\repos\bimodal-transport-argentina\_local_geocoding_1960"
OFI = os.path.join(BASE, "ref", "deptos_1960_oficial.csv")
VISTA = os.path.join(BASE, "vista_ancha.csv")

ALIAS = {"BUENOS AIRES": "Buenos Aires", "CATAMARCA": "Catamarca", "CORDOBA": "Córdoba",
         "CORRIENTES": "Corrientes", "CHACO": "Chaco", "CHUBUT": "Chubut",
         "ENTRE RIOS": "Entre Ríos", "FORMOSA": "Formosa", "JUJUY": "Jujuy",
         "LA PAMPA": "La Pampa", "LA RIOJA": "La Rioja", "MENDOZA": "Mendoza",
         "MISIONES": "Misiones", "NEUQUEN": "Neuquén", "RIO NEGRO": "Río Negro",
         "SALTA": "Salta", "SAN JUAN": "San Juan", "SAN LUIS": "San Luis",
         "SANTA CRUZ": "Santa Cruz", "SANTA FE": "Santa Fe",
         "SANTIAGO DEL ESTERO": "Santiago del Estero", "TUCUMAN": "Tucumán",
         "TIERRA DEL FUEGO": "Tierra del Fuego"}

def norm(s):
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.upper()
    s = s.replace("1", "L").replace("0", "O")          # OCR: Linco1n, Genera1
    s = re.sub(r"[^A-Z ]", " ", s)
    return " ".join(s.split())

def main():
    ofi = defaultdict(list)
    for r in csv.DictReader(open(OFI, encoding="utf-8")):
        p = ALIAS.get(" ".join(r["provincia_pdf"].upper().split()))
        if p:
            ofi[p].append(r["nombre_pdf"])
    canon = defaultdict(set)
    for r in csv.DictReader(open(VISTA, encoding="utf-8")):
        canon[r["provincia_canon"]].add(r["departamento_canon"])

    for prov in sorted(canon):
        o = ofi.get(prov, [])
        onorm = {norm(x): x for x in o}
        cnorm = {norm(x): x for x in canon[prov]}
        solo_canon, solo_pdf = [], []
        usados = set()
        for k, v in cnorm.items():
            if k in onorm:
                usados.add(k); continue
            m = difflib.get_close_matches(k, [x for x in onorm if x not in usados], n=1, cutoff=0.82)
            if m:
                usados.add(m[0])
            else:
                solo_canon.append(v)
        solo_pdf = [onorm[k] for k in onorm if k not in usados]
        if solo_canon or solo_pdf:
            print(f"--- {prov}  (PDF {len(o)} | canon {len(canon[prov])}) ---")
            for x in sorted(solo_canon):
                print(f"    SOLO CANON (no esta en PDF 1960): {x!r}")
            for x in sorted(solo_pdf):
                print(f"    SOLO PDF   (no lo transcribimos): {x!r}")

if __name__ == "__main__":
    main()

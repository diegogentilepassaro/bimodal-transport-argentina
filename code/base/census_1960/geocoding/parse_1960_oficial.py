"""Paso 3.1b — parsea la FUENTE PRIMARIA del censo 1960 (CELADE/IPUMS 'Codigos de Provincias
Argentina 1960', ref/ar60divp.pdf) que enumera los departamentos EXISTENTES en el censo 1960 por
provincia, y los compara con nuestro vocabulario canonico 1B (vista_ancha.csv *_canon).

Valor: (a) baseline autoritativo 1960 para el crosswalk; (b) validacion independiente del Paso 1B.
OJO: el PDF esta OCR-eado -> hay ruido en los NOMBRES (Linco1n, Genera1, Quitillipi, Dapenaga...).
Los CONTEOS y la ESTRUCTURA si son confiables. No se usa como fuente de grafia.
Salida: ref/deptos_1960_oficial.csv (provincia, codigo, nombre_pdf) + reporte de conteos.
"""
import re, csv, os, unicodedata
from collections import defaultdict
from pypdf import PdfReader

BASE = r"C:\Users\josem\repos\bimodal-transport-argentina\_local_geocoding_1960"
PDF = os.path.join(BASE, "ref", "ar60divp.pdf")
OUT = os.path.join(BASE, "ref", "deptos_1960_oficial.csv")
VISTA = os.path.join(BASE, "vista_ancha.csv")

ALIAS = {  # nombre en el PDF (normalizado) -> provincia_canon nuestra
    "BUENOS AIRES": "Buenos Aires", "CATAMARCA": "Catamarca", "CORDOBA": "Córdoba",
    "CORRIENTES": "Corrientes", "CHACO": "Chaco", "CHUBUT": "Chubut",
    "ENTRE RIOS": "Entre Ríos", "FORMOSA": "Formosa", "JUJUY": "Jujuy",
    "LA PAMPA": "La Pampa", "LA RIOJA": "La Rioja", "MENDOZA": "Mendoza",
    "MISIONES": "Misiones", "NEUQUEN": "Neuquén", "RIO NEGRO": "Río Negro",
    "SALTA": "Salta", "SAN JUAN": "San Juan", "SAN LUIS": "San Luis",
    "SANTA CRUZ": "Santa Cruz", "SANTA FE": "Santa Fe",
    "SANTIAGO DEL ESTERO": "Santiago del Estero", "TUCUMAN": "Tucumán",
    "TIERRA DEL FUEGO": "Tierra del Fuego",
}

def norm(s):
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return " ".join(s.upper().split())

def main():
    txt = "\n".join((p.extract_text() or "") for p in PdfReader(PDF).pages)
    # encabezados: "PROVINCIA DE X NN" y el caso especial "TERRITORIO NACIONAL DE LA TIERRA DEL FUEGO... NN"
    hdr = re.compile(
        r"PROV[I]*NCIA\s+DE\s+([A-ZÁÉÍÓÚÑ'. ]+?)\s+(\d{2})\b"
        r"|TERRITORIO\s+NACIONAL\s+DE\s+LA\s+(TIERRA\s+DEL\s+FUEGO)[^\d]*?(\d{2})\b")
    marks = []
    for m in hdr.finditer(txt):
        nom = (m.group(1) or m.group(3) or "").strip()
        cod = m.group(2) or m.group(4)
        marks.append((m.start(), re.sub(r"\s+", " ", nom), cod))

    oficial = []
    per = defaultdict(list)
    for i, (pos, nom, cod) in enumerate(marks):
        fin = marks[i + 1][0] if i + 1 < len(marks) else len(txt)
        blk = txt[pos:fin]
        # OJO OCR: el codigo aparece como o68 / U31 / ol8 / '004.' -> tolerar o,O,l,U,I y punto final.
        # El nombre PUEDE empezar con digito (9 de Julio, 25 de Mayo, 3 de Febrero, 12 de Octubre).
        for m in re.finditer(r"(?:\d{2}-\s*)?([0-9oOlUI]{3})\.?\s+([^\n]+)", blk):
            cod_dep = m.group(1)
            if not re.search(r"\d", cod_dep):        # descartar falsos positivos sin ningun digito
                continue
            nm = re.sub(r"\s+", " ", m.group(2)).strip()
            if not nm or nm.isupper() or not re.search(r"[A-Za-zÁ-úÑñ]", nm):
                continue
            per[nom].append((cod_dep, nm))
            oficial.append({"provincia_pdf": nom, "cod_prov": cod,
                            "codigo": cod_dep, "nombre_pdf": nm})

    with open(OUT, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["provincia_pdf", "cod_prov", "codigo", "nombre_pdf"])
        w.writeheader(); w.writerows(oficial)

    canon = defaultdict(set)
    for row in csv.DictReader(open(VISTA, encoding="utf-8")):
        canon[row["provincia_canon"]].add(row["departamento_canon"])

    print(f"Provincias en el PDF: {len(marks)} | filas depto extraidas: {len(oficial)}")
    print(f"{'provincia (PDF)':<26}{'PDF 1960':>9}{'canon 1B':>10}{'delta':>7}")
    tot_p = tot_c = 0
    for nom, lst in per.items():
        ours = ALIAS.get(norm(nom))
        c = len(canon.get(ours, set())) if ours else 0
        d = len(lst) - c if ours else None
        tot_p += len(lst); tot_c += c
        flag = "" if d == 0 else "   <<<"
        print(f"{nom[:25]:<26}{len(lst):>9}{c:>10}{(d if d is not None else '?'):>7}{flag}")
    print(f"{'TOTAL':<26}{tot_p:>9}{tot_c:>10}{tot_p-tot_c:>7}")

if __name__ == "__main__":
    main()

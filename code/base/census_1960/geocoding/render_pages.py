"""Etapa 1.1 — renderiza los PDFs de localidades del censo 1960 a PNG (pypdfium2).
READ-ONLY sobre el backup; escribe PNGs solo en _local_geocoding_1960/pages/.
"""
import os, glob
import pypdfium2 as pdfium

SRC = (r"C:\Users\josem\Dropbox\Documents\Economia\__Brown\Research"
       r"\train_backUp_DiegoGoogleDrive_07_17_2024\Train\raw_data\censo1960"
       r"\2_extractedPages\localidades")
OUT = r"C:\Users\josem\repos\bimodal-transport-argentina\_local_geocoding_1960\pages"
os.makedirs(OUT, exist_ok=True)

SCALE = 200 / 72  # ~200 dpi

pdfs = sorted(glob.glob(os.path.join(SRC, "*.pdf")))
total = 0
for p in pdfs:
    base = os.path.splitext(os.path.basename(p))[0]
    # tag: "Pages from 1c1960_8" -> "v8"
    tag = "v" + base.split("1c1960_")[-1].replace(" ", "")
    doc = pdfium.PdfDocument(p)
    n = len(doc)
    total += n
    for i in range(n):
        page = doc[i]
        pil = page.render(scale=SCALE).to_pil()
        pil.save(os.path.join(OUT, f"{tag}_p{i+1:02d}.png"))
    doc.close()
    print(f"{n:3d} pp  ->  {tag:8s}  ({base})")
print(f"TOTAL pages rendered: {total}")

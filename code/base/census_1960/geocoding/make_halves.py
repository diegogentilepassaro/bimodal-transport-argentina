"""make_halves.py — regenera pages/halves/ (recortes ampliados sup/inf) desde
los PNG de pagina completa pages/v*.png. Insumo para la transcripcion por vision.
Reproducible: borrar pages/halves/ y correr este script.
"""
import os, glob
from PIL import Image

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PAGES = os.path.join(BASE, "pages")
OUT = os.path.join(PAGES, "halves")
os.makedirs(OUT, exist_ok=True)

n = 0
for p in sorted(glob.glob(os.path.join(PAGES, "v*.png"))):
    tag = os.path.basename(p).replace(".png", "")
    im = Image.open(p); W, H = im.size
    for nm, (y0, y1) in {"h1": (430, 1300), "h2": (1280, min(2120, H))}.items():
        c = im.crop((25, y0, W, y1))
        c = c.resize((int(c.width * 1.7), int(c.height * 1.7)))
        c.save(os.path.join(OUT, f"{tag}_{nm}.png")); n += 1
print(f"regenerados {n} recortes en {OUT}")

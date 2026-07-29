"""AUDITORIA de caracteres especiales (read-only) — en los nombres de Georef (vistos en el cache) y en
nuestro vocabulario canonico. Motivo: `exacto=true` de Georef fallaba en nombres con U+00AD (soft hyphen)
incrustado (mojibake sistematico tras la letra 'i' acentuada) -> 18 flags mal clasificados (125.463 hab),
p.ej. Junin 'Juni­n', Olavarria 'Olavarri­a'.

Objetivo: ver TODO el universo de caracteres problematicos antes de fijar el normalizador, en vez de
parchear solo el soft hyphen.
"""
import json, csv, os, unicodedata
from collections import Counter

BASE = r"C:\Users\josem\repos\bimodal-transport-argentina\_local_geocoding_1960"

SOSPECHOSOS = "‘’“”–—‐´` "  # comillas, guiones, acento, NBSP

def audit(nombres, etiqueta):
    cats, ejemplos = Counter(), {}
    for n in nombres:
        for ch in n:
            c = unicodedata.category(ch)
            if c == "Cf" or (c == "Zs" and ch != " ") or ch in SOSPECHOSOS:
                key = f"{c} U+{ord(ch):04X} {unicodedata.name(ch, '?')}"
                cats[key] += 1
                ejemplos.setdefault(key, set()).add(n)
    print(f"--- {etiqueta} ---")
    if not cats:
        print("   sin caracteres problematicos")
    for k, v in cats.most_common():
        print(f"   {k:<48} x{v:<4} ej: {[repr(x) for x in list(ejemplos[k])[:2]]}")
    nfd = [n for n in nombres if unicodedata.normalize("NFC", n) != n]
    print(f"   nombres NO en forma NFC (descompuestos): {len(nfd)}  {[repr(x) for x in nfd[:3]]}")
    print()

def main():
    cache = json.load(open(os.path.join(BASE, "georef_cache.json"), encoding="utf-8"))
    g = set()
    for v in cache.values():
        for h in (v or []):
            if h.get("nombre"): g.add(h["nombre"])
            if h.get("depto"): g.add(h["depto"])
            if h.get("prov"): g.add(h["prov"])
    audit(g, f"GEOREF (nombres/deptos/provs vistos en cache: {len(g)})")

    ours = set()
    for r in csv.DictReader(open(os.path.join(BASE, "vista_ancha.csv"), encoding="utf-8")):
        ours.add(r["localidad_canon"]); ours.add(r["departamento_canon"]); ours.add(r["provincia_canon"])
    audit(ours, f"NUESTRO CANON (localidad+depto+prov distintos: {len(ours)})")

    # crosswalk: los depto_moderno que escribimos a mano podrian traer basura
    xw = {r["depto_moderno"] for r in csv.DictReader(open(os.path.join(BASE, "crosswalk_indec.csv"), encoding="utf-8")) if r["depto_moderno"]}
    audit(xw, f"CROSSWALK depto_moderno (escritos a mano: {len(xw)})")

if __name__ == "__main__":
    main()

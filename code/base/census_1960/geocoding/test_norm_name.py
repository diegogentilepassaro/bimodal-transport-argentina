"""VERIFICACION del fix de caracteres especiales (correr ANTES de re-matchear).
(1) tests unitarios de G.norm_name(): invisibles, NFC/NFD, acentos, comillas, guiones, NBSP.
(2) los 18 casos reales que Georef devuelve con U+00AD -> deben dar match exacto contra nuestro nombre.
Sale con codigo != 0 si algo falla.
"""
import os, sys, unicodedata
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import geocode_georef as G

SH   = "­"   # SOFT HYPHEN (el bug real de Georef)
ZWSP = "​"   # zero-width space
BOM  = "﻿"
NBSP = " "

CASOS = [
    # (a, b, deben_ser_iguales, descripcion)
    ("Junín", "JuníSHn".replace("SH", SH),        True,  "soft hyphen incrustado (el bug de Georef)"),
    ("Olavarría", "Olavarrí" + SH + "a",           True,  "soft hyphen (caso real Olavarria)"),
    ("Frías", "Frí" + SH + "as",                   True,  "soft hyphen (caso real Frias)"),
    ("Junín", "Junín" + ZWSP,                           True,  "zero-width space"),
    ("Junín", BOM + "Junín",                            True,  "BOM al inicio"),
    ("San Martín", "San" + NBSP + "Martín",             True,  "NBSP en vez de espacio"),
    ("Junín", unicodedata.normalize("NFD", "Junín"),    True,  "entrada descompuesta (NFD)"),
    ("Junín", "Junin",                                  True,  "acento-insensible (norma README 3.2E)"),
    ("O'Higgins", "O’Higgins",                          True,  "apostrofe tipografico"),
    ("Cura-Có", "Cura – Co",                            True,  "guion largo + espacios"),
    ("Villa  Nueva", "Villa Nueva",                     True,  "espacios repetidos"),
    ("Junín", "Junín (Est.)",                           False, "nombre realmente distinto NO debe matchear"),
    ("San José", "San Javier",                          False, "nombres distintos"),
    ("Tafí del Valle", "Tafí Viejo",                    False, "nombres distintos"),
]

# los 18 reales detectados por diag_bahra.py (nombre nuestro, nombre Georef con el invisible)
REALES = [
    ("Junín", "Juní" + SH + "n"), ("Olavarría", "Olavarrí" + SH + "a"),
    ("Frías", "Frí" + SH + "as"), ("Lobería", "Loberí" + SH + "a"),
    ("María Ignacia", "Marí" + SH + "a Ignacia"), ("Santa Lucía", "Santa Lucí" + SH + "a"),
    ("Salvador María", "Salvador Marí" + SH + "a"), ("El Paraíso", "El Paraí" + SH + "so"),
    ("Agustín Roca", "Agustí" + SH + "n Roca"), ("Fortín Olavarría", "Fortí" + SH + "n Olavarría"),
    ("Benjamín Gould", "Benjamí" + SH + "n Gould"), ("Chavarría", "Chavarrí" + SH + "a"),
]

def main():
    fallos = []
    print("=== (1) tests unitarios de norm_name() ===")
    for a, b, iguales, desc in CASOS:
        na, nb = G.norm_name(a), G.norm_name(b)
        got = (na == nb)
        ok = (got == iguales)
        print(f"  [{'OK ' if ok else 'FAIL'}] {desc:<44} {a!r} vs {b!r} -> {'=' if got else '!='}")
        if not ok:
            fallos.append(f"{desc}: {a!r} vs {b!r} -> norm {na!r} vs {nb!r}")

    print("\n=== (2) los casos REALES de Georef (soft hyphen) deben matchear ===")
    for nuestro, georef in REALES:
        ok = G.norm_name(nuestro) == G.norm_name(georef)
        print(f"  [{'OK ' if ok else 'FAIL'}] {nuestro:<20} == {georef!r}")
        if not ok:
            fallos.append(f"real: {nuestro!r} vs {georef!r}")

    print("\n=== (3) el normalizador NO debe borrar informacion util ===")
    distintos = ["Junín", "Junin Viejo", "San José", "San Josecito", "Tafí", "Tafí del Valle"]
    normed = [G.norm_name(x) for x in distintos]
    colisiones = [(distintos[i], distintos[j]) for i in range(len(distintos))
                  for j in range(i + 1, len(distintos)) if normed[i] == normed[j]]
    print(f"  colisiones inesperadas: {colisiones if colisiones else 'ninguna'}")
    if colisiones:
        fallos.append(f"colisiones: {colisiones}")

    print()
    if fallos:
        print(f"FALLARON {len(fallos)}:")
        for f in fallos: print("   -", f)
        sys.exit(1)
    print("TODOS LOS TESTS PASAN")

if __name__ == "__main__":
    main()

"""Paso 3.3a — GENERADOR DE CANDIDATOS de nombre (mecanico, con rationale).

POR QUE: los `flag_sin_match` no son 318 problemas distintos. El nombre impreso en 1960 y el nombre
moderno de Georef difieren por TRANSFORMACIONES CONVENCIONALES (abreviatura, parentesis, numero-palabra,
prefijo). En vez de hardcodear sub-clases, se genera el conjunto de strings alternativos plausibles y se
consulta cada uno DENTRO del conjunto permitido de deptos.

LIMITE DE ALCANCE (README 3.3a): esto **PROPONE**, no decide. Cada propuesta registra la transformacion
que la produjo (`rationale`) para que Jose confirme. Motivo: expandir 'Pcia.' -> 'Presidencia' es una
INTERPRETACION (en castellano 'Pcia.' suele ser 'Provincia'), no una igualdad de strings; la capa
automatica tiene PROHIBIDO decidir valores.
"""
import re, unicodedata

# --- diccionarios de expansion (solo PROPUESTAS) ---
ABREV = {
    "pcia": "Presidencia", "pte": "Presidente", "gral": "General", "gob": "Gobernador",
    "cnel": "Coronel", "cap": "Capitan", "cte": "Comandante", "lib": "Libertador",
    "sta": "Santa", "sto": "Santo", "sn": "San", "dr": "Doctor", "ing": "Ingeniero",
    "est": "Estacion", "pto": "Puerto", "vla": "Villa", "grl": "General", "alte": "Almirante",
    "brig": "Brigadier", "tte": "Teniente", "sgto": "Sargento", "my": "Mayor", "fray": "Fray",
    "ntra": "Nuestra", "sra": "Senora", "flia": "Familia", "pcial": "Provincial",
}
NUM2DIG = {
    "primero": "1", "primera": "1", "dos": "2", "tres": "3", "cuatro": "4", "cinco": "5",
    "seis": "6", "siete": "7", "ocho": "8", "nueve": "9", "diez": "10", "once": "11",
    "doce": "12", "trece": "13", "catorce": "14", "quince": "15", "dieciseis": "16",
    "veinte": "20", "veinticinco": "25", "treinta": "30",
}
DIG2NUM = {v: k for k, v in NUM2DIG.items() if v not in ("1",)}
DIG2NUM["1"] = "primero"
PREFIJOS = ["Balneario", "Colonia", "Villa", "Ingenio", "Estacion", "Estación", "Puerto",
            "Barrio", "Paraje", "Campo", "Fortin", "Fortín", "Establecimiento", "Base",
            # ferroviarios / de camino (H3: aparecieron al estudiar C5)
            "Desvío", "Desvio", "Empalme", "Pueblo", "Kilómetro", "Kilometro", "Km.", "Km"]

def _sin_acentos(s):
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in s if not unicodedata.combining(c))

def candidatos(nombre):
    """Devuelve [(candidato, rationale), ...] SIN incluir el nombre original.
    Cada rationale explica que transformacion se aplico (para que Jose la evalue)."""
    out = []
    n = (nombre or "").strip()

    # 1) PARENTESIS: 'Gral. Alvarado (Miramar)' -> 'Miramar'  |  y tambien la parte de afuera
    m = re.match(r"^(.*?)\s*\(([^)]+)\)\s*$", n)
    if m:
        fuera, dentro = m.group(1).strip(), m.group(2).strip()
        if dentro: out.append((dentro, "parentesis: se usa el contenido del parentesis"))
        if fuera:  out.append((fuera, "parentesis: se usa el texto fuera del parentesis"))

    # 2) ABREVIATURAS: 'Pcia. Roque Saenz Peña' -> 'Presidencia Roque Saenz Peña'
    def expandir(s):
        toks = s.split()
        cambios, out_toks = [], []
        for t in toks:
            base = _sin_acentos(t).lower().rstrip(".")
            if t.endswith(".") and base in ABREV:
                out_toks.append(ABREV[base]); cambios.append(f"{t}->{ABREV[base]}")
            else:
                out_toks.append(t)
        return (" ".join(out_toks), cambios) if cambios else (None, None)
    exp, cambios = expandir(n)
    if exp: out.append((exp, f"abreviatura: {', '.join(cambios)}"))

    # 3) NUMERO-PALABRA <-> DIGITO: 'Veinticinco de Mayo' <-> '25 de Mayo'
    def num_a_dig(s):
        toks = s.split(); ch = []
        for i, t in enumerate(toks):
            base = _sin_acentos(t).lower()
            if base in NUM2DIG:
                toks[i] = NUM2DIG[base]; ch.append(f"{t}->{NUM2DIG[base]}")
        return (" ".join(toks), ch) if ch else (None, None)
    def dig_a_num(s):
        toks = s.split(); ch = []
        for i, t in enumerate(toks):
            base = t.rstrip("ºo°").rstrip(".")
            if base in DIG2NUM:
                toks[i] = DIG2NUM[base].capitalize(); ch.append(f"{t}->{DIG2NUM[base]}")
        return (" ".join(toks), ch) if ch else (None, None)
    for fn, et in ((num_a_dig, "numero-palabra -> digito"), (dig_a_num, "digito -> numero-palabra")):
        c, ch = fn(n)
        if c: out.append((c, f"{et}: {', '.join(ch)}"))

    # 4) PREFIJO: 'Balneario Claromecó' -> 'Claromecó' | 'Empalme Lobos' -> 'Lobos'
    for p in PREFIJOS:
        if _sin_acentos(n).lower().startswith(_sin_acentos(p).lower() + " "):
            resto = n[len(p):].strip()
            if resto: out.append((resto, f"prefijo: se quita '{p}'"))
            break

    # 5) VARIANTE de/del: 'Santa Rosa del Río Primero' <-> 'Santa Rosa de Río Primero'
    if re.search(r"\bdel\b", n, re.I):
        out.append((re.sub(r"\bdel\b", "de", n, flags=re.I), "variante: 'del' -> 'de'"))
    if re.search(r"\bde\b(?!l)", n, re.I):
        out.append((re.sub(r"\bde\b(?!l)", "del", n, count=1, flags=re.I), "variante: 'de' -> 'del'"))

    # dedupe conservando orden y sacando el original
    vistos, fin = set(), []
    for c, r in out:
        k = c.casefold()
        if c and k != n.casefold() and k not in vistos:
            vistos.add(k); fin.append((c, r))
    return fin

if __name__ == "__main__":
    for x in ["Gral. Alvarado (Miramar)", "Pcia. Roque Saenz Peña", "Veinticinco de Mayo",
              "Humberto 1º", "Balneario Claromecó", "Cte. Nicanor Otamendi", "Rodolfo Iselin (La Llave)",
              "Sta. Rosa del Río Primero", "Lib. Gral. San Martín"]:
        print(f"{x!r}")
        for c, r in candidatos(x):
            print(f"     -> {c!r:<38} [{r}]")

"""validar_depto_xlsx.py — (Paso 2.4) suma de `total` por (provincia, departamento)
en el crudo vs el xlsx de control 1c1960_3_* (Buenos Aires + La Pampa).

xlsx = SOLO check, NUNCA fuente (regla E): solo marca deltas; no fija valores.
delta==0 -> ok ; delta!=0 -> flag (todo el depto a revision humana 2.3).

GATED: si faltan los xlsx en data/raw/census/censo1960/ escribe un validacion_depto.csv
vacio (solo header) y termina sin error, dejando el paso documentado como diferido.
"""
import os, csv, glob, unicodedata, re

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO = os.path.dirname(BASE)
CRUDO = os.path.join(BASE, "poblados_1960.csv")
XLSX_DIR = os.path.join(REPO, "data", "raw", "census", "censo1960")
OUT = os.path.join(BASE, "validacion_depto.csv")
HEADER = ["provincia", "departamento", "suma_transcripcion", "suma_xlsx", "delta", "estado"]


def _norm(s):
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", s).strip().casefold()


def _suma_crudo():
    agg = {}
    with open(CRUDO, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["total"] == "":
                continue
            k = (r["provincia"], r["departamento"])
            agg[k] = agg.get(k, 0) + int(r["total"])
    return agg


def _crosswalk_localidad_depto():
    """norm(provincia, localidad) -> departamento, desde mi transcripcion (para
    asignar las localidades del xlsx a su depto)."""
    cw = {}
    with open(CRUDO, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            cw[(_norm(r["provincia"]), _norm(r["localidad"]))] = r["departamento"]
    return cw


def run():
    files = sorted(glob.glob(os.path.join(XLSX_DIR, "1c1960_3_*.xlsx")))
    if not files:
        with open(OUT, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(HEADER)
        print("2.4 depto: DIFERIDO — faltan los xlsx 1c1960_3_* en", XLSX_DIR)
        print("   (restaurar/indicar ruta para correr el cruce; validacion_depto.csv vacio)")
        return None

    import pandas as pd
    cw = _crosswalk_localidad_depto()
    xlsx_agg, sin_cw = {}, []
    for fp in files:
        df = pd.read_excel(fp)
        cols = {c.lower(): c for c in df.columns}
        cprov, cdist, cpop = cols["provincia"], cols["distrito"], cols["pop"]
        for _, row in df.iterrows():
            prov, dist, pop = row[cprov], row[cdist], row[cpop]
            if pd.isna(pop):
                continue
            dep = cw.get((_norm(str(prov)), _norm(str(dist))))
            if dep is None:
                sin_cw.append((prov, dist)); continue
            # provincia canonica = la de mi transcripcion para ese depto (match por norm)
            xlsx_agg[(_norm(str(prov)), dep)] = xlsx_agg.get((_norm(str(prov)), dep), 0) + int(pop)

    crudo_agg = _suma_crudo()
    rows_out = []
    for (prov, dep), st in sorted(crudo_agg.items()):
        sx = xlsx_agg.get((_norm(prov), dep), "")
        delta = (st - sx) if sx != "" else ""
        estado = "ok" if delta == 0 else ("flag" if delta != "" else "sin_xlsx")
        rows_out.append({"provincia": prov, "departamento": dep,
                         "suma_transcripcion": st, "suma_xlsx": sx,
                         "delta": delta, "estado": estado})
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HEADER)
        w.writeheader(); w.writerows(rows_out)
    nflag = sum(1 for r in rows_out if r["estado"] == "flag")
    print(f"2.4 depto: {len(rows_out)} deptos | flag={nflag} | sin_xlsx="
          f"{sum(1 for r in rows_out if r['estado']=='sin_xlsx')} | "
          f"xlsx sin crosswalk={len(sin_cw)}")
    return rows_out


if __name__ == "__main__":
    run()

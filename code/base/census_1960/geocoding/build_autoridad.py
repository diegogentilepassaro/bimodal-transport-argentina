"""build_autoridad.py — (Paso 1B) vocabulario canonico jerarquico (provincia, departamento).

Capa automatica determinista: normaliza + clusteriza + detecta near-variants, y PARTE
en `pasa-limpio` / `flag`. NO decide: emite autoridad_*.csv TENTATIVOS + la lista de
flags para revision humana (1B.4) + la lista ordenada por provincia para eyeball.
El crudo NO se toca. Reusa norm/lev de validar_nombres_wiki.

Salidas:
- autoridad_prov.csv  (provincia_cruda, provincia_canonica, clave_norm)
- autoridad_depto.csv (provincia, departamento_crudo, departamento_canonico, clave_norm, status, motivo)  [TENTATIVO]
- imprime FLAGS (clusters >1, near-variants, invalidos) y la lista por provincia.
"""
import os, csv, unicodedata, re
from collections import defaultdict, Counter
import validar_nombres_wiki as V   # reusa lev()

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CRUDO = os.path.join(BASE, "poblados_1960.csv")
A_PROV = os.path.join(BASE, "autoridad_prov.csv")
A_DEPTO = os.path.join(BASE, "autoridad_depto.csv")

# set cerrado de provincias/territorios 1960 (forma canonica linda)
PROV_24 = ["Buenos Aires","Capital Federal","Catamarca","Córdoba","Corrientes","Chaco","Chubut",
           "Entre Ríos","Formosa","Jujuy","La Pampa","La Rioja","Mendoza","Misiones","Neuquén",
           "Río Negro","Salta","San Juan","San Luis","Santa Cruz","Santa Fe",
           "Santiago del Estero","Tierra del Fuego","Tucumán"]


def clean_name(s):
    """MAYUSCULA sin acentos/espacios/puntuacion (estilo clean_name del pipeline R)."""
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.upper()
    return re.sub(r"[^A-Z0-9]", "", s)


def norm_prov(s):
    return clean_name(s)


def run():
    rows = list(csv.DictReader(open(CRUDO, encoding="utf-8")))
    prov_canon = {clean_name(p): p for p in PROV_24}

    # 1B.0 provincia
    prov_crudas = sorted({r["provincia"] for r in rows})
    prov_out = []
    for pc in prov_crudas:
        k = norm_prov(pc)
        canon = prov_canon.get(k, "")
        prov_out.append({"provincia_cruda": pc, "provincia_canonica": canon or "(SIN MATCH -> flag)",
                         "clave_norm": k})
    with open(A_PROV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["provincia_cruda","provincia_canonica","clave_norm"])
        w.writeheader(); w.writerows(prov_out)

    # 1B.1 clustering depto por provincia
    by_prov = defaultdict(Counter)
    for r in rows:
        by_prov[r["provincia"]][r["departamento"]] += 1

    depto_out, flags = [], []
    for prov in sorted(by_prov):
        cnt = by_prov[prov]
        clusters = defaultdict(list)
        for dep, c in cnt.items():
            clusters[clean_name(dep)].append((dep, c))
        # canonico tentativo por cluster: la grafia mas frecuente (desempate: mas larga/acentuada)
        for key, variants in clusters.items():
            variants_sorted = sorted(variants, key=lambda x: (-x[1], -len(x[0])))
            canon_tent = variants_sorted[0][0]
            multi = len(variants) > 1
            for dep, c in variants:
                status = "unico"
                motivo = ""
                if dep == "":
                    status = "flag-vacio"; motivo = "departamento vacio (footnote 2?)"
                elif multi:
                    status = "flag-variante"; motivo = f"cluster {clean_name(dep)}: " + \
                        "/".join(f"{d}({cc})" for d, cc in variants)
                depto_out.append({"provincia": prov, "departamento_crudo": dep,
                                  "departamento_canonico": canon_tent if dep != "" else "",
                                  "clave_norm": key, "status": status, "motivo": motivo})
                if status.startswith("flag"):
                    flags.append((prov, dep, c, status, motivo))

        # 1B.2 near-variants entre claves normalizadas (dist<=2)
        keys = [k for k in clusters if k]
        for i in range(len(keys)):
            for j in range(i+1, len(keys)):
                d = V.lev(keys[i], keys[j])
                if 0 < d <= 2:
                    a = clusters[keys[i]][0][0]; b = clusters[keys[j]][0][0]
                    flags.append((prov, f"{a} ~ {b}", "", "flag-near", f"dist={d} (posible mismo depto)"))

    with open(A_DEPTO, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["provincia","departamento_crudo","departamento_canonico",
                                          "clave_norm","status","motivo"])
        w.writeheader(); w.writerows(depto_out)

    # reporte
    print("== 1B.0 PROVINCIA ==")
    for p in prov_out: print(f"   {p['provincia_cruda']} -> {p['provincia_canonica']}")
    print(f"\n== FLAGS para revision humana ({len(flags)}) ==")
    for prov, dep, c, status, motivo in flags:
        print(f"   [{status}] {prov} | {dep} {('('+str(c)+')') if c!='' else ''}  {motivo}")
    print("\n== LISTA por provincia (canonico tentativo + #localidades) para eyeball ==")
    for prov in sorted(by_prov):
        canon_counts = Counter()
        for d in depto_out:
            if d["provincia"] == prov and d["departamento_canonico"]:
                canon_counts[d["departamento_canonico"]] += by_prov[prov][d["departamento_crudo"]]
        print(f"\n--- {prov}: {len(canon_counts)} deptos canonicos (tentativo) ---")
        line = sorted(canon_counts.items())
        for k, v in line:
            print(f"     {k} ({v})")
    return flags


if __name__ == "__main__":
    run()

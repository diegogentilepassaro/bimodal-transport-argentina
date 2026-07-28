"""DIAGNOSTICO C5 — antes de mandar 266 flags a investigacion manual, medir que rescataria cada
extension SISTEMATICA y DETERMINISTICA del algoritmo. No escribe propuestas: solo mide.

Hipotesis a medir (en orden de aplicacion):
  H1  BAHRA-fuzzy dentro del conjunto  <- HUECO del algoritmo: hoy hacemos censal-fuzzy y BAHRA-exacto,
                                          pero nunca BAHRA-fuzzy.
  H2  candidatos del generador x capas BAHRA (hoy el generador solo consulta censal).
  H3  prefijos que faltaban: Desvio / Empalme / Pueblo / Kilometro.
  H4  variantes de/del  ('Santa Rosa del Rio Primero' <-> 'Santa Rosa de Rio Primero').
"""
import csv, os, re, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import geocode_georef as G
from candidatos_nombre import candidatos

BASE = r"C:\Users\josem\repos\bimodal-transport-argentina\_local_geocoding_1960"
BAHRA = [c for c in G.CASCADA if c != G.CENSAL]

def variantes_de_del(n):
    out = []
    if re.search(r"\bdel\b", n, re.I): out.append((re.sub(r"\bdel\b", "de", n, flags=re.I), "variante: 'del'->'de'"))
    if re.search(r"\bde\b(?!l)", n, re.I): out.append((re.sub(r"\bde\b(?!l)", "del", n, count=1, flags=re.I), "variante: 'de'->'del'"))
    return out

def prefijos_extra(n):
    out = []
    for p in ["Desvío", "Desvio", "Empalme", "Pueblo", "Kilómetro", "Kilometro", "Km", "Km."]:
        if n.lower().startswith(p.lower() + " "):
            r = n[len(p):].strip()
            if r: out.append((r, f"prefijo extra: se quita '{p}'"))
            break
    return out

def main():
    xw = {}
    for r in csv.DictReader(open(os.path.join(BASE, "crosswalk_indec.csv"), encoding="utf-8")):
        if r["depto_moderno"]:
            xw.setdefault((r["provincia_canon"], r["departamento_canon"]), []).append(r["depto_moderno"])
    va = {(r["page"], r["n_orden"], r["localidad_canon"]): r
          for r in csv.DictReader(open(os.path.join(BASE, "vista_ancha.csv"), encoding="utf-8"))}
    c5 = [r for r in csv.DictReader(open(os.path.join(BASE, "investigacion_flags.csv"), encoding="utf-8"))
          if r["tier"] == "C5-pendiente"]

    cache = G._load_cache()
    res = {"H1 BAHRA-fuzzy in-set": [], "H2 generador x BAHRA": [],
           "H3 prefijos extra": [], "H4 de/del": [], "sin rescate": []}
    n = 0
    for r in c5:
        n += 1
        if n % 50 == 0:
            G._save_cache(cache); print(f"  ... {n}/{len(c5)}", flush=True)
        loc, prov = r["localidad"], r["provincia"]
        key = (r["page"], r["n_orden"], loc)
        v = va.get(key, {})
        expected = xw.get((v.get("provincia_canon"), v.get("departamento_canon")), [])
        pob = int(r["poblacion"] or 0)
        if not expected:
            res["sin rescate"].append((pob, loc, "conjunto de deptos VACIO (artefacto/sin_equivalente)"))
            continue
        expn = {G.norm_name(e) for e in expected}
        hallado = None

        # H1: BAHRA fuzzy dentro del conjunto
        for capa in BAHRA:
            for dep in expected:
                for h in G.query(loc, prov, dep, capa=capa, exacto=False, mx=10, cache=cache):
                    if G.norm_name(h["depto"]) in expn:
                        hallado = ("H1 BAHRA-fuzzy in-set", f"{h['nombre']} [{capa}]"); break
                if hallado: break
            if hallado: break
        # H3/H4: prefijos extra y de/del (censal)
        if not hallado:
            for cand, rat in prefijos_extra(loc) + variantes_de_del(loc):
                for dep in expected:
                    hits = G.query(cand, prov, dep, capa=G.CENSAL, exacto=False, mx=10, cache=cache)
                    ex = [h for h in hits if G.norm_name(h["nombre"]) == G.norm_name(cand)
                          and G.norm_name(h["depto"]) in expn]
                    if ex:
                        k = "H3 prefijos extra" if "prefijo" in rat else "H4 de/del"
                        hallado = (k, f"{cand} -> {ex[0]['nombre']}"); break
                if hallado: break
        # H2: candidatos del generador x capas BAHRA
        if not hallado:
            for cand, rat in candidatos(loc):
                for capa in BAHRA:
                    for dep in expected:
                        hits = G.query(cand, prov, dep, capa=capa, exacto=False, mx=10, cache=cache)
                        ex = [h for h in hits if G.norm_name(h["nombre"]) == G.norm_name(cand)
                              and G.norm_name(h["depto"]) in expn]
                        if ex:
                            hallado = ("H2 generador x BAHRA", f"{cand} -> {ex[0]['nombre']} [{capa}]"); break
                    if hallado: break
                if hallado: break

        if hallado: res[hallado[0]].append((pob, loc, hallado[1]))
        else: res["sin rescate"].append((pob, loc, ""))
    G._save_cache(cache)

    tot_n = len(c5); tot_p = sum(int(x["poblacion"] or 0) for x in c5)
    print(f"\nC5 = {tot_n} flags, pob {tot_p:,}")
    print(f"\n{'hipotesis':<26}{'n':>5}{'poblacion':>11}{'%pob C5':>9}")
    for k, v in res.items():
        p = sum(x[0] for x in v)
        print(f"{k:<26}{len(v):>5}{p:>11,}{100*p/tot_p:>8.1f}%")
    for k in ["H1 BAHRA-fuzzy in-set", "H2 generador x BAHRA", "H3 prefijos extra", "H4 de/del"]:
        if res[k]:
            print(f"\n--- {k} (top 8) ---")
            for pob, loc, det in sorted(res[k], reverse=True)[:8]:
                print(f"    {pob:>6,}  {loc:<30} => {det}")
    print(f"\n--- SIN RESCATE (top 10) — estos si van a investigacion manual ---")
    for pob, loc, det in sorted(res["sin rescate"], reverse=True)[:10]:
        print(f"    {pob:>6,}  {loc:<30} {det}")

if __name__ == "__main__":
    main()

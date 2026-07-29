"""Paso 3.3a — corre el GENERADOR DE CANDIDATOS sobre los flag_sin_match y consulta cada candidato
DENTRO del conjunto permitido de deptos. Produce PROPUESTAS (no decide) con el rationale de la
transformacion que la genero, para que Jose confirme en 3.3.

Salida: propuestas_sin_match.csv
"""
import csv, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import geocode_georef as G
from candidatos_nombre import candidatos

BASE = r"C:\Users\josem\repos\bimodal-transport-argentina\_local_geocoding_1960"

def main():
    xw = {}
    for r in csv.DictReader(open(os.path.join(BASE, "crosswalk_indec.csv"), encoding="utf-8")):
        if r["depto_moderno"]:
            xw.setdefault((r["provincia_canon"], r["departamento_canon"]), []).append(r["depto_moderno"])
    va = {(r["page"], r["n_orden"], r["localidad_canon"]): r
          for r in csv.DictReader(open(os.path.join(BASE, "vista_ancha.csv"), encoding="utf-8"))}
    rows = [r for r in csv.DictReader(open(os.path.join(BASE, "geo_match_simple.csv"), encoding="utf-8"))
            if r["estado"] == "flag_sin_match"]

    cache = G._load_cache()
    out, n = [], 0
    for r in rows:
        n += 1
        if n % 60 == 0:
            G._save_cache(cache); print(f"  ... {n}/{len(rows)}", flush=True)
        key = (r["page"], r["n_orden"], r["localidad_canon"])
        v = va.get(key)
        if not v: continue
        expected = xw.get((v["provincia_canon"], v["departamento_canon"]), [])
        if not expected: continue
        expn = {G.norm_name(e) for e in expected}
        try: pob = int(v.get("total_canon") or 0)
        except: pob = 0

        # H2: probar cada candidato contra la capa CENSAL y tambien contra las BAHRA (el censal pesa mas,
        # por eso va primero). Se anota la capa en el rationale para que Jose sepa de donde sale.
        capas = [G.CENSAL] + [c for c in G.CASCADA if c != G.CENSAL]
        hallado = None
        for capa in capas:
            for cand, rationale in candidatos(r["localidad_canon"]):
                for dep in expected:
                    hits = G.query(cand, r["provincia_georef"], dep, capa=capa,
                                   exacto=False, mx=10, cache=cache)
                    ex = [h for h in hits
                          if G.norm_name(h["nombre"]) == G.norm_name(cand) and G.norm_name(h["depto"]) in expn]
                    if ex:
                        rat = rationale if capa == G.CENSAL else f"{rationale} [capa {capa}]"
                        hallado = (cand, rat, ex[0], capa); break
                if hallado: break
            if hallado: break

        if hallado:
            cand, rationale, h, capa = hallado
            out.append({"page": r["page"], "n_orden": r["n_orden"], "localidad_1960": r["localidad_canon"],
                        "provincia": r["provincia_georef"], "depto_1960": v["departamento_canon"],
                        "poblacion": pob, "candidato_probado": cand, "rationale": rationale,
                        "georef_nombre": h["nombre"], "georef_depto": h["depto"], "georef_id": h["id"],
                        "lat": h["lat"], "lon": h["lon"], "fuente": f"georef/{capa}",
                        "confianza": ("alta (match exacto en depto del conjunto)" if capa == G.CENSAL
                                      else "media (match exacto pero en capa BAHRA)")})
    G._save_cache(cache)

    fields = ["page","n_orden","localidad_1960","provincia","depto_1960","poblacion","candidato_probado",
              "rationale","georef_nombre","georef_depto","georef_id","lat","lon","fuente","confianza"]
    with open(os.path.join(BASE, "propuestas_sin_match.csv"), "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader()
        w.writerows(sorted(out, key=lambda x: -x["poblacion"]))

    from collections import Counter
    tot_pob = sum(int(va[(r["page"],r["n_orden"],r["localidad_canon"])].get("total_canon") or 0) for r in rows
                  if (r["page"],r["n_orden"],r["localidad_canon"]) in va)
    print(f"\nflag_sin_match analizados: {len(rows)} (pob {tot_pob:,})")
    print(f"CON PROPUESTA de Georef: {len(out)} (pob {sum(o['poblacion'] for o in out):,})")
    print("  por transformacion:", dict(Counter(o["rationale"].split(":")[0] for o in out)))
    print("\n  top 15 por poblacion:")
    for o in sorted(out, key=lambda x: -x["poblacion"])[:15]:
        print(f"    {o['poblacion']:>7,}  {o['localidad_1960']:<28} -> {o['georef_nombre']:<28} [{o['rationale'][:34]}]")

if __name__ == "__main__":
    main()

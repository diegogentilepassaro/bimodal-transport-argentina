"""Paso 3.3a — arma investigacion_flags.csv: los 1070 flags de 3.2 con una PROPUESTA + FUENTE cada uno,
ordenados por poblacion 1960 (concentra la revision donde pesa el MA).

3.3a SOLO PROPONE. Nada se auto-confirma: la confirmacion es de Jose (3.3).

Fuentes de propuesta, por tier:
  A  flag_bahra / flag_variante -> el candidato de Georef ES la evidencia (fuente=georef/<capa>).
     [APARCADO: no se revisa en este paso, pero la propuesta queda grabada]
  B  flag_depto -> dist_km entre centroides (evidencia dura; NO decide).
  C  flag_sin_match -> (i) generador de candidatos (propuestas_sin_match.csv, con rationale);
                       (ii) regla CABECERA (investigada con fuente_url);
                       (iii) casos especiales con manual_coord citada;
                       (iv) residuo -> investigacion web pendiente.
  D  flag_ambiguo / bahra con >1 candidato -> desambiguar.
"""
import csv, os, re, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import geocode_georef as G

BASE = r"C:\Users\josem\repos\bimodal-transport-argentina\_local_geocoding_1960"
W = "https://es.wikipedia.org/wiki/"

# --- CABECERAS investigadas (agente + fuente citada). Solo para los casos C1 (loc==depto / f(1)). ---
# (provincia_canon, departamento_canon) -> (cabecera, fuente_url, nota)
CABECERAS = {
 ("Buenos Aires","General Sarmiento"): ("San Miguel", W+"Partido_de_General_Sarmiento",
    "f(1)=total del partido. Partido DISUELTO 1994 -> San Miguel + Jose C. Paz + Malvinas Argentinas; "
    "no existe localidad 'General Sarmiento'. San Miguel fue la cabecera toda la vida del partido. "
    "DECISION de Jose: 1 punto en San Miguel (la poblacion 1960 estaba repartida sobre los 3)"),
 ("Buenos Aires","General Viamonte"): ("Los Toldos", W+"Partido_de_General_Viamonte", "cabecera"),
 ("Buenos Aires","Adolfo Alsina"): ("Carhué", W+"Partido_de_Adolfo_Alsina", "cabecera"),
 ("Buenos Aires","Rivadavia"): ("América", W+"Partido_de_Rivadavia", "cabecera"),
 ("Buenos Aires","General Paz"): ("Ranchos", W+"Partido_de_General_Paz", "cabecera"),
 ("Salta","General Martín Miguel de Güemes"): ("General Güemes",
    W+"Departamento_General_G%C3%BCemes_(Salta)", "cabecera (mismo nombre que el depto; 'Martin Miguel de' es la forma ceremonial)"),
 ("San Juan","Albardón"): ("Villa General San Martín - Campo Afuera", W+"Departamento_Albard%C3%B3n",
    "cabecera = Villa General San Martin (NO existe pueblo 'Albardon'); Georef la lista con el nombre "
    "compuesto 'Villa General San Martin - Campo Afuera'"),
 ("San Juan","Ullún"): ("Villa Ibáñez", W+"Departamento_Ullum", "cabecera: NO existe pueblo 'Ullum'"),
 ("Misiones","Lib. Gral. San Martín"): ("Puerto Rico",
    W+"Departamento_Libertador_General_San_Mart%C3%ADn_(Misiones)", "cabecera"),
 ("Corrientes","General Paz"): ("Nuestra Señora del Rosario de Caá Catí", W+"General_Paz_(departamento)",
    "cabecera = Caa Cati. Ley 1910 renombro depto Y pueblo de Caa Cati a General Paz; el depto quedo "
    "'General Paz' y el pueblo volvio a 'Caa Cati' -> el censo 1960 nombro la cabecera con el nombre del "
    "depto. Georef la lista con el nombre completo 'Nuestra Senora del Rosario de Caa Cati'"),
 ("La Rioja","San Blas de los Sauces"): ("Salicas - San Blas", "https://es.wikipedia.org/wiki/San_Blas_(La_Rioja)",
    "cabecera = San Blas; INDEC/Georef reportan el aglomerado junto como 'Salicas - San Blas'"),
}
# --- casos ESPECIALES: no hay localidad; coordenada manual citada ---
ESPECIALES = {
 ("Buenos Aires","Isla Martín García"): (-34.1825, -58.2500, W+"Isla_Mart%C3%ADn_Garc%C3%ADa",
    "unidad censal 1960 propia (cod 119). Isla con un unico poblado/puerto sin nombre censal. "
    "OJO: depende administrativamente del partido de La Plata pero esta a ~35 km al NO -> NO usar el "
    "punto de La Plata. Coord = el poblado de la isla"),
 ("Buenos Aires","Puerto de la Plata"): (-34.85, -57.8667, W+"Puerto_de_La_Plata",
    "= 'Zona Nacional Puerto La Plata' (cod 57-120, 17.338 hab). NO es una localidad: es la zona portuaria, "
    "repartida entre Ensenada y Berisso (sin fuente que la reparta). Los 17.338 son PERSONAS del cordon "
    "Ensenada-Berisso, no infraestructura. Coord = punto del puerto (entre ambas). AMBIGUO, a confirmar"),
}

def main():
    rows = list(csv.DictReader(open(os.path.join(BASE, "geo_match_simple.csv"), encoding="utf-8")))
    va = {(r["page"], r["n_orden"], r["localidad_canon"]): r
          for r in csv.DictReader(open(os.path.join(BASE, "vista_ancha.csv"), encoding="utf-8"))}
    fla = {(r["page"], r["n_orden"], r["localidad"]) for r in
           csv.DictReader(open(os.path.join(BASE, "flags_filas.csv"), encoding="utf-8"))}
    gen = {(r["page"], r["n_orden"], r["localidad_1960"]): r for r in
           csv.DictReader(open(os.path.join(BASE, "propuestas_sin_match.csv"), encoding="utf-8"))}
    # investigacion C5 (protocolo web + guard); puede no existir aun
    C5 = {}
    _c5p = os.path.join(BASE, "c5_investigacion.csv")
    if os.path.exists(_c5p):
        C5 = {(r["page"], r["n_orden"], r["localidad"]): r for r in csv.DictReader(open(_c5p, encoding="utf-8"))}

    cache = G._load_cache()
    def buscar(nombre, prov, dep):
        """resuelve la cabecera contra Georef para obtener id + coord"""
        for h in G.query(nombre, prov, dep, capa=G.CENSAL, exacto=False, mx=10, cache=cache):
            if G.norm_name(h["nombre"]) == G.norm_name(nombre):
                return h
        return None

    out = []
    for r in rows:
        if r["estado"] == "auto_ok":
            continue
        key = (r["page"], r["n_orden"], r["localidad_canon"])
        v = va.get(key, {})
        try: pob = int(v.get("total_canon") or 0)
        except: pob = 0
        prov_c, dep_c = v.get("provincia_canon", ""), v.get("departamento_canon", "")
        base = dict(page=r["page"], n_orden=r["n_orden"], localidad=r["localidad_canon"],
                    provincia=r["provincia_georef"], depto_1960=dep_c, poblacion=pob,
                    estado_flag=r["estado"], tier="", propuesta="", tipo_resolucion="",
                    georef_id="", id_no_censal="", nombre_oficial="", lat="", lon="",
                    fuente="", fuente_url="", confianza="", nota="")

        # ============================================================================================
        # PRECEDENCIA POR CALIDAD DE EVIDENCIA (no por el estado de 3.2).
        # Las reglas INVESTIGADAS CON FUENTE (cabecera / especial) van PRIMERO: una propuesta mecanica
        # y laxa NUNCA debe pisar una respuesta investigada y verificada.
        # (Regresion real detectada: al agregar el tier BAHRA-fuzzy, 'Ullun' paso de la cabecera
        #  investigada 'Villa Ibanez' a la propuesta mecanica 'Dique Ullum' -- una REPRESA, no el pueblo.)
        # ============================================================================================
        aplica_cabecera = (G.norm_name(r["localidad_canon"]) == G.norm_name(dep_c)) or (key in fla)
        esp_r = ESPECIALES.get((prov_c, dep_c))
        cab_r = CABECERAS.get((prov_c, dep_c))
        if esp_r and aplica_cabecera:
            lat, lon, url, nota = esp_r
            base.update(tier="C-especial", tipo_resolucion="manual_coord", propuesta="(coord citada)",
                        lat=lat, lon=lon, fuente="web", fuente_url=url, confianza="media", nota=nota)
            out.append(base); continue
        if cab_r and aplica_cabecera:
            nombre, url, nota = cab_r
            h = None
            for dq in [dep_c, None]:
                h = buscar(nombre, r["provincia_georef"], dq)
                if h: break
            base.update(tier="C1-cabecera", tipo_resolucion="cabecera_del_depto", propuesta=nombre,
                        nombre_oficial=h["nombre"] if h else nombre,
                        georef_id=h["id"] if h else "", lat=h["lat"] if h else "",
                        lon=h["lon"] if h else "", fuente="web+georef", fuente_url=url,
                        confianza="alta" if h else "media (cabecera no resuelta en Georef)",
                        nota=f"el censo 1960 listo la cabecera con el nombre del depto. {nota}")
            out.append(base); continue

        # ---- Tier E: variante en capa BAHRA (tier H1). NO se aparca con el A: el fuzzy es LAXO
        # (p.ej. 'Algarrobo' -> 'Chosoico Algarrobo' casi seguro NO es el mismo lugar) -> confianza baja,
        # revision explicita. Aporta candidato donde antes no habia NINGUNO (sin_match).
        if r["estado"] == "flag_variante_bahra":
            base.update(tier="E-bahra-fuzzy", tipo_resolucion="confirmar_o_rechazar_candidato",
                        propuesta=(re.match(r"(.+?)/", r["candidatos"]).group(1)
                                   if re.match(r"(.+?)/", r["candidatos"]) else ""),
                        fuente=f"georef/{r['georef_capa']}", fuente_url="https://apis.datos.gob.ar/georef/api",
                        confianza="BAJA (fuzzy en capa BAHRA: puede no ser el mismo lugar)",
                        id_no_censal="true" if r["georef_capa"] == "localidades" else "",
                        nota=f"antes era sin_match. candidatos: {r['candidatos'][:80]}")
        # ---- Tier A: bahra / variante (APARCADO, pero la propuesta queda) ----
        elif r["estado"] in ("flag_bahra", "flag_variante"):
            n_cand = len([x for x in r["candidatos"].split(";") if x.strip()])
            if n_cand > 1:
                base.update(tier="D", tipo_resolucion="desambiguar", confianza="baja",
                            nota=f">1 candidato: {r['candidatos']}", fuente="georef")
            else:
                m = re.match(r"(.+?)/(.+?) \[(.+?)\]", r["candidatos"])
                base.update(tier="A (aparcado)", tipo_resolucion="confirmar_candidato_georef",
                            propuesta=m.group(1) if m else "", nombre_oficial=m.group(1) if m else "",
                            georef_id=m.group(3) if m else "", fuente=f"georef/{r['georef_capa']}",
                            fuente_url="https://apis.datos.gob.ar/georef/api",
                            confianza="alta" if r["estado"] == "flag_bahra" else "media (nombre difiere)",
                            id_no_censal="true" if r["georef_capa"] == "localidades" else "",
                            nota=("BAHRA: el nombre no es localidad censal -> BAHRA es la fuente correcta (P7). "
                                  "id de capa 'localidades' NO es id censal INDEC valido"
                                  if r["georef_capa"] == "localidades" else
                                  ("BAHRA: el nombre no es localidad censal -> fuente correcta (P7)"
                                   if r["estado"] == "flag_bahra" else "variante de nombre en el depto correcto")))
        # ---- Tier B: flag_depto ----
        elif r["estado"] == "flag_depto":
            d = r.get("dist_km", "")
            try: dv = float(d)
            except: dv = None
            veredicto = ("transferencia de limite plausible (adyacente)" if dv is not None and dv < 60
                         else "homonimo probable (lejano)" if dv is not None and dv >= 150
                         else "zona gris")
            base.update(tier="B", tipo_resolucion="transferencia_vs_homonimo",
                        confianza="media", fuente="georef+centroides",
                        fuente_url="https://apis.datos.gob.ar/georef/api/departamentos",
                        nota=f"dist_km={d} -> {veredicto}. candidatos: {r['candidatos'][:70]}")
        # ---- Tier D: ambiguo ----
        elif r["estado"] == "flag_ambiguo":
            base.update(tier="D", tipo_resolucion="desambiguar", confianza="baja", fuente="georef",
                        nota=f"candidatos: {r['candidatos']}")
        # ---- Tier C: sin_match (cabecera/especial ya se resolvieron arriba, por precedencia) ----
        elif r["estado"] == "flag_sin_match":
            c5 = C5.get(key)
            g = gen.get(key)
            if c5 and c5.get("lat"):   # investigacion C5 (protocolo web + guard) lo resolvio con coord
                base.update(tier="C5-" + c5["tipo_resolucion"], tipo_resolucion=c5["tipo_resolucion"],
                            propuesta=c5["propuesta"], lat=c5["lat"], lon=c5["lon"],
                            fuente=c5["fuente"], fuente_url=c5["fuente_url"],
                            confianza="verde (guard: coord en depto esperado)" if c5["verificado_geo"] == "verde"
                                      else f"REVISAR ({c5['verificado_geo']})",
                            nota=f"[guard={c5['verificado_geo']}; punto en {c5['depto_del_punto']}] {c5['nota'][:120]}")
            elif c5 and c5["tipo_resolucion"] == "sin_coordenada":
                base.update(tier="C5-sin_coordenada", tipo_resolucion="sin_coordenada",
                            confianza="", nota=f"[C5 web] {c5['nota'][:140]}")
            elif g:   # el generador de candidatos lo resolvio
                base.update(tier="C-generador", tipo_resolucion="confirmar_candidato_georef",
                            propuesta=g["georef_nombre"], nombre_oficial=g["georef_nombre"],
                            georef_id=g["georef_id"], lat=g["lat"], lon=g["lon"],
                            fuente=g.get("fuente", "georef/localidades-censales"),
                            fuente_url="https://apis.datos.gob.ar/georef/api",
                            confianza=g.get("confianza", "alta (match exacto en depto del conjunto)"),
                            nota=f"candidato probado: {g['candidato_probado']!r} [{g['rationale']}]")
            else:
                base.update(tier="C5-pendiente", tipo_resolucion="", confianza="",
                            nota=("investigacion web/imagen PENDIENTE. pista: " + r["candidatos"][:60])
                                 if r["candidatos"].strip() else "investigacion web/imagen PENDIENTE (sin pista)")
        out.append(base)
    G._save_cache(cache)

    out.sort(key=lambda x: -x["poblacion"])
    fields = list(out[0].keys())
    with open(os.path.join(BASE, "investigacion_flags.csv"), "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(out)

    from collections import Counter
    tc = Counter(x["tier"] for x in out)
    pc = Counter()
    for x in out: pc[x["tier"]] += x["poblacion"]
    tot = sum(x["poblacion"] for x in out)
    print(f"investigacion_flags.csv: {len(out)} flags (control: deben ser 1070) | poblacion {tot:,}")
    print(f"\n{'tier':<18}{'n':>6}{'poblacion':>12}{'%pob':>7}")
    for k, n in tc.most_common():
        print(f"{k:<18}{n:>6}{pc[k]:>12,}{100*pc[k]/tot:>6.1f}%")
    con = sum(1 for x in out if x["propuesta"] or x["lat"])
    print(f"\nCON propuesta: {con} ({sum(x['poblacion'] for x in out if x['propuesta'] or x['lat']):,} hab)")
    print(f"SIN propuesta (C5 pendiente): {len(out)-con} ({sum(x['poblacion'] for x in out if not (x['propuesta'] or x['lat'])):,} hab)")
    sinf = [x for x in out if (x["propuesta"] or x["lat"]) and not x["fuente"]]
    print(f"VERIFICACION - propuestas sin fuente: {len(sinf)} (debe ser 0)")

if __name__ == "__main__":
    main()

"""Paso 3.1 + 3.1b — construye crosswalk_indec.csv v2 = CONJUNTO PERMITIDO de deptos modernos por
depto 1960. Reemplaza al v1 (que solo hacia name-match y no modelaba la historia territorial).

Composicion (3 capas):
  (1) name-match automatico  -> identidad (nombre 1960 == nombre moderno de Georef)
  (2) resoluciones humanas   -> rename / especial / sin_equivalente  (los 45 flags del v1, P3 corregido)
  (3) HISTORIA 3.1b          -> splits: deptos modernos CREADOS DESPUES del censo (30-sep-1960) y su
                                padre 1960. Investigacion documentada (4 agentes) + validada contra la
                                fuente primaria CELADE 1960 (ref/ar60divp.pdf).

Salida: crosswalk_indec.csv (una fila por (prov, depto_1960, depto_moderno)); el CONJUNTO PERMITIDO de
un depto 1960 = todas sus filas. 3.2 exige depto_georef ∈ conjunto.
"""
import csv, json, os, time, unicodedata, urllib.request, urllib.parse

BASE = r"C:\Users\josem\repos\bimodal-transport-argentina\_local_geocoding_1960"
VISTA, PROV = os.path.join(BASE, "vista_ancha.csv"), os.path.join(BASE, "autoridad_prov.csv")
OUT = os.path.join(BASE, "crosswalk_indec.csv")
GRF = "https://apis.datos.gob.ar/georef/api/departamentos"

def norm(s):
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c)).casefold()
    for ch in "-.,":
        s = s.replace(ch, " ")
    return " ".join(s.split())

# ---- (2) resoluciones humanas de los 45 flags del v1 (P3 CORREGIDO) -------------------------------
W = "https://es.wikipedia.org/wiki/"
RES = {
 ("Buenos Aires","3 de Febrero"): [("identidad","Tres de Febrero","georef",GRF,"grafia numero->palabra")],
 ("Buenos Aires","General Lamadrid"): [("identidad","General La Madrid","georef",GRF,"grafia espaciado")],
 ("Buenos Aires","González Chaves"): [("rename","Adolfo Gonzales Chaves","georef",GRF,"nombre expandido")],
 ("Buenos Aires","Juárez"): [("rename","Benito Juárez","georef",GRF,"nombre expandido")],
 ("Chaco","Cap. Gral. O'Higgins"): [("identidad","O'Higgins","georef",GRF,"abreviatura")],
 ("Chaco","Doce de Octubre"): [("identidad","12 de Octubre","georef",GRF,"numero->palabra (fuente 1960 usa '12 de Octubre')")],
 ("Chaco","Fray J. Sta. María de Oro"): [("identidad","Fray Justo Santa María de Oro","georef",GRF,"abreviatura")],
 ("Chaco","Lib. Gral. San Martín"): [("identidad","Libertador General San Martín","georef",GRF,"abreviatura")],
 ("Chaco","Nueve de Julio"): [("identidad","9 de Julio","georef",GRF,"numero->palabra")],
 ("Chaco","Primero de Mayo"): [("identidad","1° de Mayo","georef",GRF,"numero->palabra")],
 ("Chaco","Veinticinco de Mayo"): [("identidad","25 de Mayo","georef",GRF,"numero->palabra")],
 ("Córdoba","Pte. Roque Sáenz Peña"): [("identidad","Presidente Roque Sáenz Peña","georef",GRF,"abreviatura")],
 ("La Pampa","Conhello"): [("identidad","Conhelo","georef",GRF,"grafia")],
 ("La Pampa","Cura-Có"): [("identidad","Curacó","georef",GRF,"grafia")],
 ("Misiones","General Belgrano"): [("rename","General Manuel Belgrano","georef",GRF,"nombre expandido")],
 ("Misiones","Lib. Gral. San Martín"): [("identidad","Libertador General San Martín","georef",GRF,"abreviatura")],
 ("Salta","Candelaria"): [("identidad","La Candelaria","georef",GRF,"articulo")],
 ("Salta","General Martín Miguel de Güemes"): [("identidad","General Güemes","georef",GRF,"forma corta")],
 ("Salta","Gral. J. de San Martín"): [("identidad","General José de San Martín","georef",GRF,"abreviatura")],
 ("San Juan","Nueve de Julio"): [("identidad","9 de Julio","georef",GRF,"numero->palabra")],
 ("San Juan","Ullún"): [("identidad","Ullum","georef",GRF,"grafia")],
 ("San Juan","Veinticinco de Mayo"): [("identidad","25 de Mayo","georef",GRF,"numero->palabra")],
 ("Santa Fe","Nueve de Julio"): [("identidad","9 de Julio","georef",GRF,"DOBLE-GRAFIA 1B: = '9 de Julio'; fuente 1960 lista 'Nueve de Julio' (19 deptos)")],
 ("Río Negro","General Conesa"): [("rename","Conesa","georef",GRF,"forma corta")],
 ("Buenos Aires","Bartolomé Mitre"): [("rename","Arrecifes","web",W+"Partido_de_Arrecifes","nombre 'Bartolomé Mitre' 1901-1997")],
 ("Buenos Aires","Caseros"): [("rename","Daireaux","web",W+"Partido_de_Daireaux","Ley prov. 7613/1970")],
 ("Jujuy","Capital"): [("rename","Dr. Manuel Belgrano","web","https://en.wikipedia.org/wiki/Doctor_Manuel_Belgrano_Department","fuente 1960 lista 'Capital'; nombre moderno Georef = 'Dr. Manuel Belgrano'")],
 ("Mendoza","Luján"): [("rename","Luján de Cuyo","web",W+"Departamento_de_Luj%C3%A1n_de_Cuyo","renombrado 1964")],
 ("San Luis","La Capital"): [("rename","Juan Martín de Pueyrredón","web",W+"Departamento_La_Capital_(San_Luis)","Ley V-0748/2010")],
 ("San Luis","San Martín"): [("rename","Libertador General San Martín","web",W+"Departamento_La_Capital_(San_Luis)","Georef no tiene 'San Martín' a secas en San Luis")],
 ("Santiago del Estero","Matará"): [("rename","Juan Felipe Ibarra","web","https://en.wikipedia.org/wiki/Juan_Felipe_Ibarra_Department","Ley prov. 4091/1974 (RENAME, no split — confirmado 3.1b)")],
 ("Tierra del Fuego","San Sebastián"): [("rename","Río Grande","web",W+"Anexo:Departamentos_de_la_provincia_de_Tierra_del_Fuego,_Ant%C3%A1rtida_e_Islas_del_Atl%C3%A1ntico_Sur","Decreto 149/70 (RENAME)")],
 ("La Rioja","General Lavalle"): [("rename","General Felipe Varela","web",W+"Departamento_General_Felipe_Varela","cabecera Villa Unión")],
 ("La Rioja","General Ocampo"): [("rename","General Ortiz de Ocampo","web",W+"Departamento_General_Ortiz_de_Ocampo","forma completa")],
 ("La Rioja","General Roca"): [("rename","Rosario Vera Peñaloza","web",W+"Departamento_Rosario_Vera_Pe%C3%B1aloza","Ley 2890/1964")],
 ("La Rioja","General Sarmiento"): [("rename","Vinchina","web",W+"Departamento_Vinchina","revertido a Vinchina 1989")],
 ("La Rioja","Gobernador Gordillo"): [("rename","Chamical","web","https://en.wikipedia.org/wiki/Chamical_Department","renombrado 1987")],
 ("La Rioja","Rivadavia"): [("rename","General Juan Facundo Quiroga","web",W+"Departamento_General_Juan_Facundo_Quiroga","renombrado 1948; la fuente 1960 aun imprime 'Rivadavia'")],
 # P3 CORREGIDO: Georef = 'Ángel Vicente Peñaloza' (SIN 'General')
 ("La Rioja","Vélez Sarsfield"): [("rename","Ángel Vicente Peñaloza","web",W+"Departamento_General_%C3%81ngel_V._Pe%C3%B1aloza","Ley 2890/1964. P3 CORREGIDO: Georef lo llama 'Ángel Vicente Peñaloza' [46056], sin 'General'")],
 ("Buenos Aires","Isla Martín García"): [("especial","La Plata","web","https://en.wikipedia.org/wiki/Mart%C3%ADn_Garc%C3%ADa_Island","unidad censal 1960 propia (cod 119, confirmado en fuente primaria); exclave asignado a La Plata")],
 ("Buenos Aires","Puerto de la Plata"): [("especial","Ensenada","web","https://observatorioconurbano.ungs.edu.ar/","= 'Zona Nacional Puerto La Plata' (cod 57-120 en fuente primaria 1960); reparto Ensenada/Berisso")],
 ("La Pampa","Caseros"): [("sin_equivalente","","web",W+"Partido_de_Daireaux","ANOMALIA confirmada vs fuente primaria: La Pampa 1960 NO tiene depto 'Caseros' (22 deptos). Localidad (La Larga) seria de BA Daireaux -> 3.3")],
 ("Chubut","Chubut"): [("sin_equivalente","","web",W+"Anexo:Departamentos_de_la_Provincia_del_Chubut","ARTEFACTO confirmado vs fuente primaria: Chubut 1960 tiene 15 deptos, ninguno 'Chubut' -> REVISAR IMAGEN")],
}

# ---- (3) HISTORIA 3.1b: deptos modernos creados DESPUES del censo (30-sep-1960) -> padre(s) 1960 ----
# (provincia, padre_1960) -> [(hijo_moderno, nota_ley, fuente_url), ...]
N = "https://normas.gba.gob.ar/"
SPLITS = {
 # --- Buenos Aires (18 creaciones) ---
 ("Buenos Aires","Quilmes"): [("Berazategui","Ley 6317 (4-nov-1960): creado 5 semanas DESPUES del censo -> en 1960 estaba dentro de Quilmes", W+"Partido_de_Berazategui")],
 ("Buenos Aires","Bartolomé Mitre"): [("Capitán Sarmiento","Ley 6485 (1961); padre = Arrecifes/Bartolomé Mitre", W+"Partido_de_Capitán_Sarmiento")],
 ("Buenos Aires","Pellegrini"): [("Salliqueló","Ley 6625 (1961)", N), ("Tres Lomas","Ley 10469 (1986)", N)],
 ("Buenos Aires","General Lavalle"): [("La Costa","Dec-ley 9024/78", N)],
 ("Buenos Aires","General Juan Madariaga"): [("Pinamar","Dec-ley 9024/78", N), ("Villa Gesell","Dec-ley 9024/78", N)],
 ("Buenos Aires","Coronel Dorrego"): [("Monte Hermoso","Dec-ley 9245/79", W+"Partido_de_Monte_Hermoso")],
 ("Buenos Aires","Coronel de Marina Leonardo Rosales"): [("Monte Hermoso","Dec-ley 9245/79 (2do padre)", W+"Partido_de_Monte_Hermoso")],
 ("Buenos Aires","General Pinto"): [("Florentino Ameghino","Ley 11071 (1991): SOLO de General Pinto", N)],
 ("Buenos Aires","Esteban Echeverría"): [("Presidente Perón","Ley 11480 (1993)", N), ("Ezeiza","Ley 11550 (1994): SOLO de Esteban Echeverría", N)],
 ("Buenos Aires","San Vicente"): [("Presidente Perón","Ley 11480 (1993)", N)],
 ("Buenos Aires","Florencio Varela"): [("Presidente Perón","Ley 11480 (1993)", N)],
 ("Buenos Aires","General Sarmiento"): [("José C. Paz","Ley 11551 (1994): Gral Sarmiento DISUELTO", N),
                                        ("Malvinas Argentinas","Ley 11551 (1994)", N),
                                        ("San Miguel","Ley 11551 (1994)", N)],
 ("Buenos Aires","Pilar"): [("Malvinas Argentinas","Ley 11551 (1994): Tortuguitas pasa a Malvinas Arg.", N)],
 ("Buenos Aires","Magdalena"): [("Punta Indio","Ley 11584 (1994)", N)],
 ("Buenos Aires","Morón"): [("Hurlingham","Ley 11610 (1994/95)", N), ("Ituzaingó","Ley 11610 (1994/95)", N)],
 ("Buenos Aires","Chascomús"): [("Lezama","Ley 14087 (2009)", W+"Partido_de_Lezama")],
 # --- Tucumán (6 creaciones, ley 4518/1976; re-creadas por ley 6143/1991) ---
 ("Tucumán","Río Chico"): [("Juan Bautista Alberdi","ley 4518 (1976)", "http://biblioteca.cfi.org.ar/"),
                           ("Simoca","ley 4518 (1976)", "http://biblioteca.cfi.org.ar/")],
 ("Tucumán","Graneros"): [("La Cocha","ley 4518 (1976)", "http://biblioteca.cfi.org.ar/")],
 ("Tucumán","Famaillá"): [("Lules","ley 4518 (1976)", "http://biblioteca.cfi.org.ar/"),
                          ("Yerba Buena","ley 4518 (1976)", W+"Anexo:Departamentos_de_la_provincia_de_Tucumán")],
 ("Tucumán","Chicligasta"): [("Simoca","ley 4518 (1976)", "http://biblioteca.cfi.org.ar/")],
 ("Tucumán","Monteros"): [("Simoca","ley 4518 (1976)", "http://biblioteca.cfi.org.ar/")],
 ("Tucumán","Capital"): [("Yerba Buena","ley 4518 (1976)", W+"Anexo:Departamentos_de_la_provincia_de_Tucumán")],
 ("Tucumán","Tafí"): [("Tafí del Valle","ley 4518 (1976): 'Tafí' 1960 se divide", "http://biblioteca.cfi.org.ar/"),
                      ("Tafí Viejo","ley 4518 (1976)", W+"Anexo:Departamentos_de_la_provincia_de_Tucumán"),
                      ("Yerba Buena","ley 4518 (1976): 3er destino de Tafí", W+"Anexo:Departamentos_de_la_provincia_de_Tucumán")],
 # --- Entre Ríos (3 creaciones) ---
 ("Entre Ríos","Concordia"): [("Federal","Ley 5169 (1972)", W+"Departamento_Federal"),
                              ("San Salvador","Ley 8981 (1995)", W+"Departamento_San_Salvador")],
 ("Entre Ríos","La Paz"): [("Federal","Ley 5169 (1972)", W+"Departamento_Federal")],
 ("Entre Ríos","Villaguay"): [("Federal","Ley 5169 (1972)", W+"Departamento_Federal"),
                              ("San Salvador","Ley 8981 (1995)", W+"Departamento_San_Salvador")],
 ("Entre Ríos","Colón"): [("San Salvador","Ley 8981 (1995)", W+"Departamento_San_Salvador")],
 ("Entre Ríos","Gualeguaychú"): [("Islas del Ibicuy","Ley 7297 (1984)", W+"Departamento_Islas_del_Ibicuy")],
 # --- Chaco (1) ---
 ("Chaco","Doce de Octubre"): [("2 de Abril","ley 3814 (1992)", W+"Anexo:Departamentos_de_la_provincia_del_Chaco")],
 ("Chaco","Fray J. Sta. María de Oro"): [("2 de Abril","ley 3814 (1992) (2do padre)", W+"Anexo:Departamentos_de_la_provincia_del_Chaco")],
 # --- Jujuy (1) ---
 ("Jujuy","Capital"): [("Palpalá","ley 4252 (1986)", "https://eltribunodejujuy.com/")],
 # --- Tierra del Fuego (1 relevante) ---
 ("Tierra del Fuego","San Sebastián"): [("Tolhuin","Ley prov. 1186 (2017), de Río Grande", "https://www.tierradelfuego.gob.ar/")],
}

def fetch_modern(prov_georef):
    q = urllib.parse.urlencode({"provincia": prov_georef, "campos": "id,nombre", "max": 300})
    with urllib.request.urlopen(f"{GRF}?{q}", timeout=60) as r:
        return {d["nombre"]: d["id"] for d in json.load(r)["departamentos"]}

def main():
    prov_rows = list(csv.DictReader(open(PROV, encoding="utf-8")))
    modern = {}
    for p in prov_rows:
        modern[p["provincia_canonica"]] = fetch_modern(p["provincia_georef"]); time.sleep(0.2)

    pares = sorted({(r["provincia_canon"], r["departamento_canon"])
                    for r in csv.DictReader(open(VISTA, encoding="utf-8"))})

    out, sin_id = [], []
    for prov, dep in pares:
        mods = modern.get(prov, {})
        idx = {norm(k): (k, v) for k, v in mods.items()}
        base = []   # [(tipo, depto_moderno, fuente, url, nota)]
        if (prov, dep) in RES:
            for (tipo, dm, fu, url, nota) in RES[(prov, dep)]:
                base.append((tipo, dm, fu, url, nota))
        else:
            hit = idx.get(norm(dep))
            if hit:
                base.append(("identidad", hit[0], "georef", GRF, ""))
            elif (prov, dep) not in SPLITS:
                base.append(("flag_sin_resolver", "", "", "", "no resuelto por name-match ni por RES"))
            # si NO matchea por nombre PERO tiene hijos -> padre DISUELTO: el conjunto = los hijos.
            # No se emite fila base vacia (los hijos llevan el mapeo); se anota en el primer hijo.
        # (3) agregar hijos post-1960 (historia 3.1b)
        splits = SPLITS.get((prov, dep), [])
        disuelto = bool(splits) and not base   # ni RES ni name-match -> el padre no sobrevive
        for i, (hijo, nota, url) in enumerate(splits):
            tipo = "split_disuelto" if disuelto else "split"
            if disuelto and i == 0:
                nota = f"PADRE DISUELTO (no sobrevive ningun depto con ese nombre). {nota}"
            base.append((tipo, hijo, "web", url, nota))
        for (tipo, dm, fu, url, nota) in base:
            idi = mods.get(dm, "")
            if dm and not idi:
                sin_id.append((prov, dep, tipo, dm))
            out.append({"provincia_canon": prov, "departamento_canon": dep, "tipo": tipo,
                        "depto_moderno": dm, "id_indec": idi, "fuente": fu,
                        "fuente_url": url, "nota": nota})

    fields = ["provincia_canon","departamento_canon","tipo","depto_moderno","id_indec","fuente","fuente_url","nota"]
    with open(OUT, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(out)

    from collections import Counter
    tc = Counter(r["tipo"] for r in out)
    conj = Counter()
    for r in out:
        conj[(r["provincia_canon"], r["departamento_canon"])] += 1
    print(f"crosswalk_indec.csv v2: {len(out)} filas | {len(pares)} deptos 1960")
    print("por tipo:", dict(tc))
    print(f"deptos 1960 con CONJUNTO >1 (split): {sum(1 for v in conj.values() if v>1)}")
    if sin_id:
        print(f"\n!! depto_moderno SIN id en Georef ({len(sin_id)}) -> revisar nombre exacto:")
        for x in sin_id: print("   ", x)

if __name__ == "__main__":
    main()

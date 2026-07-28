"""Paso 3.1 (resolucion humana) — graba las decisiones de Jose sobre los 45 flags en crosswalk_indec.csv.
Reemplaza cada fila tipo=flag por su(s) fila(s) resuelta(s) (un SPLIT produce N filas, un destino c/u).
Las 442 filas identidad quedan intactas. Fuente Georef para Cubeta A; fuente web (Jose confirmo) para B/C.
Idempotente: reconstruye desde las identidades + la tabla RES de abajo. NO toca el crudo.
"""
import csv, os

BASE = r"C:\Users\josem\repos\bimodal-transport-argentina\_local_geocoding_1960"
XW = os.path.join(BASE, "crosswalk_indec.csv")
GRF = "https://apis.datos.gob.ar/georef/api/departamentos"

# (prov, depto_1960) -> [ (tipo, depto_moderno, id_indec, fuente, fuente_url, nota), ... ]
RES = {
 # ---- Cubeta A: normalizacion/grafia (fuente=Georef) ----
 ("Buenos Aires","3 de Febrero"): [("identidad","Tres de Febrero","06840","georef",GRF,"grafia: numero->palabra")],
 ("Buenos Aires","General Lamadrid"): [("identidad","General La Madrid","06322","georef",GRF,"grafia: espaciado")],
 ("Buenos Aires","González Chaves"): [("rename","Adolfo Gonzales Chaves","06014","georef",GRF,"nombre expandido")],
 ("Buenos Aires","Juárez"): [("rename","Benito Juárez","06084","georef",GRF,"nombre expandido")],
 ("Chaco","Cap. Gral. O'Higgins"): [("identidad","O'Higgins","22112","georef",GRF,"abreviatura")],
 ("Chaco","Doce de Octubre"): [("identidad","12 de Octubre","22036","georef",GRF,"numero->palabra")],
 ("Chaco","Fray J. Sta. María de Oro"): [("identidad","Fray Justo Santa María de Oro","22043","georef",GRF,"abreviatura")],
 ("Chaco","Lib. Gral. San Martín"): [("identidad","Libertador General San Martín","22084","georef",GRF,"abreviatura")],
 ("Chaco","Nueve de Julio"): [("identidad","9 de Julio","22105","georef",GRF,"numero->palabra")],
 ("Chaco","Primero de Mayo"): [("identidad","1° de Mayo","22126","georef",GRF,"numero->palabra")],
 ("Chaco","Veinticinco de Mayo"): [("identidad","25 de Mayo","22168","georef",GRF,"numero->palabra")],
 ("Córdoba","Pte. Roque Sáenz Peña"): [("identidad","Presidente Roque Sáenz Peña","14084","georef",GRF,"abreviatura")],
 ("La Pampa","Conhello"): [("identidad","Conhelo","42035","georef",GRF,"grafia")],
 ("La Pampa","Cura-Có"): [("identidad","Curacó","42042","georef",GRF,"grafia")],
 ("Misiones","General Belgrano"): [("rename","General Manuel Belgrano","54049","georef",GRF,"nombre expandido")],
 ("Misiones","Lib. Gral. San Martín"): [("identidad","Libertador General San Martín","54077","georef",GRF,"abreviatura")],
 ("Salta","Candelaria"): [("identidad","La Candelaria","66084","georef",GRF,"articulo")],
 ("Salta","General Martín Miguel de Güemes"): [("identidad","General Güemes","66049","georef",GRF,"forma corta")],
 ("Salta","Gral. J. de San Martín"): [("identidad","General José de San Martín","66056","georef",GRF,"abreviatura")],
 ("San Juan","Nueve de Julio"): [("identidad","9 de Julio","70063","georef",GRF,"numero->palabra")],
 ("San Juan","Ullún"): [("identidad","Ullum","70112","georef",GRF,"grafia")],
 ("San Juan","Veinticinco de Mayo"): [("identidad","25 de Mayo","70126","georef",GRF,"numero->palabra")],
 ("Santa Fe","Nueve de Julio"): [("identidad","9 de Julio","82077","georef",GRF,"doble-grafia 1B: = '9 de Julio' [82077], mismo depto")],
 ("Río Negro","General Conesa"): [("rename","Conesa","62028","georef",GRF,"forma corta")],
 # ---- Cubeta B: renames/splits historicos (fuente web, Jose confirmo) ----
 ("Buenos Aires","Bartolomé Mitre"): [("rename","Arrecifes","06077","web",
     "https://es.wikipedia.org/wiki/Partido_de_Arrecifes","nombre 'Bartolomé Mitre' 1901-1997")],
 ("Buenos Aires","Caseros"): [("rename","Daireaux","06231","web",
     "https://es.wikipedia.org/wiki/Partido_de_Daireaux","Ley prov. 7613/1970")],
 ("Buenos Aires","General Sarmiento"): [
     ("split","José C. Paz","06412","web","https://es.wikipedia.org/wiki/Partido_de_General_Sarmiento","Ley 11.551/1994"),
     ("split","Malvinas Argentinas","06515","web","https://es.wikipedia.org/wiki/Partido_de_General_Sarmiento","Ley 11.551/1994"),
     ("split","San Miguel","06760","web","https://es.wikipedia.org/wiki/Partido_de_General_Sarmiento","Ley 11.551/1994")],
 ("Jujuy","Capital"): [("rename","Doctor Manuel Belgrano","38021","web",
     "https://en.wikipedia.org/wiki/Doctor_Manuel_Belgrano_Department","capital=San Salvador de Jujuy; año exacto incierto")],
 ("Mendoza","Luján"): [("rename","Luján de Cuyo","50063","web",
     "https://es.wikipedia.org/wiki/Departamento_de_Luj%C3%A1n_de_Cuyo","renombrado 1964")],
 ("San Luis","La Capital"): [("rename","Juan Martín de Pueyrredón","74056","web",
     "https://es.wikipedia.org/wiki/Departamento_La_Capital_(San_Luis)","Ley V-0748/2010")],
 ("San Luis","San Martín"): [("rename","Libertador General San Martín","74063","web",
     "https://es.wikipedia.org/wiki/Departamento_La_Capital_(San_Luis)","Georef no tiene 'San Martín' a secas; moderno = Lib. Gral. San Martín")],
 ("Santiago del Estero","Matará"): [("rename","Juan Felipe Ibarra","86098","web",
     "https://en.wikipedia.org/wiki/Juan_Felipe_Ibarra_Department","Ley prov. 4091/1974; cabecera Suncho Corral")],
 ("Tierra del Fuego","San Sebastián"): [("rename","Río Grande","94008","web",
     "https://es.wikipedia.org/wiki/Anexo:Departamentos_de_la_provincia_de_Tierra_del_Fuego,_Ant%C3%A1rtida_e_Islas_del_Atl%C3%A1ntico_Sur","reorg. 1970")],
 ("Tucumán","Tafí"): [
     ("split","Tafí del Valle","90098","web","https://es.wikipedia.org/wiki/Anexo:Departamentos_de_la_provincia_de_Tucum%C3%A1n","reorg. 1970s"),
     ("split","Tafí Viejo","90105","web","https://es.wikipedia.org/wiki/Anexo:Departamentos_de_la_provincia_de_Tucum%C3%A1n","reorg. 1970s")],
 ("La Rioja","General Lavalle"): [("rename","General Felipe Varela","46028","web",
     "https://es.wikipedia.org/wiki/Departamento_General_Felipe_Varela","cabecera Villa Unión")],
 ("La Rioja","General Ocampo"): [("rename","General Ortiz de Ocampo","46084","web",
     "https://es.wikipedia.org/wiki/Departamento_General_Ortiz_de_Ocampo","forma completa; cabecera Milagro")],
 ("La Rioja","General Roca"): [("rename","Rosario Vera Peñaloza","46112","web",
     "https://es.wikipedia.org/wiki/Departamento_Rosario_Vera_Pe%C3%B1aloza","Ley prov. 2890/1964; cabecera Chepes")],
 ("La Rioja","General Sarmiento"): [("rename","Vinchina","46098","web",
     "https://es.wikipedia.org/wiki/Departamento_Vinchina","revertido a Vinchina 1989")],
 ("La Rioja","Gobernador Gordillo"): [("rename","Chamical","46035","web",
     "https://en.wikipedia.org/wiki/Chamical_Department","renombrado 1987")],
 ("La Rioja","Rivadavia"): [("rename","General Juan Facundo Quiroga","46070","web",
     "https://es.wikipedia.org/wiki/Departamento_General_Juan_Facundo_Quiroga","renombrado 1948; cabecera Malanzán")],
 ("La Rioja","Vélez Sarsfield"): [("rename","General Ángel Vicente Peñaloza","46056","web",
     "https://es.wikipedia.org/wiki/Departamento_General_%C3%81ngel_V._Pe%C3%B1aloza","Ley prov. 2890/1964; cabecera Tama")],
 # ---- Cubeta C: no-deptos / entidades especiales / artefacto ----
 ("Buenos Aires","Isla Martín García"): [("especial","La Plata","06441","web",
     "https://en.wikipedia.org/wiki/Mart%C3%ADn_Garc%C3%ADa_Island","exclave BA asignado a La Plata; footnote-2 nombre propio")],
 ("Buenos Aires","Puerto de la Plata"): [("especial","Ensenada","06245","web",
     "https://observatorioconurbano.ungs.edu.ar/","zona nacional puerto La Plata (reparto Ensenada/Berisso); revisar imagen")],
 ("La Pampa","Caseros"): [("especial","Daireaux","06231","web",
     "https://es.wikipedia.org/wiki/Partido_de_Daireaux","ANOMALIA cross-provincial: localidad (La Larga) es de BA Daireaux, no La Pampa; revisar en 3.2")],
 ("Chubut","Chubut"): [("sin_equivalente","","","web",
     "https://es.wikipedia.org/wiki/Anexo:Departamentos_de_la_Provincia_del_Chubut","artefacto: prob. nombre de provincia en campo depto; REVISAR IMAGEN")],
}

def main():
    rows = list(csv.DictReader(open(XW, encoding="utf-8")))
    ident = [r for r in rows if r["tipo"] == "identidad"]
    flags = [(r["provincia_canon"], r["departamento_canon"]) for r in rows if r["tipo"] == "flag"]

    missing = [k for k in flags if k not in RES]
    assert not missing, f"flags sin resolucion: {missing}"

    out = list(ident)
    n_split = 0
    for r in rows:
        if r["tipo"] != "flag":
            continue
        key = (r["provincia_canon"], r["departamento_canon"])
        res = RES[key]
        if len(res) > 1:
            n_split += 1
        for (tipo, dm, idi, fu, url, nota) in res:
            out.append({"provincia_canon": r["provincia_canon"], "departamento_canon": r["departamento_canon"],
                        "tipo": tipo, "depto_moderno": dm, "id_indec": idi,
                        "fuente": fu, "fuente_url": url, "nota": nota})

    out.sort(key=lambda r: (r["provincia_canon"], r["departamento_canon"], r["depto_moderno"]))
    fields = ["provincia_canon", "departamento_canon", "tipo", "depto_moderno", "id_indec", "fuente", "fuente_url", "nota"]
    with open(XW, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader(); w.writerows(out)

    from collections import Counter
    tc = Counter(r["tipo"] for r in out)
    print(f"crosswalk_indec.csv reescrito: {len(out)} filas (identidad={len(ident)}, flags resueltos={len(flags)}, splits={n_split})")
    print("por tipo:", dict(tc))
    print("filas de split:")
    for r in out:
        if r["tipo"] == "split":
            print(f"  {r['provincia_canon']} | {r['departamento_canon']} -> {r['depto_moderno']} [{r['id_indec']}]")

if __name__ == "__main__":
    main()

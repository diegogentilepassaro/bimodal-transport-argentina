"""Paso 3.4 (PROVISIONAL) — ensambla coordenadas_1960.csv: una fila por localidad 1960 (3.063), con su
coordenada (si la tiene) + procedencia completa. NADA confirmado por Jose todavia: cada fila lleva su
confianza. Fuente de la coord por origen:
  auto_ok / TierA(bahra,variante) / E(variante_bahra) / B(depto) / D(ambiguo) -> geo_match_simple.csv
  C-generador / C1-cabecera / C-especial / C5(rename,manual_coord,barrio_de)  -> investigacion_flags.csv
  C5-sin_coordenada / bloqueado_artefacto -> sin coord (con motivo).
verificado_geo: de c5_investigacion.csv para C5; por construccion para el resto (in-set=verde, depto=rojo).
"""
import csv, os

BASE = r"C:\Users\josem\repos\bimodal-transport-argentina\_local_geocoding_1960"

FECHA = "2026-07-22"

# PASO 0 — correcciones image-confirmadas (ratificar Jose). Destraban los 2 bloqueado_artefacto.
# key (page, n_orden, localidad_canon) -> override
OVERRIDES = {
 ("v9_1_2_p04", "16", "Puerto Madryn"): dict(
    nombre="Puerto Madryn", georef_id="26007020", georef_depto="Biedma", lat="-42.7673", lon="-65.0366",
    depto_corregido="Biedma",
    nota="PASO0: fuente 1960 invirtio prov/depto (imprimio Prov=Biedma, Depto=Chubut); correcto Chubut/Biedma. Imagen v9_1_2_p04/16",
    fuente="imagen+georef", fuente_url="https://apis.datos.gob.ar/georef/api"),
 ("v3_p12", "482", "La Larga"): dict(
    nombre="La Larga", georef_id="06231040", georef_depto="Daireaux", lat="-36.675", lon="-61.93",
    depto_corregido="Daireaux (Buenos Aires)",
    nota="PASO0: fuente imprimio prov 'La Pampa' erronea; La Pampa no tiene depto Caseros; Caseros=viejo Daireaux(BA); La Larga en BA/Daireaux. Imagen v3_p12/482",
    fuente="imagen+georef", fuente_url="https://apis.datos.gob.ar/georef/api"),
}

# tier -> criterio_deteccion (COMO se obtuvo la coord)
DET = {"auto_ok": "match_exacto_3campos", "A (aparcado)": "bahra_georef", "E-bahra-fuzzy": "bahra_fuzzy",
       "C-generador": "generador_transformacion", "C1-cabecera": "cabecera_documentada",
       "C-especial": "especial_manual", "C5-rename": "web_rename", "C5-manual_coord": "web_manual_coord",
       "C5-barrio_de": "web_barrio", "B": "depto_transferencia_o_homonimo", "D": "ambiguo",
       "C5-sin_coordenada": "sin_coordenada", "C5-pendiente": "sin_coordenada"}

def load(f, keyf):
    return {keyf(r): r for r in csv.DictReader(open(os.path.join(BASE, f), encoding="utf-8"))}

def main():
    K = lambda r, ln="localidad_canon": (r["page"], r["n_orden"], r[ln])
    va = list(csv.DictReader(open(os.path.join(BASE, "vista_ancha.csv"), encoding="utf-8")))
    gm = load("geo_match_simple.csv", lambda r: K(r))
    inv = load("investigacion_flags.csv", lambda r: (r["page"], r["n_orden"], r["localidad"]))
    c5 = load("c5_investigacion.csv", lambda r: (r["page"], r["n_orden"], r["localidad"]))
    fla = {(r["page"], r["n_orden"], r["localidad"]) for r in
           csv.DictReader(open(os.path.join(BASE, "flags_filas.csv"), encoding="utf-8"))}

    # Paso 3.3b — resoluciones ACEPTADas del Grupo A (web + doc + decisiones de Jose)
    import unicodedata
    def _n(s):
        s = unicodedata.normalize("NFKD", s or ""); s = "".join(c for c in s if not unicodedata.combining(c))
        return s.casefold().strip()
    WEB33 = {}
    _w33 = os.path.join(BASE, "revision_3.3_web.csv")
    if os.path.exists(_w33):
        for r in csv.DictReader(open(_w33, encoding="utf-8")):
            if r["veredicto"] == "aceptar":
                WEB33[(r["page"], r["n_orden"], _n(r["localidad"]))] = r

    out = []
    for r in va:
        k = K(r)
        kw = (r["page"], r["n_orden"], _n(r["localidad_canon"]))
        g = gm.get(k, {})
        est_gm = g.get("estado", "")
        base = {"page": r["page"], "n_orden": r["n_orden"], "provincia_canon": r["provincia_canon"],
                "departamento_canon": r["departamento_canon"], "localidad_canon": r["localidad_canon"],
                "footnote": r.get("footnote_canon", "") or ("1" if k in fla else ""),
                "total": r.get("total_canon", ""), "georef_id": "", "nombre_oficial": "",
                "georef_depto": "", "lat": "", "lon": "", "estado": "", "tier": "",
                "confianza": "", "fuente": "", "fuente_url": "", "verificado_geo": "", "nota": ""}

        if k in OVERRIDES:   # PASO 0: correccion image-confirmada (destraba el bloqueado)
            o = OVERRIDES[k]
            base.update(estado="propuesto", tier="PASO0-correccion", georef_id=o["georef_id"],
                        nombre_oficial=o["nombre"], georef_depto=o["georef_depto"], lat=o["lat"], lon=o["lon"],
                        confianza="alta (imagen+georef; ratificar)", fuente=o["fuente"], fuente_url=o["fuente_url"],
                        verificado_geo="verde", nota=o["nota"])
            out.append(base); continue

        if kw in WEB33:      # PASO 3.3b: resolucion aceptada del Grupo A (web+doc+Jose) — CONFIRMADA
            w = WEB33[kw]
            tipo = w["estado_final"] or w["tipo_resolucion"]
            url = w["fuente_url_cambio"] or w["fuente_url"]
            base.update(estado="confirmado_3.3b", tier="3.3b:" + tipo, georef_id=w["georef_id"],
                        nombre_oficial=w["propuesta"], georef_depto=w["depto_del_punto"],
                        lat=w["lat"], lon=w["lon"], confianza=w["confianza"] or "media",
                        fuente=w["decidido_por"] + " + " + (w["fuente"] or "web"), fuente_url=url,
                        verificado_geo=w["verificado_geo"],
                        nota=(w["motivo_veredicto"] or "") +
                             ((" | ley: " + w["ley_o_anio"]) if w["ley_o_anio"] else ""))
            out.append(base); continue

        if est_gm == "auto_ok":
            base.update(estado="auto_ok", tier="auto_ok", georef_id=g["georef_id"],
                        nombre_oficial=g["georef_nombre"], georef_depto=g["georef_depto"],
                        lat=g["lat"], lon=g["lon"], confianza="alta", fuente="georef/localidades-censales",
                        fuente_url="https://apis.datos.gob.ar/georef/api", verificado_geo="verde")
            out.append(base); continue

        iv = inv.get(k, {})
        tier = iv.get("tier", "")
        cc = c5.get(k, {})
        # coord: inv primero (C-*, C5), luego gm (A/E/B/D)
        lat = (iv.get("lat") or "").strip() or (g.get("lat") or "").strip()
        lon = (iv.get("lon") or "").strip() or (g.get("lon") or "").strip()
        nombre = (iv.get("nombre_oficial") or iv.get("propuesta") or g.get("georef_nombre") or "").strip()
        gid = (iv.get("georef_id") or g.get("georef_id") or "").strip()
        gdep = (g.get("georef_depto") or "").strip()
        fuente = (iv.get("fuente") or "").strip() or ("georef" if lat else "")
        url = (iv.get("fuente_url") or "").strip()
        conf = (iv.get("confianza") or "").strip()
        nota = (iv.get("nota") or "").strip()

        # verificado_geo
        if cc:
            vg = cc.get("verificado_geo", "")
        elif est_gm == "flag_depto":
            vg = "rojo"; nota = (f"dist_km={g.get('dist_km','')}. " + nota).strip()
        elif est_gm == "flag_ambiguo":
            vg = "revisar_ambiguo"
        elif tier == "C-especial":
            vg = "especial"
        else:
            vg = "verde"   # in-set por construccion (match filtro por conjunto permitido)

        # coord que viene de Georef (gm) sin url explicita -> setear la url oficial (es trazable)
        if lat and not url:
            fuente = fuente or "georef"
            if fuente.startswith("georef"):
                url = "https://apis.datos.gob.ar/georef/api"
        # REGLA DURA: coord con guard 'sin_depto' (mar/borde) se RECHAZA -> sin coordenada
        if lat and vg == "sin_depto":
            nota = ("COORD RECHAZADA por guard (cae fuera de todo departamento: mar/borde). " + nota).strip()
            lat = lon = ""

        if tier == "C5-pendiente":
            base.update(estado="bloqueado_artefacto", tier=tier, confianza=conf,
                        fuente=fuente, fuente_url=url, verificado_geo="", nota=nota or "conjunto de deptos vacio (artefacto)")
        elif not lat:
            base.update(estado="sin_coordenada", tier=tier, confianza=conf,
                        fuente=fuente, fuente_url=url, verificado_geo="", nota=nota)
        else:
            base.update(estado="propuesto", tier=tier, georef_id=gid, nombre_oficial=nombre,
                        georef_depto=gdep, lat=lat, lon=lon, confianza=conf, fuente=fuente,
                        fuente_url=url, verificado_geo=vg, nota=nota)
        out.append(base)

    # ---- post-pase de AUDITORIA: criterio_deteccion / criterio_aceptacion / en_muestra / decidido_por / fecha ----
    for b in out:
        k = (b["page"], b["n_orden"], b["localidad_canon"]); est = b["estado"]; vg = b["verificado_geo"]
        kw = (b["page"], b["n_orden"], _n(b["localidad_canon"]))
        fu = b["fuente"] or ""
        # criterio_deteccion (COMO se obtuvo la coord)
        if k in OVERRIDES: det = "correccion_fuente_imagen"
        elif kw in WEB33: det = WEB33[kw]["estado_final"] or WEB33[kw]["tipo_resolucion"]
        else: det = DET.get(b["tier"], b["tier"])
        # criterio_aceptacion (COMO entra al set; en esta etapa PROVISIONAL, refleja el camino propuesto)
        if kw in WEB33:               # Grupo A resuelto en 3.3b (CONFIRMADO esta sesion)
            if WEB33[kw]["decidido_por"] == "humano (Jose)": ca, dp = "humano_individual", "humano:Jose(3.3b)"
            else:                                            ca, dp = "documentado_3.3b", "auto:3.3b(web+doc+guard)"
        elif k in OVERRIDES:          ca, dp = "humano_individual", "humano:Jose(Paso0)"
        elif est == "auto_ok":        ca, dp = "auto_match", "auto"
        elif est == "sin_coordenada": ca, dp = "sin_coordenada", ""
        elif est == "bloqueado_artefacto": ca, dp = "bloqueado_artefacto", ""
        elif vg == "verde" and fu.startswith("georef"): ca, dp = "auto_muestreo", "auto(pendiente_muestra)"
        elif vg == "verde":           ca, dp = "humano_lote", "pendiente:Jose"        # verde no-oficial (Grupo B)
        else:                          ca, dp = "humano_individual", "pendiente:Jose"  # rojo/ambiguo/especial (Grupo A)
        b["criterio_deteccion"] = det; b["criterio_aceptacion"] = ca
        b["decidido_por"] = dp; b["en_muestra"] = "no"; b["fecha"] = FECHA

    fields = ["page","n_orden","provincia_canon","departamento_canon","localidad_canon","footnote","total",
              "georef_id","nombre_oficial","georef_depto","lat","lon","estado","tier",
              "criterio_deteccion","criterio_aceptacion","en_muestra","decidido_por","fecha",
              "confianza","fuente","fuente_url","verificado_geo","nota"]
    with open(os.path.join(BASE, "coordenadas_1960.csv"), "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(out)

    from collections import Counter
    ce, cv = Counter(x["estado"] for x in out), Counter(x["verificado_geo"] for x in out if x["lat"])
    con = [x for x in out if x["lat"]]
    pobf = lambda rs: sum(int(x["total"] or 0) for x in rs)
    print(f"coordenadas_1960.csv: {len(out)} filas (control vista_ancha={len(va)})")
    print("por estado:", dict(ce))
    print("verificado_geo (de las con coord):", dict(cv))
    print(f"CON coordenada: {len(con)} ({pobf(con):,} hab) | SIN: {len(out)-len(con)}")
    print(f"VERIF con coord pero sin fuente_url: {sum(1 for x in con if not x['fuente_url'])} (debe ser 0)")
    print(f"VERIF con coord y verificado_geo=sin_depto: {sum(1 for x in con if x['verificado_geo']=='sin_depto')} (debe ser 0)")

if __name__ == "__main__":
    main()

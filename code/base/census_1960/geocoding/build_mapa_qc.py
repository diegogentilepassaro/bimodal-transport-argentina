"""Paso 3.4 (QC) — mapa_qc.html autocontenido (SVG, offline, sin CDN) de coordenadas_1960.csv.
Colorea por verificado_geo con el grueso RECESIVO para que los problemas RESALTEN:
  verde (auto_ok + propuestas in-set)  -> gris-teal chico, opacidad baja (el fondo)
  rojo (coord en depto fuera del conjunto: transferencia/homonimo) -> rojo, grande, con anillo
  revisar_ambiguo / especial -> ambar
Panel lateral: leyenda con conteos + lista de sin_coordenada. Hover -> tooltip por punto.
Proyeccion equirectangular sobre el bbox de los puntos. Referencia: centroides de provincia rotulados.
"""
import csv, json, os, html

BASE = r"C:\Users\josem\repos\bimodal-transport-argentina\_local_geocoding_1960"

# paleta status (reservada; cada color va con etiqueta en la leyenda)
COL = {"verde": "#8aa1a8", "rojo": "#d1495b", "revisar_ambiguo": "#e0a458",
       "especial": "#e0a458", "": "#b0b0b0"}
W, H, PAD = 900, 1500, 40   # Argentina es alta y angosta

def main():
    rows = list(csv.DictReader(open(os.path.join(BASE, "coordenadas_1960.csv"), encoding="utf-8")))
    cent = json.load(open(os.path.join(BASE, "_prov_centroides.json"), encoding="utf-8"))
    pts = [r for r in rows if r["lat"].strip() and r["lon"].strip()]
    for r in pts:
        r["_lat"], r["_lon"] = float(r["lat"]), float(r["lon"])
    lats = [r["_lat"] for r in pts]; lons = [r["_lon"] for r in pts]
    la0, la1 = min(lats) - 1, max(lats) + 1
    lo0, lo1 = min(lons) - 1, max(lons) + 1

    def xy(lat, lon):
        x = PAD + (lon - lo0) / (lo1 - lo0) * (W - 2 * PAD)
        y = PAD + (la1 - lat) / (la1 - la0) * (H - 2 * PAD)   # lat invertida
        return round(x, 1), round(y, 1)

    # orden de dibujo: verde primero (fondo), luego ambar, luego rojo (encima)
    orden = {"verde": 0, "": 0, "especial": 1, "revisar_ambiguo": 1, "rojo": 2}
    pts.sort(key=lambda r: orden.get(r["verificado_geo"], 0))

    circles = []
    for r in pts:
        x, y = xy(r["_lat"], r["_lon"])
        vg = r["verificado_geo"]
        c = COL.get(vg, "#b0b0b0")
        if vg == "rojo":
            rad, op, ring = 4.5, 0.95, ' stroke="#7a1020" stroke-width="1.2"'
        elif vg in ("revisar_ambiguo", "especial"):
            rad, op, ring = 4, 0.9, ' stroke="#8a5a12" stroke-width="1"'
        else:
            rad, op, ring = 2, 0.5, ''
        tip = f"{r['localidad_canon']} → {r['nombre_oficial'] or '?'} · {r['departamento_canon']} · {r['estado']}/{r['tier']} · guard:{vg or '-'}"
        circles.append(f'<circle cx="{x}" cy="{y}" r="{rad}" fill="{c}" fill-opacity="{op}"{ring}'
                       f' data-t="{html.escape(tip, quote=True)}"><title>{html.escape(tip)}</title></circle>')

    labels = []
    for nom, (lat, lon) in cent.items():
        if not (la0 <= lat <= la1 and lo0 <= lon <= lo1):
            continue
        x, y = xy(lat, lon)
        labels.append(f'<text x="{x}" y="{y}" font-size="10" fill="#33475b" opacity="0.55" '
                      f'text-anchor="middle">{html.escape(nom[:14])}</text>')

    from collections import Counter
    cv = Counter(r["verificado_geo"] for r in pts)
    sin = [r for r in rows if not r["lat"].strip()]
    est = Counter(r["estado"] for r in rows)

    leg = "".join(
        f'<div><span class="sw" style="background:{COL[k]}"></span>{lbl} — {cv.get(k,0)}</div>'
        for k, lbl in [("verde", "verde (in-set / auto_ok)"), ("rojo", "rojo (fuera del depto → revisar)"),
                       ("revisar_ambiguo", "ambiguo"), ("especial", "especial")])
    sinlist = "".join(f"<li>{html.escape(r['localidad_canon'])} · {html.escape(r['provincia_canon'])}/"
                      f"{html.escape(r['departamento_canon'])} · {html.escape(r['estado'])}</li>" for r in sin)

    doc = f"""<!doctype html><html lang="es"><head><meta charset="utf-8">
<title>QC coordenadas 1960 (provisional)</title><style>
body{{margin:0;font:13px system-ui,sans-serif;background:#f7f8fa;color:#1a2230;display:flex}}
#map{{flex:1}} aside{{width:320px;padding:16px;background:#fff;border-left:1px solid #e2e6ea;height:100vh;overflow:auto}}
h1{{font-size:15px;margin:0 0 4px}} .sub{{color:#667;margin-bottom:12px}}
.sw{{display:inline-block;width:12px;height:12px;border-radius:50%;margin-right:6px;vertical-align:middle}}
.leg div{{margin:3px 0}} .card{{background:#f0f2f5;border-radius:8px;padding:8px 10px;margin:10px 0}}
#tip{{position:fixed;pointer-events:none;background:#1a2230;color:#fff;padding:5px 8px;border-radius:6px;
font-size:12px;max-width:280px;display:none;z-index:9}} ul{{padding-left:16px;margin:6px 0}} li{{margin:2px 0;font-size:12px}}
circle{{cursor:crosshair}} circle:hover{{fill-opacity:1}}
</style></head><body>
<svg id="map" viewBox="0 0 {W} {H}" preserveAspectRatio="xMidYMid meet">
{''.join(labels)}
{''.join(circles)}
</svg>
<aside>
<h1>QC coordenadas 1960 — PROVISIONAL</h1>
<div class="sub">{len(pts)} puntos · {sum(int(r['total'] or 0) for r in pts):,} hab. Nada confirmado aún.</div>
<div class="card leg"><b>verificado_geo</b>{leg}</div>
<div class="card"><b>estado</b><br>{' · '.join(f'{k}: {v}' for k,v in est.most_common())}</div>
<div class="card"><b>Sin coordenada ({len(sin)})</b><ul>{sinlist}</ul></div>
<div class="sub">Los ROJOS y AMBAR están dibujados encima y más grandes: son los que piden tu ojo.
El fondo gris-teal es lo verificado (in-set). Hover = detalle.</div>
</aside>
<div id="tip"></div>
<script>
const tip=document.getElementById('tip');
document.querySelectorAll('circle').forEach(c=>{{
 c.addEventListener('mousemove',e=>{{tip.style.display='block';tip.style.left=(e.clientX+12)+'px';
   tip.style.top=(e.clientY+12)+'px';tip.textContent=c.getAttribute('data-t');}});
 c.addEventListener('mouseleave',()=>tip.style.display='none');}});
</script></body></html>"""
    with open(os.path.join(BASE, "mapa_qc.html"), "w", encoding="utf-8") as f:
        f.write(doc)
    print(f"mapa_qc.html: {len(pts)} puntos dibujados | verificado_geo={dict(cv)} | sin coord={len(sin)}")

if __name__ == "__main__":
    main()

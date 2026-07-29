# Census 1960 — Geocoding to locality level (own digitization)

## What this is
Digitized + georeferenced localities of Argentina's 1960 Population Census, at the
locality level (3.063 rows): printed name + modern official name, 1960 population,
lat/lon coordinates, INDEC id and modern departamento. Multiple points per
departamento (one row per locality) to capture intra-departamento economic geography
for the Market Access pipeline.

## Source
INDEC, Censo Nacional de Población 1960, published volumes (official scans). Digitized
by the authors. Reference boundaries in `ref/` (see below).

## Method (3 lines)
1. Verbatim vision transcription of the scanned page halves → immutable `poblados_1960.csv`.
2. Deterministic normalization + INDEC/Georef/Wiki matching, every transformation logged
   in `decisiones.csv`; human confirmation ledgers (`revision_*`, `dudas_*`, `veredictos_*`).
3. Coordinates assembled into `coordenadas_1960.csv`. Rule: **"sin fuente no hay coordenada."**

## Scanned pages (not in git)
`pages/` (~1240 PNG, ~607 MB) is distributed separately.
- Dropbox: https://www.dropbox.com/scl/fi/ds3wicy3w53a58rvbuccf/pages_geocoding_1960.zip?rlkey=o4qs4ayu5kfferu9ml8ebxche&dl=0
- Archive: `pages_geocoding_1960.zip` (pages/ excluding regenerable `halves/`; 1085 PNG = 80 original scans + 1005 `checks/`; ~414 MB)
- `sha256`: `59a11fbebe3f985ba7481dd58fe02e5d0daba865415e7089791fbdd779452ac7`

## Layout
- root: docs (`PROTOCOLO.md` = full method/protocol, `ROADMAP.md`, `PROBLEMAS_paso3.md`,
  `INFORME_geocoding_1960.md`), `poblados_1960.csv` (raw), `coordenadas_1960.csv` (output),
  ledgers (`decisiones.csv`, `revision_aproximados.csv`, `dudas_resueltas.csv`,
  `veredictos_3.3b.csv`), `mapa_qc.html`.
- `intermedios/`: traceable intermediates + agent-run outputs (`web33_agent_out/`, `_c5_lotes/`).
- `cache/`: pinned API caches (Georef/Wiki/ubicación) for reproducibility.
- `ref/`: reference boundaries/map (`deptos_argentina.geojson`, `ar60divp.pdf`,
  `deptos_1960_oficial.csv`).
- Scripts: `code/base/census_1960/geocoding/` (~35 Python, one-time digitization,
  outside `main.R`).

## Notes
- The full digitization protocol is in `PROTOCOLO.md` (this folder).
- Some scripts hardcode `_local_geocoding_1960/` paths (19/35 files, 22 refs); left as-is
  per coauthor decision, to be fixed later. This is one-time digitization, not part of the
  R replication pipeline.
- FULLER PICTURE on those paths (cr-review PR #146): fixing the `BASE`
  constant alone would not make them runnable, because this commit
  reorganised the files into `intermedios/`, `cache/` and `ref/` while
  every script joins flat filenames to `BASE`. In particular the pinned
  caches are not where the scripts look, so a re-run would query the
  live Georef API rather than replay them, and Georef drifts. Until that
  is resolved these scripts are an **archived record of how the data was
  produced, not an executable pipeline** — which is a legitimate status
  for one-time digitization, but it has to be stated rather than implied.
  A pinned `requirements.txt` sits beside them for whoever picks this up.

## Coverage — read before using this file
- **23 provinces, not 24: Capital Federal is NOT here.** The transcription
  covers volumes v3–v9, which is the "rest of country" set; volume 2 is
  Capital Federal. That matches the non-CABA universe of our other 1960
  digitization: this file sums to 13,544,686 against 13,558,587 for the
  311 non-CABA districts in `census_1960_ipums.parquet`, i.e. 99.90%.
  Anything using these points as market-access weights must add CABA
  (2,966,634 in 1960) from that dataset rather than inheriting the gap.
- **Localities only.** Like every source built from these volumes, this is
  a locality list, so dispersed rural population is absent. See
  `results/tables/diagnostic_pop1960_universe.txt` for what that does.

## QC status — a third of the rows are not human-confirmed
| `estado` | rows | population |
|---|---|---|
| `auto_ok` | 1,993 | |
| `propuesto` | 1,003 | 1,637,588 (12.1%) |
| `confirmado_3.3b` | 50 | |
| `sin_coordenada` | 17 | 34,509 |

Also: 23 rows have `verificado_geo = rojo` (the point falls outside its
expected departamento) **with** coordinates, and 67 read
`confianza = "BAJA (fuzzy en capa BAHRA…)"`. The confirmation ledgers are
committed in their **pending** state (`intermedios/revision_3.3.csv` has
52 `aceptar` and 189 blank; `en_muestra` is `"no"` for all 3,063 rows).
Any downstream use needs an explicit rule about which tiers are usable.

## Join key
`(page, n_orden)` is **not** unique — it collides on four pairs, each a
genuinely different locality. Keys that hold, both 3,063 distinct:
`(page, n_orden, localidad_canon)` and
`(provincia_canon, departamento_canon, localidad_canon)`. `georef_id` is
not a key (193 blanks, 16 reused). There is no `geolev2` column, and
`georef_depto` has 418 distinct values against the project's 312
time-invariant districts, so a crosswalk is required.

Known defect to handle on read: 27 values in `nombre_oficial` contain an
invisible U+00AD soft hyphen (an upstream Georef artifact, handled for
*matching* in `geocode_georef.py` but not stripped from the output
column), so exact joins on that column fail silently.

## Citation
Dirección Nacional de Estadística y Censos. 1960. "Censo Nacional de
Población 1960 [dataset]." Buenos Aires. (INDEC did not exist until
1968; the agency that ran the 1960 census is the one named here. The
paper's bibliography uses the same attribution.)

Coordinates are derived from third-party sources whose licences differ
and are not yet resolved: Georef / `apis.datos.gob.ar` (2,804 rows),
Wikipedia (58, CC-BY-SA), OpenStreetMap–Nominatim (33, ODbL), plus
`dices.net` and BAHRA. Attribution and share-alike implications need an
explicit decision before deposit.

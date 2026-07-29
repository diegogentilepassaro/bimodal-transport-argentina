# Census 1960 — Geocoding to locality level (own digitization)

## What this is
Digitized + georeferenced localities of Argentina's 1960 Population Census, at the
locality level (3.063 rows): printed name + modern official name, 1960 population,
lat/lon coordinates, INDEC id and modern departamento. Multiple points per
departamento (one row per locality) to capture intra-departamento economic geography
for the Market Access pipeline.

**Before using this file, read the Coverage, QC status, Join key and Geometry
sections below.** Four things will bite otherwise: Capital Federal is absent,
27.9% of the population is a whole-partido total rather than a locality, only
0.7% of rows are human-confirmed, and the obvious join key is not unique.

## Source
Dirección Nacional de Estadística y Censos, Censo Nacional de Población 1960,
published volumes v3–v9 (official scans). Digitized by the authors. Reference
boundaries in `ref/` (see below). On the agency name, see Citation at the end
of this file: INDEC did not exist in 1960.

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
- Those hardcoded paths are Windows absolutes embedding a coauthor's home
  directory (`C:\Users\josem\repos\…`). Scrub them before any public deposit,
  independently of whether the scripts are ever made runnable.
- `intermedios/cola_sumas_pendientes.csv` is **stale, not pending**: it lists
  6 rows, and 2 of them (Simbolar `v6_p06/140`, Sumamao `v6_p06/202`) are
  already resolved in `decisiones.csv`. Do not read it as an open queue.

## Coverage — read before using this file
- **23 of the 24 first-order jurisdictions; Capital Federal is NOT here.**
  The transcription covers volumes v3–v9, the "rest of country" set; volume 2
  is Capital Federal. Anything using these points as market-access weights
  must add CABA (2,966,634 in 1960, `geolev2` 32002001) from
  `census_1960_ipums.parquet` rather than inheriting the gap.
- **27.9% of the population is NOT at locality resolution.** 18 rows carry
  `footnote == "1"`, which the census uses for a whole-*partido* total in the
  Gran Buenos Aires conurbation rather than a named locality
  (`PROTOCOLO.md:186`: "18 partidos del conurbano = una fila"). They total
  **3,772,411 people = 27.85%** of the file — La Matanza 401,738,
  Lanús 375,428, Morón 341,920, Avellaneda 326,531, Quilmes 317,783,
  General San Martín 278,751, and 12 more. For those 18 districts the file
  does **not** deliver the intra-departamento geography that is its stated
  purpose, and the point attached to each is a single representative location
  for the whole partido. Two further rows carry `footnote == "2"`
  (Isla Martín García 1,712; Puerto de la Plata 17,338 = 19,050).
- **Localities only, otherwise.** For the remaining 3,043 rows this is a
  locality list, so dispersed rural population is absent. See
  `results/tables/diagnostic_pop1960_universe.txt`, produced by
  `code/analysis/diagnostic_pop1960_universe.R`, for what that does to the
  1960 population figures used in the paper.
- **On the 99.90% reconciliation, and what it does not show.** This file sums
  to 13,544,686 against 13,558,587 for the 311 non-CABA districts in
  `data/derived/base/census_1960/census_1960_ipums.parquet` (gitignored,
  regenerated by `clean_census_1960.R`), i.e. 99.90%. That is agreement
  between **two independent transcriptions of the same printed volumes** —
  `clean_census_1960.R` reads `1c1960_*.xlsx`, transcribed separately from the
  same source — so it validates the *transcription*, not the *coverage*. Both
  inherit the locality-universe gap: the 312-district file sums to 16,525,221,
  below the published national total. A high number here is not evidence that
  the universe is complete.

## QC status — almost nothing here is human-confirmed
`estado` is the coarse label. Percentages are of the file's 13,544,686 people.

| `estado` | rows | population |
|---|---|---|
| `auto_ok` | 1,993 | 11,796,589 (87.09%) |
| `propuesto` | 1,003 | 1,637,588 (12.09%) |
| `confirmado_3.3b` | 50 | 76,000 (0.56%) |
| `sin_coordenada` | 17 | 34,509 (0.25%) |

**`estado` is not a confirmation status, and reading it as one overstates the
QC by roughly fifty times.** `criterio_aceptacion` is the column that says how
each row was decided:

| `criterio_aceptacion` | rows | what it means |
|---|---|---|
| `auto_match` | 1,993 | machine exact match on three fields |
| `auto_muestreo` | 829 | accepted by sampling rule, row never inspected |
| `humano_lote` | 172 | queued for batch human review, **not yet done** (`decidido_por = "pendiente:Jose"`) |
| `documentado_3.3b` | 31 | machine web+doc+guard agreement |
| `humano_individual` | **21** | a person looked at this row |
| `sin_coordenada` | 17 | no coordinate assigned |

So **21 of 3,063 rows (0.7%)** carry an individual human decision. The
`confirmado_3.3b` label is 31 automatic to 19 human. The "third of the file"
figure that an earlier version of this readme quoted is the `propuesto` share
(32.7%), which means *not auto-matched* — a different and much narrower claim.
The confirmation ledgers are committed in their **pending** state
(`intermedios/revision_3.3.csv`: 52 `aceptar`, 189 blank; `en_muestra` is
`"no"` for all 3,063 rows). Any downstream use needs an explicit rule about
which tiers are usable, and that rule cannot lean on `estado` alone.

Location flags:
- 23 rows have `verificado_geo = rojo` — the point falls outside its expected
  departamento — and all 23 **do** carry coordinates. For market access this
  is the one location error that matters, since it moves population across
  district boundaries.
- The 3,023 `verde` rows are **not** all point-in-polygon validated.
  `build_coordenadas.py` (module docstring) assigns `verificado_geo` from
  `c5_investigacion.csv` for C5 rows and "por construccion para el resto
  (in-set=verde, depto=rojo)". Only the 161 rows reading
  `confianza = "verde (guard: coord en depto esperado)"` reflect an actual
  containment check.
- Low confidence totals **88**, not 67: 67 `BAJA (fuzzy en capa BAHRA: puede
  no ser el mismo lugar)`, 19 `baja`, and 2 `REVISAR (sin_depto)`. A further
  199 are middling (`media` in three variants).

## Join key
`(page, n_orden)` is **not** unique. It collides on four pairs, and because
`page` is a reused page-image label the two rows in each pair are in
**different provinces**:

| `(page, n_orden)` | row A | row B |
|---|---|---|
| `v3_p08` / 25 | Buenos Aires, Ensenada | Buenos Aires, Florencio Varela |
| `v5_p05` / 221 | Entre Ríos, Nueva Vizcaya | Corrientes, Perugorría |
| `v7_p04` / 84 | Tucumán, Esquina | Jujuy, Fraile Pintado |
| `v8_p04` / 69 | Mendoza, Rodeo del Medio | San Luis, San Francisco |

A merge on that key therefore does not just drop rows, it **moves population
across provinces**. Keys that hold, both 3,063 distinct:
`(page, n_orden, localidad_canon)` and
`(provincia_canon, departamento_canon, localidad_canon)`.

`georef_id` is not a key: 193 blanks, and 15 ids appear exactly twice
(30 rows). There is no `geolev2` column. `georef_depto` has 417 distinct
non-blank values against the project's 312 time-invariant districts (418
counting the blank), **and 236 rows (7.7%) have it blank** — the blanks are
the harder crosswalk problem, not the cardinality.

Known defect to handle on read: 27 values in `nombre_oficial` contain an
invisible U+00AD soft hyphen (an upstream Georef artifact, handled for
*matching* in `geocode_georef.py` but not stripped from the output
column), so exact joins on that column fail silently. No other column is
affected.

## Which file is authoritative for population
`coordenadas_1960.csv`, not `poblados_1960.csv`. The two disagree on `total`
for **6 rows**, net **+292** people (13,544,686 vs 13,544,394):

| row | `poblados` | `coordenadas` |
|---|---|---|
| `v3_p08` / 204 Francisco Madero | 1,871 | 1,873 |
| `v3_p14` / 342 Luan Toro | 918 | 938 |
| `v3_p14` / 462 Mamaguita | 508 | 608 |
| `v4_p17` / 41 Sunchales | 7,890 | 7,880 |
| `v6_p06` / 140 Simbolar | 525 | 625 |
| `v6_p06` / 202 Sumamao | 305 | 385 |

All six are logged in `decisiones.csv` as step 2.3 `correccion_lectura`, so the
provenance is sound — but "immutable `poblados_1960.csv`" in the Method section
above describes the transcription discipline, not equality of the two files.
(Aside: comparing them on `(page, n_orden)` reports ten differences rather
than six. Four are artifacts of the non-unique key above. It is a live example
of the hazard.)

## Geometry
- Coordinates are decimal degrees in `lat` / `lon`; 3,046 of 3,063 rows have
  them. Envelope: lat −54.813 to −21.947, lon −72.338 to −53.647, i.e. inside
  Argentina.
- **The CRS is not recorded anywhere in the source material.** Georef returns
  EPSG:4326 and the values are consistent with it, so that is the working
  assumption — but it is an assumption, and it is on the ask list for the
  digitizing coauthor rather than something this readme can assert.
- What the point represents (locality centroid, town centre, station) is also
  unrecorded, and varies with `fuente`.
- 6 coordinate pairs are shared by two rows each (12 rows, 38,856 people).
  Expected where a locality was matched to a nearby parent settlement, but it
  means coordinates are not a key either.
- `ref/deptos_argentina.geojson` is modern departamento geometry, not 1960
  boundaries; `ref/ar60divp.pdf` and `ref/deptos_1960_oficial.csv` are the
  1960 division reference.

## Citation
Dirección Nacional de Estadística y Censos. 1960. "Censo Nacional de
Población 1960 [dataset]." Buenos Aires. (INDEC did not exist until
1968; the agency that ran the 1960 census is the one named here. The
paper's bibliography uses the same attribution.)

### Coordinate sources and their licences (unresolved)
Coordinates come from third-party gazetteers whose licences differ. `fuente`
is **free text**, 88 distinct values, many of them compound
(`"auto + Georef + Wikipedia"`), so there is no exact per-source row count.
Counting rows whose `fuente` mentions each source, which double-counts
compound rows:

| source | rows mentioning it | licence |
|---|---|---|
| Georef / `apis.datos.gob.ar` | 2,871 | Argentine open government data |
| Wikipedia | 128 | **CC-BY-SA** (share-alike) |
| OpenStreetMap / Nominatim | 54 | **ODbL** (share-alike) |
| `dices.net` | 16 | unresolved |
| blank `fuente` | 15 | — |

Wikipedia at 128 rows and OSM at 54 are the share-alike exposures, and both
are larger than an earlier version of this readme stated (58 and 33, which
counted only literal machine-written values and missed the compound
human-entered ones).

BAHRA does **not** appear in `fuente` at all; it appears in `confianza` for 72
rows, as the layer a fuzzy match was made against. Do not look for it in the
provenance column.

The tail of `fuente` names roughly fifteen further third-party sites, each on
one or two rows: Mapcarta, Wikimapia, GeoNames/Tripadvisor, getamap, Mindat,
db-city, citypopulation, Globefeed, derutasymapas, Welcome Argentina, SIPAR,
`treslomas.gob.ar`, `eltiempo`, `todo-argentina`, and several provincial or
municipal government pages. Attribution and share-alike implications need an
explicit decision before deposit; the share-alike sources are the ones that
could attach conditions to redistributing the coordinate column.

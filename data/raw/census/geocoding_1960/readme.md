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

## Citation
INDEC. 1960. "Censo Nacional de Población 1960 [dataset]." Instituto Nacional de
Estadística y Censos, Buenos Aires.

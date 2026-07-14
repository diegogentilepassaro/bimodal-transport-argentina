# City / locality universe (source for node selection)

These are the **source** city and locality datasets from which the curated
hypothetical-network node set (`../ciudades_seleccion2.shp`, 68 cities) was
selected. Added so that per-district urban-center reference points can be
constructed (e.g. the largest city in each district), rather than relying on
geometric centroids.

## Provenance

- **Source**: Project team, derived from the INDEC 1960 census and IGN
  geographic data. Dropped in by the author on **2026-06-04** from a local
  archive (the "old repo" generic-Argentina shapefiles referenced in
  `Plan/hypothetical_networks_pipeline.md`).
- **CRS**: EPSG:4326 (WGS84) for all shapefiles.
- **Status**: raw, read-only. Not modified by code. Not used by the current
  pipeline (the pipeline uses the curated `../ciudades_seleccion2.shp`); these
  are the broader universe for the urban-center reference-point work.

## Contents

| File(s) | n | Key fields | Notes |
|---|---|---|---|
| `asentamientos_humanos_coord.csv` | 517 | `fna` (name), `X`, `Y` (coords) | IGN human-settlement points with coordinates. Largest universe; **no population field**. |
| `ciudades_seleccion.{shp,dbf,shx,prj}` | 50 | `localidad`, `provincia`, `capital`, `pop1960`, `coordX/Y` | Cities with 1960 population. Precursor to `ciudades_seleccion2` (68). |
| `top5perProv_regiones.xlsx - TOP5_PROV.csv` | — | `localidad`, `provincia`, `pop1960`, `top5prov`, `select*` | Top-5 cities per province with 1960 population and selection flags. |
| `capitales.{shp,dbf,shx,prj,qpj,csv}` | 24 | `fna`, `nam` | Provincial capitals (point geometry). |
| `ciudades_zona_1..9.{shp,dbf,shx,prj}` | — | zone-specific city subsets | The 9 geographic-zone node sets referenced as legacy alternatives in the hypothetical-networks pipeline. |

## Known issues

- **`ciudades_zona_6.dbf` is missing** (only `.shp/.shx/.prj` present). The
  geometry loads but has no attribute table. Carried over as-is from the source
  archive; flag before relying on zona_6 attributes.
- `asentamientos_humanos_coord.csv` and `capitales` share the IGN schema
  (`entidad, objeto, fna, gna, nam, ...`) but have **no population** — usable for
  location, not for population ranking.
- Population (`pop1960`) is available in `ciudades_seleccion` and `top5perProv`
  only. `pop1960` is stored as text in some files; coerce before use.

## Relationship to the active pipeline

The active pipeline node set is `../ciudades_seleccion2.shp` (68 cities,
provincial capitals + cities >=15,000 in 1960). These files are its lineage and
a richer universe. Whether any of them enters the pipeline (e.g. as per-district
urban-center reference points) is an open decision — not yet wired in.

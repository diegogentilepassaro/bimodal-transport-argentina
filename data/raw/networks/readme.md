# Raw Data: Transport Networks

Georeferenced shapefiles of Argentina's rail and road networks.

## Files

### Roads

| File | Description | Source |
|------|-------------|--------|
| `comparacion_54_70_86.*` | Road network comparison across three periods | Digitized from historical maps |

The road comparison shapefile has 1741 LineString segments with a `type2`
field encoding presence/absence in each period:

| type2 | 1954 | 1970 | 1986 | Meaning |
|-------|------|------|------|---------|
| 1 | ✓ | ✓ | ✓ | Present in all three periods |
| 2 | ✗ | ✓ | ✓ | New in 1970 |
| 3 | ✗ | ✗ | ✓ | New in 1986 |
| 4 | ✓ | ✓ | ✗ | Disappeared by 1986 |
| 5 | ✓ | ✗ | ✓ | Absent in 1970 (cartographic error) |
| 6 | ✗ | ✓ | ✗ | Present only in 1970 |
| 7 | ✓ | ✗ | ✗ | Present only in 1954 |

Taxonomy verified against old Stata preclean code (lines 616-619).

### Railroads

| File | Description | Source |
|------|-------------|--------|
| `lp_1979.*` | Larkin Plan rail segments with 1979/1986 status and Larkin-study flags | Digitized + joined by project team |

The `lp_1979` shapefile has 565 MULTILINESTRING segments in WGS 84.
Already joined to district boundaries (no district JOIN step needed in
the cleaning code).

| Field | Type | Values | Meaning |
|-------|------|--------|---------|
| `id_main`   | int | 1–1101 | Main segment identifier |
| `status1979`| int | 1, 2, 3 | 1 = active 1979 and 1986; 2 = closed during the military dictatorship (1976–1983); 3 = closed before 1976 |
| `id_1979`   | int |        | Secondary identifier tying rows to the 1979 network |
| `studied_co`| int | 0, 1   | 1 = studied in the Larkin Plan (instrument); 0 = not studied |
| `recom_code`| int | 1, 2, 3| Larkin-plan recommendation category |

Counts: status1979 = {1: 424, 2: 69, 3: 72}. studied_co = {0: 328, 1: 237}.
recom_code = {1: 355, 2: 160, 3: 50}.

**This is one snapshot, and `status1979` is a status, not a date.** There
is no closure-date field and no second rail map at another date. The 1960
and 1986 rail networks the pipeline reports are status buckets of this one
file, so the file supports two clean cross sections and one assumption:

| cross section | statuses | basis |
|---|---|---|
| 1986 | 1 | the field's own definition, "active in 1979 and 1986" |
| before the dictatorship | 1 + 2 | the field's own definition |
| 1960 | 1 + 2 + 3 | **assumption**: that the status-3 segments, closed at some point before 1976, were still open in 1960 |

Note that the first two definitions overlap in a way the field does not
resolve: status 2 is labelled 1976--1983, so any status-2 closure after
1979 would mean the network shrank between 1979 and 1986, while status 1
asserts its segments were active in both years. The 1986 cross section is
safe either way, because status 1 records 1986 activity directly. What
cannot be inferred from this file is *when* inside either window a
closure happened.

Do not label the status-3 kilometres with a narrower window. Section 3 of
the paper once called them "closed between 1960 and 1966", which
contradicts this field's own documentation and the clean_railroads
manifest, both of which say "closed before 1976". Corrected 2026-09-17.

Measured lengths, `sf::st_length()` on EPSG:4326, unclipped: status 1 =
33,304.1 km, status 2 = 5,564.1, status 3 = 3,921.6, total 42,789.9.
Clipping to the 312 district polygons removes 9.9 km, 0.02% of the total
but 0.19% of the status-3 bucket. These figures are typed here for
orientation only; the authoritative, regenerated version is
`results/tables/diagnostic_rail_km.txt`, written by
`code/analysis/diagnostic_rail_km.R`. Verify against that file, not this
paragraph.

A second rail source exists outside this package: a national
track-kilometre series, 1857--1989, which runs above this map by a
similar proportion at each date we can compare. It is not part of the
package, is read by no script, and is not cited in the paper, because its
provenance is unresolved. Every statement about it, including the
comparison and the numbers behind it, is recorded in
`.kiro/rail_km_sources_note.md` as an internal check that a replicator
cannot reproduce from this package.

## Provenance

Road shapefiles digitized manually from historical maps by project team.
Original maps: Automóvil Club Argentino road maps (1954, 1970, 1986).

Rail shapefile (`lp_1979`) prepared by Ma. Cote Schettino from the
project's earlier data (originally in `Train/raw_data/` from the old
repo). The district JOIN and Larkin-plan attribute join were performed
outside this repo.

## Citation

TODO: formal citation for the digitized road network data.
TODO: citation for the Larkin Plan segment data.

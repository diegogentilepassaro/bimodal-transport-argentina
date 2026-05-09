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

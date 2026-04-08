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

### Railroads (to be added)

Rail network shapefiles and Larkin Plan data will be added when the
`larkin_scores/` directory is located.

## Provenance

Road shapefiles digitized manually from historical maps by project team.
Original maps: Automóvil Club Argentino road maps (1954, 1970, 1986).

## Citation

TODO: formal citation for the digitized road network data.

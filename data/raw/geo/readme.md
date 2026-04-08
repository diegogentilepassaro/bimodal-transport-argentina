# Raw Data: Geographic Controls

Rasters and shapefiles used to construct district-level geographic control
variables (elevation, ruggedness, wheat suitability, caloric potential,
distance to Buenos Aires, area).

## Files

| File | Description | Source |
|------|-------------|--------|
| `geo2_ar1970_2010.*` | IPUMS 312-district boundaries (time-invariant) | IPUMS International |
| `gt30w060s10.tif` | GTOPO30 DEM elevation, tile 1 | USGS |
| `gt30w100s10.tif` | GTOPO30 DEM elevation, tile 2 | USGS |
| `wheatlo.tif` | Wheat suitability (tons/ha, low inputs) | FAO-GAEZ v3.0 |
| `pre1500AverageCalories.tif` | Caloric potential pre-1500 | Galor & Özak (2016) |
| `post1500AverageCalories.tif` | Caloric potential post-1500 | Galor & Özak (2016) |
| `tri.tif` | Terrain Ruggedness Index | Riley et al. (1999) — **MISSING** |
| `areas_de_asentamientos_y_edificios_020105.*` | IGN settlements (for BA centroid) | IGN Argentina |

## Citations

Minnesota Population Center. IPUMS International: Version 7.3 [dataset].
Minneapolis, MN: IPUMS, 2020. https://doi.org/10.18128/D020.V7.3

USGS. GTOPO30 Global Digital Elevation Model. U.S. Geological Survey, 1996.
https://www.usgs.gov/centers/eros/science/usgs-eros-archive-digital-elevation-gtopo30

FAO/IIASA. Global Agro-Ecological Zones (GAEZ v3.0). FAO, Rome and IIASA,
Laxenburg, 2012. https://gaez.fao.org/

Galor, Oded, and Ömer Özak. "The Agricultural Origins of Time Preference."
American Economic Review 106, no. 10 (2016): 3064–3103.

Riley, Shawn J., Stephen D. DeGloria, and Robert Elliot. "A Terrain
Ruggedness Index That Quantifies Topographic Heterogeneity." Intermountain
Journal of Sciences 5, no. 1-4 (1999): 23–27.

## Notes

- `tri.tif` is missing from the current data deposit. Cote is searching
  the backup archive. The script produces all other variables and flags
  ruggedness as NA until the file is located.

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
| `tri.tif` | Terrain Ruggedness Index | Riley et al. (1999), via Puga (diegopuga.org/data/rugged/) |
| `areas_de_asentamientos_y_edificios_020105.*` | IGN settlements (for BA centroid) | IGN Argentina |
| `HMI.tif` | Human Mobility Index (potential travel time on land) | Özak (2010, 2018), human-mobility-index.github.io |

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

Özak, Ömer. "The Voyage of Homo-Economicus: Some Economic Measures of
Distance." Department of Economics, Brown University (2010).
http://omerozak.com/pdf/Ozak_voyage.pdf

Özak, Ömer. "Distance to the Pre-Industrial Technological Frontier and
Economic Development." Journal of Economic Growth 23, no. 2 (2018):
175–221. https://doi.org/10.1007/s10887-018-9154-6

## Notes

- `tri.tif` is the Puga terrain ruggedness index raster (Riley et al. 1999
  formula without the square root, unit (100m)², derived from GTOPO30).
  Downloaded from https://diegopuga.org/data/rugged/ and converted from
  the source txt to GeoTIFF. Global coverage at 30" (~1 km) resolution.
  File size: ~1.5 GB (gitignored).
- `HMI.tif` is Özak's Human Mobility Index global raster at ~1km cylindrical
  equal-area projection (ESRI:54034). Values measure potential minimum
  travel time (hours). Downloaded from
  https://zenodo.org/records/14285746/files/HMI.tif. File size: ~2.2 GB
  (gitignored). Used as the off-network walking-cost surface in the tau
  pipeline.

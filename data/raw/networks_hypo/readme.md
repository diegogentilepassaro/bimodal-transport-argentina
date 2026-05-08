# Raw Data: Hypothetical Networks

Inputs for the construction cost raster and hypothetical network
(LCP/MST/EUC) pipeline.

## Files

| File | Description | Source |
|------|-------------|--------|
| `pais.shp` | Argentina country polygon | IGN Argentina |
| `banados.shp` | Wetlands polygons | IGN Argentina |
| `cuerpos_de_agua.shp` | Water bodies (lakes, reservoirs) | IGN Argentina |
| `cursos_de_agua.shp` | Watercourses (rivers) | IGN Argentina |
| `areas_de_aguas_continentales_BH140.shp` | Continental water areas | IGN Argentina |
| `ciudades_seleccion2.shp` | Selected cities (68 nodes) | Project team, from INDEC 1960 census |
| `pendiente_2.tif` | Slope raster (degrees) | Derived from DEM (pre-computed) |

The `areas_de_asentamientos_y_edificios_020105.shp` (urban settlements)
layer is read from `data/raw/geo/` — it is shared with the geo controls
pipeline.

## CRS
All files in EPSG:4326 (WGS84 geographic).

## Notes

- `ciudades_seleccion2.shp` has 68 cities with fields `localidad`,
  `provincia`, `pop1960`, and zone assignments. This is the active node
  set for the hypothetical networks. Alternative sets (`capitales`,
  `ciudades_zona_*`) are available in the old repo but not used in the
  main pipeline.
- `pendiente_2.tif` is a 0.01° resolution slope raster covering
  Argentina and neighboring areas.

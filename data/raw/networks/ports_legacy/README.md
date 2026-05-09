# ports_legacy

Salvaged from the old repo before deletion of `Old data/Train/` on 2026-05-09.

## Files

- `ports_id.{shp,shx,dbf,prj}` — IGN ports point shapefile used by the old
  unit-cost-raster pipeline to seed navigation least-cost paths.
- `portsCreateShp.py` — Python script that built the shapefile from the
  imported IGN data.
- `portsImport.do` — Stata script that imported/cleaned the raw IGN ports
  list.

## Original location

`Old data/Train/derived/unitcostrasters/{output,code}/`

## Why preserved

Needed for Phase 2 of the tau rebuild (navigation cost layer). Phase 1 is
land-only so this is not a current blocker, but keeping a local copy avoids
having to ask Cote to re-share.

## CRS

See `ports_id.prj`. Verify before use — the old pipeline reprojected to
ESRI:54034 (World Cylindrical Equal Area).

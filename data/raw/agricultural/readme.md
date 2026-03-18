# Raw Data: Agricultural Census

Argentina National Agricultural Censuses, district level.
Digitized from published INDEC volumes, one xlsx file per province.

## Source

- Provider: INDEC (Instituto Nacional de Estadística y Censos)
- Years: 1960 (22 provinces), 1988 (23 provinces, includes Tierra del Fuego)
- Format: xlsx, one file per province
- Variables: nexp (number of farms/EAPs), areatot_ha (total area in hectares)

## Files in This Directory

| Pattern | Year | Count | Format |
|---------|------|-------|--------|
| Agropecuario1960_*.xlsx | 1960 | 22 files | Wide: provincia, distrito, nexp, areatot_ha |
| Agricola1988_*.xlsx | 1988 | 23 files | Long: provincia, distrito, unidad, valor |

## Notes

- 1960 format is wide (one row per district, columns for nexp and areatot_ha)
- 1988 format is long (two rows per district: one for EAPs, one for ha)
- 1988 has forward-fill needed on provincia/distrito columns
- District names require extensive harmonization to match IPUMS geolev2 codes
- Capital Federal excluded from 1988 (not in IPUMS geolev2 system)

## Citation

INDEC. Censo Nacional Agropecuario 1960. Buenos Aires: INDEC, 1960.
INDEC. Censo Nacional Agropecuario 1988. Buenos Aires: INDEC, 1988.

## License

Argentine government publications; public domain.

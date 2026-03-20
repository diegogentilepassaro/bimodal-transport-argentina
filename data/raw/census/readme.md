# Raw Data: Census (IPUMS International + INDEC Publications)

## Source

**IPUMS International Census Microdata**
- Provider: Minnesota Population Center, University of Minnesota
- URL: https://international.ipums.org/international/
- Extract: ipumsi_00007 (person-level records for Argentina)
- Years in extract: 1970, 1980, 1991, 2001, 2010
- Format: Stata .dta (~2GB, 15M person records, 35 variables)
- Access: Registration required; agree to IPUMS terms of use

**Province Labels**
- File: prov_labels.xlsx
- Maps geolev1 codes to province name strings
- Used by clean_ipums.R to attach province names

## Files in This Directory

| File | Description | Format | Size |
|------|-------------|--------|------|
| ipumsi_00007.dta | IPUMS person-level microdata | .dta | ~2GB |
| prov_labels.xlsx | Province code → name mapping | .xlsx | <1KB |

## Key Variables (from IPUMS extract)

- geolev2: district identifier (312 departamentos, time-invariant boundaries)
- year: census year
- hhwt: household weight (used for aggregation)
- urban: urban/rural classification
- edattain: educational attainment
- empstat: employment status
- indgen: industry (general classification)
- occisco: occupation (ISCO classification)
- classwk: class of worker
- migrate5: 5-year migration status
- geo2_ar: district geography variable (contains name labels for crosswalk)

## Provenance

IPUMS extract #00007. TODO: record exact download date.

## Citation

Minnesota Population Center. Integrated Public Use Microdata Series,
International: Version 7.3 [dataset]. Minneapolis, MN: IPUMS, 2020.
https://doi.org/10.18128/D020.V7.3

## License / Redistribution

IPUMS data: NOT redistributable without permission. Replicators must
register at https://international.ipums.org/ and download their own extract.
See terms at https://international.ipums.org/international/terms.shtml.

# Census 1947 — Cuarto Censo General de la Nación

## Source
INDEC (Instituto Nacional de Estadística y Censos), Cuarto Censo General
de la Nación, 1947. Digitized from published volumes.

## How obtained
Scanned and digitized by the authors from physical volumes held at
Biblioteca del Congreso de la Nación and INDEC library. Excel files
created by manual transcription of printed tables.

## Files
- `1947_Cuadro1_*.xlsx` — Total population by distrito (one file per
  province, 23 files). Columns: `provincia`, `partido`, `n1947`.
- `1947_Cuadro14_*.xlsx` — Urban population by distrito and urban/rural
  classification (one file per province, 23 files). Columns: `provincia`,
  `partido`, `cUrbano`, `n1947`.

Province files: BuenosAires, Catamarca, Chaco, Chubut, Cordoba,
Corrientes, EntreRios, Formosa, Jujuy, LaPampa, LaRioja, Mendoza,
Misiones, Neuquen, RioNegro, Salta, SanJuan, SanLuis, SantaCruz,
SantaFe, SantiagoDelEstero, Tucuman, ZonaMilitardeComodoroRivadavia.

## Key variables
- `n1947`: population count (total in Cuadro 1, urban in Cuadro 14)
- `cUrbano`: the urban centre's NAME (Cuadro 14 only). An earlier
  version of this readme called it a classification flag; it is not.
- URBAN THRESHOLD (established 2026-07-29; see
  `results/tables/diagnostic_placebo_universe.txt`): across the 24
  Cuadro 14 sheets the smallest positive population is **2,002** and
  **none** falls below 2,000, so the 1947 urban classification uses the
  same 2,000-inhabitant rule that `clean_census_1960.R` applies to the
  1960 localities. That makes `urbpop_1947` and `urbpop_1960`
  comparable concepts, which matters because Cuadro 1 (`pop_1947`) is a
  full-universe district total while `pop_1960` is a locality-list sum —
  so the urban pair is the only like-for-like 1947/1960 comparison
  available without new archival data. See
  `results/tables/diagnostic_pop1960_universe.txt` for the mismatch.

## Notes
- The Zona Militar de Comodoro Rivadavia was a federal territory in 1947,
  dissolved in 1955. Its districts were split between Chubut and Santa Cruz.
- District boundaries differ substantially from 1991 IPUMS boundaries.
  The cleaning script handles all name changes and boundary reassignments.

## Citation
INDEC. 1947. "Cuarto Censo General de la Nación [dataset]." Instituto
Nacional de Estadística y Censos, Buenos Aires.

# Census 1960 — Censo Nacional de Población 1960

## Source
INDEC (Instituto Nacional de Estadística y Censos), Censo Nacional de
Población 1960. Digitized from published volumes.

## How obtained
Scanned and digitized by the authors from physical volumes held at
Biblioteca del Congreso de la Nación and INDEC library. Excel files
created by manual transcription of printed tables.

## Files
Organized by geographic region (matching the published volume structure):
- `1c1960_2.xlsx` — Part 2: Capital Federal (single file)
- `1c1960_3_1.xlsx` to `1c1960_3_8.xlsx` — Part 3: Buenos Aires, La Pampa
- `1c1960_4_1.xlsx` to `1c1960_4_7.xlsx` — Part 4: Córdoba, Santa Fe
- `1c1960_5_1.xlsx` to `1c1960_5_3.xlsx` — Part 5: Corrientes, Entre Ríos, Misiones
- `1c1960_6_1.xlsx` to `1c1960_6_3.xlsx` — Part 6: Chaco, Formosa, Santiago del Estero
- `1c1960_7_1.xlsx` to `1c1960_7_4.xlsx` — Part 7: Catamarca, Jujuy, La Rioja, Salta, Tucumán
- `1c1960_8_1.xlsx` to `1c1960_8_2.xlsx` — Part 8: Mendoza, San Juan, San Luis
- `1c1960_9_1.xlsx` to `1c1960_9_2.xlsx` — Part 9: Chubut, Neuquén, Río Negro, Santa Cruz, Tierra del Fuego

Columns: `provincia`, `distrito` (the departamento/partido the locality
belongs to), `pop` (population). There is NO locality-name column: each
row is one locality, identified only by the district containing it.

## Key variables
- `pop`: locality-level population count
- `provincia`: province name
- `distrito`: departamento/partido name (NOT the locality's own name)

## Notes
- Data is at the locality level (sub-district). The cleaning script
  collapses to distrito (departamento/partido) level.
- UNIVERSE (established 2026-07-29; see
  `results/tables/diagnostic_pop1960_universe.txt`): these tables list
  LOCALITIES, so dispersed rural population --- people not living in a
  named locality --- is absent from the source and therefore from
  `pop_1960`. The variable is "population living in named localities",
  not district population. It sums to 16.5M, and on the 237 districts
  where 1947 district totals exist (Cuadro 1, a full universe), 143 show
  `pop_1960` BELOW `pop_1947`. Anything that compares `pop_1960` with the
  IPUMS 1970+ population is comparing two different universes.
- Urban is defined as localities with population > 2000 (standard
  Argentine census definition).
- The raw files contain many OCR/transcription errors in district names,
  all corrected in the cleaning script.

## Citation
INDEC. 1960. "Censo Nacional de Población 1960 [dataset]." Instituto
Nacional de Estadística y Censos, Buenos Aires.

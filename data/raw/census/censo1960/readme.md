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

Columns: `provincia`, `distrito` (locality name), `pop` (population).

## Key variables
- `pop`: locality-level population count
- `provincia`: province name
- `distrito`: locality or partido name

## Notes
- Data is at the locality level (sub-district). The cleaning script
  collapses to distrito (departamento/partido) level.
- Urban is defined as localities with population > 2000 (standard
  Argentine census definition).
- The raw files contain many OCR/transcription errors in district names,
  all corrected in the cleaning script.

## Citation
INDEC. 1960. "Censo Nacional de Población 1960 [dataset]." Instituto
Nacional de Estadística y Censos, Buenos Aires.

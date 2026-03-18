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
- `cUrbano`: urban/rural classification flag (Cuadro 14 only)

## Notes
- The Zona Militar de Comodoro Rivadavia was a federal territory in 1947,
  dissolved in 1955. Its districts were split between Chubut and Santa Cruz.
- District boundaries differ substantially from 1991 IPUMS boundaries.
  The cleaning script handles all name changes and boundary reassignments.

## Citation
INDEC. 1947. "Cuarto Censo General de la Nación [dataset]." Instituto
Nacional de Estadística y Censos, Buenos Aires.

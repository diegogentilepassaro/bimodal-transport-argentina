# Replication Package for: "Transport Restructuring and Regional Development: Evidence from Argentina, 1960–1991"

Authors: José Belmar and Diego Gentile Passaro.

This README follows the [AEA Data Editor template](https://social-science-data-editors.github.io/template_README/template-README.html).
Status note: the package accompanies a working paper under coauthor review; items
marked `[AUTHORS: ...]` are formal statements the authors must confirm before
data deposit. Final exhibit numbering is locked at submission; output file names
encode the intended numbering.

## Overview

The code in this replication package cleans nine raw data sources (historical
transport-network shapefiles, digitized census publications, IPUMS
International microdata, and geographic rasters), builds transport-cost
surfaces and least-cost-path market access (MA) measures for 312 Argentine
districts, and estimates the causal effect of MA changes on regional outcomes
using two instruments (Larkin Plan closure rules and hypothetical road
networks). One master script, `code/main.R`, runs everything: raw data in,
every table, figure, and in-text number out.

Approximate total runtime: 2 hours on the reference machine (see Computational
Requirements). The dominant step is the least-cost-path (tau) computation.

Note on repository vs. deposit: the git repository contains **code only**
(all raw-data formats are gitignored). The data deposit archive contains
`data/raw/` in full, except the IPUMS microdata (see Data Availability).
Code license: MIT (see `LICENSE`); data are subject to the per-source
licenses listed below.

## Data Availability and Provenance Statements

### Statement about Rights

- [ ] `[AUTHORS: confirm before deposit]` We certify that the author(s) of the
  manuscript have legitimate access to and permission to use the data used in
  this manuscript.
- [ ] `[AUTHORS: confirm before deposit]` We certify that the author(s) of the
  manuscript have documented permission to redistribute/publish the data
  contained within this replication package, except where noted below.

### Summary of Availability

- Some data **cannot be made** publicly available (IPUMS microdata; see below).
- All other data are provided in the package or freely downloadable from the
  documented sources.

### Details on each Data Source

Every `data/raw/` subdirectory contains a `readme.md` with per-source
provenance. Summary:

| Source | Location | Provided? | Access and license |
|---|---|---|---|
| IPUMS International census microdata (Argentina 1970–2010), extract `ipumsi_00007` | `data/raw/census/` | **No** | Registration required at [international.ipums.org](https://international.ipums.org/); IPUMS terms prohibit redistribution. Replicators must download an extract with the variables listed in `data/raw/census/readme.md`. DOI: 10.18128/D020.V7.3 |
| IPUMS `geolev2` district boundaries (time-invariant, 312 districts) | `data/raw/geo/` | Yes | Distributed with IPUMS International; shapefile included. |
| Censo Nacional de Población 1960 (digitized) | `data/raw/census/censo1960/` | Yes | Argentine government publication (public domain); transcribed by the authors from volumes at the Biblioteca del Congreso de la Nación and INDEC library. |
| Cuarto Censo General de la Nación 1947 (digitized) | `data/raw/census/censo1947/` | Yes | Public domain; transcribed by the authors (same libraries). |
| Industrial censuses 1954, 1985 (digitized) | `data/raw/industrial/` | Yes | Public domain; transcribed by the authors. Issuing agency of the 1954 census under confirmation (repo issue #91). |
| Agricultural censuses 1960, 1988 (digitized) | `data/raw/agricultural/` | Yes | Public domain; transcribed by the authors. |
| Rail network 1979 with Larkin attributes (`lp_1979.shp`) | `data/raw/networks/` | Yes | Digitized by the project team from the 1979 *Política Ferroviaria* report (government publication). |
| Larkin Plan segment data | `data/raw/larkin/` | Yes | Public-domain government document (1962); digitized from volumes at the Biblioteca Nacional Mariano Moreno. |
| Road networks 1954/1970/1986 (`comparacion_54_70_86.shp`) | `data/raw/networks/` | Yes | Digitized by the project team from Automóvil Club Argentino road maps. `[AUTHORS: confirm redistribution rights for the digitized geometry before deposit]` |
| Transport cost parameters (Baumgartner and Palazzo 1969, Table II) | `code/config.R` | Yes | Parameter values transcribed from the published article (El Trimestre Económico 36(143)); no raw file needed. |
| GTOPO30 elevation | `data/raw/geo/` | Yes | Public domain, USGS. DOI: 10.5066/F7DF6PQS |
| Terrain Ruggedness Index (Nunn–Puga) | `data/raw/geo/tri.tif` | Yes (large, 7.5 GB) | Included in the data deposit. Source data are freely available from [diegopuga.org/data/rugged](https://diegopuga.org/data/rugged/) as text, converted to GeoTIFF by the authors outside this pipeline (no conversion script ships; see pre-deposit checklist). If absent, `clean_geo_controls.R` sets ruggedness to NA. |
| Human Mobility Index (Özak) | `data/raw/geo/HMI.tif` | Yes (large) | Free download from [Zenodo record 14285746](https://zenodo.org/records/14285746); ~2.2 GB. |
| FAO-GAEZ v3.0 wheat suitability | `data/raw/geo/wheatlo.tif` | Yes | Free download from [gaez.fao.org](https://gaez.fao.org/). |
| Galor–Özak caloric suitability rasters | `data/raw/geo/` | Yes | Distributed by the authors of Galor and Özak (2016). |
| IGN shapefiles (settlements, hydrography, country polygon, wetlands, water bodies) | `data/raw/geo/`, `data/raw/networks_hypo/` | Yes | Instituto Geográfico Nacional, Argentina ([ign.gob.ar](https://www.ign.gob.ar/)); open government data. |

All data citations appear in the paper's References section
(`paper/references.bib`, entries flagged `DATA CITATION`).

## Dataset List

See the table above; per-file inventories (file names, formats, key variables)
live in the `readme.md` of each `data/raw/` subdirectory (exceptions: the
`larkin/` and `costs/` readmes are stubs pending completion — see the
pre-deposit checklist). Raw data total approximately 11 GB, dominated by the
geographic rasters.

## Computational Requirements

### Software

- R 4.5.1. Package versions are locked in `renv.lock` (16 direct packages:
  arrow, exactextractr, fixest, gdistance, haven, here, igraph, kableExtra,
  modelsummary, raster, readxl, renv, sf, sp, terra, tibble).
- `code/00_setup.R` (called by `main.R` Stage A) restores the environment via
  `renv::restore()`, falling back to `install.packages()` from the lockfile.
  No manual package installation is needed.
- Python is **not** used (`requirements.txt` is intentionally empty).
- LaTeX (pdflatex + bibtex) to compile the paper; not needed for the results.

### Memory, Runtime, Storage

- Last run on: Apple M3 Pro (12 cores), 36 GB RAM, macOS.
- Runtime: the tau step (`C.3c`) is the bottleneck. `main.R` runs the
  parallel implementation (`03c_compute_taus_parallel.R`, 3–4 workers) by
  default: 27 least-cost-path runs (9 network cases × 3 sectors) took
  ≈61 minutes wall time in five batches on the reference machine
  (11,611 s summed across runs; per `logs/03c_compute_taus.log`). A serial
  backup (`03c_compute_taus.R`) exists and takes ≈3.2 h. All other steps
  run in seconds to minutes; end-to-end estimate ≈2 h (estimate, not a
  measured single run).
- The pipeline is deterministic: no random number generation is used
  (no seeds required).
- Memory: raster stages (C.3a–C.3c) are the peak; 16 GB RAM recommended
  (estimate; not formally profiled).
- Storage: ~11 GB raw data (of which `tri.tif` is 7.5 GB) + ~3 GB derived
  (`02_transition_grids` alone is 2.4 GB) + <100 MB results.
- `renv.lock` pins the 16 directly-used packages; transitive dependencies
  are resolved by `renv::restore()` at install time.

## Description of Programs

- `code/config.R` — all paths (rooted via `here::here()`), parameters
  (θ = 4.55/8.11, Baumgartner–Palazzo cost vectors), and main-spec constants.
- `code/00_setup.R` — environment restore, directory creation.
- `code/main.R` — master script; four stages, each step wrapped in
  `run_step()` (START/END logging) with `verify_outputs()` assertions:
  - **Stage A** Bootstrap (config + setup).
  - **Stage B** Base cleaning, one script per raw source:
    `code/base/*/clean_*.R` (geo controls, then IPUMS — which produces the
    district crosswalk the other census scripts merge against — then census
    1947, census 1960, industrial, agricultural, railroads, roads).
  - **Stage C** Pipeline: `code/pipeline/01_cost_raster.R` →
    `02_hypothetical_networks.R` → `clean_hypo_networks.R` (district
    intersection of the hypothetical networks; depends on the previous
    step's geometries) → `03a_build_cost_raster.R` →
    `03b_transition_grids.R` → `03c_compute_taus(_parallel).R` →
    `04_market_access.R` → `05_build_panel.R` → `06_merge_ma_into_panel.R`.
  - **Stage D** Analysis: `code/analysis/build_estimation_sample.R`, the
    figure and table scripts listed below, a heterogeneity diagnostic
    (`diagnostic_heterogeneity.R`), `generate_scalars.R` (writes
    `results/scalars.tex`, the AutoFill macros for in-text numbers), and
    the appendix figure scripts (A1–A3, counterfactual trio) as the final
    steps.
- `code/analysis/_iv_helpers.R` — shared IV estimation template
  (OLS / IV-LP / IV-Hypo / IV-Both grid, HC1 SEs).
- Data saves are validated in each script (key uniqueness checks and
  summary statistics appended to `data_file_manifest.log`).

## Instructions to Replicators

1. Install R 4.5.1.
2. Download the data deposit archive (the git repository alone contains no
   raw data), preserving the directory structure. Paths are resolved by
   `here::here()`; no path editing is required.
3. Obtain the IPUMS extract (see Data Availability) and place it at
   `data/raw/census/ipumsi_00007.dta`. If your copy lacks `HMI.tif`,
   download it from the Zenodo record above into `data/raw/geo/`
   (`tri.tif` has no direct download in raster form; see its row in the
   data source table).
4. Run everything:
   ```bash
   R CMD BATCH --no-save --no-restore code/main.R logs/main.Rout
   # or: Rscript code/main.R
   ```
   Stage A restores the R environment automatically on first run.
5. Outputs land in `results/tables/` (.tex + .csv), `results/figures/`
   (.pdf + .png), and `results/scalars.tex`. Per-step logs in `logs/`.
6. Compile the paper (optional):
   ```bash
   cd paper && pdflatex paper && bibtex paper && pdflatex paper && pdflatex paper
   ```

`results/` and `data/derived/` are fully regenerable: both can be deleted and
are rebuilt by step 4.

## List of Tables, Figures, and Programs

Output file names encode the paper's intended exhibit numbering (final
numbering locked at submission).

| Exhibit | Program | Output file(s) |
|---|---|---|
| Table 1 (network changes) | `code/base/networks/make_table_network_changes.R` | `results/tables/table_1_network_changes.tex` |
| Table: cost parameters (§3) | values from `code/config.R` (Baumgartner–Palazzo Table II) | typeset inline in `paper/section_3_data.tex` |
| Table 6 (pre-period balance) | `code/analysis/table_6_pre_balance.R` | `results/tables/table_6_pre_balance.tex` |
| Table 7 (pre-trends placebo) | `code/analysis/table_7_pre_trends.R` | `results/tables/table_7_pre_trends.tex` |
| Table 8 (first stage) | `code/analysis/table_8_first_stage.R` | `results/tables/table_8_first_stage.tex` |
| Table 9 (population IV) | `code/analysis/table_9_population.R` | `results/tables/table_9_population_iv.tex` |
| Table 10 (sectoral IV) | `code/analysis/table_10_sectoral.R` | `results/tables/table_10_sectoral_iv.tex` |
| Table 11 (other outcomes) | `code/analysis/table_11_other_outcomes.R` | `results/tables/table_11_other_outcomes_iv.tex` |
| Table 12 (robustness) | `code/analysis/table_12_robustness.R` | `results/tables/table_12_robustness.tex` |
| Table 13 (counterfactual MA) | `code/analysis/table_13_counterfactual.R` | `results/tables/table_13_counterfactual.tex` |
| Table 14 (mechanisms) | `code/analysis/table_14_mechanisms.R` | `results/tables/table_14_mechanisms.tex` |
| Figure 1 (network changes) | `code/base/networks/plot_figure_1.R` | `results/figures/figure_1_network_changes.pdf` |
| Figure 2 (Δlog MA choropleth) | `code/analysis/plot_figure_2.R` | `results/figures/figure_2_ma_change_choropleth.pdf` |
| Figure 3 in draft (counterfactual MA trio) | `code/analysis/plot_figure_c13.R` | `results/figures/figure_c13_ma_counterfactual_trio.pdf` |
| Figure A1 (cost schedule, appendix) | `code/analysis/plot_figure_a1_cost_schedule.R` | `results/figures/figure_a1_cost_schedule.pdf` |
| In-text numbers | `code/analysis/generate_scalars.R` | `results/scalars.tex` (51 macros) |

Generated but not currently included in the draft: `figure_3_rail_vs_road_change`
(`code/base/networks/plot_figure_3.R`), `figure_4_infra_vs_ma_scatter`
(`code/analysis/plot_figure_4.R`), `figure_a2_hypothetical_networks`,
`figure_a3_larkin_studied`, and the `diagnostic_*` outputs (exploratory
diagnostics documented in the repo's PR history).

## References

Full citations for all data sources are in `paper/references.bib` (rendered in
the paper's References section). Key data sources: Minnesota Population Center
(IPUMS International v7.3, DOI 10.18128/D020.V7.3); Dirección Nacional de
Estadística y Censos (Censo Nacional de Población 1960; Censo Nacional
Agropecuario 1960); Dirección Nacional del Servicio Estadístico (Cuarto Censo
General de la Nación 1947); Instituto Nacional de Estadística y Censos (Censo
Nacional Económico 1985; Censo Nacional Agropecuario 1988); Larkin (1962),
*Transportes Argentinos: Plan de Largo Alcance*; Baumgartner and Palazzo
(1969), *El Trimestre Económico* 36(143): 381–394; Secretaría de Estado de
Transporte y Obras Públicas (1981), *Política Ferroviaria 1979–1981*;
Automóvil Club Argentino road maps (1954–1986); Instituto Geográfico Nacional
shapefiles; U.S. Geological Survey GTOPO30 (DOI 10.5066/F7DF6PQS); FAO/IIASA
GAEZ v3.0; Nunn and Puga (2012); Galor and Özak (2016); Özak (2018).

---

### Pre-deposit checklist (authors)

- [ ] Confirm the two rights certifications above.
- [ ] Confirm redistribution rights for the digitized ACA road geometry.
- [ ] Resolve issue #91 (1954 census issuing agency) in `references.bib`.
- [ ] If deposit slips past 2026: update the IGN access-year fields in
      `references.bib` (currently `year = {2026}`), the accessed dates in
      their notes, and this README together.
- [ ] Complete the `data/raw/larkin/` and `data/raw/costs/` readmes (file
      inventories; the costs readme cites a retired Python script).
- [ ] Ship or document the TRI text-to-GeoTIFF conversion (currently done
      outside the pipeline), or include `tri.tif` in the deposit as-is.
- [ ] Delete `results/` and `data/derived/`, rerun `R CMD BATCH code/main.R`,
      and verify all exhibits regenerate and match the manuscript.
- [ ] Lock final exhibit numbering and update the mapping table above.

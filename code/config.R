# ==============================================================================
# config.R
#
# PURPOSE: Single source of truth for all paths and parameters in the
#          replication package. Every script sources this file first.
#          No magic numbers appear anywhere else in the codebase.
#
# READS:   Nothing (pure configuration)
# PRODUCES: All path variables and parameter constants in the R environment
#
# USAGE:   source(file.path(here::here(), "code", "config.R"))
#
# NOTES:
#   - rootdir is set via here::here() so the package is portable across
#     machines. A .here sentinel file at new_repo/ root ensures here resolves
#     to new_repo/ even when .git lives at a parent directory.
#   - All subdirectory paths are derived from rootdir using file.path().
#   - Never use setwd() anywhere in the project.
#   - Cost parameters sourced from Baumgartner & Palazzo (1969) as used in
#     the original QGIS pipeline. See data/raw/costs/readme.md for citation.
#   - Elasticity values sourced from the trade literature; see Section 3.3.2
#     of the paper for justification.
# ==============================================================================

# ---- 0. Guard against re-sourcing in child environments -------------------
if (!exists("config_loaded") || !isTRUE(config_loaded)) {

# ---- 1. Root directory (the ONLY absolute path in the whole project) ------
rootdir <- here::here()
message(sprintf("[config.R] rootdir = %s", rootdir))

# ---- 2. First-level subdirectory paths ------------------------------------
dir_code    <- file.path(rootdir, "code")
dir_data    <- file.path(rootdir, "data")
dir_results <- file.path(rootdir, "results")
dir_logs    <- file.path(rootdir, "logs")

# ---- 3. Raw data paths (READ-ONLY — never write here) ---------------------
dir_raw              <- file.path(dir_data, "raw")
dir_raw_census       <- file.path(dir_raw, "census")       # IPUMS + INDEC pubs
dir_raw_networks     <- file.path(dir_raw, "networks")     # rail / road shapefiles
dir_raw_larkin       <- file.path(dir_raw, "larkin")       # Larkin Plan segments
dir_raw_costs        <- file.path(dir_raw, "costs")        # Baumgartner & Palazzo
dir_raw_geo          <- file.path(dir_raw, "geo")          # district bounds, DEM
dir_raw_agricultural <- file.path(dir_raw, "agricultural") # Agr. census 1960, 1988
dir_raw_industrial   <- file.path(dir_raw, "industrial")   # Ind. census 1954, 1985

# ---- 4. Derived data paths (written by pipeline scripts) ------------------
dir_derived <- file.path(dir_data, "derived")

# base/ — cleaned source datasets (one subfolder per raw data source)
dir_derived_base        <- file.path(dir_derived, "base")
dir_derived_ipums       <- file.path(dir_derived_base, "ipums")
dir_derived_census1947 <- file.path(dir_derived_base, "census_1947")
dir_derived_census1960 <- file.path(dir_derived_base, "census_1960")
dir_derived_agr         <- file.path(dir_derived_base, "agricultural")
dir_derived_ind         <- file.path(dir_derived_base, "industrial")
dir_derived_geo         <- file.path(dir_derived_base, "geo")
dir_derived_networks    <- file.path(dir_derived_base, "networks")

# Pipeline step outputs
# 01 — cost rasters (.tif) produced by 01_build_rasters.py
dir_derived_rasters     <- file.path(dir_derived, "01_cost_rasters")
# 02 — gdistance transition grid objects (.rds) from 02_transition_grids.R
dir_derived_transitions <- file.path(dir_derived, "02_transition_grids")
# 03 — pairwise tau (transport cost) matrices (.parquet) from 03_compute_taus.R
dir_derived_taus        <- file.path(dir_derived, "03_taus")
# 04 — market access indices (.parquet) from 04_market_access.R
dir_derived_ma          <- file.path(dir_derived, "04_market_access")
# 05 — final estimation panel (merges all base/ sources + MA)
dir_derived_panel       <- file.path(dir_derived, "05_panel")
# 06 — regression output (model objects, coefficient extracts)
dir_derived_analysis    <- file.path(dir_derived, "06_analysis")

# ---- 5. Results paths -----------------------------------------------------
dir_tables  <- file.path(dir_results, "tables")
dir_figures <- file.path(dir_results, "figures")

# ---- 6. Spatial parameters ------------------------------------------------

# CRS used for all spatial operations.
# EPSG:4326  — WGS84 geographic (degrees), used for raw shapefiles.
# ESRI:54034 — World Cylindrical Equal Area, used for rasterisation so that
#              each cell has equal area (important for cost-distance calcs).
crs_geo      <- "EPSG:4326"
crs_raster   <- "ESRI:54034"

# Raster extent covering mainland Argentina (excludes Malvinas / Antarctica).
# Format: xmin, xmax, ymin, ymax in EPSG:4326 degrees.
raster_xmin  <- -73.560360
raster_xmax  <- -53.700000
raster_ymin  <- -55.100000
raster_ymax  <- -21.780814

# Raster grid dimensions (pixels) in ESRI:54034 projection.
# Verified from old pipeline output headers: 2399 cols × 3090 rows.
raster_ncols <- 2399L
raster_nrows <- 3090L

# ---- 6b. Cost-raster pipeline parameters (01_cost_raster.R) ---------------
#
# The cost raster is built on a larger grid than the final analysis extent
# and then cropped. Rasterization extent is wider to leave margin around the
# country polygon during rasterize(), so coastlines near the final extent
# don't get clipped by rasterization artifacts.
#
# Cost formula (Faber 2014, RES):
#   c_i = cost_land_baseline
#       + slope_deg_i
#       + cost_developed * is_developed_i
#       + cost_wetland   * is_wetland_i
#       + cost_water     * is_water_i
# Cells outside Argentina are set to cost_out_of_country (hard barrier).
# See Plan/cost_raster_and_lcp_decisions.md for documentation.

# Rasterization extent (EPSG:4326 degrees). Covers mainland + margin.
cost_raster_rast_xmin <- -80.0
cost_raster_rast_xmax <- -47.9
cost_raster_rast_ymin <- -55.5
cost_raster_rast_ymax <- -21.4

# Grid resolution for the cost raster (before crop to final extent).
cost_raster_ncols <- 1000L
cost_raster_nrows <- 1000L

# Cost values for 01_cost_raster.R (Faber 2014 formula).
cost_out_of_country  <- 1000000L  # cells outside Argentina, beyond coastal buffer
cost_land_baseline   <- 1L        # inside Argentina, no features
cost_developed       <- 25L       # urban settlements surcharge
cost_wetland         <- 25L       # wetlands surcharge
cost_water           <- 25L       # water bodies / rivers surcharge

# Out-of-country cells within this distance (km) of Argentine land are
# treated as water crossings (cost_land_baseline + cost_water = 26) rather
# than hard barriers. Lets LCPs cross the Paraná river and the Strait of
# Magellan while still blocking Río de la Plata mouth and open Atlantic.
cost_coastal_buffer_km <- 30

# ---- 6c. Transport cost raster: navigation-layer parameters --------------
#
# Linear network buffer (rivers in cursos_de_agua.shp are linear).
# 1 km buffer guarantees at least one full raster cell of navigable water
# per river segment at the ~1.2 km/cell resolution of this raster grid.
nav_linear_buffer_m <- 1000L

# Strait of Magellan additional buffer (on top of the polygon outline).
# The IGN polygon traces the water surface; in a ~1.2 km raster the
# polygon leaves a gap between the water and both coastlines. A 5 km
# buffer extends the polygon across both shorelines so at least one
# raster cell per coast is navigable, allowing Dijkstra to cross.
# Rule of thumb: ≥ 2 × ceiling(cellsize_km), giving one cell of slack
# per shore. At 1.2 km/cell, 5 km = ~4 cells per shore.
nav_magellan_buffer_m <- 5000L
stopifnot(nav_magellan_buffer_m >= 2 * 1200L)  # ≥ 2 cells per shore

# ---- 7. Transport cost parameters (Baumgartner & Palazzo 1969) -----------
#
# Source: Baumgartner, A. and Palazzo, L. (1969). "Estructura económica del
# transporte de carga automotor y ferroviario en la Argentina." Argentine
# Ministry of Economy.
#
# Unit: 1960 Argentine pesos per ton-km (nominal). All sectors use the same
# currency year, so ratios across sectors and across modes are inflation-
# invariant. Absolute levels are scale-free in the MA formula
# (MA_i = sum_j Pop_j / tau_ij^theta) — only ratios between on-network
# and off-network cost matter for routing.
#
# Three cost "sectors" are the tabulated cargo-density scenarios in
# Baumgartner & Palazzo Table II (see Plan/tau_calculation_review.md):
#   overall        = medium density (500 t/day) — main specification
#   agricultural   = high  density (1,000 t/day) — bulk grain shipment
#   manufacturing  = low   density (100 t/day) — small-batch goods
#
# At high density (agriculture), rail is cheaper than road. At low density
# (manufacturing), road is cheaper than rail. This is the scale-economies
# mechanism that makes railroads specialise in bulk goods.
#
# Named vectors allow unambiguous access: cost_road[["manufacturing"]]

# Cargo density (tons/day) of each tabulated B&P scenario. These are source
# parameters from Baumgartner & Palazzo Table II, not plot cosmetics: the
# sector -> density mapping documented above lives here so consumers
# (e.g. Figure A1) cannot drift from it.
cost_density <- c(
    overall       = 500,
    agricultural  = 1000,
    manufacturing = 100
)

# Road cost per ton-km by sector
cost_road <- c(
    overall       = 1.777,
    agricultural  = 1.000,
    manufacturing = 8.266
)

# Rail cost per ton-km by sector
cost_rail <- c(
    overall       = 1.874,
    agricultural  = 0.465,
    manufacturing = 13.490
)

# Navigation (waterway) cost per ton-km — same across sectors
cost_nav <- c(
    overall       = 0.621,
    agricultural  = 0.621,
    manufacturing = 0.621
)

# Minimum infrastructure cost: min(road, rail) by sector.
# Used when a cell is traversed by both rail and road — agent uses cheaper mode.
cost_mininfra <- c(
    overall       = 1.777,
    agricultural  = 0.465,
    manufacturing = 8.266
)
stopifnot(all(cost_mininfra == pmin(cost_road, cost_rail)))

# Land (off-network) traversal cost per ton-km per unit HMI.
#
# Derivation: cost_land = k × cost_road[manufacturing] / hmi_argentina
#
#   k = 17.9 — ratio of motor-vehicle travel speed to walking-equivalent
#     off-network speed. Project-specific calibration, not from a published
#     source: estimated by the original authors as the Buenos Aires–Rosario
#     Google Maps driving-vs-walking ratio. Documented in
#     Plan/tau_calculation_review.md, "Off-Network Travel Cost" section.
#
#   cost_road[manufacturing] = 8.266 — the highest road cost across sectors,
#     chosen as the anchor so that off-network cost dominates road cost even
#     in the manufacturing (low-cargo-density) scenario.
#
#   hmi_argentina = 0.2018 — mean Human Mobility Index (HMI) for Argentina,
#     in the global raster from Özak (2010, 2018). HMI measures potential
#     minimum travel time (hours) per 1km pixel accounting for terrain and
#     pre-industrial technological constraints. Özak's dataset is often
#     miscited as "Human Modification Index" — it is the Human *Mobility*
#     Index. Global raster at https://zenodo.org/records/14285746,
#     DOI:10.5281/zenodo.14285746, CC BY-SA 4.0.
#     Reference: Özak, Ömer. 2018. "Distance to the Pre-Industrial
#     Technological Frontier and Economic Development." Journal of
#     Economic Growth 23(2): 175–221.
#
# cost_land ≈ 733.2 (constant across sectors). Per-pixel off-network cost
# is cost_land × HMI_pixel_value. HMI ranges 0 to ~8 globally, so per-pixel
# off-network cost ranges 0 to ~5,900 — two to three orders of magnitude
# above on-network cost (0.465 to 13.5), which routes the Dijkstra path
# onto rail/road whenever those are available.
hmi_argentina <- 0.2018
cost_land     <- 17.9 * cost_road[["manufacturing"]] / hmi_argentina
cost_land_vec <- c(
    overall       = cost_land,
    agricultural  = cost_land,
    manufacturing = cost_land
)

# High-impedance sentinel value: cells outside Argentina or missing data.
cost_nodata_sentinel <- 999999L

# ---- 7b. Tau unit conversion (raster cost units -> 1960 pesos per ton) ----
#
# The cached tau matrices (data/derived/03_taus/) are in cost-raster units.
# Derivation of the conversion (verified in code, demonstrated numerically
# on a synthetic raster, and checked on cached taus; see
# code/analysis/diagnostic_tau_units.R and its report):
#   - 03b_transition_grids.R builds conductance = 1/mean(cost) and applies
#     gdistance::geoCorrection, which divides by inter-cell distance in the
#     CRS map units; crs_raster (ESRI:54034) is metre-based.
#   - costDistance therefore accumulates cost-value x METRES along the path.
#   - Raster cost values are Baumgartner & Palazzo 1960 pesos per ton-KM.
#   => tau_raster_units = (pesos/ton-km) x m = pesos/ton x 1000, exactly.
# Corridor sanity checks (BA-Rosario, Cordoba-Mendoza, Salta-Cordoba) are
# in results/tables/diagnostic_tau_units.txt.
#
# Use: tau_pesos_per_ton = tau_raster_units / tau_units_to_pesos.
# Needed by any iceberg normalization (1 + cost/V) with V in 1960 pesos/ton
# (Decision A option 1a; .kiro/decision_a_option1_scoping.md).
tau_units_to_pesos <- 1000

# ---- 8. Trade elasticity (theta) parameters -------------------------------
#
# Used in market access formula:
#   MA_i = sum_{j != i} Pop_j / tau_ij^theta
#
# theta["low"]  = 4.55
# theta["high"] = 8.11
#
# PENDING JUSTIFICATION — and a deeper conceptual flag (see
# .kiro/theta_benchmark_note.md for the full analysis).
#
# These two values were inherited from the old pipeline. They ARE
# trade-elasticity numbers: 8.11 = Caliendo-Parro agricultural trade
# elasticity; 4.55 is near Simonovska-Waugh (4.10) / Donaldson-Raj (3.80).
# The literature trade-elasticity range is ~3.6-12.9 (Eaton & Kortum 2002;
# Donaldson & Hornbeck 2016 use theta=8.22 estimated by NLS).
#
# BUT: the trade elasticity is the correct exponent ONLY on a NORMALIZED
# ICEBERG trade cost (a dimensionless multiplier >= 1, = absolute cost
# divided by the value of goods shipped; D&H 2016 footnote 32). Our tau is
# the RAW accumulated generalized transport cost (pesos/ton-km over the
# least-cost path), NOT normalized. Applying a trade-elasticity exponent to
# a raw cost is a category mismatch and is the leading suspect for why the
# estimated MA elasticity (0.046) is an order of magnitude below the
# Gibbons et al. 2024 benchmark (~0.3, which they obtain with a decay
# exponent of 0.5 on raw travel time). The theta sweep (PR #67) shows our
# elasticity reaches ~0.3 at theta ~ 1.
#
# RESOLUTION PENDING coauthor decision: either (a) normalize tau to a true
# iceberg cost and keep theta ~ 4-8, or (b) treat the index as a centrality
# measure with decay ~ 0.5-1. Do NOT cite a specific source as
# justification for 4.55/8.11 until this is resolved.
#
# Main results use theta["low"]; theta["high"] is reported in the appendix
# robustness table (task C34).
theta <- c(low = 4.55, high = 8.11)

# ---- 9. Network period codes ----------------------------------------------
#
# Period codes used as the first index in tau/raster filenames.
#   1 = ~1960  (pre-reform)
#   2 = ~1986  (post-reform)
#   3 = alternative post period (sensitivity check)
network_periods_main   <- c("1", "2", "3")
network_periods_instru <- c(
    "stu", "euc", "lcp", "euc_mst", "lcp_mst",
    "stu_euc", "stu_lcp", "stu_euc_mst", "stu_lcp_mst",
    "stu_only", "euc_only", "lcp_only", "euc_mst_only", "lcp_mst_only",
    "rails1", "rails2", "rails3",
    "roads1", "roads2", "roads3",
    "hmi_only"
)
# Sector codes: 0 = overall, 1 = agricultural, 2 = manufacturing
network_sectors <- 0:2

# Road-comparison layer (comparacion_54_70_86.shp) type2 codes present in
# 1986: kept {1, 5} + added {2, 3}. Single source of truth for what
# "1986 road network" means — used by the cost-raster builder (03a) and by
# Figure A2. Taxonomy documented in plot_figure_1.R.
roads_type2_1986 <- c(1L, 2L, 3L, 5L)

# ---- 9b. Main-spec analysis constants -------------------------------------
#
# The paper's main specification uses a single hypothetical-road instrument
# (the LCP minimum spanning tree). Other variants (euc_mst, lcp, euc) appear
# only in the robustness table. Centralizing the choice here keeps Tables
# 6, 7, 9 (and any future tables using the hypo instrument) in sync.
main_hypo_instrument <- "chg_logMA_lcp_mst_s0_elow"

# Baseline MA and pop controls common to Tables 6, 7, 8, 9.
# Standardized geographic controls + baseline log MA (1960) + log pop (1960).
geo_controls_main <- c(
    "elev_mean_std", "rugged_mea_std", "wheat_std",
    "preCal_std", "postCal_std", "dist_to_BA_std",
    "logMA_actual_1960_s0_elow", "log_pop_1960"
)

# ---- 10. Sample parameters ------------------------------------------------

n_districts <- 312L

census_years <- c(1947L, 1960L, 1970L, 1980L, 1991L, 2001L, 2010L)

# Main estimation window
year_pre    <- 1960L
year_post   <- 1991L

# Agricultural census years
year_agr_pre  <- 1960L
year_agr_post <- 1988L

# Industrial census years
year_ind_pre  <- 1954L
year_ind_post <- 1985L

# Road network years
year_roads_pre  <- 1954L
year_roads_post <- 1986L

# Rail network years
year_rail_pre   <- 1960L
year_rail_post  <- 1986L

# ---- 11. Geographic identifiers to exclude --------------------------------
# IPUMS geolev2 codes for non-mainland territories.
# CONVENTION: geolev2 is ALWAYS stored as character to avoid issues with
# leading zeros, type coercion, and silent comparison failures.
# All comparisons must use character values (e.g., "32006032", not 32006032L).
geolev2_exclude <- c(
    "238094004",   # Islas Malvinas
    "239094003"    # South Georgia and South Sandwich Islands
)

# ---- 12. Parallel computation parameters ----------------------------------
n_cores_default <- max(1L, min(20L, parallel::detectCores() - 2L))

# Cap for MEMORY-HEAVY forked sections (gdistance shortestPath /
# costDistance workers). 8 workers exhausted a 36 GB machine in the
# 2026-07-16 clean rerun (hard crash). Measured directly afterward
# (/tmp/measure_worker_rss.R, one shortestPath call per fork on the
# actual 977x618 construction_costs.tif transition object): each worker
# peaks at ~1.1 GB RSS, well above the 60 MB size of the transition
# object itself (gdistance/raster materialize extra structures per
# call). 8 workers x 1.1 GB = ~8.8 GB just for this step, on top of the
# ~4 GB the parent process already holds after loading the raster and
# building the transition matrix -- on machines with less headroom than
# the 36 GB reference machine (or with other processes competing for
# RAM) this alone can exhaust available memory. Capping at 4 keeps this
# step's peak to <= 4.4 GB of worker RSS, leaving headroom for the
# parent and other processes. Used by 02_hypothetical_networks.R and
# 03c_compute_taus_parallel.R.
n_cores_heavy <- max(1L, min(4L, n_cores_default))

# ---- 13. Regression specification parameters ------------------------------

geo_controls <- c(
    "elev_mean_std",
    "rugged_mea_std",
    "wheat_std",
    "area_km2",
    "dist_to_BA_std"
)

baseline_outcome <- "log_urbpop_1960"

star_thresholds <- c("*" = 0.10, "**" = 0.05, "***" = 0.01)

# ---- 15. Recentering diagnostic (Stage 1, papers-toolkit plan) ------------
# Parameters for the Borusyak-Hull recentering diagnostic
# (code/analysis/diagnostic_recentering_*.R). Diagnostic-only: nothing in
# the paper reads these outputs until Stage 2 is approved.
dir_derived_recentering <- file.path(dir_derived, "07_recentering")
# Endpoint snap tolerance for grouping rail segments into line units and
# for the free-end (branch) definition. 500 m: half the 1 km rasterization
# buffer, comfortably above digitization slack, below inter-line spacing.
recentering_snap_tol_m <- 500
# Deterministic base seed; draw s uses recentering_seed + s.
recentering_seed <- 20260720L
# Number of permutation draws for the diagnostic run.
recentering_S <- 100L
# Minimum studied and non-studied line units per stratum cell; thinner
# cells are merged (region cell first, then pooled) and documented.
recentering_min_cell <- 4L

# ---- 14. Sentinel / flag --------------------------------------------------
config_loaded <- TRUE
message("[config.R] Configuration loaded successfully.")

}  # end guard: if (!exists("config_loaded") || !isTRUE(config_loaded))

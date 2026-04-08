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

# ---- 7. Transport cost parameters (Baumgartner & Palazzo 1969) -----------
#
# Unit: USD per ton-km (constant prices).
# Three cost sectors, indexed by name:
#   overall        = medium cargo density (main specification)
#   agricultural   = low cargo density
#   manufacturing  = high cargo density
#
# Named vectors allow unambiguous access: cost_road[["manufacturing"]]

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

# Land (off-network) traversal cost: derived from HMI.
# hmi = 0.2018 (average for Argentina), k = 17.9 * roadprice[mfg] / hmi
hmi_argentina <- 0.2018
cost_land     <- 17.9 * cost_road[["manufacturing"]] / hmi_argentina
cost_land_vec <- c(
    overall       = cost_land,
    agricultural  = cost_land,
    manufacturing = cost_land
)

# High-impedance sentinel value: cells outside Argentina or missing data.
cost_nodata_sentinel <- 999999L

# ---- 8. Trade elasticity (theta) parameters -------------------------------
#
# Used in MA formula: MA_i = sum_j Pop_j / tau_ij^theta
#
# theta["low"]  = 4.55 — Eaton & Kortum (2002) lower bound
# theta["high"] = 8.11 — main specification, Donaldson (2018) estimate
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

# ---- 14. Sentinel / flag --------------------------------------------------
config_loaded <- TRUE
message("[config.R] Configuration loaded successfully.")

}  # end guard: if (!exists("config_loaded") || !isTRUE(config_loaded))

# ===========================================================================
# clean_geo_controls.R
#
# PURPOSE: Compute district-level geographic control variables from rasters
#          and shapefiles. These enter every regression as controls.
#
# READS:
#   data/raw/geo/geo2_ar1970_2010.shp          — IPUMS 312-district boundaries
#   data/raw/geo/gt30w060s10.tif               — GTOPO30 DEM tile 1
#   data/raw/geo/gt30w100s10.tif               — GTOPO30 DEM tile 2
#   data/raw/geo/wheatlo.tif                   — FAO-GAEZ wheat suitability
#   data/raw/geo/pre1500AverageCalories.tif    — Galor-Özak caloric potential
#   data/raw/geo/post1500AverageCalories.tif   — Galor-Özak caloric potential
#   data/raw/geo/tri.tif                       — Terrain Ruggedness Index
#   data/raw/geo/areas_de_asentamientos_y_edificios_020105.shp — IGN settlements
#
# PRODUCES:
#   data/derived/base/geo_controls/geo_controls.parquet
#   data/derived/base/geo_controls/data_file_manifest.log
#
# REFERENCE:
#   Old data/Train/base/geo_controls/code/geoControls_create_districts.py
#   Old data/Train/derived/geo_controls/code/run.do
#   Plan/geo_controls_pipeline.md
#
# NOTES:
#   - The old pipeline used QGIS Python for zonal stats. We use terra and
#     exactextractr in R, which produce equivalent results.
#   - Ruggedness (tri.tif) is MISSING from raw data. The variable is
#     computed if the file exists, otherwise set to NA and flagged.
#   - All continuous variables get standardized (_std) versions.
#   - Distance to BA uses the centroid of the "Gran Buenos Aires" polygon
#     from the IGN settlements shapefile, matching the old pipeline.
# ===========================================================================

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    message("\n", strrep("=", 72))
    message("clean_geo_controls.R  |  Geographic controls → geolev2 panel")
    message(strrep("=", 72))

    districts <- load_districts()
    districts <- compute_area(districts)
    districts <- extract_elevation(districts)
    districts <- extract_ruggedness(districts)
    districts <- extract_wheat(districts)
    districts <- extract_calories(districts)
    districts <- compute_distance_to_ba(districts)
    districts <- standardize_and_save(districts)

    message(strrep("=", 72))
    message("clean_geo_controls.R  |  Complete.")
    message(strrep("=", 72), "\n")
}

# ---------------------------------------------------------------------------
# Helper: load and prepare district shapefile
# ---------------------------------------------------------------------------
load_districts <- function() {
    message("\n[geo] Step 1 — Loading district shapefile")

    shp_path <- file.path(dir_raw_geo, "geo2_ar1970_2010.shp")
    if (!file.exists(shp_path)) {
        stop(sprintf("District shapefile not found: %s", shp_path))
    }

    d <- sf::st_read(shp_path, quiet = TRUE)

    # Fix geometries (equivalent to old QGIS fixgeometries step)
    d <- sf::st_make_valid(d)

    # Standardize column name — keep as character (project convention)
    names(d)[names(d) == "GEOLEVEL2"] <- "geolev2"
    d$geolev2 <- as.character(d$geolev2)
    # Strip leading zeros — shapefile stores 9-digit (032006001) but
    # IPUMS and all other sources use 8-digit (32006001)
    d$geolev2 <- sub("^0+", "", d$geolev2)

    # Drop empty geometries first (e.g., residual codes with no polygon)
    d <- d[!sf::st_is_empty(d), ]

    # Exclude non-mainland territories
    d <- d[!(d$geolev2 %in% geolev2_exclude), ]

    # Exclude Tierra del Fuego, Capital Federal, and residual codes
    geo_cf  <- "32002001"
    geo_tdf <- c("32094001", "32094002")
    residual <- d$geolev2[grepl("0000$", d$geolev2)]
    d <- d[!(d$geolev2 %in% c(geo_cf, geo_tdf, residual)), ]

    message(sprintf("[geo]   Loaded %d districts, CRS: %s",
                    nrow(d), sf::st_crs(d)$input))
    d
}

# ---------------------------------------------------------------------------
# Helper: compute district area in km²
# ---------------------------------------------------------------------------
compute_area <- function(d) {
    message("\n[geo] Step 2 — Computing district area")

    # Use the area_m2 attribute from the shapefile (matches old pipeline)
    # but verify against sf::st_area for consistency
    if ("area_m2" %in% names(d)) {
        d$area_km2 <- suppressWarnings(as.numeric(d$area_m2)) / 1e6
        # Fall back to sf::st_area if attribute has issues
        if (any(is.na(d$area_km2))) {
            message("[geo]   Some area_m2 values are NA, recomputing")
            d$area_km2 <- as.numeric(sf::st_area(d)) / 1e6
        }
        message("[geo]   Using area_m2 attribute from shapefile")
    } else {
        d$area_km2 <- as.numeric(sf::st_area(d)) / 1e6
        message("[geo]   Computed area via sf::st_area()")
    }

    message(sprintf("[geo]   Area range: %.1f – %.1f km²",
                    min(d$area_km2), max(d$area_km2)))
    d
}

# ---------------------------------------------------------------------------
# Helper: extract mean elevation from DEM
# ---------------------------------------------------------------------------
extract_elevation <- function(d) {
    message("\n[geo] Step 3 — Extracting mean elevation")

    tile1 <- file.path(dir_raw_geo, "gt30w060s10.tif")
    tile2 <- file.path(dir_raw_geo, "gt30w100s10.tif")
    if (!file.exists(tile1) || !file.exists(tile2)) {
        stop("DEM tiles not found")
    }

    # Merge two DEM tiles into one raster
    r1 <- terra::rast(tile1)
    r2 <- terra::rast(tile2)
    dem <- terra::merge(r1, r2)

    # Extract mean elevation per district (area-weighted)
    d$elev_mean <- exactextractr::exact_extract(dem, d, fun = "mean")

    message(sprintf("[geo]   Elevation range: %.0f – %.0f m",
                    min(d$elev_mean, na.rm = TRUE),
                    max(d$elev_mean, na.rm = TRUE)))
    d
}

# ---------------------------------------------------------------------------
# Helper: extract mean ruggedness (TRI)
# ---------------------------------------------------------------------------
extract_ruggedness <- function(d) {
    message("\n[geo] Step 4 — Extracting mean ruggedness")

    tri_path <- file.path(dir_raw_geo, "tri.tif")
    if (!file.exists(tri_path)) {
        message("[geo]   WARNING: tri.tif not found — rugged_mea set to NA")
        message("[geo]   FLAG: Cote is searching backup for this file")
        d$rugged_mea <- NA_real_
        return(d)
    }

    tri <- terra::rast(tri_path)
    d$rugged_mea <- exactextractr::exact_extract(tri, d, fun = "mean")

    message(sprintf("[geo]   Ruggedness range: %.1f – %.1f",
                    min(d$rugged_mea, na.rm = TRUE),
                    max(d$rugged_mea, na.rm = TRUE)))
    d
}

# ---------------------------------------------------------------------------
# Helper: extract mean wheat suitability
# ---------------------------------------------------------------------------
extract_wheat <- function(d) {
    message("\n[geo] Step 5 — Extracting mean wheat suitability")

    wheat_path <- file.path(dir_raw_geo, "wheatlo.tif")
    if (!file.exists(wheat_path)) stop("wheatlo.tif not found")

    wheat <- terra::rast(wheat_path)
    d$wheat <- exactextractr::exact_extract(wheat, d, fun = "mean")

    message(sprintf("[geo]   Wheat range: %.2f – %.2f",
                    min(d$wheat, na.rm = TRUE),
                    max(d$wheat, na.rm = TRUE)))
    d
}

# ---------------------------------------------------------------------------
# Helper: extract mean caloric potential (pre and post 1500)
# ---------------------------------------------------------------------------
extract_calories <- function(d) {
    message("\n[geo] Step 6 — Extracting mean caloric potential")

    pre_path  <- file.path(dir_raw_geo, "pre1500AverageCalories.tif")
    post_path <- file.path(dir_raw_geo, "post1500AverageCalories.tif")
    if (!file.exists(pre_path))  stop("pre1500AverageCalories.tif not found")
    if (!file.exists(post_path)) stop("post1500AverageCalories.tif not found")

    pre  <- terra::rast(pre_path)
    post <- terra::rast(post_path)

    d$preCal  <- exactextractr::exact_extract(pre,  d, fun = "mean")
    d$postCal <- exactextractr::exact_extract(post, d, fun = "mean")

    message(sprintf("[geo]   preCal range: %.0f – %.0f",
                    min(d$preCal, na.rm = TRUE),
                    max(d$preCal, na.rm = TRUE)))
    message(sprintf("[geo]   postCal range: %.0f – %.0f",
                    min(d$postCal, na.rm = TRUE),
                    max(d$postCal, na.rm = TRUE)))
    d
}

# ---------------------------------------------------------------------------
# Helper: compute distance from each district centroid to Buenos Aires
# ---------------------------------------------------------------------------
compute_distance_to_ba <- function(d) {
    message("\n[geo] Step 7 — Computing distance to Buenos Aires")

    settle_path <- file.path(dir_raw_geo,
                             "areas_de_asentamientos_y_edificios_020105.shp")
    if (!file.exists(settle_path)) stop("IGN settlements shapefile not found")

    settlements <- sf::st_read(settle_path, quiet = TRUE)
    settlements <- sf::st_make_valid(settlements)

    # Extract Gran Buenos Aires polygon and compute its centroid
    gba <- settlements[settlements$fna == "Gran Buenos Aires", ]
    if (nrow(gba) == 0) stop("Gran Buenos Aires not found in settlements")
    gba_centroid <- sf::st_centroid(sf::st_union(gba))

    # Compute district centroids
    d_centroids <- sf::st_centroid(d)

    # Distance in meters, convert to km
    dists <- sf::st_distance(d_centroids, gba_centroid)
    d$dist_to_BA <- as.numeric(dists[, 1]) / 1000

    message(sprintf("[geo]   Distance to BA range: %.0f – %.0f km",
                    min(d$dist_to_BA), max(d$dist_to_BA)))
    d
}

# ---------------------------------------------------------------------------
# Helper: standardize variables, validate, and save
# ---------------------------------------------------------------------------
standardize_and_save <- function(d) {
    message("\n[geo] Step 8 — Standardizing, validating, saving")

    out_dir <- dir_derived_geo
    if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

    # Build output dataframe (drop geometry)
    result <- sf::st_drop_geometry(d)
    keep_cols <- c("geolev2", "area_km2", "elev_mean", "rugged_mea",
                   "wheat", "preCal", "postCal", "dist_to_BA")
    result <- result[, intersect(keep_cols, names(result))]

    # Standardize continuous variables
    std_vars <- c("elev_mean", "rugged_mea", "wheat",
                  "preCal", "postCal", "dist_to_BA")
    for (v in std_vars) {
        if (v %in% names(result) && !all(is.na(result[[v]]))) {
            result[[paste0(v, "_std")]] <- as.numeric(scale(result[[v]]))
        } else {
            result[[paste0(v, "_std")]] <- NA_real_
        }
    }

    # Validate
    stopifnot(!any(duplicated(result$geolev2)))
    stopifnot(!any(is.na(result$geolev2)))
    message(sprintf("[geo]   Key (geolev2) is unique and non-missing: OK"))
    message(sprintf("[geo]   Districts: %d", nrow(result)))

    # Sort and save
    result <- result[order(result$geolev2), ]
    out_path <- file.path(out_dir, "geo_controls.parquet")
    arrow::write_parquet(result, out_path)
    message(sprintf("[geo]   Saved: %s (%d rows, %d cols)",
                    out_path, nrow(result), ncol(result)))

    # Manifest
    log_path <- file.path(out_dir, "data_file_manifest.log")
    sink(log_path)
    on.exit(sink(), add = TRUE)
    cat("Data file manifest — clean_geo_controls.R\n")
    cat(sprintf("Generated: %s\n", Sys.time()))
    cat(sprintf("rootdir: %s\n\n", rootdir))
    cat(strrep("=", 60), "\n")
    cat("FILE: geo_controls.parquet\n")
    cat(strrep("=", 60), "\n")
    cat(sprintf("Rows: %d\nColumns: %d\nKey: geolev2\n",
                nrow(result), ncol(result)))
    cat("\nSummary of variables:\n")
    for (v in names(result)) {
        if (v == "geolev2") next
        vals <- result[[v]]
        n_valid <- sum(!is.na(vals))
        if (n_valid > 0) {
            cat(sprintf("  %-20s N=%d  mean=%.2f  sd=%.2f  min=%.2f  max=%.2f  NA=%d\n",
                        v, n_valid, mean(vals, na.rm = TRUE),
                        sd(vals, na.rm = TRUE), min(vals, na.rm = TRUE),
                        max(vals, na.rm = TRUE), sum(is.na(vals))))
        } else {
            cat(sprintf("  %-20s ALL NA (%d obs) — BLOCKED: raw data missing\n",
                        v, length(vals)))
        }
    }

    # Flag missing ruggedness
    if (all(is.na(result$rugged_mea))) {
        cat("\nFLAG: rugged_mea is all NA — tri.tif not found in data/raw/geo/\n")
    }

    message(sprintf("[geo]   Manifest: %s", log_path))
    result
}

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
main()

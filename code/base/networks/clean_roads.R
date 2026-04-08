# ===========================================================================
# clean_roads.R
#
# PURPOSE: Intersect the road comparison shapefile with district boundaries
#          to compute road km per district for each period (1954, 1970, 1986).
#          Produces the road treatment variables for the estimation panel.
#
# READS:
#   data/raw/networks/comparacion_54_70_86.shp  — road network (1741 segments)
#   data/raw/geo/geo2_ar1970_2010.shp           — IPUMS 312-district boundaries
#
# PRODUCES:
#   data/derived/base/networks/roads_by_district.parquet
#       Key: geolev2. Variables: km of road by type2, period totals, changes.
#   data/derived/base/networks/data_file_manifest.log
#
# REFERENCE:
#   Old data/Train/derived/networks_to_districts/code/networks_to_districts.py
#   Old data/Train/derived/networks_to_districts/code/infra_to_Stata.do
#   Old data/Train/derived/preclean/code/preclean_departments_wide.do
#   Plan/roads_pipeline.md
#
# NOTES:
#   - type2 taxonomy (verified against old preclean code):
#       1 = present 1954+1970+1986, 2 = new in 1970, 3 = new in 1986,
#       4 = gone by 1986 (was 1954+1970), 5 = 1954+1986 (absent 1970,
#       treated as cartographic error), 6 = 1970 only, 7 = 1954 only.
#   - Types 4, 6, 7 are excluded from period totals (roads that disappear).
#   - The old pipeline used QGIS for intersection; we use sf in R.
#   - Lengths computed in EPSG:4326 via sf::st_length() with s2 geometry.
# ===========================================================================

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    message("\n", strrep("=", 72))
    message("clean_roads.R  |  Road network → district-level km")
    message(strrep("=", 72))

    districts <- load_districts()
    roads     <- load_roads()
    clipped   <- intersect_roads_districts(roads, districts)
    result    <- collapse_and_compute(clipped)
    save_output(result)

    message(strrep("=", 72))
    message("clean_roads.R  |  Complete.")
    message(strrep("=", 72), "\n")
}

# ---------------------------------------------------------------------------
# Helper: load district shapefile
# ---------------------------------------------------------------------------
load_districts <- function() {
    message("\n[roads] Step 1 — Loading district shapefile")

    shp_path <- file.path(dir_raw_geo, "geo2_ar1970_2010.shp")
    if (!file.exists(shp_path)) stop("District shapefile not found")

    d <- sf::st_read(shp_path, quiet = TRUE)
    d <- sf::st_make_valid(d)
    names(d)[names(d) == "GEOLEVEL2"] <- "geolev2"
    d$geolev2 <- sub("^0+", "", as.character(d$geolev2))
    d <- d[!sf::st_is_empty(d), ]
    d <- d[!(d$geolev2 %in% geolev2_exclude), ]

    message(sprintf("[roads]   Loaded %d districts", nrow(d)))
    d
}

# ---------------------------------------------------------------------------
# Helper: load road shapefile
# ---------------------------------------------------------------------------
load_roads <- function() {
    message("\n[roads] Step 2 — Loading road shapefile")

    shp_path <- file.path(dir_raw_networks, "comparacion_54_70_86.shp")
    if (!file.exists(shp_path)) stop("Road shapefile not found")

    roads <- sf::st_read(shp_path, quiet = TRUE)
    roads <- sf::st_make_valid(roads)

    message(sprintf("[roads]   Loaded %d road segments", nrow(roads)))
    message(sprintf("[roads]   type2 distribution:"))
    for (v in sort(unique(roads$type2))) {
        message(sprintf("[roads]     type2=%d: %d segments", v,
                        sum(roads$type2 == v)))
    }

    roads
}

# ---------------------------------------------------------------------------
# Helper: intersect roads with districts
# ---------------------------------------------------------------------------
intersect_roads_districts <- function(roads, districts) {
    message("\n[roads] Step 3 — Intersecting roads with districts")

    # Ensure same CRS
    if (sf::st_crs(roads) != sf::st_crs(districts)) {
        message("[roads]   Reprojecting roads to district CRS")
        roads <- sf::st_transform(roads, sf::st_crs(districts))
    }

    # Intersection: each road segment split by district boundaries
    clipped <- suppressWarnings(
        sf::st_intersection(roads, districts[, c("geolev2", "geometry")])
    )

    # Compute length in km
    clipped$length_km <- as.numeric(sf::st_length(clipped)) / 1000

    message(sprintf("[roads]   Clipped segments: %d", nrow(clipped)))
    message(sprintf("[roads]   Total road km: %.0f",
                    sum(clipped$length_km)))

    clipped
}

# ---------------------------------------------------------------------------
# Helper: collapse by district and compute period variables
# ---------------------------------------------------------------------------
collapse_and_compute <- function(clipped) {
    message("\n[roads] Step 4 — Collapsing by district, computing periods")

    df <- sf::st_drop_geometry(clipped)

    # Collapse: sum of length_km by geolev2 × type2
    agg <- aggregate(length_km ~ geolev2 + type2, data = df, FUN = sum)

    # Reshape wide: one column per type2
    wide <- reshape(agg, idvar = "geolev2", timevar = "type2",
                    direction = "wide", sep = "_")
    names(wide) <- sub("length_km_", "roadsall_type_", names(wide))

    # Fill NAs with 0 (district has no road of that type)
    type_cols <- grep("^roadsall_type_", names(wide), value = TRUE)
    for (col in type_cols) {
        wide[[col]][is.na(wide[[col]])] <- 0
    }

    # Ensure all 7 type columns exist (some types may be absent)
    for (t in 1:7) {
        col <- sprintf("roadsall_type_%d", t)
        if (!(col %in% names(wide))) wide[[col]] <- 0
    }

    # Period totals (verified against old preclean lines 616-619)
    wide$pav_and_grav_1954 <- wide$roadsall_type_1 +
                              wide$roadsall_type_5
    wide$pav_and_grav_1970 <- wide$roadsall_type_1 +
                              wide$roadsall_type_2 +
                              wide$roadsall_type_5
    wide$pav_and_grav_1986 <- wide$roadsall_type_1 +
                              wide$roadsall_type_2 +
                              wide$roadsall_type_3 +
                              wide$roadsall_type_5

    # Changes (treatment variables)
    wide$chg_pav_and_grav_86_54 <- wide$pav_and_grav_1986 -
                                   wide$pav_and_grav_1954
    wide$chg_pav_and_grav_70_54 <- wide$pav_and_grav_1970 -
                                   wide$pav_and_grav_1954
    wide$chg_pav_and_grav_86_70 <- wide$pav_and_grav_1986 -
                                   wide$pav_and_grav_1970

    # Connection dummies
    wide$connected_pav_grav_86_54 <- as.integer(
        wide$chg_pav_and_grav_86_54 > 0 & wide$pav_and_grav_1954 == 0
    )

    message(sprintf("[roads]   Districts with road data: %d", nrow(wide)))
    message(sprintf("[roads]   Mean pav+grav 1954: %.1f km",
                    mean(wide$pav_and_grav_1954)))
    message(sprintf("[roads]   Mean pav+grav 1986: %.1f km",
                    mean(wide$pav_and_grav_1986)))
    message(sprintf("[roads]   Mean change 86-54: %.1f km",
                    mean(wide$chg_pav_and_grav_86_54)))

    wide
}

# ---------------------------------------------------------------------------
# Helper: save output with manifest
# ---------------------------------------------------------------------------
save_output <- function(result) {
    message("\n[roads] Step 5 — Validating and saving")

    out_dir <- dir_derived_networks
    if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

    # Validate
    result <- result[order(result$geolev2), ]
    stopifnot(!any(duplicated(result$geolev2)))
    stopifnot(!any(is.na(result$geolev2)))
    message("[roads]   Key (geolev2) is unique and non-missing: OK")
    message(sprintf("[roads]   Districts: %d", nrow(result)))

    # Save
    out_path <- file.path(out_dir, "roads_by_district.parquet")
    arrow::write_parquet(result, out_path)
    message(sprintf("[roads]   Saved: %s (%d rows, %d cols)",
                    out_path, nrow(result), ncol(result)))

    # Manifest
    log_path <- file.path(out_dir, "data_file_manifest.log")
    sink(log_path)
    on.exit(sink(), add = TRUE)
    cat("Data file manifest — clean_roads.R\n")
    cat(sprintf("Generated: %s\n", Sys.time()))
    cat(sprintf("rootdir: %s\n\n", rootdir))
    cat(strrep("=", 60), "\n")
    cat("FILE: roads_by_district.parquet\n")
    cat(strrep("=", 60), "\n")
    cat(sprintf("Rows: %d\nColumns: %d\nKey: geolev2\n",
                nrow(result), ncol(result)))
    cat("\ntype2 taxonomy (verified):\n")
    cat("  1=1954+1970+1986, 2=new 1970, 3=new 1986,\n")
    cat("  4=gone by 1986, 5=1954+1986 (absent 1970),\n")
    cat("  6=1970 only, 7=1954 only\n")
    cat("\nSummary of variables:\n")
    for (v in names(result)) {
        if (v == "geolev2") next
        vals <- result[[v]]
        cat(sprintf("  %-30s mean=%.2f  sd=%.2f  min=%.2f  max=%.2f\n",
                    v, mean(vals), sd(vals), min(vals), max(vals)))
    }
    message(sprintf("[roads]   Manifest: %s", log_path))
}

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
main()

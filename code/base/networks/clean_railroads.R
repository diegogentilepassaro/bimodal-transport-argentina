# ===========================================================================
# clean_railroads.R
#
# PURPOSE: Build district-level railroad length variables and the Larkin
#          Plan instrument from the lp_1979 shapefile.
#
# READS:
#   data/raw/networks/lp_1979.shp          — 565 rail segments (WGS 84),
#                                             already joined to districts
#   data/raw/geo/geo2_ar1970_2010.shp      — IPUMS 312-district boundaries
#
# PRODUCES:
#   data/derived/base/networks/rails_by_district.parquet
#   data/derived/base/networks/data_file_manifest_rails.log
#
# FIELDS IN lp_1979 (per Cote):
#   status1979 ∈ {1, 2, 3}
#       1 = active in 1979 and 1986
#       2 = closed during the military dictatorship (1976–1983)
#       3 = closed before 1976 ("pre-dictatorship")
#   studied_co ∈ {0, 1}
#       1 = studied in the Larkin Plan (instrument)
#   recom_code ∈ {1, 2, 3} — Larkin-plan recommendation category
#
# OUTPUT VARIABLES (per district):
#   status1979_1, status1979_2, status1979_3   — km by status code
#   studied_0, studied_1                       — km by Larkin-study flag
#   tot_rails_1960, tot_rails_1986             — period rail-km totals
#   chg_tot_rails_86_60                        — change in rail km 1960→1986
#   studied_larkin                             — km of Larkin-studied rail
#   share_studied_larkin                       — studied_1 / (studied_0 +
#                                                 studied_1), NA if no rail
#
# ASSUMPTION (flag for Diego):
#   `status1979 == 3` = closed *before* 1976. I interpret this as "closed
#   between 1960 and 1976" — i.e., these segments WERE active in 1960.
#   So `tot_rails_1960 = status1979_1 + status1979_2 + status1979_3` and
#   `tot_rails_1986 = status1979_1`.
#   If instead status == 3 means "closed before 1960", switch to
#   `tot_rails_1960 = status1979_1 + status1979_2` and set the status3
#   column to an informational-only field. The manifest prints both
#   interpretations side by side for inspection.
# ===========================================================================

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    message("\n", strrep("=", 72))
    message("clean_railroads.R  |  Rail network → district-level km")
    message(strrep("=", 72))

    districts <- load_districts()
    rails     <- load_rails()
    clipped   <- intersect_rails_districts(rails, districts)
    result    <- collapse_and_compute(clipped, districts)
    save_output(result)

    message(strrep("=", 72))
    message("clean_railroads.R  |  Complete.")
    message(strrep("=", 72), "\n")
}

# ---------------------------------------------------------------------------
# Helper: load district shapefile (same pattern as clean_roads.R)
# ---------------------------------------------------------------------------
load_districts <- function() {
    message("\n[rails] Step 1 — Loading district shapefile")

    shp_path <- file.path(dir_raw_geo, "geo2_ar1970_2010.shp")
    if (!file.exists(shp_path)) stop("District shapefile not found")

    d <- sf::st_read(shp_path, quiet = TRUE)
    d <- sf::st_make_valid(d)
    names(d)[names(d) == "GEOLEVEL2"] <- "geolev2"
    d$geolev2 <- sub("^0+", "", as.character(d$geolev2))
    d <- d[!sf::st_is_empty(d), ]
    d <- d[!(d$geolev2 %in% geolev2_exclude), ]

    message(sprintf("[rails]   Loaded %d districts", nrow(d)))
    d
}

# ---------------------------------------------------------------------------
# Helper: load rail shapefile
# ---------------------------------------------------------------------------
load_rails <- function() {
    message("\n[rails] Step 2 — Loading rail shapefile")

    shp_path <- file.path(dir_raw_networks, "lp_1979.shp")
    if (!file.exists(shp_path)) stop("Rail shapefile not found")

    rails <- sf::st_read(shp_path, quiet = TRUE)
    rails <- sf::st_make_valid(rails)

    # Validate expected fields
    required <- c("status1979", "studied_co", "recom_code", "id_main")
    missing <- setdiff(required, names(rails))
    if (length(missing)) {
        stop("lp_1979.shp missing fields: ",
             paste(missing, collapse = ", "))
    }
    stopifnot(all(rails$status1979 %in% c(1, 2, 3)))
    stopifnot(all(rails$studied_co %in% c(0, 1)))

    message(sprintf("[rails]   Loaded %d rail segments", nrow(rails)))
    message("[rails]   status1979 distribution:")
    for (v in sort(unique(rails$status1979))) {
        message(sprintf("[rails]     status1979=%d: %d segments", v,
                        sum(rails$status1979 == v)))
    }
    message("[rails]   studied_co distribution:")
    for (v in sort(unique(rails$studied_co))) {
        message(sprintf("[rails]     studied_co=%d: %d segments", v,
                        sum(rails$studied_co == v)))
    }

    rails
}

# ---------------------------------------------------------------------------
# Helper: intersect rails with districts (same pattern as roads)
# ---------------------------------------------------------------------------
intersect_rails_districts <- function(rails, districts) {
    message("\n[rails] Step 3 — Intersecting rails with districts")

    if (sf::st_crs(rails) != sf::st_crs(districts)) {
        message("[rails]   Reprojecting rails to district CRS")
        rails <- sf::st_transform(rails, sf::st_crs(districts))
    }

    clipped <- suppressWarnings(
        sf::st_intersection(
            rails[, c("id_main", "status1979", "studied_co", "recom_code",
                      "geometry")],
            districts[, c("geolev2", "geometry")]
        )
    )

    clipped$length_km <- as.numeric(sf::st_length(clipped)) / 1000

    message(sprintf("[rails]   Clipped segments: %d", nrow(clipped)))
    message(sprintf("[rails]   Total rail km: %.0f",
                    sum(clipped$length_km)))

    clipped
}

# ---------------------------------------------------------------------------
# Helper: collapse by district and compute period + instrument variables
# ---------------------------------------------------------------------------
collapse_and_compute <- function(clipped, districts) {
    message("\n[rails] Step 4 — Collapsing by district")

    df <- sf::st_drop_geometry(clipped)

    # --- By status1979 ---
    agg_status <- aggregate(length_km ~ geolev2 + status1979,
                            data = df, FUN = sum)
    wide_status <- reshape(agg_status, idvar = "geolev2",
                           timevar = "status1979",
                           direction = "wide", sep = "_")
    names(wide_status) <- sub("length_km_", "status1979_", names(wide_status))

    # --- By studied_co ---
    agg_studied <- aggregate(length_km ~ geolev2 + studied_co,
                             data = df, FUN = sum)
    wide_studied <- reshape(agg_studied, idvar = "geolev2",
                            timevar = "studied_co",
                            direction = "wide", sep = "_")
    names(wide_studied) <- sub("length_km_", "studied_", names(wide_studied))

    # --- Merge the two wide tables ---
    wide <- merge(wide_status, wide_studied, by = "geolev2", all = TRUE)

    # --- Ensure all expected columns exist ---
    for (v in 1:3) {
        col <- sprintf("status1979_%d", v)
        if (!(col %in% names(wide))) wide[[col]] <- 0
    }
    for (v in 0:1) {
        col <- sprintf("studied_%d", v)
        if (!(col %in% names(wide))) wide[[col]] <- 0
    }

    # --- Fill NA with 0 (district has no rail of that category) ---
    num_cols <- setdiff(names(wide), "geolev2")
    for (col in num_cols) {
        wide[[col]][is.na(wide[[col]])] <- 0
    }

    # --- Period totals (primary interpretation: status 3 active in 1960) ---
    wide$tot_rails_1986 <- wide$status1979_1
    wide$tot_rails_1960 <- wide$status1979_1 +
                           wide$status1979_2 +
                           wide$status1979_3
    wide$chg_tot_rails_86_60 <- wide$tot_rails_1986 - wide$tot_rails_1960

    # --- Larkin instrument variables ---
    wide$studied_larkin <- wide$studied_1
    wide$share_studied_larkin <- ifelse(
        (wide$studied_0 + wide$studied_1) > 0,
        wide$studied_1 / (wide$studied_0 + wide$studied_1),
        NA_real_
    )

    # --- Add districts with NO rail segments: row of zeros (share = NA) ---
    all_districts <- sf::st_drop_geometry(districts)$geolev2
    missing_geos <- setdiff(all_districts, wide$geolev2)
    if (length(missing_geos)) {
        add <- data.frame(geolev2 = missing_geos)
        for (v in setdiff(names(wide), "geolev2")) {
            if (v == "share_studied_larkin") {
                add[[v]] <- NA_real_
            } else {
                add[[v]] <- 0
            }
        }
        wide <- rbind(wide, add[, names(wide)])
    }

    # --- Diagnostics ---
    # Alt interpretation (status 3 closed before 1960) computed on final table
    tot_1960_alt <- wide$status1979_1 + wide$status1979_2

    message(sprintf("[rails]   Districts with rail: %d / %d",
                    length(unique(df$geolev2)), nrow(wide)))
    message(sprintf("[rails]   Mean tot_rails_1960: %.1f km  (alt: %.1f km)",
                    mean(wide$tot_rails_1960), mean(tot_1960_alt)))
    message(sprintf("[rails]   Mean tot_rails_1986: %.1f km",
                    mean(wide$tot_rails_1986)))
    message(sprintf("[rails]   Mean chg 86-60: %.1f km",
                    mean(wide$chg_tot_rails_86_60)))
    message(sprintf("[rails]   Mean studied_larkin: %.1f km",
                    mean(wide$studied_larkin)))
    message(sprintf("[rails]   Mean share_studied_larkin: %.3f (N=%d)",
                    mean(wide$share_studied_larkin, na.rm = TRUE),
                    sum(!is.na(wide$share_studied_larkin))))

    # Attach alt total as attribute for the manifest to read
    attr(wide, "tot_1960_alt_mean") <- mean(tot_1960_alt)
    attr(wide, "tot_1960_alt_sum")  <- sum(tot_1960_alt)

    wide
}

# ---------------------------------------------------------------------------
# Helper: save output with manifest
# ---------------------------------------------------------------------------
save_output <- function(result) {
    message("\n[rails] Step 5 — Validating and saving")

    out_dir <- dir_derived_networks
    if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

    result <- result[order(result$geolev2), ]
    stopifnot(!any(duplicated(result$geolev2)))
    stopifnot(!any(is.na(result$geolev2)))
    message("[rails]   Key (geolev2) is unique and non-missing: OK")
    message(sprintf("[rails]   Districts: %d", nrow(result)))

    # Enforce character key
    result$geolev2 <- as.character(result$geolev2)

    out_path <- file.path(out_dir, "rails_by_district.parquet")
    # Strip attributes before writing (arrow can warn)
    attr_alt_mean <- attr(result, "tot_1960_alt_mean")
    attr_alt_sum  <- attr(result, "tot_1960_alt_sum")
    clean <- result
    attr(clean, "tot_1960_alt_mean") <- NULL
    attr(clean, "tot_1960_alt_sum")  <- NULL
    arrow::write_parquet(clean, out_path)
    message(sprintf("[rails]   Saved: %s (%d rows, %d cols)",
                    out_path, nrow(clean), ncol(clean)))

    log_path <- file.path(out_dir, "data_file_manifest_rails.log")
    sink(log_path)
    on.exit(sink(), add = TRUE)
    cat("Data file manifest — clean_railroads.R\n")
    cat(sprintf("Generated: %s\n", Sys.time()))
    cat(sprintf("rootdir: %s\n\n", rootdir))
    cat(strrep("=", 60), "\n")
    cat("FILE: rails_by_district.parquet\n")
    cat(strrep("=", 60), "\n")
    cat(sprintf("Rows: %d\nColumns: %d\nKey: geolev2\n",
                nrow(clean), ncol(clean)))
    cat("\nstatus1979 taxonomy:\n")
    cat("  1 = active in 1979 and 1986\n")
    cat("  2 = closed during the military dictatorship (1976-1983)\n")
    cat("  3 = closed before 1976 (pre-dictatorship)\n")
    cat("\nPeriod-total interpretation:\n")
    cat("  tot_rails_1986 = status1979_1\n")
    cat("  tot_rails_1960 = status1979_1 + status1979_2 + status1979_3\n")
    cat("    (assumes status==3 segments existed in 1960,\n")
    cat("     i.e., were closed between 1960 and 1976)\n")
    cat("\nSanity check — alternate interpretation\n")
    cat("  (if status==3 means closed before 1960):\n")
    cat(sprintf("  tot_rails_1960_alt mean = %.1f km\n", attr_alt_mean))
    cat(sprintf("  tot_rails_1960_alt sum  = %.0f km\n", attr_alt_sum))
    cat("\nSummary of variables:\n")
    for (v in names(clean)) {
        if (v == "geolev2") next
        vals <- clean[[v]]
        nna  <- sum(!is.na(vals))
        cat(sprintf(
            "  %-25s N=%d  mean=%.3f  sd=%.3f  min=%.3f  max=%.3f  NA=%d\n",
            v, nna, mean(vals, na.rm = TRUE), sd(vals, na.rm = TRUE),
            min(vals, na.rm = TRUE), max(vals, na.rm = TRUE),
            sum(is.na(vals))
        ))
    }
    message(sprintf("[rails]   Manifest: %s", log_path))
}

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
main()

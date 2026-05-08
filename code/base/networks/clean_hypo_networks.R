# ===========================================================================
# clean_hypo_networks.R
#
# PURPOSE: Intersect the four hypothetical network shapefiles (LCP, LCP-MST,
#          EUC, EUC-MST) with district boundaries to compute per-district
#          km totals. Produces the instrument variables for the
#          estimation panel.
#
# READS:
#   data/derived/02_hypothetical_networks/lcp_network.gpkg
#   data/derived/02_hypothetical_networks/lcp_mst.gpkg
#   data/derived/02_hypothetical_networks/euc_network.gpkg
#   data/derived/02_hypothetical_networks/euc_mst.gpkg
#   data/raw/geo/geo2_ar1970_2010.shp
#
# PRODUCES:
#   data/derived/base/networks/hypo_networks_by_district.parquet
#       Key: geolev2. Variables: hypo_LCP_kms, hypo_LCP_MST_kms,
#                                hypo_EUC_kms, hypo_EUC_MST_kms.
#   data/derived/base/networks/data_file_manifest_hypo.log
#
# REFERENCE:
#   code/base/networks/clean_roads.R  (same intersection pattern)
#   Plan/cost_raster_and_lcp_decisions.md
#   Plan/hypothetical_networks_plan.md
#
# NOTES:
#   - Districts with no network coverage get 0, not NA.
#   - Lengths computed via sf::st_length() with s2 geometry (geodesic).
# ===========================================================================

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    message("\n", strrep("=", 72))
    message("clean_hypo_networks.R  |  Hypothetical networks → district km")
    message(strrep("=", 72))

    districts <- load_districts()

    net_dir <- file.path(dir_derived, "02_hypothetical_networks")
    networks <- list(
        hypo_LCP_kms     = "lcp_network.gpkg",
        hypo_LCP_MST_kms = "lcp_mst.gpkg",
        hypo_EUC_kms     = "euc_network.gpkg",
        hypo_EUC_MST_kms = "euc_mst.gpkg"
    )

    result <- data.frame(geolev2 = districts$geolev2,
                         stringsAsFactors = FALSE)

    for (var_name in names(networks)) {
        message(sprintf("\n[hypo] Intersecting %s", var_name))
        net_path <- file.path(net_dir, networks[[var_name]])
        if (!file.exists(net_path)) {
            stop(sprintf("Network file not found: %s", net_path))
        }
        km_by_district <- intersect_and_collapse(net_path, districts)
        km_by_district <- setNames(km_by_district,
                                   c("geolev2", var_name))
        result <- merge(result, km_by_district, by = "geolev2",
                        all.x = TRUE)
        result[[var_name]][is.na(result[[var_name]])] <- 0
    }

    save_output(result)

    message(strrep("=", 72))
    message("clean_hypo_networks.R  |  Complete.")
    message(strrep("=", 72), "\n")
}

# ---------------------------------------------------------------------------
# Helper: load district shapefile
# ---------------------------------------------------------------------------
load_districts <- function() {
    message("\n[hypo] Loading district shapefile")

    shp_path <- file.path(dir_raw_geo, "geo2_ar1970_2010.shp")
    if (!file.exists(shp_path)) stop("District shapefile not found")

    d <- sf::st_read(shp_path, quiet = TRUE)
    d <- sf::st_make_valid(d)
    names(d)[names(d) == "GEOLEVEL2"] <- "geolev2"
    d$geolev2 <- sub("^0+", "", as.character(d$geolev2))
    d <- d[!sf::st_is_empty(d), ]
    d <- d[!(d$geolev2 %in% geolev2_exclude), ]

    message(sprintf("[hypo]   Loaded %d districts", nrow(d)))
    d
}

# ---------------------------------------------------------------------------
# Helper: intersect a single network with districts and collapse.
# Returns a data.frame with columns geolev2 and total_km.
# ---------------------------------------------------------------------------
intersect_and_collapse <- function(net_path, districts) {
    net <- sf::st_read(net_path, quiet = TRUE)
    net <- sf::st_make_valid(net)

    if (sf::st_crs(net) != sf::st_crs(districts)) {
        net <- sf::st_transform(net, sf::st_crs(districts))
    }

    clipped <- suppressWarnings(
        sf::st_intersection(net, districts[, c("geolev2", "geometry")])
    )
    clipped$length_km <- as.numeric(sf::st_length(clipped)) / 1000

    df <- sf::st_drop_geometry(clipped)
    agg <- aggregate(length_km ~ geolev2, data = df, FUN = sum)

    message(sprintf("[hypo]   %d edges → %d districts, total %.0f km",
                    nrow(net), nrow(agg), sum(agg$length_km)))
    agg
}

# ---------------------------------------------------------------------------
# Helper: save output with manifest
# ---------------------------------------------------------------------------
save_output <- function(result) {
    message("\n[hypo] Validating and saving")

    out_dir <- dir_derived_networks
    if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

    result <- result[order(result$geolev2), ]
    stopifnot(!any(duplicated(result$geolev2)))
    stopifnot(!any(is.na(result$geolev2)))
    message("[hypo]   Key (geolev2) is unique and non-missing: OK")
    message(sprintf("[hypo]   Districts: %d", nrow(result)))

    out_path <- file.path(out_dir, "hypo_networks_by_district.parquet")
    arrow::write_parquet(result, out_path)
    message(sprintf("[hypo]   Saved: %s (%d rows, %d cols)",
                    out_path, nrow(result), ncol(result)))

    log_path <- file.path(out_dir, "data_file_manifest_hypo.log")
    sink(log_path)
    on.exit(sink(), add = TRUE)
    cat("Data file manifest — clean_hypo_networks.R\n")
    cat(sprintf("Generated: %s\n", Sys.time()))
    cat(sprintf("rootdir: %s\n\n", rootdir))
    cat(strrep("=", 60), "\n")
    cat("FILE: hypo_networks_by_district.parquet\n")
    cat(strrep("=", 60), "\n")
    cat(sprintf("Rows: %d\nColumns: %d\nKey: geolev2\n",
                nrow(result), ncol(result)))
    cat("\nSummary of variables:\n")
    for (v in names(result)) {
        if (v == "geolev2") next
        vals <- result[[v]]
        cat(sprintf("  %-20s mean=%.2f  sd=%.2f  min=%.2f  max=%.2f  n_pos=%d\n",
                    v, mean(vals), sd(vals), min(vals), max(vals),
                    sum(vals > 0)))
    }
    message(sprintf("[hypo]   Manifest: %s", log_path))
}

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
main()

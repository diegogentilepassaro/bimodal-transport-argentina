# ===========================================================================
# 03c_compute_taus.R
#
# PURPOSE: Run Dijkstra least-cost-path distances between every pair of
#          IPUMS district centroids on each case's transition grid.
#          Produces a pairwise tau table per case.
#
# READS:
#   data/derived/02_transition_grids/transition_<case>.rds
#   data/raw/geo/geo2_ar1970_2010.shp                         (district polygons)
#
# PRODUCES:
#   data/derived/03_taus/tau_<case>.parquet
#       Columns: origin_geolev2 (chr), destination_geolev2 (chr), tau (num).
#       Long format, one row per unordered pair of distinct districts
#       (48,516 pairs = choose(312, 2)). Symmetric, so only the
#       lower-triangle is stored.
#
# DESIGN:
#   - Centroids: sf::st_centroid(sf::st_make_valid(polygon)) on the IPUMS
#     312-district shapefile. Matches the old pipeline's `centroids.shp`.
#   - gdistance::costDistance(transition, points, points) returns a 312×312
#     symmetric matrix. We take the strictly lower triangle for storage.
#     By convention, tau(i, i) = 0 (not stored; add back when re-materialising).
#   - costDistance() parallelises across destinations internally, so
#     we do not add R-level parallelism across cases in Phase 1.
#
# SANITY CHECKS (run automatically):
#   - No NaN (would indicate a numeric problem in the graph).
#   - No negative tau.
#   - Symmetry: tau(i, j) == tau(j, i), ignoring Inf entries (which can
#     occur for disconnected districts; on a land-only surface, TdF is
#     unreachable from mainland — expected for Phase 1).
#   - BA → Córdoba τ is printed for manual eyeball.
#
# USAGE:
#   Rscript code/pipeline/03c_compute_taus.R <case_label> [<case_label> ...]
#   If no args, processes all transition files in dir_derived_transitions/.
# ===========================================================================

suppressPackageStartupMessages({
    library(sf)
    library(sp)
    library(raster)
    library(gdistance)
    library(arrow)
})

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    if (!dir.exists(dir_derived_taus)) {
        dir.create(dir_derived_taus, recursive = TRUE)
    }

    args <- commandArgs(trailingOnly = TRUE)
    if (length(args) > 0) {
        cases <- args
    } else {
        rds_files <- list.files(dir_derived_transitions,
                                pattern = "^transition_.+\\.rds$",
                                full.names = FALSE)
        cases <- sub("^transition_", "", sub("\\.rds$", "", rds_files))
    }

    message("\n", strrep("=", 72))
    message("03c_compute_taus.R  |  Dijkstra on 312 district centroids")
    message(strrep("=", 72))
    message(sprintf("Cases: %s\n", paste(cases, collapse = ", ")))

    # Load centroids once (shared across all cases)
    centroids <- load_centroids()

    for (case in cases) compute_one_case(case, centroids)

    message(strrep("=", 72))
    message("03c_compute_taus.R  |  Complete.")
    message(strrep("=", 72), "\n")
}

# ---------------------------------------------------------------------------
# District centroids as SpatialPoints (gdistance needs sp)
# ---------------------------------------------------------------------------
load_centroids <- function() {
    message("[tau] Loading and centroid-ing district polygons")
    shp <- sf::st_read(file.path(dir_raw_geo, "geo2_ar1970_2010.shp"),
                       quiet = TRUE)
    shp <- sf::st_make_valid(shp)
    names(shp)[names(shp) == "GEOLEVEL2"] <- "geolev2"
    shp$geolev2 <- sub("^0+", "", as.character(shp$geolev2))

    # Drop Malvinas/South Georgia (geolev2_exclude) and empty geometries
    shp <- shp[!sf::st_is_empty(shp), ]
    shp <- shp[!(shp$geolev2 %in% geolev2_exclude), ]

    # Drop residual geolev2 codes (ending in 0000)
    shp <- shp[!grepl("0000$", shp$geolev2), ]

    stopifnot(nrow(shp) == 312L)
    stopifnot(!any(duplicated(shp$geolev2)))

    message(sprintf("[tau]   %d districts loaded", nrow(shp)))

    # Centroid, then reproject to the transition's CRS (ESRI:54034)
    cents_sf <- sf::st_centroid(shp)
    cents_sf <- sf::st_transform(cents_sf, crs = crs_raster)

    # Convert to sp::SpatialPoints for gdistance::costDistance
    cents_sp <- sf::as_Spatial(cents_sf[, "geolev2"])
    cents_sp
}

# ---------------------------------------------------------------------------
# One case
# ---------------------------------------------------------------------------
compute_one_case <- function(case, centroids) {
    message(sprintf("\n[tau] ==== CASE: %s ====", case))

    tg_path <- file.path(dir_derived_transitions,
                         sprintf("transition_%s.rds", case))
    if (!file.exists(tg_path)) stop("Transition not found: ", tg_path)

    message(sprintf("[tau]   Reading %s", tg_path))
    tg <- readRDS(tg_path)

    message("[tau]   Running gdistance::costDistance (312x312 Dijkstra)")
    t0 <- Sys.time()
    mat <- gdistance::costDistance(tg, centroids, centroids)
    mat <- as.matrix(mat)
    elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
    message(sprintf("[tau]   costDistance() took %.1f s", elapsed))

    # Validate: square, no NaN, no negative
    stopifnot(nrow(mat) == 312L, ncol(mat) == 312L)
    if (any(is.nan(mat))) stop("NaN in τ matrix")
    # Inf is allowed (disconnected districts on a land-only surface, e.g. TdF)
    finite_mat <- mat
    finite_mat[!is.finite(mat)] <- NA
    stopifnot(all(mat >= 0 | is.na(finite_mat)))

    # Count Inf so the log records them
    n_inf <- sum(is.infinite(mat))
    if (n_inf > 0) {
        message(sprintf(
            "[tau]   %d Inf entries (disconnected; e.g. TdF on land-only surface)",
            n_inf
        ))
    }

    # Symmetry check, ignoring Inf entries
    asym <- abs(mat - t(mat))
    asym[!is.finite(asym)] <- 0
    max_asym <- max(asym)
    if (max_asym > 1e-6) {
        warning(sprintf("[tau] Asymmetry > 1e-6: max |τ_ij - τ_ji| = %g",
                        max_asym))
    }
    # Diagonal should be 0 (within tolerance)
    diag_max <- max(abs(diag(mat)))
    stopifnot(diag_max < 1e-6)

    # Long-format lower triangle
    geolev2 <- as.character(centroids$geolev2)
    ij <- which(lower.tri(mat, diag = FALSE), arr.ind = TRUE)
    tau_df <- data.frame(
        origin_geolev2      = geolev2[ij[, "row"]],
        destination_geolev2 = geolev2[ij[, "col"]],
        tau                 = mat[ij],
        stringsAsFactors    = FALSE
    )
    # Sanity check: no self-pairs
    stopifnot(nrow(tau_df) == choose(312L, 2L))

    # BA to Córdoba eyeball (CF = 32002001; Córdoba capital = 32014014
    # per IPUMS; pick whichever code exists in centroids)
    print_eyeball_pair(tau_df, "32002001", "32014014",
                       "BA (32002001) → Córdoba (32014014)")

    # Summary (finite values only)
    finite_tau <- tau_df$tau[is.finite(tau_df$tau)]
    n_inf_pairs <- sum(!is.finite(tau_df$tau))
    message(sprintf(
        "[tau]   τ summary (finite): min=%.3f  median=%.1f  mean=%.1f  max=%.1f  (Inf pairs: %d)",
        min(finite_tau), median(finite_tau),
        mean(finite_tau), max(finite_tau), n_inf_pairs
    ))

    out_path <- file.path(dir_derived_taus,
                          sprintf("tau_%s.parquet", case))
    arrow::write_parquet(tau_df, out_path)
    message(sprintf("[tau]   Saved: %s (%d rows)", out_path, nrow(tau_df)))
}

# ---------------------------------------------------------------------------
# Helper: look up τ for a single (origin, destination) pair and print
# ---------------------------------------------------------------------------
print_eyeball_pair <- function(tau_df, i, j, label) {
    tau <- tau_df$tau[tau_df$origin_geolev2 == i &
                      tau_df$destination_geolev2 == j]
    if (length(tau) == 0) {
        tau <- tau_df$tau[tau_df$origin_geolev2 == j &
                          tau_df$destination_geolev2 == i]
    }
    if (length(tau) == 1) {
        message(sprintf("[tau]   eyeball %s: τ = %.1f", label, tau))
    } else {
        message(sprintf("[tau]   eyeball %s: (not found in table)", label))
    }
}

main()

# ===========================================================================
# 03c_compute_taus_parallel.R
#
# PURPOSE: Run the Dijkstra tau computation for multiple cases in parallel
#          via parallel::mclapply (forking on Unix-like). Each worker
#          handles one case; workers are independent.
#
# Replicates the logic of 03c_compute_taus.R but standalone so mclapply
# doesn't accidentally re-trigger the serial main().
#
# USAGE:
#   Rscript code/pipeline/03c_compute_taus_parallel.R <ncores> <case1> ...
#   If no case args, processes every transition in dir_derived_transitions/
#   that doesn't already have a corresponding tau file.
#
# NOTES:
#   - On macOS, mclapply forks. Each worker inherits the loaded packages
#     and globals (config.R sourced once in the parent).
#   - Memory: each worker holds a ~500 MB transition object during its
#     costDistance call. With ncores = 4, ~2 GB peak.
#   - Writes from each worker; parent only collects status.
# ===========================================================================

suppressPackageStartupMessages({
    library(parallel)
    library(sf)
    library(sp)
    library(raster)
    library(gdistance)
    library(arrow)
})

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    args <- commandArgs(trailingOnly = TRUE)
    if (length(args) < 1) {
        stop("Usage: Rscript 03c_compute_taus_parallel.R <ncores> [<case> ...]")
    }
    ncores <- as.integer(args[1])
    stopifnot(!is.na(ncores), ncores >= 1L)

    if (length(args) > 1) {
        cases <- args[-1]
    } else {
        rds_files <- list.files(dir_derived_transitions,
                                pattern = "^transition_.+\\.rds$",
                                full.names = FALSE)
        all_cases <- sub("^transition_", "", sub("\\.rds$", "", rds_files))
        done <- list.files(dir_derived_taus,
                           pattern = "^tau_.+\\.parquet$", full.names = FALSE)
        done_cases <- sub("^tau_", "", sub("\\.parquet$", "", done))
        cases <- setdiff(all_cases, done_cases)
    }

    if (length(cases) == 0L) {
        message("No cases to process.")
        return(invisible())
    }

    message("\n", strrep("=", 72))
    message(sprintf("03c_compute_taus_parallel.R  |  ncores = %d, %d cases",
                    ncores, length(cases)))
    message(strrep("=", 72))
    for (c in cases) message("  ", c)

    if (!dir.exists(dir_derived_taus)) {
        dir.create(dir_derived_taus, recursive = TRUE)
    }

    centroids <- load_centroids()

    t0 <- Sys.time()
    results <- mclapply(cases,
                        function(case) {
                            tryCatch(
                                compute_one_case(case, centroids),
                                error = function(e) {
                                    list(case = case,
                                         status = "FAIL",
                                         msg = conditionMessage(e))
                                }
                            )
                        },
                        mc.cores = ncores,
                        mc.preschedule = FALSE)
    elapsed <- as.numeric(difftime(Sys.time(), t0, units = "mins"))

    message(sprintf("\nAll cases done in %.1f minutes.", elapsed))
    fails <- 0L
    for (r in results) {
        if (is.null(r)) next
        if (is.list(r) && !is.null(r$status) && r$status == "FAIL") {
            message(sprintf("  FAIL  %s: %s", r$case, r$msg))
            fails <- fails + 1L
        } else {
            message(sprintf("  OK    %s", r$case))
        }
    }
    if (fails > 0L) stop(sprintf("%d cases failed", fails))
}

load_centroids <- function() {
    message("[tau] Loading district centroids")
    shp <- sf::st_read(file.path(dir_raw_geo, "geo2_ar1970_2010.shp"),
                       quiet = TRUE)
    shp <- sf::st_make_valid(shp)
    names(shp)[names(shp) == "GEOLEVEL2"] <- "geolev2"
    shp$geolev2 <- sub("^0+", "", as.character(shp$geolev2))
    shp <- shp[!sf::st_is_empty(shp), ]
    shp <- shp[!(shp$geolev2 %in% geolev2_exclude), ]
    shp <- shp[!grepl("0000$", shp$geolev2), ]
    stopifnot(nrow(shp) == 312L, !any(duplicated(shp$geolev2)))
    cents_sf <- suppressWarnings(sf::st_centroid(shp))
    cents_sf <- sf::st_transform(cents_sf, crs = crs_raster)
    sf::as_Spatial(cents_sf[, "geolev2"])
}

compute_one_case <- function(case, centroids) {
    tg_path <- file.path(dir_derived_transitions,
                         sprintf("transition_%s.rds", case))
    if (!file.exists(tg_path)) stop("Transition not found: ", tg_path)
    tg <- readRDS(tg_path)

    t0 <- Sys.time()
    mat <- gdistance::costDistance(tg, centroids, centroids)
    mat <- as.matrix(mat)
    elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))

    stopifnot(nrow(mat) == 312L, ncol(mat) == 312L)
    if (any(is.nan(mat))) stop("NaN in τ matrix for ", case)
    finite_mat <- mat
    finite_mat[!is.finite(mat)] <- NA
    stopifnot(all(mat >= 0 | is.na(finite_mat)))

    n_inf <- sum(is.infinite(mat))
    asym <- abs(mat - t(mat))
    asym[!is.finite(asym)] <- 0
    if (max(asym) > 1) {
        warning(sprintf("[tau:%s] asymmetry %.3f > 1", case, max(asym)))
    }
    stopifnot(max(abs(diag(mat))) < 1e-6)

    geolev2 <- as.character(centroids$geolev2)
    ij <- which(lower.tri(mat, diag = FALSE), arr.ind = TRUE)
    tau_df <- data.frame(
        origin_geolev2      = geolev2[ij[, "row"]],
        destination_geolev2 = geolev2[ij[, "col"]],
        tau                 = mat[ij],
        stringsAsFactors    = FALSE
    )
    stopifnot(nrow(tau_df) == choose(312L, 2L))

    out_path <- file.path(dir_derived_taus,
                          sprintf("tau_%s.parquet", case))
    arrow::write_parquet(tau_df, out_path)

    finite_tau <- tau_df$tau[is.finite(tau_df$tau)]
    list(case = case, status = "OK",
         elapsed_sec = elapsed, n_inf = n_inf,
         median_tau = median(finite_tau))
}

main()

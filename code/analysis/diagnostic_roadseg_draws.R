# ===========================================================================
# diagnostic_roadseg_draws.R
#
# PURPOSE: Permutation draws for the corridor-timing design instrument
#          (successor to PR #115's settlement design; see
#          diagnostic_roadseg_prep.R). Draw s permutes "paved early"
#          (by 1970, type2 = 2) status among the 893 expansion corridor
#          chains WITHIN strata (counts fixed); the draw's network =
#          1960 rails + 1954-convention roads + the draw's early
#          chains. Reuses the 03a case instrument_roadtiming_s0 (same
#          rail/road selectors; the RECENTER_EXTRA_FILE layer is what
#          differs) with tag namespace rs### and its own draws dir, so
#          the two designs' draws never collide.
#
#          Dose: the early margin is ~10,700 km of real trunk paving
#          (vs 4,600 km of stub connectors in the settlement design) --
#          the recentered instrument's first stage is the design's
#          relevance test, not a foregone collapse.
#
# IDENTITY GATE (s = 0, observed early chains): physical, as in the
# settlement design -- the identity raster must be cellwise <= the
# actual_1960 raster and strictly cheaper on a positive number of cells.
#
# SEED STREAM (cr-review PR #117 should-fix 2): draw s uses
# set.seed(recentering_seed + s), the SAME stream as the settlement
# engine (diagnostic_roadtiming_draws.R). Within-design randomization
# inference is unaffected, and the designs' permutation universes
# differ (893 chains vs 216 settlements), but any draw-by-draw
# CROSS-design statistic would inherit correlated Monte Carlo error.
# If either draw set is ever regenerated, add a per-design offset
# (e.g. recentering_seed + 200000 + s here).
#
# READS:   data/derived/07_recentering/roadseg/{chains.parquet,
#          chains.gpkg}; ucost_actual_1960_s0.tif (gate)
# PRODUCES:
#   data/derived/07_recentering/draws_roadseg/z_rc<s>.parquet
#       (geolev2, logMA, draw, n_early, early_km)
#   data/derived/07_recentering/draws_roadseg_manifest.log
#
# USAGE:
#   Rscript code/analysis/diagnostic_roadseg_draws.R [S] [n_workers]
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
    library(sf)
    library(terra)
    library(parallel)
})

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    args <- commandArgs(trailingOnly = TRUE)
    S <- if (length(args) >= 1) as.integer(args[1]) else recentering_S
    n_workers <- if (length(args) >= 2) as.integer(args[2]) else
        recentering_n_workers
    stopifnot(!is.na(S), S >= 1L, !is.na(n_workers), n_workers >= 1L)

    message("\n", strrep("=", 72))
    message(sprintf(
        "diagnostic_roadseg_draws.R  |  S = %d + identity, %d workers",
        S, n_workers))
    message(strrep("=", 72))

    dir_rs    <- file.path(dir_derived_recentering, "roadseg")
    dir_draws <- file.path(dir_derived_recentering, "draws_roadseg")
    if (!dir.exists(dir_draws)) dir.create(dir_draws, recursive = TRUE)

    ch <- as.data.frame(arrow::read_parquet(
        file.path(dir_rs, "chains.parquet")))
    geoms <- sf::st_read(file.path(dir_rs, "chains.gpkg"), quiet = TRUE)
    stopifnot(nrow(ch) == nrow(geoms),
              setequal(ch$chain_id, geoms$chain_id))
    ch <- ch[order(ch$chain_id), ]

    rscript <- file.path(R.home("bin"), "Rscript")
    p <- function(f) file.path(dir_code, "pipeline", f)
    run_child <- function(script, cargs) {
        status <- system2(rscript, c(shQuote(script), shQuote(cargs)),
                          stdout = "", stderr = "")
        if (!identical(status, 0L)) stop("[rs] child failed: ",
                                         basename(script))
    }

    run_draw <- function(s) {
        tag <- sprintf("rs%03d", s)
        out <- file.path(dir_draws, sprintf("z_rc%03d.parquet", s))
        if (file.exists(out)) {
            message(sprintf("[rs] %s: exists, skipping", tag))
            return(invisible())
        }
        t0 <- Sys.time()

        early <- ch$early
        if (s > 0L) {
            set.seed(recentering_seed + s)
            for (str_cell in unique(ch$stratum)) {
                sel <- which(ch$stratum == str_cell)
                early[sel] <- sample(ch$early[sel])
            }
            stopifnot(identical(
                tapply(early, ch$stratum, sum),
                tapply(ch$early, ch$stratum, sum)))
        }
        e_ids <- ch$chain_id[early]
        net <- geoms[geoms$chain_id %in% e_ids, ]
        net_file <- file.path(tempdir(),
                              sprintf("rs_chains_%s.gpkg", tag))
        sf::st_write(net, net_file, delete_dsn = TRUE, quiet = TRUE)

        case   <- "instrument_roadtiming_s0"
        case_t <- sprintf("%s_%s", case, tag)

        Sys.setenv(RECENTER_EXTRA_FILE = net_file, RECENTER_TAG = tag)
        on.exit(Sys.unsetenv(c("RECENTER_EXTRA_FILE", "RECENTER_TAG")),
                add = TRUE)
        run_child(p("03a_build_cost_raster.R"), "instrument_roadtiming_s0")
        Sys.unsetenv(c("RECENTER_EXTRA_FILE", "RECENTER_TAG"))

        # Physical identity gate BEFORE spending Dijkstra time.
        if (s == 0L) {
            u_id <- terra::rast(file.path(dir_derived_rasters,
                sprintf("ucost_%s.tif", case_t)))
            u_60 <- terra::rast(file.path(dir_derived_rasters,
                                          "ucost_actual_1960_s0.tif"))
            v_id <- terra::values(u_id); v_60 <- terra::values(u_60)
            ok <- !is.na(v_id) & !is.na(v_60)
            n_higher  <- sum(v_id[ok] > v_60[ok] + 1e-9)
            n_cheaper <- sum(v_id[ok] < v_60[ok] - 1e-9)
            message(sprintf(
                "[rs] identity gate: cheaper cells = %d, higher = %d",
                n_cheaper, n_higher))
            if (n_higher > 0L || n_cheaper == 0L) {
                stop("[rs] IDENTITY GATE FAILED: adding chains must ",
                     "only cheapen cells (higher = ", n_higher,
                     ", cheaper = ", n_cheaper, ")")
            }
        }

        run_child(p("03b_transition_grids.R"), case_t)
        run_child(p("03c_compute_taus_parallel.R"), c("1", case_t))
        run_child(p("04_market_access.R"), case_t)

        ma <- ensure_geolev2_char(arrow::read_parquet(file.path(
            dir_derived_ma, sprintf("ma_%s_elow.parquet", case_t))))
        stopifnot(nrow(ma) == 312L)
        z <- data.frame(geolev2 = ma$geolev2, logMA = ma$logMA,
                        draw = s, n_early = length(e_ids),
                        early_km = sum(net$length_km))
        z <- z[order(z$geolev2), ]
        arrow::write_parquet(z, out)

        unlink(c(
            file.path(dir_derived_rasters, sprintf("ucost_%s.tif", case_t)),
            file.path(dir_derived_transitions,
                      sprintf("transition_%s.rds", case_t)),
            file.path(dir_derived_taus, sprintf("tau_%s.parquet", case_t)),
            file.path(dir_derived_ma, sprintf("ma_%s_elow.parquet", case_t)),
            file.path(dir_derived_ma, sprintf("ma_%s_ehigh.parquet", case_t)),
            net_file))
        message(sprintf(
            "[rs] %s done in %.1f min (early = %d chains, %.0f km)",
            tag, as.numeric(difftime(Sys.time(), t0, units = "mins")),
            length(e_ids), sum(net$length_km)))
    }

    run_draw(0L)
    results <- mclapply(seq_len(S), function(s) {
        tryCatch({ run_draw(s); NULL },
                 error = function(e) sprintf("rs%03d: %s", s,
                                             conditionMessage(e)))
    }, mc.cores = n_workers, mc.preschedule = FALSE)
    fails <- Filter(Negate(is.null), results)
    if (length(fails) > 0L) {
        for (f in fails) message("[rs] FAIL ", f)
        stop(sprintf("[rs] %d of %d draws failed", length(fails), S))
    }

    done <- list.files(dir_draws, pattern = "^z_rc\\d+\\.parquet$")
    sink(file.path(dir_derived_recentering,
                   "draws_roadseg_manifest.log"))
    cat("Data file manifest -- diagnostic_roadseg_draws.R\n")
    cat(sprintf("Generated: %s\n", Sys.time()))
    cat(sprintf("Draw files: %d (incl. identity)  |  seed base: %d\n",
                length(done), recentering_seed))
    sink()
    message(sprintf("[rs] Complete: %d draw files.", length(done)))
}

main()

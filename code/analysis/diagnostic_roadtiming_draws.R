# ===========================================================================
# diagnostic_roadtiming_draws.R
#
# PURPOSE: Permutation draws for the road-timing design instrument
#          (issue #114). Draw s permutes "connected early" status among
#          the 1954-unconnected settlements WITHIN strata (counts
#          fixed); the draw's network = 1960 rails + 1954 roads +
#          predicted LCP links of the draw's early settlements
#          (03a case instrument_roadtiming, RECENTER_EXTRA_FILE hook).
#          Unlike the hypo families, the randomized margin here is the
#          WORLD's shock (actual connection timing), so the recentered
#          instrument is relevant by construction if timing predicts
#          actual MA changes -- exactly what the results script tests.
#
# IDENTITY GATE (s = 0, observed early set): no committed baseline
# exists for this new case, so the gate is physical: the identity cost
# raster must be cellwise <= the actual_1960 raster (adding roads can
# only cheapen cells) and strictly cheaper on a positive number of
# cells (the predicted links exist off the 1954 network).
#
# SEED STREAM (cr-review PR #117 should-fix 2): draw s uses
# set.seed(recentering_seed + s), the SAME stream as the corridor
# engine (diagnostic_roadseg_draws.R). Within-design randomization
# inference is unaffected, but any draw-by-draw CROSS-design statistic
# would inherit correlated Monte Carlo error. If either draw set is
# ever regenerated, add a per-design offset.
#
# READS:   data/derived/07_recentering/roadtiming/{settlements.parquet,
#          links.gpkg}; ucost_actual_1960_s0.tif (gate)
# PRODUCES:
#   data/derived/07_recentering/draws_roadtiming/z_rc<s>.parquet
#       (geolev2, logMA, draw, n_early, links_km)
#   data/derived/07_recentering/draws_roadtiming_manifest.log
#
# USAGE:
#   Rscript code/analysis/diagnostic_roadtiming_draws.R [S] [n_workers]
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
        "diagnostic_roadtiming_draws.R  |  S = %d + identity, %d workers",
        S, n_workers))
    message(strrep("=", 72))

    dir_rt    <- file.path(dir_derived_recentering, "roadtiming")
    dir_draws <- file.path(dir_derived_recentering, "draws_roadtiming")
    if (!dir.exists(dir_draws)) dir.create(dir_draws, recursive = TRUE)

    st <- as.data.frame(arrow::read_parquet(
        file.path(dir_rt, "settlements.parquet")))
    links <- sf::st_read(file.path(dir_rt, "links.gpkg"), quiet = TRUE)
    stopifnot(nrow(st) == nrow(links),
              setequal(st$sid, links$sid))
    st <- st[order(st$sid), ]

    rscript <- file.path(R.home("bin"), "Rscript")
    p <- function(f) file.path(dir_code, "pipeline", f)
    run_child <- function(script, cargs) {
        status <- system2(rscript, c(shQuote(script), shQuote(cargs)),
                          stdout = "", stderr = "")
        if (!identical(status, 0L)) stop("[rt] child failed: ",
                                         basename(script))
    }

    run_draw <- function(s) {
        tag <- sprintf("rt%03d", s)
        out <- file.path(dir_draws, sprintf("z_rc%03d.parquet", s))
        if (file.exists(out)) {
            message(sprintf("[rt] %s: exists, skipping", tag))
            return(invisible())
        }
        t0 <- Sys.time()

        early <- st$early
        if (s > 0L) {
            set.seed(recentering_seed + s)
            for (str_cell in unique(st$stratum)) {
                sel <- which(st$stratum == str_cell)
                early[sel] <- sample(st$early[sel])
            }
            stopifnot(identical(
                tapply(early, st$stratum, sum),
                tapply(st$early, st$stratum, sum)))
        }
        e_sids <- st$sid[early]
        net <- links[links$sid %in% e_sids, ]
        net_file <- file.path(tempdir(),
                              sprintf("rt_links_%s.gpkg", tag))
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
                "[rt] identity gate: cheaper cells = %d, higher = %d",
                n_cheaper, n_higher))
            if (n_higher > 0L || n_cheaper == 0L) {
                stop("[rt] IDENTITY GATE FAILED: adding links must ",
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
                        draw = s, n_early = length(e_sids),
                        links_km = sum(net$len_km))
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
            "[rt] %s done in %.1f min (early = %d, links = %.0f km)",
            tag, as.numeric(difftime(Sys.time(), t0, units = "mins")),
            length(e_sids), sum(net$len_km)))
    }

    run_draw(0L)
    results <- mclapply(seq_len(S), function(s) {
        tryCatch({ run_draw(s); NULL },
                 error = function(e) sprintf("rt%03d: %s", s,
                                             conditionMessage(e)))
    }, mc.cores = n_workers, mc.preschedule = FALSE)
    fails <- Filter(Negate(is.null), results)
    if (length(fails) > 0L) {
        for (f in fails) message("[rt] FAIL ", f)
        stop(sprintf("[rt] %d of %d draws failed", length(fails), S))
    }

    done <- list.files(dir_draws, pattern = "^z_rc\\d+\\.parquet$")
    sink(file.path(dir_derived_recentering,
                   "draws_roadtiming_manifest.log"))
    cat("Data file manifest -- diagnostic_roadtiming_draws.R\n")
    cat(sprintf("Generated: %s\n", Sys.time()))
    cat(sprintf("Draw files: %d (incl. identity)  |  seed base: %d\n",
                length(done), recentering_seed))
    sink()
    message(sprintf("[rt] Complete: %d draw files.", length(done)))
}

main()

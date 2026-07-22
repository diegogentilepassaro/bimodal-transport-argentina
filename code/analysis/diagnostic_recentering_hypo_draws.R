# ===========================================================================
# diagnostic_recentering_hypo_draws.R
#
# PURPOSE: Part C of the recentering deep dive (design approved by
#          Diego 2026-07-22): node-permutation draws for the
#          hypothetical-road instrument. Per draw s = 1..S:
#            1. Within each INDEC region, hold the OBSERVED number of
#               included band cities (10k <= pop1960 < 25k, non-capital)
#               fixed and randomly sample WHICH ones join the network;
#               anchors (capitals + >= 25k) are always in, 'out' cities
#               never. The observed node set is one admissible
#               realization; draw 0 is the identity.
#            2. Rebuild the LCP-MST over the drawn node set from the
#               precomputed 68-city pair-cost matrix and pair
#               geometries (diagnostic_recentering_hypo_prep.R).
#            3. Drive the existing 03a -> 03b -> 03c -> 04 chain
#               (case instrument_lcp_mst_s0, RECENTER_HYPO_FILE +
#               RECENTER_TAG hooks) and store the draw's logMA vector.
#
#          EPISTEMIC STATUS (stated in the plan and the PR): this
#          randomness is the researcher's, not the world's. The output
#          characterizes the hypo instrument (how much is generic
#          backbone geography = mu_hypo vs marginal-node choices), it
#          does NOT certify design-based validity as the Larkin
#          permutation does.
#
#          Identity gate: draw 0 must reproduce the baseline
#          ma_instrument_lcp_mst_s0_elow to float precision, both
#          validating the hooks and proving the observed set is one
#          realization of the process.
#
# READS:   data/derived/07_recentering/hypo/{city_pool.parquet,
#          pair_costs.parquet, pair_geoms.gpkg}
# PRODUCES:
#   data/derived/07_recentering/draws_hypo/z_rc<s>.parquet
#       (geolev2, logMA, draw, n_nodes)
#   data/derived/07_recentering/draws_hypo_manifest.log
#
# RUNTIME: ~4-6 min per draw; parallel across n_workers as in the
#          Larkin engine. Run AFTER the sector draw runs finish.
#
# USAGE:
#   Rscript code/analysis/diagnostic_recentering_hypo_draws.R [S] [n_workers]
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
    library(sf)
    library(igraph)
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
        "diagnostic_recentering_hypo_draws.R  |  S = %d + identity, %d workers",
        S, n_workers))
    message(strrep("=", 72))

    dir_hypo  <- file.path(dir_derived_recentering, "hypo")
    dir_draws <- file.path(dir_derived_recentering, "draws_hypo")
    if (!dir.exists(dir_draws)) dir.create(dir_draws, recursive = TRUE)

    pool  <- as.data.frame(arrow::read_parquet(
        file.path(dir_hypo, "city_pool.parquet")))
    costs <- as.data.frame(arrow::read_parquet(
        file.path(dir_hypo, "pair_costs.parquet")))
    geoms <- sf::st_read(file.path(dir_hypo, "pair_geoms.gpkg"),
                         quiet = TRUE)
    stopifnot(nrow(pool) == 68L, nrow(costs) == choose(68L, 2L),
              nrow(geoms) == nrow(costs))
    pool <- pool[order(pool$city_id), ]

    # Fast pair-cost lookup matrix.
    cm <- matrix(NA_real_, 68, 68)
    cm[cbind(costs$i, costs$j)] <- costs$cost
    cm[cbind(costs$j, costs$i)] <- costs$cost
    diag(cm) <- 0  # igraph rejects NA; 0 self-cost = no self-edge
    stopifnot(!anyNA(cm))
    # Pair geometry index: key "i_j" with i < j.
    gkey <- sprintf("%d_%d", geoms$i, geoms$j)

    anchors <- pool$city_id[pool$class == "anchor"]
    band    <- pool[pool$class == "band", ]
    band_by_region <- split(band, band$region)
    n_in_by_region <- vapply(band_by_region,
                             function(b) sum(b$observed_in), integer(1))
    message(sprintf(
        "[hypo] anchors %d | band %d (observed-in %d) | regions: %s",
        length(anchors), nrow(band), sum(band$observed_in),
        paste(sprintf("%s %d/%d", names(band_by_region),
                      n_in_by_region,
                      vapply(band_by_region, nrow, integer(1))),
              collapse = ", ")))

    rscript <- file.path(R.home("bin"), "Rscript")
    p <- function(fname) file.path(dir_code, "pipeline", fname)
    run_child <- function(script, args) {
        status <- system2(rscript, c(shQuote(script), shQuote(args)),
                          stdout = "", stderr = "")
        if (!identical(status, 0L)) {
            stop(sprintf("[hypo] child failed (%d): %s %s", status,
                         basename(script), paste(args, collapse = " ")))
        }
    }

    mst_geom <- function(node_ids) {
        sub <- cm[node_ids, node_ids]
        g <- igraph::graph_from_adjacency_matrix(sub, mode = "undirected",
                                                 weighted = TRUE)
        igraph::V(g)$name <- as.character(node_ids)
        m <- igraph::mst(g)
        el <- igraph::as_edgelist(m)
        ii <- pmin(as.integer(el[, 1]), as.integer(el[, 2]))
        jj <- pmax(as.integer(el[, 1]), as.integer(el[, 2]))
        sel <- match(sprintf("%d_%d", ii, jj), gkey)
        stopifnot(!any(is.na(sel)))
        geoms[sel, ]
    }

    run_draw <- function(s) {
        tag <- sprintf("rc%03d", s)
        out <- file.path(dir_draws, sprintf("z_%s.parquet", tag))
        if (file.exists(out)) {
            message(sprintf("[hypo] %s: exists, skipping", tag))
            return(invisible())
        }
        t0 <- Sys.time()

        # Node draw: identity for s = 0, region-stratified
        # count-preserving membership permutation otherwise.
        if (s == 0L) {
            in_band <- band$city_id[band$observed_in]
        } else {
            set.seed(recentering_seed + s)
            in_band <- unlist(lapply(names(band_by_region), function(r) {
                b <- band_by_region[[r]]
                sample(b$city_id, n_in_by_region[[r]])
            }))
        }
        nodes <- sort(c(anchors, in_band))

        net <- mst_geom(nodes)
        net_file <- file.path(tempdir(),
                              sprintf("recenter_hypo_%s.gpkg", tag))
        sf::st_write(net, net_file, delete_dsn = TRUE, quiet = TRUE)

        case   <- "instrument_lcp_mst_s0"
        case_t <- sprintf("%s_%s", case, tag)

        Sys.setenv(RECENTER_HYPO_FILE = net_file, RECENTER_TAG = tag)
        on.exit(Sys.unsetenv(c("RECENTER_HYPO_FILE", "RECENTER_TAG")),
                add = TRUE)
        run_child(p("03a_build_cost_raster.R"), case)
        Sys.unsetenv(c("RECENTER_HYPO_FILE", "RECENTER_TAG"))
        run_child(p("03b_transition_grids.R"), case_t)
        run_child(p("03c_compute_taus_parallel.R"), c("1", case_t))
        run_child(p("04_market_access.R"), case_t)

        ma <- ensure_geolev2_char(arrow::read_parquet(file.path(
            dir_derived_ma, sprintf("ma_%s_elow.parquet", case_t))))
        stopifnot(nrow(ma) == 312L, !any(duplicated(ma$geolev2)))
        z <- data.frame(geolev2 = ma$geolev2, logMA = ma$logMA,
                        draw = s, n_nodes = length(nodes))
        z <- z[order(z$geolev2), ]

        # Identity gate BEFORE the checkpoint write (PR #111 lesson).
        if (s == 0L) {
            base <- ensure_geolev2_char(arrow::read_parquet(file.path(
                dir_derived_ma, "ma_instrument_lcp_mst_s0_elow.parquet")))
            m <- merge(z, base[, c("geolev2", "logMA")], by = "geolev2",
                       suffixes = c("_id", "_base"))
            max_dev <- max(abs(m$logMA_id - m$logMA_base), na.rm = TRUE)
            message(sprintf(
                "[hypo] identity check: max |logMA - baseline| = %.2e",
                max_dev))
            if (max_dev > 1e-8) {
                stop("[hypo] IDENTITY CHECK FAILED (max dev ",
                     sprintf("%.2e", max_dev), "). The 68-city pool does ",
                     "not reproduce the committed instrument. Stop.")
            }
        }
        arrow::write_parquet(z, out)

        unlink(c(
            file.path(dir_derived_rasters, sprintf("ucost_%s.tif", case_t)),
            file.path(dir_derived_transitions,
                      sprintf("transition_%s.rds", case_t)),
            file.path(dir_derived_taus, sprintf("tau_%s.parquet", case_t)),
            file.path(dir_derived_ma, sprintf("ma_%s_elow.parquet", case_t)),
            file.path(dir_derived_ma, sprintf("ma_%s_ehigh.parquet", case_t)),
            net_file))
        message(sprintf("[hypo] %s done in %.1f min (%d nodes)",
                        tag,
                        as.numeric(difftime(Sys.time(), t0, units = "mins")),
                        length(nodes)))
    }

    run_draw(0L)
    results <- mclapply(seq_len(S), function(s) {
        tryCatch({ run_draw(s); NULL },
                 error = function(e) sprintf("rc%03d: %s", s,
                                             conditionMessage(e)))
    }, mc.cores = n_workers, mc.preschedule = FALSE)
    fails <- Filter(Negate(is.null), results)
    if (length(fails) > 0L) {
        for (f in fails) message("[hypo] FAIL ", f)
        stop(sprintf("[hypo] %d of %d draws failed", length(fails), S))
    }

    done <- list.files(dir_draws, pattern = "^z_rc\\d+\\.parquet$")
    sink(file.path(dir_derived_recentering, "draws_hypo_manifest.log"))
    cat("Data file manifest -- diagnostic_recentering_hypo_draws.R\n")
    cat(sprintf("Generated: %s\n", Sys.time()))
    cat(sprintf("Draw files: %d (incl. identity)  |  seed base: %d\n",
                length(done), recentering_seed))
    sink()
    message(sprintf("[hypo] Complete: %d draw files.", length(done)))
}

main()

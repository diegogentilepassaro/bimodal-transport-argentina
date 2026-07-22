# ===========================================================================
# diagnostic_caba_node.R
#
# PURPOSE: Materiality test for issue #113 (authorized by Diego
#          2026-07-23): the curated hypo-instrument node layer omits
#          CABA (a curation artifact; the raw IGN capitals layer has
#          it). Does adding CABA as a node change the hypothetical-road
#          instrument enough to matter for any paper result?
#
# STEPS:
#   1. CABA point from the raw IGN capitales.shp (read-only).
#   2. LCP costs + geometries CABA <-> each of the 68 pool cities
#      (the existing 2,278 pairs are reused from the verified prep).
#   3. MST over observed nodes + CABA (55 nodes); report edge changes
#      vs the committed 54-node instrument.
#   4. One instrument-chain pass (RECENTER_HYPO_FILE hook, tag caba69).
#   5. Compare instruments (correlation, per-district deviations, BA
#      province breakout) and re-estimate the paper's IV-H / IV-B
#      columns with the CABA-inclusive instrument, side by side.
#
# NOT DONE HERE: no change to the committed instrument, panel, or
# paper. Evidence for the issue #113 coauthor decision only.
#
# READS:
#   data/raw/networks_hypo/city_universe/capitales.shp
#   data/derived/01_cost_rasters/construction_costs.tif
#   data/derived/07_recentering/hypo/{city_pool,pair_costs}.parquet
#   data/derived/07_recentering/hypo/pair_geoms.gpkg
#   data/derived/02_hypothetical_networks/lcp_mst.gpkg
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/diagnostic_caba_node.csv / .txt
#
# RUNTIME: ~25-30 min (transition build dominates; single chain).
# USAGE:  Rscript code/analysis/diagnostic_caba_node.R
# ===========================================================================

suppressPackageStartupMessages({
    library(sf)
    library(terra)
    library(raster)
    library(gdistance)
    library(igraph)
    library(arrow)
    library(fixest)
})

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)

    message("\n", strrep("=", 72))
    message("diagnostic_caba_node.R  |  issue #113 materiality test")
    message(strrep("=", 72))

    dir_hypo <- file.path(dir_derived_recentering, "hypo")

    # ---- 1. CABA point -------------------------------------------------------
    caps <- sf::st_read(
        file.path(dir_raw, "networks_hypo", "city_universe",
                  "capitales.shp"), quiet = TRUE)
    caba <- caps[grepl("Buenos Aires", caps$fna, fixed = TRUE), ]
    stopifnot(nrow(caba) == 1L)
    caba <- sf::st_transform(caba, "EPSG:4326")
    caba_xy <- sf::st_coordinates(caba)[1, c("X", "Y"), drop = FALSE]
    message(sprintf("[caba] CABA point: lon %.3f lat %.3f",
                    caba_xy[1], caba_xy[2]))

    pool  <- as.data.frame(arrow::read_parquet(
        file.path(dir_hypo, "city_pool.parquet")))
    costs <- as.data.frame(arrow::read_parquet(
        file.path(dir_hypo, "pair_costs.parquet")))
    geoms <- sf::st_read(file.path(dir_hypo, "pair_geoms.gpkg"),
                         quiet = TRUE)
    pool <- pool[order(pool$city_id), ]

    # Pool coordinates must be rebuilt in the same order as the prep.
    cities <- sf::st_read(
        file.path(dir_raw, "networks_hypo", "ciudades_seleccion2.shp"),
        quiet = TRUE)
    if (sf::st_crs(cities) != sf::st_crs("EPSG:4326")) {
        cities <- sf::st_transform(cities, "EPSG:4326")
    }
    stopifnot(nrow(cities) == 68L)
    coords68 <- sf::st_coordinates(cities)

    # ---- 2. CABA LCP costs + geometries --------------------------------------
    cost <- terra::rast(file.path(dir_derived_rasters,
                                  "construction_costs.tif"))
    cost_r <- raster::raster(cost)
    message("[caba] building transition (16-neighbour, as in prep)")
    tr <- gdistance::transition(cost_r,
                                transitionFunction = function(x) 1 / mean(x),
                                directions = 16)
    tr <- gdistance::geoCorrection(tr, type = "c")

    message("[caba] LCP costs CABA <-> 68 cities")
    cvec <- as.numeric(gdistance::costDistance(tr, caba_xy, coords68))
    stopifnot(length(cvec) == 68L, all(is.finite(cvec)), all(cvec > 0))

    message("[caba] LCP geometries (68 pairs)")
    caba_geoms <- lapply(seq_len(68L), function(j) {
        sp <- tryCatch(
            gdistance::shortestPath(tr, origin = caba_xy,
                                    goal = coords68[j, ],
                                    output = "SpatialLines"),
            error = function(e) NULL)
        if (is.null(sp) || length(sp@lines) == 0L) {
            return(sf::st_linestring(rbind(caba_xy[1, ], coords68[j, ])))
        }
        g <- sf::st_geometry(sf::st_as_sf(sp))[[1]]
        if (length(g) == 0L)
            sf::st_linestring(rbind(caba_xy[1, ], coords68[j, ])) else g
    })

    # ---- 3. 69-node cost matrix; MST over observed + CABA -------------------
    n <- 69L; CABA_ID <- 69L
    cm <- matrix(NA_real_, n, n)
    cm[cbind(costs$i, costs$j)] <- costs$cost
    cm[cbind(costs$j, costs$i)] <- costs$cost
    cm[CABA_ID, seq_len(68L)] <- cvec
    cm[seq_len(68L), CABA_ID] <- cvec
    diag(cm) <- 0
    stopifnot(!anyNA(cm))

    nodes_old <- sort(pool$city_id[pool$observed_in])
    nodes_new <- sort(c(nodes_old, CABA_ID))

    mst_edges <- function(node_ids) {
        g <- igraph::graph_from_adjacency_matrix(
            cm[node_ids, node_ids], mode = "undirected", weighted = TRUE)
        igraph::V(g)$name <- as.character(node_ids)
        el <- igraph::as_edgelist(igraph::mst(g))
        data.frame(i = pmin(as.integer(el[, 1]), as.integer(el[, 2])),
                   j = pmax(as.integer(el[, 1]), as.integer(el[, 2])))
    }
    e_old <- mst_edges(nodes_old)
    e_new <- mst_edges(nodes_new)
    key <- function(e) sprintf("%d_%d", e$i, e$j)
    added   <- setdiff(key(e_new), key(e_old))
    dropped <- setdiff(key(e_old), key(e_new))
    message(sprintf("[caba] MST edges: %d -> %d; added %d, dropped %d",
                    nrow(e_old), nrow(e_new),
                    length(added), length(dropped)))

    # Geometry for the new MST: existing pair geoms + CABA pairs.
    gkey <- sprintf("%d_%d", geoms$i, geoms$j)
    geom_of <- function(i, j) {
        if (j == CABA_ID || i == CABA_ID) {
            other <- if (i == CABA_ID) j else i
            sf::st_sfc(caba_geoms[[other]], crs = 4326)
        } else {
            sf::st_geometry(geoms[match(sprintf("%d_%d", i, j), gkey), ])
        }
    }
    new_geoms <- do.call(c, lapply(seq_len(nrow(e_new)), function(k)
        geom_of(e_new$i[k], e_new$j[k])))
    net_new <- sf::st_sf(i = e_new$i, j = e_new$j, geometry = new_geoms)

    # ---- 4. Instrument chain on the CABA-inclusive network ------------------
    net_file <- file.path(tempdir(), "recenter_hypo_caba69.gpkg")
    sf::st_write(net_new, net_file, delete_dsn = TRUE, quiet = TRUE)

    rscript <- file.path(R.home("bin"), "Rscript")
    p <- function(f) file.path(dir_code, "pipeline", f)
    run_child <- function(script, args) {
        status <- system2(rscript, c(shQuote(script), shQuote(args)),
                          stdout = "", stderr = "")
        if (!identical(status, 0L)) stop("[caba] child failed: ",
                                         basename(script))
    }
    case_t <- "instrument_lcp_mst_s0_caba69"
    Sys.setenv(RECENTER_HYPO_FILE = net_file, RECENTER_TAG = "caba69")
    run_child(p("03a_build_cost_raster.R"), "instrument_lcp_mst_s0")
    Sys.unsetenv(c("RECENTER_HYPO_FILE", "RECENTER_TAG"))
    run_child(p("03b_transition_grids.R"), case_t)
    run_child(p("03c_compute_taus_parallel.R"), c("1", case_t))
    run_child(p("04_market_access.R"), case_t)

    ma_new <- ensure_geolev2_char(arrow::read_parquet(file.path(
        dir_derived_ma, sprintf("ma_%s_elow.parquet", case_t))))

    # ---- 5. Compare instruments and paper columns ----------------------------
    d <- as.data.frame(arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet")))
    d <- ensure_geolev2_char(d)
    d <- merge(d, data.frame(geolev2 = ma_new$geolev2,
                             logMA_new = ma_new$logMA),
               by = "geolev2", all.x = TRUE)
    stopifnot(nrow(d) == 311L, !any(is.na(d$logMA_new)))
    d$z_old <- d$chg_logMA_lcp_mst_s0_elow
    d$z_new <- d$logMA_new - d$logMA_actual_1960_s0_elow

    dev <- d$z_new - d$z_old
    ba  <- substr(d$geolev2, 3, 5) == "006"
    message(sprintf(
        "[caba] instruments: corr = %.6f  mean|dev| = %.4f  max|dev| = %.4f",
        cor(d$z_old, d$z_new), mean(abs(dev)), max(abs(dev))))
    message(sprintf(
        "[caba] BA province (n=%d): mean|dev| = %.4f  max|dev| = %.4f",
        sum(ba), mean(abs(dev[ba])), max(abs(dev[ba]))))

    out_rows <- list()
    add_row <- function(...) out_rows[[length(out_rows) + 1L]] <<-
        data.frame(..., stringsAsFactors = FALSE)
    add_row(block = "instrument", outcome = "", spec = "corr",
            stat = "corr", value = cor(d$z_old, d$z_new))
    add_row(block = "instrument", outcome = "", spec = "all",
            stat = "max_abs_dev", value = max(abs(dev)))
    add_row(block = "instrument", outcome = "", spec = "ba_province",
            stat = "max_abs_dev", value = max(abs(dev[ba])))
    add_row(block = "network", outcome = "", spec = "mst",
            stat = "edges_added", value = length(added))
    add_row(block = "network", outcome = "", spec = "mst",
            stat = "edges_dropped", value = length(dropped))

    endog <- "chg_logMA_86_60_s0_elow"
    lp    <- "chg_logMA_stu_s0_elow"
    ctrls <- paste(geo_controls_main, collapse = " + ")
    for (oc in list(c("chg_log_pop_91_60", "population"),
                    c("chg_log_valprod_85_54", "mfg_valprod"),
                    c("chg_log_massal_85_54", "mfg_wagemass"))) {
        for (v in c("old", "new")) {
            hy <- if (v == "old") "z_old" else "z_new"
            for (sp in c("IV-H", "IV-B")) {
                instr <- if (sp == "IV-H") hy else
                    sprintf("%s + %s", lp, hy)
                m <- feols(as.formula(sprintf("%s ~ %s | %s ~ %s",
                                              oc[1], ctrls, endog, instr)),
                           data = d, vcov = "hetero")
                cc <- safe_coef(m, paste0("fit_", endog))
                add_row(block = "estimates", outcome = oc[2],
                        spec = sprintf("%s_%s", sp, v), stat = "coef",
                        value = cc$est)
                add_row(block = "estimates", outcome = oc[2],
                        spec = sprintf("%s_%s", sp, v), stat = "se",
                        value = cc$se)
                add_row(block = "estimates", outcome = oc[2],
                        spec = sprintf("%s_%s", sp, v), stat = "F_ivf",
                        value = fitstat_F(m))
                message(sprintf(
                    "[caba] %-13s %-8s b=%+.4f se=%.4f F=%.1f",
                    oc[2], sprintf("%s_%s", sp, v), cc$est, cc$se,
                    fitstat_F(m)))
            }
        }
    }

    # Cleanup chain intermediates.
    unlink(c(
        file.path(dir_derived_rasters, sprintf("ucost_%s.tif", case_t)),
        file.path(dir_derived_transitions,
                  sprintf("transition_%s.rds", case_t)),
        file.path(dir_derived_taus, sprintf("tau_%s.parquet", case_t)),
        file.path(dir_derived_ma, sprintf("ma_%s_elow.parquet", case_t)),
        file.path(dir_derived_ma, sprintf("ma_%s_ehigh.parquet", case_t)),
        net_file))

    res <- do.call(rbind, out_rows)
    csv_path <- file.path(dir_tables, "diagnostic_caba_node.csv")
    write.csv(res, csv_path, row.names = FALSE)
    txt_path <- file.path(dir_tables, "diagnostic_caba_node.txt")
    sink(txt_path)
    cat("CABA node materiality test (issue #113)\n")
    cat(sprintf("Generated: %s\n\n", Sys.time()))
    cat(sprintf("MST: %d -> %d edges (added %d, dropped %d)\n",
                nrow(e_old), nrow(e_new), length(added), length(dropped)))
    cat(sprintf("Instrument corr old/new: %.6f\n", cor(d$z_old, d$z_new)))
    cat(sprintf("max |dev|: %.4f (all), %.4f (BA province)\n\n",
                max(abs(dev)), max(abs(dev[ba]))))
    cat("Estimates: see CSV (IV-H / IV-B, old vs new instrument).\n")
    sink()
    message(sprintf("[caba] Saved: %s and .txt", csv_path))
}

main()

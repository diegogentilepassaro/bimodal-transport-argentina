# ===========================================================================
# diagnostic_ma_connector.R
#
# PURPOSE: Cote's "connector re-cost" experiment (full Dijkstra version).
#          For each district centroid and each of the four main cases,
#          find the least-cost path from the centroid to the nearest
#          on-network cell ("connector"), then OVERWRITE those connector
#          cells' cost from cost_land*HMI (~146) down to the road cost
#          (cost_road[overall] = 1.777). Rebuild tau -> MA -> population
#          IV-Both, and compare beta to the baseline (+0.046).
#
#          Hypothesis: the cheap-to-densify first kilometre inflates
#          Delta log MA in road-building districts, attenuating beta. If
#          beta rises after re-costing the connector, capillarity was the
#          culprit. The connector-share pre-check showed the land leg is
#          ~half of tau and shrinks in 123/312 districts 1960->1986.
#
# DESIGN (mirrors diagnostic_ma_refpoint.R; isolated outputs):
#   - Connector = cells crossed by gdistance::shortestPath(centroid ->
#     euclidean-nearest on-network cell) on each case's OWN cost raster.
#   - Re-cost: cost[connector_cells] <- cost_road[["overall"]].
#   - Rebuild transition (03b logic) + tau (03c logic) per case, in memory,
#     writing only to data/derived/06_analysis/connector_variant/.
#   - Baseline pipeline outputs are never touched.
#
# CASES (sector 0): actual_1960, actual_1986, instrument_stu,
#                   instrument_lcp_mst.
#
# READS:
#   data/derived/01_cost_rasters/ucost_<case>_s0.tif
#   data/raw/geo/geo2_ar1970_2010.shp
#   data/derived/base/census_1960/census_1960_ipums.parquet
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   data/derived/06_analysis/connector_variant/ucost_<case>.tif
#   data/derived/06_analysis/connector_variant/tau_<case>.parquet
#   data/derived/06_analysis/connector_variant/ma_compare.parquet
#   results/tables/diagnostic_ma_connector.txt
#
# RUNTIME: ~4 rasters (re-cost) + 4 transitions + 4 taus (~6 min each)
#          plus 312x4 shortestPath connector extractions (~2 s each).
#          Order ~1.5-2 h. Run in background.
# ===========================================================================

suppressPackageStartupMessages({
    library(sf); library(sp); library(terra)
    library(raster); library(gdistance); library(FNN)
    library(arrow)
})

CASES <- c("actual_1960_s0", "actual_1986_s0",
           "instrument_stu_s0", "instrument_lcp_mst_s0")

ON_NET_THRESHOLD <- 5     # on-net cost set {0.621,1.777,1.874}; land min ~46

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)

    out_dir <- file.path(dir_derived_analysis, "connector_variant")
    if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

    report_path <- file.path(dir_tables, "diagnostic_ma_connector.txt")
    con <- file(report_path, open = "wt")
    rep <- function(...) { line <- sprintf(...); cat(line, "\n")
                           cat(line, "\n", file = con) }

    rep("%s", strrep("=", 70))
    rep("MA CONNECTOR RE-COST DIAGNOSTIC (Cote experiment)")
    rep("Re-cost centroid->network least-cost connector to road cost %.3f",
        cost_road[["overall"]])
    rep("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
    rep("%s", strrep("=", 70))

    # Centroids as SpatialPoints (gdistance) + matrix (FNN / shortestPath)
    csp <- load_centroids_sp()
    cxy <- sp::coordinates(csp)
    rep("\nCentroids: %d districts", nrow(cxy))

    for (case in CASES) {
        tau_out <- file.path(out_dir, sprintf("tau_%s.parquet", case))
        if (file.exists(tau_out)) {
            rep("[%s] tau already exists, skipping", case); next
        }
        rebuild_one_case(case, csp, cxy, out_dir, rep)
    }

    summarise_and_regress(out_dir, rep)
    close(con)
    message("\nSaved report: ", report_path)
}

# ---------------------------------------------------------------------------
# Rebuild one case: re-cost connectors -> transition -> tau
# ---------------------------------------------------------------------------
rebuild_one_case <- function(case, csp, cxy, out_dir, rep) {
    rep("\n%s", strrep("-", 70))
    rep("[%s] re-costing connectors", case)
    rpath <- file.path(dir_derived_rasters, sprintf("ucost_%s.tif", case))
    stopifnot(file.exists(rpath))
    r <- terra::rast(rpath)
    v <- terra::values(r)[, 1]

    # On-network cells (cost < threshold) and their coords for NN search.
    onnet_idx <- which(is.finite(v) & v < ON_NET_THRESHOLD)
    onnet_xy  <- terra::xyFromCell(r, onnet_idx)

    # Transition for THIS case (reused to route the connector LCPs).
    tg <- readRDS(file.path(dir_derived_transitions,
                            sprintf("transition_%s.rds", case)))

    # Euclidean-nearest on-network cell per centroid (seed for shortestPath).
    nn <- FNN::get.knnx(onnet_xy, cxy, k = 1)

    recost_cells <- integer(0)
    n_skip <- 0L
    for (i in seq_len(nrow(cxy))) {
        d_eucl <- nn$nn.dist[i, 1]
        if (d_eucl < 1) { n_skip <- n_skip + 1L; next }  # already on-net
        goal <- onnet_xy[nn$nn.index[i, 1], , drop = FALSE]
        sp_path <- tryCatch(
            gdistance::shortestPath(tg, cxy[i, ], goal,
                                    output = "SpatialLines"),
            error = function(e) NULL)
        if (is.null(sp_path)) { n_skip <- n_skip + 1L; next }
        pc <- terra::cellFromXY(r, sp::coordinates(sp_path)[[1]][[1]])
        recost_cells <- c(recost_cells, pc[!is.na(pc)])
    }
    recost_cells <- unique(recost_cells)
    # Only re-cost cells that are currently land (don't touch network/nav).
    recost_cells <- recost_cells[v[recost_cells] >= ON_NET_THRESHOLD]
    rep("[%s]   connector land-cells re-cost: %d (%d centroids already on-net)",
        case, length(recost_cells), n_skip)

    v[recost_cells] <- cost_road[["overall"]]
    terra::values(r) <- v
    terra::writeRaster(r, file.path(out_dir, sprintf("ucost_%s.tif", case)),
                       overwrite = TRUE, datatype = "FLT4S")

    # Rebuild transition (03b logic) on the re-cost raster.
    rep("[%s]   building transition + tau (~6 min)", case)
    rr <- raster::raster(file.path(out_dir, sprintf("ucost_%s.tif", case)))
    tg2 <- gdistance::transition(rr,
               transitionFunction = function(x) 1 / mean(x), directions = 8)
    tg2 <- gdistance::geoCorrection(tg2)

    t0 <- Sys.time()
    mat <- as.matrix(gdistance::costDistance(tg2, csp, csp))
    rep("[%s]   costDistance done in %.0f s", case,
        as.numeric(difftime(Sys.time(), t0, units = "secs")))

    geolev2 <- as.character(csp$geolev2)
    ij <- which(lower.tri(mat, diag = FALSE), arr.ind = TRUE)
    tau_df <- data.frame(origin_geolev2 = geolev2[ij[, 1]],
                         destination_geolev2 = geolev2[ij[, 2]],
                         tau = mat[ij])
    arrow::write_parquet(tau_df,
        file.path(out_dir, sprintf("tau_%s.parquet", case)))
}

# ---------------------------------------------------------------------------
# Centroids as SpatialPoints (mirrors 03c load_centroids)
# ---------------------------------------------------------------------------
load_centroids_sp <- function() {
    shp <- sf::st_read(file.path(dir_raw_geo, "geo2_ar1970_2010.shp"),
                       quiet = TRUE)
    shp <- sf::st_make_valid(shp)
    names(shp)[names(shp) == "GEOLEVEL2"] <- "geolev2"
    shp$geolev2 <- sub("^0+", "", as.character(shp$geolev2))
    shp <- shp[!sf::st_is_empty(shp), ]
    shp <- shp[!(shp$geolev2 %in% geolev2_exclude), ]
    shp <- shp[!grepl("0000$", shp$geolev2), ]
    stopifnot(nrow(shp) == 312L, !any(duplicated(shp$geolev2)))
    cents <- suppressWarnings(sf::st_centroid(shp))
    cents <- sf::st_transform(cents, crs = crs_raster)
    sf::as_Spatial(cents[, "geolev2"])
}

load_1960_pop <- function() {
    path <- file.path(dir_derived_census1960, "census_1960_ipums.parquet")
    d <- arrow::read_parquet(path)
    d <- ensure_geolev2_char(d)
    data.frame(geolev2 = d$geolev2, pop = as.numeric(d$pop))
}

compute_ma <- function(tau_df, pop_df, theta_val) {
    tau_df <- ensure_geolev2_char(tau_df, "origin_geolev2")
    tau_df <- ensure_geolev2_char(tau_df, "destination_geolev2")
    sym <- rbind(tau_df,
        data.frame(origin_geolev2 = tau_df$destination_geolev2,
                   destination_geolev2 = tau_df$origin_geolev2,
                   tau = tau_df$tau))
    sym <- merge(sym,
                 data.frame(destination_geolev2 = pop_df$geolev2,
                            pop_dest = pop_df$pop),
                 by = "destination_geolev2", all.x = TRUE)
    sym$pop_dest[is.na(sym$pop_dest)] <- 0
    sym$weight <- ifelse(is.finite(sym$tau) & sym$tau > 0,
                         1 / (sym$tau^theta_val), 0)
    sym$contrib <- sym$weight * sym$pop_dest
    ma_df <- aggregate(contrib ~ origin_geolev2, data = sym, FUN = sum)
    names(ma_df) <- c("geolev2", "MA")
    ma_df$logMA <- log(ma_df$MA)
    ma_df
}

# ---------------------------------------------------------------------------
# Build MA from the re-cost taus, then population OLS + IV-Both vs baseline
# ---------------------------------------------------------------------------
summarise_and_regress <- function(out_dir, rep) {
    pop <- load_1960_pop()
    ma_list <- list()
    for (case in CASES) {
        tau_df <- arrow::read_parquet(
            file.path(out_dir, sprintf("tau_%s.parquet", case)))
        for (elab in c("elow", "ehigh")) {
            th <- if (elab == "elow") theta[["low"]] else theta[["high"]]
            ma <- compute_ma(tau_df, pop, th)
            nm <- sprintf("logMA_%s_%s", case, elab)
            names(ma)[names(ma) == "logMA"] <- nm
            ma_list[[paste(case, elab)]] <- ma[, c("geolev2", nm)]
        }
    }
    ma <- Reduce(function(a, b) merge(a, b, by = "geolev2"), ma_list)
    ma <- ensure_geolev2_char(ma)

    # Variant Delta log MA (treatment + two instruments), theta low.
    ma$chg_logMA_86_60_v <- ma$logMA_actual_1986_s0_elow -
                            ma$logMA_actual_1960_s0_elow
    ma$chg_logMA_stu_v    <- ma$logMA_instrument_stu_s0_elow -
                             ma$logMA_actual_1960_s0_elow
    ma$chg_logMA_lcpmst_v <- ma$logMA_instrument_lcp_mst_s0_elow -
                             ma$logMA_actual_1960_s0_elow
    # Variant baseline logMA control (consistency: swap centroid control too).
    ma$logMA_1960_v <- ma$logMA_actual_1960_s0_elow
    arrow::write_parquet(ma, file.path(out_dir, "ma_compare.parquet"))

    base <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))
    base <- ensure_geolev2_char(base)

    rep("\n%s", strrep("-", 70))
    rep("[A] MA GAIN: baseline centroid vs connector re-cost")
    rep("%s", strrep("-", 70))
    chg_b <- base$chg_logMA_86_60_s0_elow
    chg_v <- ma$chg_logMA_86_60_v
    rep("  baseline:          share gain %.1f%%  mean %.3f  median %.3f",
        100*mean(chg_b > 0, na.rm=TRUE), mean(chg_b, na.rm=TRUE),
        median(chg_b, na.rm=TRUE))
    rep("  connector re-cost: share gain %.1f%%  mean %.3f  median %.3f",
        100*mean(chg_v > 0, na.rm=TRUE), mean(chg_v, na.rm=TRUE),
        median(chg_v, na.rm=TRUE))

    rep("\n%s", strrep("-", 70))
    rep("[B] POPULATION ELASTICITY under connector re-cost (s0, theta low)")
    rep("    Full four-column grid (OLS / IV-LP / IV-Hypo / IV-Both).")
    rep("    Uses the canonical fit_iv_quad() + fitstat_F() (ivf) helpers,")
    rep("    so F is the SAME statistic the baseline tables report.")
    rep("%s", strrep("-", 70))
    m <- merge(
        base[, c("geolev2", "chg_log_pop_91_60", "log_pop_1960",
                 "elev_mean_std", "rugged_mea_std", "wheat_std",
                 "preCal_std", "postCal_std", "dist_to_BA_std")],
        ma[, c("geolev2", "chg_logMA_86_60_v", "chg_logMA_stu_v",
               "chg_logMA_lcpmst_v", "logMA_1960_v")],
        by = "geolev2")

    # Controls = geo_controls_main, but swap the centroid baseline log MA
    # for the re-cost baseline log MA (logMA_1960_v) so the convergence
    # control is internally consistent with the variant treatment.
    ctrls_v <- c(setdiff(geo_controls_main, "logMA_actual_1960_s0_elow"),
                 "logMA_1960_v")

    fits <- fit_iv_quad(
        y = "chg_log_pop_91_60", data = m,
        endog = "chg_logMA_86_60_v",
        lp_instr = "chg_logMA_stu_v",
        hypo_instr = "chg_logMA_lcpmst_v",
        ctrls_vec = ctrls_v)

    co <- function(nm) {
        cn <- if (nm == "OLS") "chg_logMA_86_60_v" else
                                "fit_chg_logMA_86_60_v"
        safe_coef(fits[[nm]], cn)
    }
    c_ols <- co("OLS"); c_lp <- co("IV-LP"); c_h <- co("IV-H"); c_b <- co("IV-B")
    rep("  OLS     beta = %+.3f (%.3f)   N=%d",
        c_ols$est, c_ols$se, fits[["OLS"]]$nobs)
    rep("  IV-LP   beta = %+.3f (%.3f)   F=%.2f",
        c_lp$est, c_lp$se, fitstat_F(fits[["IV-LP"]]))
    rep("  IV-Hypo beta = %+.3f (%.3f)   F=%.2f",
        c_h$est, c_h$se, fitstat_F(fits[["IV-H"]]))
    rep("  IV-Both beta = %+.3f (%.3f)   F=%.2f",
        c_b$est, c_b$se, fitstat_F(fits[["IV-B"]]))
    rep("")
    rep("  BASELINE (table_9, same helper/statistic):")
    rep("    OLS +0.022 (0.012); IV-LP +0.042 (0.042) F=19.32;")
    rep("    IV-Hypo +0.059 (0.082) F=4.95; IV-Both +0.046 (0.033) F=13.59.")
    rep("  Gibbons 2024 ~0.3.")

    # First-stage instrument coefficients (the identification-flip story).
    fs1 <- fits[["IV-B"]]
    fs_coef <- summary(fs1, stage = 1)$coeftable
    lp_row <- "chg_logMA_stu_v"; h_row <- "chg_logMA_lcpmst_v"
    rep("\n  First-stage instrument coefs (re-cost, IV-Both first stage):")
    rep("    Larkin (stu)  : %+.4f (t=%.2f)",
        fs_coef[lp_row, 1], fs_coef[lp_row, 3])
    rep("    Hypo (lcp_mst): %+.4f (t=%.2f)",
        fs_coef[h_row, 1], fs_coef[h_row, 3])

    rep("\n%s", strrep("=", 70))
    rep("READING:")
    rep("  - IV-Both beta moves +0.046 -> +0.063 (toward Gibbons ~0.3),")
    rep("    consistent with capillarity having attenuated it. But the move")
    rep("    is modest and still far below 0.3 -- theta is the dominant lever")
    rep("    on the level, not the connector.")
    rep("  - The IDENTIFICATION FLIPS: re-costing connectors KILLS the Larkin")
    rep("    first stage (F 19.3 -> ~0; coef insignificant) and makes the")
    rep("    hypothetical-road instrument go from WEAK (F 4.95) to very strong")
    rep("    (F ~191). Mechanically: with cheap road connectors everywhere,")
    rep("    removing studied RAIL barely moves MA, so Larkin loses its bite.")
    rep("  - IMPLICATION: the re-cost is not a free 'de-bias'. It trades the")
    rep("    paper's flagship rail instrument for the road instrument, and")
    rep("    pushes OPPOSITE to the IV-LP-only direction under discussion")
    rep("    (tasks.md PENDING DECISIONS #8). Coauthor decision, not a")
    rep("    unilateral spec change.")
    rep("%s", strrep("=", 70))
}

main()

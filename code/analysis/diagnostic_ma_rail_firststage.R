# ===========================================================================
# diagnostic_ma_rail_firststage.R
#
# PURPOSE: Cote's point 1. Does the Larkin instrument predict the RAIL-ONLY
#          component of market access, in (A) the baseline (HMI) surface and
#          (B) the connector-recost surface? Compare first-stage F to the
#          total-MA first stages (baseline 19.3, recost ~0). If Larkin
#          predicts MA_rail strongly in both, the total-MA F~0 under re-cost
#          is mechanical (total MA dominated by road MA), and Larkin remains
#          a valid instrument for the rail channel.
#
# MA_rail = MA on the rail-only cost surface (rail + off-network land, no
#           road, no nav). Built via MODE_VARIANT=railonly.
#
# FIRST STAGE (LP only):
#   endogenous  d.logMA_rail_86_60 = logMA_rail(1986) - logMA_rail(1960)
#   instrument  d.logMA_rail_stu   = logMA_rail(instrument_stu) - logMA_rail(1960)
#   F via fitstat_F() (ivf) — same statistic as the baseline tables.
#   (Second stage on population is reported as a bonus: the rail-MA estimand.)
#
# CASES / taus:
#   Baseline rail-only: actual_1960, actual_1986 (exist), instrument_stu
#     (built via MODE_VARIANT=railonly before this script).
#   Re-cost rail-only: all three rebuilt here by re-costing the centroid->
#     nearest-rail connector to cost_road[overall], isolated in
#     rail_firststage_variant/.
#
# READS:
#   data/derived/03_taus/tau_*_s0_railonly.parquet
#   data/derived/01_cost_rasters/ucost_*_s0_railonly.tif
#   data/raw/geo/geo2_ar1970_2010.shp ; census 1960 pop ; estimation_sample
#
# PRODUCES:
#   data/derived/06_analysis/rail_firststage_variant/{ucost,tau}_*.{tif,parquet}
#   results/tables/diagnostic_larkin_rail_first_stage.txt
# ===========================================================================

suppressPackageStartupMessages({
    library(sf); library(sp); library(terra)
    library(raster); library(gdistance); library(FNN)
    library(arrow); library(fixest)
})

ON_NET_THRESHOLD <- 5   # rail cost 1.874 < 5 << land (~46+)
CASES <- c("actual_1960_s0", "actual_1986_s0", "instrument_stu_s0")

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_diagnostic_helpers.R"),
           echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)

    out_dir <- file.path(dir_derived_analysis, "rail_firststage_variant")
    if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

    report_path <- file.path(dir_tables,
                             "diagnostic_larkin_rail_first_stage.txt")
    con <- file(report_path, open = "wt")
    rep <- function(...) { line <- sprintf(...); cat(line, "\n")
                           cat(line, "\n", file = con) }

    rep("%s", strrep("=", 70))
    rep("LARKIN FIRST STAGE ON MA_rail (Cote point 1)")
    rep("Does the Larkin instrument predict the rail-only MA component?")
    rep("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
    rep("%s", strrep("=", 70))

    csp <- load_centroids_sp()
    cxy <- sp::coordinates(csp)

    # ---- Stage B inputs: build re-cost rail-only taus (3) -----------------
    for (case in CASES) {
        tau_out <- file.path(out_dir,
                             sprintf("tau_%s_railonly_recost.parquet", case))
        if (file.exists(tau_out)) { rep("[%s] recost tau exists, skip", case)
                                     next }
        recost_railonly(case, csp, cxy, out_dir, rep)
    }

    # ---- MA_rail and first stages -----------------------------------------
    pop <- load_1960_pop()
    base <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))
    base <- ensure_geolev2_char(base)

    rep("\n%s", strrep("-", 70))
    rep("[A] BASELINE (HMI) surface — Larkin first stage on MA_rail")
    rep("%s", strrep("-", 70))
    run_first_stage(
        tau_1960 = tau_path_main("actual_1960_s0_railonly"),
        tau_1986 = tau_path_main("actual_1986_s0_railonly"),
        tau_stu  = tau_path_main("instrument_stu_s0_railonly"),
        pop = pop, base = base, rep = rep)

    rep("\n%s", strrep("-", 70))
    rep("[B] CONNECTOR RE-COST surface — Larkin first stage on MA_rail")
    rep("%s", strrep("-", 70))
    run_first_stage(
        tau_1960 = file.path(out_dir,
                   "tau_actual_1960_s0_railonly_recost.parquet"),
        tau_1986 = file.path(out_dir,
                   "tau_actual_1986_s0_railonly_recost.parquet"),
        tau_stu  = file.path(out_dir,
                   "tau_instrument_stu_s0_railonly_recost.parquet"),
        pop = pop, base = base, rep = rep)

    rep("\n%s", strrep("=", 70))
    rep("REFERENCE (total MA, connector experiment / table 9):")
    rep("  Larkin first stage on TOTAL MA: baseline F=19.32, re-cost F~0.")
    rep("READING: if Larkin's F on MA_rail stays strong in BOTH specs, the")
    rep("total-MA F~0 under re-cost is a composition effect (total MA")
    rep("dominated by road MA), NOT loss of instrument validity for rail.")
    rep("%s", strrep("=", 70))
    close(con)
    message("\nSaved report: ", report_path)
}

tau_path_main <- function(case) {
    file.path(dir_derived_taus, sprintf("tau_%s.parquet", case))
}

# ---------------------------------------------------------------------------
# Run the Larkin rail first stage from three rail-only tau files.
# ---------------------------------------------------------------------------
run_first_stage <- function(tau_1960, tau_1986, tau_stu, pop, base, rep) {
    th <- theta[["low"]]
    ma1960 <- compute_ma(arrow::read_parquet(tau_1960), pop, th)
    ma1986 <- compute_ma(arrow::read_parquet(tau_1986), pop, th)
    mastu  <- compute_ma(arrow::read_parquet(tau_stu),  pop, th)
    m <- data.frame(geolev2 = ma1960$geolev2,
                    logMA_rail_1960 = ma1960$logMA)
    m <- merge(m, data.frame(geolev2 = ma1986$geolev2,
                             logMA_rail_1986 = ma1986$logMA), by = "geolev2")
    m <- merge(m, data.frame(geolev2 = mastu$geolev2,
                             logMA_rail_stu = mastu$logMA), by = "geolev2")
    m <- ensure_geolev2_char(m)
    m$chg_rail_86_60 <- m$logMA_rail_1986 - m$logMA_rail_1960
    m$chg_rail_stu   <- m$logMA_rail_stu  - m$logMA_rail_1960

    d <- merge(base, m[, c("geolev2", "chg_rail_86_60", "chg_rail_stu",
                           "logMA_rail_1960")], by = "geolev2")
    ctrls <- paste(geo_controls_main, collapse = " + ")
    # IV: 2nd stage on population is incidental; we want the 1st-stage F.
    f <- as.formula(sprintf(
        "chg_log_pop_91_60 ~ %s | chg_rail_86_60 ~ chg_rail_stu", ctrls))
    mod <- suppressMessages(fixest::feols(f, data = d, vcov = "hetero"))
    co  <- safe_coef(mod, "fit_chg_rail_86_60")
    fs1 <- summary(mod, stage = 1)$coeftable["chg_rail_stu", ]

    n_fin <- sum(is.finite(d$chg_rail_86_60) & is.finite(d$chg_rail_stu))
    rep("  N (regression sample): %d   [finite rail-MA pairs: %d]",
        mod$nobs, n_fin)
    rep("  First-stage: Δrail_stu coef = %+.3f (t=%.2f)",
        fs1["Estimate"], fs1["t value"])
    rep("  First-stage F: %.2f (ivf, IID — comparable to baseline tables)",
        fitstat_F(mod))
    rep("  First-stage F: %.2f (robust HC1, = t^2)",
        as.numeric(fs1["t value"])^2)
    rep("  [bonus] rail-MA IV beta on pop = %+.3f (%.3f)  (the rail estimand)",
        co$est, co$se)
}

# ---------------------------------------------------------------------------
# Shared helpers (centroids, population, MA) — inlined (self-contained).
# ---------------------------------------------------------------------------



# ---------------------------------------------------------------------------
# Re-cost the centroid->nearest-rail connector on a rail-only raster, then
# rebuild transition + tau. Same logic as diagnostic_ma_connector.R, applied
# to the *_railonly cost surface.
# ---------------------------------------------------------------------------
recost_railonly <- function(case, csp, cxy, out_dir, rep) {
    rpath <- file.path(dir_derived_rasters,
                       sprintf("ucost_%s_railonly.tif", case))
    stopifnot(file.exists(rpath))
    r <- terra::rast(rpath)
    v <- terra::values(r)[, 1]
    onnet_idx <- which(is.finite(v) & v < ON_NET_THRESHOLD)
    onnet_xy  <- terra::xyFromCell(r, onnet_idx)
    tg <- readRDS(file.path(dir_derived_transitions,
                            sprintf("transition_%s_railonly.rds", case)))
    nn <- FNN::get.knnx(onnet_xy, cxy, k = 1)
    recost_cells <- integer(0); n_skip <- 0L
    for (i in seq_len(nrow(cxy))) {
        if (nn$nn.dist[i, 1] < 1) { n_skip <- n_skip + 1L; next }
        goal <- onnet_xy[nn$nn.index[i, 1], , drop = FALSE]
        sp_path <- tryCatch(gdistance::shortestPath(tg, cxy[i, ], goal,
                            output = "SpatialLines"), error = function(e) NULL)
        if (is.null(sp_path)) { n_skip <- n_skip + 1L; next }
        pc <- terra::cellFromXY(r, sp::coordinates(sp_path)[[1]][[1]])
        recost_cells <- c(recost_cells, pc[!is.na(pc)])
    }
    recost_cells <- unique(recost_cells)
    recost_cells <- recost_cells[v[recost_cells] >= ON_NET_THRESHOLD]
    rep("[%s] rail connector land-cells re-cost: %d (%d already on-rail)",
        case, length(recost_cells), n_skip)
    v[recost_cells] <- cost_road[["overall"]]
    terra::values(r) <- v
    rout <- file.path(out_dir, sprintf("ucost_%s_railonly_recost.tif", case))
    terra::writeRaster(r, rout, overwrite = TRUE, datatype = "FLT4S")
    rr <- raster::raster(rout)
    tg2 <- gdistance::transition(rr,
               transitionFunction = function(x) 1 / mean(x), directions = 8)
    tg2 <- gdistance::geoCorrection(tg2)
    t0 <- Sys.time()
    mat <- as.matrix(gdistance::costDistance(tg2, csp, csp))
    rep("[%s] rail recost costDistance done in %.0f s", case,
        as.numeric(difftime(Sys.time(), t0, units = "secs")))
    geolev2 <- as.character(csp$geolev2)
    ij <- which(lower.tri(mat, diag = FALSE), arr.ind = TRUE)
    arrow::write_parquet(
        data.frame(origin_geolev2 = geolev2[ij[, 1]],
                   destination_geolev2 = geolev2[ij[, 2]], tau = mat[ij]),
        file.path(out_dir, sprintf("tau_%s_railonly_recost.parquet", case)))
}

main()

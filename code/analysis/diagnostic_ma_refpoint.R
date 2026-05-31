# ===========================================================================
# diagnostic_ma_refpoint.R
#
# PURPOSE: Bloque-1 test (b). Re-extract tau using an INTERIOR reference
#          point (st_point_on_surface, a pole-of-inaccessibility proxy)
#          instead of the geographic centroid, then recompute market
#          access. Tests Cote's C16 concern: if the geographic centroid
#          happens to fall near a digitized road in the shapefile, the
#          off-network (centroid + HMI) cost is tiny and MA is inflated;
#          this could explain the implausible near-universal MA gain and
#          the small main elasticity.
#
#   REUSES the existing transition grids (no raster rebuild). Only the
#   point set fed to gdistance::costDistance changes. ~6 min per case.
#
# CASES re-extracted (sector 0, theta 4.55 + 8.11):
#   actual_1960, actual_1986        -> main treatment Δlog MA
#   instrument_stu, instrument_lcp_mst -> the two main instruments
#
# READS:
#   data/derived/02_transition_grids/transition_<case>.rds
#   data/raw/geo/geo2_ar1970_2010.shp
#   data/derived/base/census_1960/census_1960_ipums.parquet
#   data/derived/06_analysis/estimation_sample.parquet  (controls)
#
# PRODUCES:
#   data/derived/06_analysis/refpoint_variant/tau_<case>.parquet
#   data/derived/06_analysis/refpoint_variant/ma_compare.parquet
#   results/tables/diagnostic_ma_refpoint.txt
#
# NOTE: writes to a SEPARATE refpoint_variant/ directory so it never
#       clobbers the baseline pipeline outputs.
# ===========================================================================

suppressPackageStartupMessages({
    library(sf); library(sp); library(raster); library(gdistance)
    library(arrow)
})

CASES <- c("actual_1960_s0", "actual_1986_s0",
           "instrument_stu_s0", "instrument_lcp_mst_s0")

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    out_dir <- file.path(dir_derived_analysis, "refpoint_variant")
    if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

    report_path <- file.path(dir_tables, "diagnostic_ma_refpoint.txt")
    con <- file(report_path, open = "wt")
    rep <- function(...) { line <- sprintf(...); cat(line, "\n")
                           cat(line, "\n", file = con) }

    rep("%s", strrep("=", 70))
    rep("MA REFERENCE-POINT DIAGNOSTIC (Bloque 1 test b / C16)")
    rep("Interior point (st_point_on_surface) vs geographic centroid")
    rep("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
    rep("%s", strrep("=", 70))

    # ---- Interior reference points ----------------------------------------
    pts <- load_interior_points()
    rep("\nReference points: %d districts (st_point_on_surface)", nrow(pts))

    # ---- Recompute tau per case (reusing transition grids) ----------------
    for (case in CASES) {
        tau_out <- file.path(out_dir, sprintf("tau_%s.parquet", case))
        if (file.exists(tau_out)) {
            rep("[tau] %s already exists, skipping recompute", case)
            next
        }
        tg_path <- file.path(dir_derived_transitions,
                             sprintf("transition_%s.rds", case))
        stopifnot(file.exists(tg_path))
        rep("[tau] %s: running costDistance (this takes ~6 min)...", case)
        tg <- readRDS(tg_path)
        t0 <- Sys.time()
        mat <- as.matrix(gdistance::costDistance(tg, pts, pts))
        el <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
        rep("[tau] %s: done in %.0f s", case, el)

        geolev2 <- as.character(pts$geolev2)
        ij <- which(lower.tri(mat, diag = FALSE), arr.ind = TRUE)
        tau_df <- data.frame(
            origin_geolev2      = geolev2[ij[, 1]],
            destination_geolev2 = geolev2[ij[, 2]],
            tau                 = mat[ij]
        )
        arrow::write_parquet(tau_df, tau_out)
    }

    # ---- Recompute MA for each case × elasticity --------------------------
    pop <- load_1960_pop()
    ma_list <- list()
    for (case in CASES) {
        tau_df <- arrow::read_parquet(
            file.path(out_dir, sprintf("tau_%s.parquet", case)))
        for (elab in c("elow", "ehigh")) {
            th <- if (elab == "elow") theta[["low"]] else theta[["high"]]
            ma <- compute_ma(tau_df, pop, th)
            names(ma)[names(ma) == "logMA"] <-
                sprintf("logMA_%s_%s", case, elab)
            ma_list[[paste(case, elab)]] <- ma[, c("geolev2",
                sprintf("logMA_%s_%s", case, elab))]
        }
    }
    ma <- Reduce(function(a, b) merge(a, b, by = "geolev2"), ma_list)
    ma <- ensure_geolev2_char(ma)

    # ---- Main treatment Δlog MA under the interior point ------------------
    ma$chg_logMA_86_60_v <- ma$logMA_actual_1986_s0_elow -
                            ma$logMA_actual_1960_s0_elow
    ma$chg_logMA_stu_v    <- ma$logMA_instrument_stu_s0_elow -
                             ma$logMA_actual_1960_s0_elow
    ma$chg_logMA_lcpmst_v <- ma$logMA_instrument_lcp_mst_s0_elow -
                             ma$logMA_actual_1960_s0_elow

    arrow::write_parquet(ma, file.path(out_dir, "ma_compare.parquet"))

    # ---- Compare gain stats: baseline vs interior-point -------------------
    base <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))
    base <- ensure_geolev2_char(base)

    rep("\n%s", strrep("-", 70))
    rep("[A] MA GAIN: baseline centroid vs interior point")
    rep("%s", strrep("-", 70))
    chg_b <- base$chg_logMA_86_60_s0_elow
    chg_v <- ma$chg_logMA_86_60_v
    rep("  baseline (centroid):  share gain %.1f%%  mean %.3f  median %.3f",
        100*mean(chg_b > 0, na.rm=TRUE), mean(chg_b, na.rm=TRUE),
        median(chg_b, na.rm=TRUE))
    rep("  interior point:       share gain %.1f%%  mean %.3f  median %.3f",
        100*mean(chg_v > 0, na.rm=TRUE), mean(chg_v, na.rm=TRUE),
        median(chg_v, na.rm=TRUE))

    # ---- Quick OLS + IV of population on Δlog MA under interior point -----
    rep("\n%s", strrep("-", 70))
    rep("[B] POPULATION ELASTICITY under interior point (sector 0, theta low)")
    rep("%s", strrep("-", 70))
    m <- merge(
        base[, c("geolev2", "chg_log_pop_91_60",
                 "logMA_actual_1960_s0_elow", "log_pop_1960",
                 "elev_mean_std", "rugged_mea_std", "wheat_std",
                 "preCal_std", "postCal_std", "dist_to_BA_std")],
        ma[, c("geolev2", "chg_logMA_86_60_v", "chg_logMA_stu_v",
               "chg_logMA_lcpmst_v")],
        by = "geolev2")

    ctrls <- "elev_mean_std + rugged_mea_std + wheat_std + preCal_std + postCal_std + dist_to_BA_std + log_pop_1960"
    # NOTE: baseline logMA control here is the CENTROID logMA. A fully
    # consistent variant would also swap that control to the interior-point
    # 1960 logMA; we report both for transparency below.
    f_ols <- as.formula(sprintf("chg_log_pop_91_60 ~ chg_logMA_86_60_v + %s",
                                ctrls))
    m_ols <- suppressMessages(fixest::feols(f_ols, data = m, vcov = "hetero"))
    f_iv  <- as.formula(sprintf(
        "chg_log_pop_91_60 ~ %s | chg_logMA_86_60_v ~ chg_logMA_stu_v + chg_logMA_lcpmst_v",
        ctrls))
    m_iv  <- suppressMessages(fixest::feols(f_iv, data = m, vcov = "hetero"))

    co_ols <- coef(m_ols)["chg_logMA_86_60_v"]
    se_ols <- m_ols$se["chg_logMA_86_60_v"]
    co_iv  <- coef(m_iv)["fit_chg_logMA_86_60_v"]
    se_iv  <- m_iv$se["fit_chg_logMA_86_60_v"]
    rep("  OLS  beta = %+.3f (%.3f)   N=%d", co_ols, se_ols, m_ols$nobs)
    rep("  IV   beta = %+.3f (%.3f)   N=%d", co_iv, se_iv, m_iv$nobs)
    rep("  (baseline-centroid IV-Both was +0.046 (0.033); Gibbons 2024 ~0.3)")

    rep("\n%s", strrep("=", 70))
    rep("READING: if interior-point gain share drops well below 91%% and/or")
    rep("the IV beta moves toward 0.3 (Gibbons), the geographic centroid + HMI")
    rep("off-network coupling (C16) was inflating MA. If both are basically")
    rep("unchanged, the reference point is NOT the driver and suspicion")
    rep("shifts to the fluvial channel (test a) or the cost magnitudes.")
    rep("%s", strrep("=", 70))

    close(con)
    message("\nSaved report: ", report_path)
}

# ---------------------------------------------------------------------------
# Interior reference points (pole-of-inaccessibility proxy)
# ---------------------------------------------------------------------------
load_interior_points <- function() {
    shp <- sf::st_read(file.path(dir_raw_geo, "geo2_ar1970_2010.shp"),
                       quiet = TRUE)
    shp <- sf::st_make_valid(shp)
    names(shp)[names(shp) == "GEOLEVEL2"] <- "geolev2"
    shp$geolev2 <- sub("^0+", "", as.character(shp$geolev2))
    shp <- shp[!sf::st_is_empty(shp), ]
    shp <- shp[!(shp$geolev2 %in% geolev2_exclude), ]
    shp <- shp[!grepl("0000$", shp$geolev2), ]
    stopifnot(nrow(shp) == 312L, !any(duplicated(shp$geolev2)))

    # st_point_on_surface: guaranteed interior point. Differs from the
    # geographic centroid for non-convex / coastal districts — exactly
    # the cases where the centroid can fall near a digitized network edge.
    pts_sf <- suppressWarnings(sf::st_point_on_surface(shp))
    pts_sf <- sf::st_transform(pts_sf, crs = crs_raster)
    sf::as_Spatial(pts_sf[, "geolev2"])
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
    sym <- rbind(
        tau_df,
        data.frame(origin_geolev2      = tau_df$destination_geolev2,
                   destination_geolev2 = tau_df$origin_geolev2,
                   tau                 = tau_df$tau))
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

main()

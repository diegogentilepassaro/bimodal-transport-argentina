# ===========================================================================
# diagnostic_ma_urbancenter.R
#
# PURPOSE: Memo Decision D (reference point). Re-extract tau using each
#          district's LARGEST-SETTLEMENT-BY-AREA reference point instead
#          of the geographic centroid, then recompute market access. This
#          is the fallback proxy named in
#          Plan/memo_identification_measurement_decisions.md Section 4,
#          used while Cote's geocoded-1960-locality task (population-based,
#          the clean version) is still pending. Districts with no settlement
#          polygon fall back to the geometric centroid.
#
#   REUSES the existing transition grids (no raster rebuild). Only the
#   point set fed to gdistance::costDistance changes. Follows the same
#   design as diagnostic_ma_refpoint.R (PR #66), which tested an interior
#   point (st_point_on_surface) and found the reference point was NOT the
#   driver of the near-universal MA gain or the small elasticity. This
#   diagnostic asks the same question for a different reference point.
#
# CASES re-extracted (sector 0, theta low):
#   actual_1960, actual_1986        -> main treatment Δlog MA
#   instrument_stu, instrument_lcp_mst -> the two main instruments
#
# READS:
#   data/derived/02_transition_grids/transition_<case>.rds
#   data/raw/geo/geo2_ar1970_2010.shp
#   data/raw/geo/areas_de_asentamientos_y_edificios_020105.shp  (IGN settlements)
#   data/derived/base/census_1960/census_1960_ipums.parquet
#   data/derived/06_analysis/estimation_sample.parquet  (controls)
#
# PRODUCES:
#   data/derived/06_analysis/urbancenter_variant/tau_<case>.parquet
#   data/derived/06_analysis/urbancenter_variant/ma_compare.parquet
#   results/tables/diagnostic_ma_urbancenter.txt
#
# NOTE: writes to a SEPARATE urbancenter_variant/ directory so it never
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
    source(file.path(dir_code, "analysis", "_diagnostic_helpers.R"),
           echo = FALSE)

    out_dir <- file.path(dir_derived_analysis, "urbancenter_variant")
    if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

    report_path <- file.path(dir_tables, "diagnostic_ma_urbancenter.txt")
    con <- file(report_path, open = "wt")
    rep <- function(...) { line <- sprintf(...); cat(line, "\n")
                           cat(line, "\n", file = con) }

    rep("%s", strrep("=", 70))
    rep("MA URBAN-CENTER REFERENCE-POINT DIAGNOSTIC (memo Decision D)")
    rep("Largest settlement by area vs geographic centroid")
    rep("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
    rep("%s", strrep("=", 70))

    # ---- Urban-center reference points ------------------------------------
    pts_info <- load_urbancenter_points()
    pts <- pts_info$points
    n_fallback <- pts_info$n_fallback
    rep("\nReference points: %d districts", nrow(pts))
    rep("  %d districts anchored at largest settlement polygon (by area)",
        nrow(pts) - n_fallback)
    rep("  %d districts fell back to geographic centroid (no settlement polygon)",
        n_fallback)

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

    # ---- Main treatment Δlog MA under the urban-center point --------------
    ma$chg_logMA_86_60_v <- ma$logMA_actual_1986_s0_elow -
                            ma$logMA_actual_1960_s0_elow
    ma$chg_logMA_stu_v    <- ma$logMA_instrument_stu_s0_elow -
                             ma$logMA_actual_1960_s0_elow
    ma$chg_logMA_lcpmst_v <- ma$logMA_instrument_lcp_mst_s0_elow -
                             ma$logMA_actual_1960_s0_elow

    arrow::write_parquet(ma, file.path(out_dir, "ma_compare.parquet"))

    # ---- Compare gain stats: baseline vs urban-center ----------------------
    base <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))
    base <- ensure_geolev2_char(base)

    rep("\n%s", strrep("-", 70))
    rep("[A] MA GAIN: baseline centroid vs urban-center reference point")
    rep("%s", strrep("-", 70))
    chg_b <- base$chg_logMA_86_60_s0_elow

    # Districts whose settlement anchor is DISCONNECTED from the network in
    # some year's transition graph get tau = Inf to everywhere -> MA = 0 ->
    # logMA = -Inf -> chg = +/-Inf. Report them explicitly and compute the
    # summary stats on the finite (connected) set only. feols drops them
    # automatically in section [B].
    disconnected <- ma$geolev2[!is.finite(ma$chg_logMA_86_60_v)]
    if (length(disconnected) > 0) {
        rep("  NOTE: %d district(s) disconnected under the urban-center anchor",
            length(disconnected))
        rep("        (tau = Inf to all destinations in at least one year):")
        rep("        %s", paste(disconnected, collapse = ", "))
        rep("        Excluded from the stats below; dropped from regressions.")
    }
    chg_v <- ma$chg_logMA_86_60_v[is.finite(ma$chg_logMA_86_60_v)]
    rep("  baseline (centroid):  share gain %.1f%%  mean %.3f  median %.3f  (N=%d)",
        100*mean(chg_b > 0, na.rm=TRUE), mean(chg_b, na.rm=TRUE),
        median(chg_b, na.rm=TRUE), sum(!is.na(chg_b)))
    rep("  urban center:         share gain %.1f%%  mean %.3f  median %.3f  (N=%d)",
        100*mean(chg_v > 0), mean(chg_v), median(chg_v), length(chg_v))

    # ---- Quick OLS + IV of population on Δlog MA under urban-center point -
    rep("\n%s", strrep("-", 70))
    rep("[B] POPULATION ELASTICITY under urban-center point (sector 0, theta low)")
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
    # NOTE: baseline logMA control here is the CENTROID logMA (same
    # transparency caveat as diagnostic_ma_refpoint.R — a fully consistent
    # variant would also swap the baseline-MA control to the urban-center
    # 1960 logMA).
    f_ols <- as.formula(sprintf("chg_log_pop_91_60 ~ chg_logMA_86_60_v + %s",
                                ctrls))
    m_ols <- suppressMessages(fixest::feols(f_ols, data = m, vcov = "hetero"))
    f_iv_lp <- as.formula(sprintf(
        "chg_log_pop_91_60 ~ %s | chg_logMA_86_60_v ~ chg_logMA_stu_v",
        ctrls))
    m_iv_lp <- suppressMessages(fixest::feols(f_iv_lp, data = m,
                                              vcov = "hetero"))
    f_iv  <- as.formula(sprintf(
        "chg_log_pop_91_60 ~ %s | chg_logMA_86_60_v ~ chg_logMA_stu_v + chg_logMA_lcpmst_v",
        ctrls))
    m_iv  <- suppressMessages(fixest::feols(f_iv, data = m, vcov = "hetero"))

    co_ols   <- coef(m_ols)["chg_logMA_86_60_v"]
    se_ols   <- m_ols$se["chg_logMA_86_60_v"]
    co_iv_lp <- coef(m_iv_lp)["fit_chg_logMA_86_60_v"]
    se_iv_lp <- m_iv_lp$se["fit_chg_logMA_86_60_v"]
    co_iv    <- coef(m_iv)["fit_chg_logMA_86_60_v"]
    se_iv    <- m_iv$se["fit_chg_logMA_86_60_v"]
    # First-stage F: essential for reading the IV beta. The connector
    # experiment (PR #69/#70) showed reference-point/cost changes can kill
    # the instruments' first stage on TOTAL MA via composition effects, so
    # a small IV beta with a dead instrument is uninformative.
    # IV-LP-only is reported alongside IV-Both because memo Decision C
    # (rail-MA vs total-MA estimand) turns on how the Larkin instrument
    # alone behaves under alternative anchoring.
    fs_F <- tryCatch(
        fixest::fitstat(m_iv, "ivf")[[1]]$stat,
        error = function(e) NA_real_)
    fs_F_lp <- tryCatch(
        fixest::fitstat(m_iv_lp, "ivf")[[1]]$stat,
        error = function(e) NA_real_)
    rep("  OLS      beta = %+.3f (%.3f)   N=%d", co_ols, se_ols, m_ols$nobs)
    rep("  IV-LP    beta = %+.3f (%.3f)   N=%d   first-stage F = %.1f",
        co_iv_lp, se_iv_lp, m_iv_lp$nobs, fs_F_lp)
    rep("  IV-Both  beta = %+.3f (%.3f)   N=%d   first-stage F = %.1f",
        co_iv, se_iv, m_iv$nobs, fs_F)
    rep("  (baseline-centroid IV-Both was +0.046 (0.033), F = 13.6;")
    rep("   Gibbons 2024 ~0.3)")
    rep("  (interior-point IV-Both from diagnostic_ma_refpoint.R was +0.036)")

    rep("\n%s", strrep("=", 70))
    rep("READING: if the urban-center gain share drops well below 91%% and/or")
    rep("the IV beta moves toward 0.3 (Gibbons), the centroid choice was")
    rep("inflating MA. The interior-point variant (PR #66) already found the")
    rep("reference point was NOT the driver (91.0%%->89.1%%, beta 0.046->0.036);")
    rep("this is a third anchoring choice (Gibbons-style urban anchoring, after")
    rep("centroid and interior point) testing whether that conclusion is robust")
    rep("to using an actual settlement location rather than an arbitrary")
    rep("interior point.")
    rep("%s", strrep("=", 70))

    close(con)
    message("\nSaved report: ", report_path)
}

# ---------------------------------------------------------------------------
# Urban-center reference points: largest settlement polygon by area, per
# district. Districts with no settlement polygon fall back to the
# geographic centroid (same construction as load_centroids() in 03c).
# ---------------------------------------------------------------------------
load_urbancenter_points <- function() {
    # District polygons via the shared loader (_diagnostic_helpers.R),
    # then projected to the raster CRS for the spatial join below.
    shp <- sf::st_transform(load_district_shapes(), crs = crs_raster)

    settle_path <- file.path(dir_raw_geo,
                             "areas_de_asentamientos_y_edificios_020105.shp")
    stopifnot(file.exists(settle_path))
    settlements <- sf::st_read(settle_path, quiet = TRUE)
    settlements <- sf::st_make_valid(settlements)
    settlements <- sf::st_transform(settlements, crs = crs_raster)
    settlements$area_m2 <- as.numeric(sf::st_area(settlements))

    # Assign each settlement polygon to a single district via its
    # point-on-surface (guaranteed interior point; a concave settlement's
    # centroid could fall outside the polygon and land in a neighboring
    # district). Avoids boundary-straddling polygons being assigned to
    # multiple districts.
    settle_pts <- suppressWarnings(sf::st_point_on_surface(settlements))
    joined <- suppressWarnings(sf::st_join(settle_pts, shp["geolev2"],
                                           join = sf::st_within))
    joined <- joined[!is.na(joined$geolev2), ]

    # Largest settlement (by polygon area) per district.
    joined_df <- sf::st_drop_geometry(joined)
    joined_df$geometry_idx <- seq_len(nrow(joined))
    ord <- order(joined_df$geolev2, -joined_df$area_m2)
    joined_df <- joined_df[ord, ]
    largest_idx <- joined_df$geometry_idx[!duplicated(joined_df$geolev2)]
    largest <- joined[largest_idx, ]

    # Merge onto the full 312-district set; districts with no settlement
    # polygon fall back to the geometric centroid.
    centroid_pts <- suppressWarnings(sf::st_centroid(shp))
    has_settlement <- shp$geolev2 %in% largest$geolev2
    n_fallback <- sum(!has_settlement)

    settle_coords <- largest[match(shp$geolev2[has_settlement], largest$geolev2), ]
    pts_sf <- centroid_pts
    pts_sf$geometry[has_settlement] <- settle_coords$geometry

    pts_sp <- sf::as_Spatial(pts_sf[, "geolev2"])
    list(points = pts_sp, n_fallback = n_fallback)
}



main()

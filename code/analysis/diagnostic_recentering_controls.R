# ===========================================================================
# diagnostic_recentering_controls.R
#
# PURPOSE: Outcome-blind control exploration for the recentered Larkin
#          instrument (extension of the Stage 1 diagnostic; protocol
#          approved by Diego 2026-07-22). DIAGNOSTIC ONLY.
#
# PROTOCOL (fixed ex ante; the outcome equation is never consulted for
# selection):
#   1. Candidate control sets, all predetermined:
#        C0  geo_controls_main                       (benchmark)
#        C1  C0 + mu                                 (BH expected-IV control)
#        C2  C1 + region FE (INDEC 5)
#        C3  C1 + province FE (24)
#        C4  C1 + baseline rail km 1960 + rail density 1960
#        C5  C1 + latitude/longitude quadratic (BH's China controls)
#        C6  C1 + region FE + rail baseline + lat/lon quadratic (all)
#   2. Selection criterion: the RECENTERED first-stage F (treatment on
#      z_rec + controls). Sets are ranked by F alone. The R2 of mu on
#      each set is reported for context.
#   3. Only after ranking: the four diagnostic outcomes are estimated
#      under EVERY set (full transparency; the top-F set is marked as
#      selected). Reduced-form RI p-values under the selected set use
#      LEAVE-ONE-OUT mu for the null draws (removes the O(1/S)
#      leave-in contamination flagged in PR #111's review).
#
# HONESTY NOTE: controls can absorb residual variance (precision); they
# cannot recreate the exposure component that recentering removes by
# construction. Whatever the estimates are, they are reported.
#
# READS:
#   data/derived/07_recentering/draws/z_rc*.parquet
#   data/derived/06_analysis/estimation_sample.parquet
#   data/raw/geo/geo2_ar1970_2010.shp   (centroid lat/lon)
#
# PRODUCES:
#   results/tables/diagnostic_recentering_controls.csv
#   results/tables/diagnostic_recentering_controls.txt
#
# USAGE:
#   Rscript code/analysis/diagnostic_recentering_controls.R
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
    library(sf)
})

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)

    message("\n", strrep("=", 72))
    message("diagnostic_recentering_controls.R  |  outcome-blind exploration")
    message(strrep("=", 72))

    # ---- 1. Load draws + sample (mirrors diagnostic_recentering_results.R) --
    dir_draws <- file.path(dir_derived_recentering, "draws")
    files <- sort(list.files(dir_draws, pattern = "^z_rc\\d+\\.parquet$",
                             full.names = TRUE))
    draws <- do.call(rbind, lapply(files, function(f)
        as.data.frame(arrow::read_parquet(f))))
    draws <- ensure_geolev2_char(draws)
    S_perm <- length(unique(draws$draw[draws$draw > 0L]))
    stopifnot(S_perm >= 100L)
    message(sprintf("[ctl] %d permuted draws", S_perm))

    d <- as.data.frame(arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet")))
    d <- ensure_geolev2_char(d)
    stopifnot(nrow(d) == 311L)

    zw <- reshape(draws[, c("geolev2", "draw", "logMA")],
                  idvar = "geolev2", timevar = "draw", direction = "wide")
    d2 <- merge(d, zw, by = "geolev2", all.x = TRUE)
    stopifnot(nrow(d2) == 311L)
    perm_cols <- sprintf("logMA.%d",
                         sort(unique(draws$draw[draws$draw > 0L])))
    zmat <- as.matrix(d2[, perm_cols]) - d2$logMA_actual_1960_s0_elow
    stopifnot(!anyNA(zmat))  # guard against a partial draw set

    d2$mu    <- rowMeans(zmat)
    d2$z_obs <- d2$chg_logMA_stu_s0_elow
    d2$z_rec <- d2$z_obs - d2$mu

    # ---- 2. Additional predetermined covariates ------------------------------
    # Region / province from geolev2 (INDEC codes; chars 3-5 after the
    # leading-zero strip).
    d2$province <- substr(d2$geolev2, 3, 5)
    stopifnot(all(d2$province %in% names(region_of_province)))
    d2$region <- region_of_province[d2$province]

    # Baseline rail intensity (predetermined exposure proxies).
    d2$rail_km_1960   <- d2$tot_rails_1960
    d2$rail_dens_1960 <- d2$tot_rails_1960 / d2$area_km2

    # Centroid latitude/longitude (WGS84), quadratic.
    shp <- sf::st_read(file.path(dir_raw_geo, "geo2_ar1970_2010.shp"),
                       quiet = TRUE)
    shp <- sf::st_make_valid(shp)
    names(shp)[names(shp) == "GEOLEVEL2"] <- "geolev2"
    shp$geolev2 <- sub("^0+", "", as.character(shp$geolev2))
    cents <- suppressWarnings(
        sf::st_coordinates(sf::st_centroid(sf::st_transform(shp, 4326))))
    geo_ll <- data.frame(geolev2 = shp$geolev2,
                         lon = cents[, "X"], lat = cents[, "Y"])
    d2 <- merge(d2, geo_ll, by = "geolev2", all.x = TRUE)
    stopifnot(nrow(d2) == 311L, !any(is.na(d2$lat)))
    d2$lat2 <- d2$lat^2; d2$lon2 <- d2$lon^2; d2$latlon <- d2$lat * d2$lon

    # ---- 3. Candidate sets (fixed ex ante) -----------------------------------
    base <- geo_controls_main
    ll_quad <- c("lat", "lon", "lat2", "lon2", "latlon")
    rails   <- c("rail_km_1960", "rail_dens_1960")
    sets <- list(
        C0 = list(ctrl = base,                              fe = NULL),
        C1 = list(ctrl = c(base, "mu"),                     fe = NULL),
        C2 = list(ctrl = c(base, "mu"),                     fe = "region"),
        C3 = list(ctrl = c(base, "mu"),                     fe = "province"),
        C4 = list(ctrl = c(base, "mu", rails),              fe = NULL),
        C5 = list(ctrl = c(base, "mu", ll_quad),            fe = NULL),
        C6 = list(ctrl = c(base, "mu", rails, ll_quad),     fe = "region")
    )

    endog <- "chg_logMA_86_60_s0_elow"
    iv_fml <- function(y, instr, ctrl, fe) {
        rhs <- paste(ctrl, collapse = " + ")
        fe_part <- if (is.null(fe)) "" else sprintf(" | %s", fe)
        as.formula(sprintf("%s ~ %s%s | %s ~ %s",
                           y, rhs, fe_part, endog, instr))
    }

    out_rows <- list()
    add_row <- function(...) out_rows[[length(out_rows) + 1L]] <<-
        data.frame(..., stringsAsFactors = FALSE)

    # ---- 4. Stage A: rank sets by recentered first-stage F (outcome-blind) --
    message("\n[ctl] Stage A: recentered first-stage F by candidate set")
    fs_F <- setNames(numeric(length(sets)), names(sets))
    for (nm in names(sets)) {
        s <- sets[[nm]]
        # First stage estimated DIRECTLY (treatment on instrument +
        # controls): no outcome object exists at selection time, so
        # outcome-blindness is structural, not incidental (cr-review
        # PR #112: an IV-object F varies with outcome missingness via
        # NA row-dropping, and was full-sample here only because the
        # population outcome happens to have zero NAs). With a single
        # instrument the robust Wald F is the squared t on z_rec, which
        # equals fitstat's ivf on the full sample.
        f1 <- as.formula(sprintf("%s ~ z_rec + %s%s", endog,
            paste(setdiff(s$ctrl, "z_rec"), collapse = " + "),
            if (is.null(s$fe)) "" else sprintf(" | %s", s$fe)))
        m1 <- feols(f1, data = d2, vcov = "hetero")
        tz <- safe_coef(m1, "z_rec")
        # NOTE: this is the HETEROSKEDASTICITY-ROBUST Wald F (squared
        # robust t on the single instrument), the metric matching the
        # HC1 inference used throughout. It is systematically larger
        # here than the classical ivf that fitstat_F reports (and that
        # PR #111's CSV shows: e.g. C1 10.8 classical vs 13.3 robust).
        # Stage B reports the classical ivf per fitstat_F for
        # comparability with the paper's tables; both are labelled.
        fs_F[nm] <- (tz$est / tz$se)^2
        m_mu <- feols(as.formula(paste(
            "mu ~", paste(setdiff(s$ctrl, "mu"), collapse = " + "),
            if (!is.null(s$fe)) sprintf("| %s", s$fe) else "")),
            data = d2, vcov = "hetero")
        r2_mu <- r2(m_mu, type = if (is.null(s$fe)) "r2" else "wr2")
        message(sprintf("[ctl]   %-3s  F(recentered) = %6.2f   R2(mu|set) = %.3f",
                        nm, fs_F[nm], r2_mu))
        add_row(block = "A_first_stage", set = nm, outcome = "",
                spec = "recentered", stat = "F_robust_wald",
                value = fs_F[nm])
        add_row(block = "A_first_stage", set = nm, outcome = "",
                spec = "recentered", stat = "r2_mu", value = r2_mu)
    }
    selected <- names(which.max(fs_F))
    message(sprintf("\n[ctl] Selected set by F: %s (F = %.2f)",
                    selected, max(fs_F)))
    add_row(block = "A_first_stage", set = selected, outcome = "",
            spec = "selected", stat = "selected", value = 1)

    # ---- 5. Stage B: outcomes under every set (top set marked) --------------
    outcomes <- list(
        c("chg_log_pop_91_60",         "population"),
        c("chg_log_valprod_85_54",     "mfg_valprod"),
        c("chg_log_massal_85_54",      "mfg_wagemass"),
        c("chg_log_placebo_pop_60_47", "placebo_pretrend")
    )
    message("\n[ctl] Stage B: outcome estimates under every set")
    for (nm in names(sets)) {
        s <- sets[[nm]]
        for (oc in outcomes) {
            m <- feols(iv_fml(oc[1], "z_rec", s$ctrl, s$fe),
                       data = d2, vcov = "hetero")
            cc <- safe_coef(m, paste0("fit_", endog))
            add_row(block = "B_outcomes", set = nm, outcome = oc[2],
                    spec = "recentered", stat = "coef", value = cc$est)
            add_row(block = "B_outcomes", set = nm, outcome = oc[2],
                    spec = "recentered", stat = "se", value = cc$se)
            add_row(block = "B_outcomes", set = nm, outcome = oc[2],
                    spec = "recentered", stat = "p", value = cc$p)
            add_row(block = "B_outcomes", set = nm, outcome = oc[2],
                    spec = "recentered", stat = "N", value = nobs(m))
            # Per-outcome-sample first-stage F (differs from Stage A's
            # full-sample F where the outcome has missing rows, e.g.
            # the 237-district placebo subsample).
            add_row(block = "B_outcomes", set = nm, outcome = oc[2],
                    spec = "recentered", stat = "F_ivf",
                    value = fitstat_F(m))
            message(sprintf(
                "[ctl]   %-3s %-16s  b=%+.4f  se=%.4f  p=%.3f%s",
                nm, oc[2], cc$est, cc$se, cc$p,
                if (nm == selected) "   <- selected" else ""))
        }
    }

    # ---- 6. Stage C: RI p-values under the selected set with LOO mu ---------
    # Leave-one-out mu for null draw s: mu_{-s} = (S*mu - z^(s))/(S-1).
    # The observed z keeps the full-sample mu (it is not in the draw set).
    message(sprintf("\n[ctl] Stage C: reduced-form RI (LOO mu), set %s",
                    selected))
    ssel <- sets[[selected]]
    ctrl_expr <- paste(setdiff(ssel$ctrl, "mu"), collapse = " + ")
    fe_expr <- if (is.null(ssel$fe)) "" else sprintf(" | %s", ssel$fe)
    mu_full <- d2$mu
    for (oc in outcomes) {
        rf_coef <- function(zv, muv) {
            dd <- cbind(d2, zv = zv, muv = muv)
            m <- feols(as.formula(sprintf("%s ~ zv + muv + %s%s",
                                          oc[1], ctrl_expr, fe_expr)),
                       data = dd, vcov = "hetero")
            unname(coef(m)[["zv"]])
        }
        b_obs <- rf_coef(d2$z_obs - mu_full, mu_full)
        b_null <- vapply(seq_len(ncol(zmat)), function(s) {
            mu_loo <- (S_perm * mu_full - zmat[, s]) / (S_perm - 1)
            rf_coef(zmat[, s] - mu_loo, mu_loo)
        }, numeric(1))
        p_rf <- (1 + sum(abs(b_null) >= abs(b_obs))) / (length(b_null) + 1)
        add_row(block = "C_ri", set = selected, outcome = oc[2],
                spec = "reduced_form_loo", stat = "ri_p", value = p_rf)
        message(sprintf("[ctl]   %-16s  RI p (LOO) = %.3f", oc[2], p_rf))
    }

    # ---- 7. Save --------------------------------------------------------------
    res <- do.call(rbind, out_rows)
    res$S_perm <- S_perm
    csv_path <- file.path(dir_tables, "diagnostic_recentering_controls.csv")
    write.csv(res, csv_path, row.names = FALSE)

    txt_path <- file.path(dir_tables, "diagnostic_recentering_controls.txt")
    sink(txt_path)
    cat("Recentered-instrument control exploration (outcome-blind)\n")
    cat(sprintf("Generated: %s  |  draws: %d  |  selected set: %s\n\n",
                Sys.time(), S_perm, selected))
    cat("Protocol: sets fixed ex ante; ranked by recentered first-stage\n")
    cat("F only; outcomes estimated for every set afterwards; RI under\n")
    cat("the selected set uses leave-one-out mu.\n\n")
    cat("Stage A (F, recentered first stage):\n")
    for (nm in names(sets)) cat(sprintf("  %-3s  F = %6.2f%s\n", nm,
        fs_F[nm], if (nm == selected) "   <- selected" else ""))
    cat("\nStage B/C: see CSV for all cells.\n")
    sink()

    message(sprintf("\n[ctl] Saved: %s and .txt", csv_path))
}

main()

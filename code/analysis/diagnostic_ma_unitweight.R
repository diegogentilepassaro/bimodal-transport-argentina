# ===========================================================================
# diagnostic_ma_unitweight.R
#
# PURPOSE: does a POPULATION-FREE baseline market-access control change the
#          headline population elasticity, and can that question be
#          answered at all? Coauthor request (ii), 2026-09.
#          DIAGNOSTIC ONLY: no paper exhibit, no scalar changes.
#
# THE CONCERN. pop_1960 is a locality-universe measure: the 1960 source is
# one row per named locality with no dispersed-rural line, so it omits
# dispersed rural population, while pop_1970 and later come from
# full-universe IPUMS microdata (diagnostic_pop1960_universe.R documents
# this and sizes it). That single measurement error enters the main
# specification in three places at once:
#
#   (1) the outcome, whose 1960 denominator is the locality universe;
#   (2) the log_pop_1960 control;
#   (3) the BASELINE MA control, because MA weights every destination j by
#       Pop_j(1960).
#
# This diagnostic addresses (3) only. It rebuilds the baseline level with
# UNIT weights,
#
#     MA^unit_i = sum_{j != i} 1 / tau_ij^theta,
#
# which keeps the spatial structure and the convergence-control role
# (baseline_ma_control_note.md) while removing 1960 population from the
# control entirely. The treatment and both instruments keep population
# weights: reweighting those would change what market access means and is
# a different exercise. So this isolates the control channel.
#
# WHAT DECIDES WHETHER THE ANSWER IS INFORMATIVE, and why that is measured
# first. PR #143 ran the nearest available version of this check by
# reweighting the same 1960 tau with 1947 population, and the two levels
# came back correlated 0.9994 raw / 0.9990 after controls. The lesson
# recorded in tasks.md is that at that correlation the contrast "never had
# power to separate the two controls" -- there was almost nothing to
# remove, so no coefficient comparison could have settled anything. Unit
# weighting is a larger departure, since it discards population rather
# than swapping one population vector for a near-identical one, but
# whether it has power is an empirical question and not an assumption.
#
# This script therefore reports the correlation FIRST and treats it as the
# gate on interpretation. The coefficient comparison is reported after it
# and is only worth reading if the correlation leaves independent
# variation. The script does not decide the threshold; it prints the number
# and says plainly what it does and does not support.
#
# A PIPELINE GATE runs before anything else: the same construction, called
# with population weights and the full destination set, must reproduce the
# committed logMA_actual_1960_s0_elow to 1e-10. Without that, a wrong
# reweighting would be indistinguishable from a real finding.
#
# READS:
#   data/derived/03_taus/tau_actual_1960_s0.parquet
#   data/derived/base/census_1960/census_1960_ipums.parquet
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/diagnostic_ma_unitweight.{txt,csv}
# ===========================================================================
suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
})

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)

    out <- character()
    rep <- function(fmt, ...) {
        line <- if (length(list(...))) sprintf(fmt, ...) else fmt
        message(line)
        out <<- c(out, line)
    }

    rep(strrep("=", 70))
    rep("POPULATION-FREE BASELINE MA CONTROL (coauthor request ii)")
    rep("Unit weights on the 1960 tau: MA_i = sum_j 1 / tau_ij^theta")
    rep("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
    rep(strrep("=", 70))

    d <- ensure_geolev2_char(as.data.frame(arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))))
    tau <- as.data.frame(arrow::read_parquet(
        file.path(dir_derived_taus, "tau_actual_1960_s0.parquet")))
    tau <- ensure_geolev2_char(tau, "origin_geolev2")
    tau <- ensure_geolev2_char(tau, "destination_geolev2")
    census60 <- ensure_geolev2_char(as.data.frame(arrow::read_parquet(
        file.path(dir_derived_census1960, "census_1960_ipums.parquet"))))
    census60 <- data.frame(geolev2 = census60$geolev2,
                           w = as.numeric(census60$pop))

    all_dest <- sort(unique(c(tau$origin_geolev2, tau$destination_geolev2)))
    stopifnot(all(all_dest %in% census60$geolev2))

    # ---- Gate: reproduce the committed control before trusting anything --
    rep("\n%s", strrep("-", 70))
    rep("[0] GATE -- population weights must reproduce the pipeline control")
    rep(strrep("-", 70))
    ma_pop <- build_ma_level(tau, census60, theta[["low"]], all_dest)
    g <- merge(d[, c("geolev2", "logMA_actual_1960_s0_elow")],
               ma_pop[, c("geolev2", "logMA")], by = "geolev2")
    stopifnot(nrow(g) == nrow(d))
    dmax <- max(abs(g$logMA - g$logMA_actual_1960_s0_elow))
    rep("  destinations: %d   estimation sample: %d", length(all_dest),
        nrow(d))
    rep("  max|constructed - committed control| = %.3e", dmax)
    stopifnot("unit-weight machinery must reproduce the pipeline MA" =
                  dmax < 1e-10)
    rep("  PASS")

    # ---- The unit-weighted level ----------------------------------------
    unit_w <- data.frame(geolev2 = census60$geolev2, w = 1)
    rows <- list()
    for (pair in list(c("elow", "low"), c("ehigh", "high"))) {
        el  <- pair[1]
        th  <- theta[[pair[2]]]
        ctrl_pop  <- sprintf("logMA_actual_1960_s0_%s", el)
        ma_unit <- build_ma_level(tau, unit_w, th, all_dest)
        names(ma_unit)[names(ma_unit) == "logMA"] <- "logMA_unit"
        dd <- merge(d, ma_unit[, c("geolev2", "logMA_unit")], by = "geolev2")
        stopifnot(nrow(dd) == nrow(d))

        # --- Correlation, the gate on interpretation ---------------------
        # Partial correlation is the margin that matters: the regression
        # already conditions on the other seven controls, so what decides
        # whether the swap can move anything is the independent variation
        # left AFTER they are partialled out. Reported both ways because
        # log_pop_1960 is itself a population term, and a reader will want
        # to know whether the two levels separate only through it.
        others_full <- setdiff(geo_controls_main, ctrl_pop)
        others_nopop <- setdiff(others_full, "log_pop_1960")
        pcor <- function(ctrls) {
            r <- function(v) residuals(lm(
                as.formula(paste(v, "~", paste(ctrls, collapse = " + "))),
                data = dd))
            cor(r("logMA_unit"), r(ctrl_pop))
        }
        raw   <- cor(dd$logMA_unit, dd[[ctrl_pop]])
        p_all <- pcor(others_full)
        p_nop <- pcor(others_nopop)

        rep("\n%s", strrep("-", 70))
        rep("[1] %s (theta = %.3f) -- CORRELATION OF THE TWO LEVELS", el, th)
        rep(strrep("-", 70))
        rep("  raw corr(unit-weighted, pop-weighted)        = %.4f", raw)
        rep("  partial, after the other 7 controls          = %.4f", p_all)
        rep("  partial, excluding log_pop_1960 from those   = %.4f", p_nop)
        # Share of the pop-weighted control's POST-CONTROL variation that
        # the unit-weighted level does not share, i.e. 1 - partial^2. This
        # is the quantity that bounds how much the swap could move: an
        # earlier version printed 1 - R^2 from a regression that also
        # included the other controls, which is a much smaller number
        # measuring something else and read as far more reassuring.
        rep("  share of post-control variation NOT shared   = %.4f",
            1 - p_all^2)
        rep("  REFERENCE: the 1947-population version of this check came")
        rep("  back at 0.9994 raw / 0.9990 partial, which tasks.md records")
        rep("  as having had no power to separate the controls (PR #143).")

        # --- The coefficient comparison ----------------------------------
        ctrls_unit <- c(others_full, "logMA_unit")
        endog <- if (el == "elow") main_treatment else
            sub("_elow$", "_ehigh", main_treatment)
        lp <- if (el == "elow") main_lp_instrument else
            sub("_elow$", "_ehigh", main_lp_instrument)
        hy <- if (el == "elow") main_hypo_instrument else
            sub("_elow$", "_ehigh", main_hypo_instrument)
        stopifnot(all(c(endog, lp, hy) %in% names(dd)))

        fit_pop  <- fit_iv_quad("chg_log_pop_91_60", dd, endog, lp, hy,
                                c(others_full, ctrl_pop))
        fit_unit <- fit_iv_quad("chg_log_pop_91_60", dd, endog, lp, hy,
                                ctrls_unit)

        rep("\n[2] %s -- HEADLINE POPULATION ELASTICITY UNDER THE SWAP", el)
        rep("  %-26s %-22s %-22s", "spec", "pop-weighted ctrl",
            "unit-weighted ctrl")
        for (key in c("OLS", "IV-LP", "IV-H", "IV-B")) {
            nm <- if (key == "OLS") endog else paste0("fit_", endog)
            a <- safe_coef(fit_pop[[key]],  nm)
            b <- safe_coef(fit_unit[[key]], nm)
            fa <- if (key == "OLS") NA_real_ else
                fitstat_F_robust(fit_pop[[key]])
            fb <- if (key == "OLS") NA_real_ else
                fitstat_F_robust(fit_unit[[key]])
            rep("  %-26s %+.3f (%.3f) p %.3f   %+.3f (%.3f) p %.3f",
                key, a$est, a$se, a$p, b$est, b$se, b$p)
            if (!is.na(fa)) {
                rep("  %-26s F %5.2f                F %5.2f", "", fa, fb)
            }
            rows[[length(rows) + 1L]] <- data.frame(
                elasticity = el, theta = th, spec = key,
                raw_corr = raw, partial_corr = p_all,
                partial_corr_no_pop60 = p_nop,
                est_pop = a$est, se_pop = a$se, p_pop = a$p, F_pop = fa,
                est_unit = b$est, se_unit = b$se, p_unit = b$p, F_unit = fb,
                n_obs = nobs(fit_pop[[key]]),
                stringsAsFactors = FALSE)
        }
    }
    df <- do.call(rbind, rows)

    # ---- What this does and does not establish ---------------------------
    pc_lo <- unique(df$partial_corr[df$elasticity == "elow"])
    pc_hi <- unique(df$partial_corr[df$elasticity == "ehigh"])
    ivb <- function(el, col) {
        v <- df[[col]][df$elasticity == el & df$spec == "IV-B"]
        stopifnot(length(v) == 1L)
        v
    }
    stopifnot(length(pc_lo) == 1L, length(pc_hi) == 1L)
    rep("\n%s", strrep("=", 70))
    rep("READING")
    rep(strrep("=", 70))
    rep("  The correlation governs, and it differs sharply by elasticity.")
    rep("  theta_low  (%.3f): partial corr %.4f", theta[["low"]], pc_lo)
    rep("  theta_high (%.3f): partial corr %.4f", theta[["high"]], pc_hi)
    rep("  Higher theta concentrates market access on nearby destinations,")
    rep("  where the choice between counting destinations and weighting")
    rep("  them by population bites hardest, so the two levels separate")
    rep("  more at theta_high. That is a mechanical consequence of the")
    rep("  formula, not a finding about Argentina.")
    rep("")
    if (pc_lo > 0.95) {
        rep("  AT THE MAIN CALIBRATION the check is near-uninformative.")
        rep("  At partial corr %.4f the two controls are near-collinear,", pc_lo)
        rep("  so IV-B moving %+.3f -> %+.3f is NOT evidence that the",
            ivb("elow", "est_pop"), ivb("elow", "est_unit"))
        rep("  pop_1960 universe problem fails to reach the estimate")
        rep("  through the control: there was little to remove. This is")
        rep("  the position PR #143 reached on the 1947 version, and on")
        rep("  this reading the check belongs in the record rather than in")
        rep("  the paper as a robustness result.")
    } else {
        rep("  At the main calibration the levels separate enough for the")
        rep("  comparison in [2] to be read directly.")
    }
    rep("")
    if (pc_hi <= 0.95) {
        rep("  AT theta_high THE COMPARISON DOES HAVE POWER, and there the")
        rep("  swap moves nothing: IV-B %+.3f -> %+.3f, p %.3f -> %.3f.",
            ivb("ehigh", "est_pop"), ivb("ehigh", "est_unit"),
            ivb("ehigh", "p_pop"), ivb("ehigh", "p_unit"))
        rep("  That is the informative cell in this table. It is partial")
        rep("  reassurance about the control channel at one calibration,")
        rep("  and it is not the calibration the paper reports.")
    }
    rep("")
    rep("  SCOPE. This addresses only the CONTROL channel. The outcome's")
    rep("  1960 denominator and the log_pop_1960 control carry the same")
    rep("  measurement error and are untouched here, so nothing above")
    rep("  speaks to those two.")
    rep("  The 0.95 threshold is a reporting convention agreed with the")
    rep("  coauthors, not a test: nothing here computes a p-value for")
    rep("  collinearity, and the cut is arbitrary at the margin.")
    rep(strrep("=", 70))

    out_txt <- file.path(dir_tables, "diagnostic_ma_unitweight.txt")
    writeLines(out, out_txt)
    message("\nSaved: ", out_txt)
    out_csv <- file.path(dir_tables, "diagnostic_ma_unitweight.csv")
    write.csv(df, out_csv, row.names = FALSE)
    message("Saved: ", out_csv)
}

# ---------------------------------------------------------------------------
# build_ma_level(tau_df, w_df, theta_val, dest_keep)
#
# MA_i = sum_{j != i, j in dest_keep} w_j / tau_ij^theta, then log.
#
# Symmetrisation and the Inf-tau -> weight 0 rule are copied from
# code/pipeline/04_market_access.R:compute_ma_one_case(); diagnostics stay
# self-contained by repo convention, and the gate in main() requires this
# to reproduce the committed control exactly when w is 1960 population.
#
# Generalised from diagnostic_placebo_ma1947.R's version by taking a
# WEIGHT column rather than a population column, so the same code path
# serves the population-weighted gate and the unit-weighted object. An
# absent weight is an error, never coerced to 0: silently weighting a
# destination 0 is the failure this diagnostic would be least able to see.
# ---------------------------------------------------------------------------
build_ma_level <- function(tau_df, w_df, theta_val, dest_keep) {
    sym <- rbind(
        data.frame(origin = tau_df$origin_geolev2,
                   dest   = tau_df$destination_geolev2,
                   tau    = tau_df$tau),
        data.frame(origin = tau_df$destination_geolev2,
                   dest   = tau_df$origin_geolev2,
                   tau    = tau_df$tau)
    )
    sym <- sym[sym$dest %in% dest_keep & sym$origin != sym$dest, ]
    sym <- merge(sym, data.frame(dest = w_df$geolev2, w_dest = w_df$w),
                 by = "dest", all.x = TRUE)
    stopifnot(!any(is.na(sym$w_dest)), all(sym$w_dest > 0))
    sym$k <- ifelse(is.finite(sym$tau) & sym$tau > 0,
                    1 / (sym$tau^theta_val), 0)
    ma <- aggregate(list(MA = sym$k * sym$w_dest),
                    by = list(geolev2 = sym$origin), FUN = sum)
    ma$logMA <- log(ma$MA)
    stopifnot(all(is.finite(ma$logMA)))
    ma[, c("geolev2", "MA", "logMA")]
}

main()

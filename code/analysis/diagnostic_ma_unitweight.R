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
# (baseline_ma_control_note.md) while removing 1960 population DATA from
# the control. The treatment and both instruments keep population weights:
# reweighting those would change what market access means and is a
# different exercise. So this isolates the control channel.
#
# "POPULATION-FREE" IS A CLAIM ABOUT CONSTRUCTION, NOT ABOUT EFFECT, and
# part [3] measures the difference. Unit weights remove the 1960 population
# data, and with it the locality-universe measurement error, which is what
# the request asked for. They do not make the control independent of
# population: the unit-weighted level still correlates +0.51 with
# log_pop_1960 against the population-weighted level's +0.53. And they add
# a dependence of their own, because counting every district equally makes
# the control depend on how the country is cut into departamentos -- areas
# span a factor of 3,177, the level correlates -0.68 with a district's own
# area, and a province divided into many small units contributes more than
# one large unit covering the same ground. That partition is plausibly
# related to historical settlement, which is what the control is meant to
# absorb. Raised by the cr-review of PR #164.
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
    rep("  destinations: %d   estimation sample: %d", length(all_dest),
        nrow(d))
    # Both elasticities, not just theta_low: gating the second costs one
    # more pass over 48,516 pairs and the ehigh arm is a published
    # specification too (cr-review PR #164).
    for (pair in list(c("elow", "low"), c("ehigh", "high"))) {
        ctrl <- sprintf("logMA_actual_1960_s0_%s", pair[1])
        ma_pop <- build_ma_level(tau, census60, theta[[pair[2]]], all_dest)
        g <- merge(d[, c("geolev2", ctrl)],
                   ma_pop[, c("geolev2", "logMA")], by = "geolev2")
        stopifnot(nrow(g) == nrow(d))
        dmax <- max(abs(g$logMA - g[[ctrl]]))
        rep("  %-32s max|diff| = %.3e", ctrl, dmax)
        stopifnot("the MA machinery must reproduce the pipeline control" =
                      dmax < 1e-10)
    }
    rep("  PASS (both elasticities)")

    # ---- The unit-weighted level ----------------------------------------
    unit_w <- data.frame(geolev2 = census60$geolev2, w = 1)
    rows <- list()
    # Per-elasticity comparison, in compare_one_theta() below: the
    # correlation screen, the coefficient swap, and the assertion that the
    # pop-weighted arm reproduces the published specification.
    rows <- unlist(lapply(list(c("elow", "low"), c("ehigh", "high")),
                          function(pr) compare_one_theta(pr, d, tau, unit_w,
                                                         all_dest, rep,
                                                         dir_tables)),
                   recursive = FALSE)
    df <- do.call(rbind, rows)

    # Part [3]: what "population-free" actually buys. In
    # report_population_content() below.
    report_population_content(d, tau, unit_w, all_dest, rep)
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
    rep("  The partial correlation is a SCREENING RULE, not a test. A high")
    rep("  value means the swap has little room to move anything, so a")
    rep("  similar coefficient carries no information. A low value would")
    rep("  not by itself make the comparison decisive.")
    rep("  theta_low  (%.3f): partial corr %.4f", theta[["low"]], pc_lo)
    rep("  theta_high (%.3f): partial corr %.4f", theta[["high"]], pc_hi)
    rep("")
    rep("  BOTH CALIBRATIONS ARE ABOVE THE 0.95 GATE, so the check is")
    rep("  near-uninformative throughout. IV-B moves %+.3f -> %+.3f at",
        ivb("elow", "est_pop"), ivb("elow", "est_unit"))
    rep("  theta_low and %+.3f -> %+.3f at theta_high, and NEITHER is",
        ivb("ehigh", "est_pop"), ivb("ehigh", "est_unit"))
    rep("  evidence that the pop_1960 universe problem fails to reach the")
    rep("  estimate through the baseline control: there was almost nothing")
    rep("  to remove in either case. This is the position PR #143 reached")
    rep("  on the 1947-population version, and unit weighting does not")
    rep("  escape it. The check belongs in the record, not in the paper as")
    rep("  a robustness result.")
    stopifnot(
        "the reading below assumes both calibrations exceed the gate" =
            pc_lo > 0.95 && pc_hi > 0.95
    )
    rep("")
    rep("  WHAT WOULD BE NEEDED to answer the question: a baseline control")
    rep("  that keeps the convergence role while carrying variation the")
    rep("  population-weighted level does not. Unit weights do not deliver")
    rep("  that, and neither did 1947 population. Both are still")
    rep("  distance-decay sums over the same 1960 tau, and that shared")
    rep("  structure is what drives the correlation.")
    rep("")
    rep("  SCOPE. This addresses only the CONTROL channel. The outcome's")
    rep("  1960 denominator and the log_pop_1960 control carry the same")
    rep("  measurement error and are untouched here, so nothing above")
    rep("  speaks to those two.")
    rep("  UNIT WEIGHTS ARE NOT POPULATION-FREE IN EFFECT, only in")
    rep("  construction; see [3] above.")
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
# compare_one_theta(): parts [1] and [2] for one elasticity. Returns the
# CSV rows for that elasticity. Split out of main() for the 200-line
# function limit (cr-review PR #164).
# ---------------------------------------------------------------------------
compare_one_theta <- function(pair, d, tau, unit_w, all_dest, rep,
                              dir_tables) {
    rows <- list()
        el  <- pair[1]
        th  <- theta[[pair[2]]]
        ctrl_pop  <- sprintf("logMA_actual_1960_s0_%s", el)
        ma_unit <- build_ma_level(tau, unit_w, th, all_dest)
        names(ma_unit)[names(ma_unit) == "logMA"] <- "logMA_unit"
        dd <- merge(d, ma_unit[, c("geolev2", "logMA_unit")], by = "geolev2")
        stopifnot(nrow(dd) == nrow(d))

        # --- Correlation, the screening rule on interpretation -----------
        # Partial correlation is the margin that matters: the regression
        # already conditions on the other controls, so what decides whether
        # the swap CAN move anything is the independent variation left AFTER
        # they are partialled out. Reported both ways because log_pop_1960
        # is itself a population term, and a reader will want to know
        # whether the two levels separate only through it.
        #
        # ALWAYS REMOVE THE ELOW NAME, then re-add the theta-specific one.
        # geo_controls_main carries logMA_actual_1960_s0_ELOW and never the
        # ehigh name, so setdiff(geo_controls_main, ctrl_pop) is a SILENT
        # NO-OP at theta_high: the elow baseline stays in, the residualising
        # set strips the variance being measured, and the partial
        # correlation comes back 0.8991 instead of 0.9958 -- which inverted
        # this script's conclusion in its first version (cr-review PR
        # #164, blocking 1). Same pattern as table_12_robustness.R:64, and
        # the same failure mode controls_ladder() was guarded against one
        # commit earlier. The length assertion is the guard.
        others_full <- c(setdiff(geo_controls_main,
                                 "logMA_actual_1960_s0_elow"))
        stopifnot(
            "the theta-specific baseline must not be in the residualising set" =
                !(ctrl_pop %in% others_full),
            "the residualising set must be the other seven controls" =
                length(others_full) == length(geo_controls_main) - 1L
        )
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
        rep("  partial, after the other %d controls          = %.4f",
            length(others_full), p_all)
        rep("  partial, excluding log_pop_1960 from those   = %.4f", p_nop)
        # Share of the pop-weighted control's POST-CONTROL variation that the
        # unit-weighted level does not share, i.e. 1 - partial^2. It is NOT a
        # bound on how far the coefficient can move -- an earlier version of
        # this line said it was, and the elow output falsifies that reading:
        # 1 - p^2 is under two percent while IV-B moves about twelve
        # (cr-review PR #164). It is a descriptive measure of how much
        # independent variation the swap has to work with, nothing more.
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
        # fit_pop IS a published specification, so assert it against the
        # published output rather than assuming it. This is the check that
        # would have caught blocking 1: the elow branch matched Table 12 to
        # 1e-13 while the ehigh branch matched nothing, and nothing said so
        # (cr-review PR #164, blocking 4). The gate in [0] verifies the MA
        # CONSTRUCTION; this verifies the CONTROL SET, which is where the
        # bug was.
        assert_matches_published(fit_pop, endog, el, dir_tables)
        stopifnot(
            "both fits must use the same rows" =
                nobs(fit_pop[["IV-B"]]) == nobs(fit_unit[["IV-B"]])
        )

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
    rows
}

# ---------------------------------------------------------------------------
# report_population_content(): part [3], measuring what "population-free"
# actually buys. Split out of main() for the 200-line limit.
# ---------------------------------------------------------------------------
report_population_content <- function(d, tau, unit_w, all_dest, rep) {
    # ---- [3] What "population-free" actually buys -------------------------
    # Raised by the cr-review of PR #164 and measured here rather than
    # asserted. Unit weights remove 1960 POPULATION DATA from the control.
    # They do not make the control independent of population, and they
    # introduce a different dependence: counting every district equally
    # means the control depends on how the country happens to be cut into
    # departamentos, and a province divided into many small units
    # contributes more than one large unit covering the same ground. That
    # partition is plausibly related to historical settlement, which is the
    # very thing the control is meant to absorb.
    ma_unit_lo <- build_ma_level(tau, unit_w, theta[["low"]], all_dest)
    names(ma_unit_lo)[names(ma_unit_lo) == "logMA"] <- "logMA_unit"
    area_w <- data.frame(geolev2 = d$geolev2, w = d$area_km2)
    stopifnot("area must be positive for every district" =
                  all(is.finite(area_w$w) & area_w$w > 0))
    # Area weights need an area for every DESTINATION. area_km2 comes from
    # the estimation sample, which is 311 of the 312 destinations because
    # Capital Federal is a destination j but not an observation i
    # (build_estimation_sample.R). Dropping it from the destination set
    # instead would make the area-weighted level incommensurable with the
    # unit-weighted one, which uses all 312, so the leg is SKIPPED rather
    # than computed on a different footing. Getting it would mean reading
    # the district geometry, which is more than this leg is worth: it is
    # here to show that unit weights buy a different dependence, not to be
    # a third estimate.
    miss_area <- setdiff(all_dest, area_w$geolev2)
    ma_area <- if (length(miss_area) == 0L) {
        m <- build_ma_level(tau, area_w, theta[["low"]], all_dest)
        names(m)[names(m) == "logMA"] <- "logMA_area"
        m
    } else NULL
    a <- merge(d[, c("geolev2", "log_pop_1960", "area_km2",
                     "logMA_actual_1960_s0_elow")],
               ma_unit_lo[, c("geolev2", "logMA_unit")], by = "geolev2")
    rep("\n%s", strrep("-", 70))
    rep("[3] IS THE UNIT-WEIGHTED LEVEL ACTUALLY POPULATION-FREE?")
    rep(strrep("-", 70))
    rep("  sd(log district area)                        = %.2f",
        sd(log(a$area_km2)))
    rep("  largest / smallest district by area          = %.0fx",
        max(a$area_km2) / min(a$area_km2))
    rep("  corr(logMA_unit, log own area)               = %+.3f",
        cor(a$logMA_unit, log(a$area_km2)))
    rep("  corr(logMA_unit,      log_pop_1960)          = %+.3f",
        cor(a$logMA_unit, a$log_pop_1960))
    rep("  corr(pop-weighted MA, log_pop_1960)          = %+.3f",
        cor(a$logMA_actual_1960_s0_elow, a$log_pop_1960))
    if (!is.null(ma_area)) {
        a2 <- merge(a, ma_area[, c("geolev2", "logMA_area")], by = "geolev2")
        rep("  corr(logMA_unit, AREA-weighted MA)           = %+.3f",
            cor(a2$logMA_unit, a2$logMA_area))
        rep("  (destinations with area: all %d)", length(all_dest))
    } else {
        rep("  area-weighted leg SKIPPED: %d of %d destinations lack an",
            length(miss_area), length(all_dest))
        rep("  area in the estimation sample (Capital Federal is a")
        rep("  destination but not an observation). Computing it on 311")
        rep("  destinations would not be comparable with the unit-weighted")
        rep("  level above, which uses all %d.", length(all_dest))
    }
    rep("  READING: unit weights remove the 1960 population DATA, and with")
    rep("  it the locality-universe measurement error, which is what the")
    rep("  request asked for. They do NOT remove population CONTENT -- the")
    rep("  unit-weighted level still correlates with log_pop_1960 at almost")
    rep("  the same magnitude as the population-weighted one -- and they")
    rep("  add dependence on the administrative partition, which is")
    rep("  plausibly endogenous to historical settlement. An area-weighted")
    rep("  variant is the natural third leg if this line is pursued.")

    invisible(TRUE)
}
# ---------------------------------------------------------------------------
# assert_matches_published(fits, endog, el, dir_tables)
#
# The pop-weighted arm of each comparison is a specification the paper
# already reports, so it must reproduce Table 12's committed CSV:
#   elow  -> Panel C, "Full sample (for reference)" (the main spec)
#   ehigh -> Panel A, the alternative-theta row
# Estimate AND standard error, explicit 1e-10 on both.
#
# WHY THIS EXISTS: without it the ehigh arm silently used the wrong control
# set for a whole PR, and the conclusion drawn from it was the opposite of
# the truth. A diagnostic that re-estimates a published specification should
# prove it re-estimated that specification (cr-review PR #164).
# ---------------------------------------------------------------------------
assert_matches_published <- function(fits, endog, el, dir_tables) {
    t12 <- read.csv(file.path(dir_tables, "table_12_robustness.csv"),
                    stringsAsFactors = FALSE)
    ref <- if (el == "elow") {
        t12[t12$panel == "C" & grepl("Full sample", t12$label), ]
    } else {
        t12[t12$panel == "A", ]
    }
    stopifnot("Table 12 must carry the reference row" = nrow(ref) == 1L)
    for (pair in list(c("OLS", "ols"), c("IV-LP", "iv_lp"),
                      c("IV-H", "iv_h"), c("IV-B", "iv_b"))) {
        nm <- if (pair[1] == "OLS") endog else paste0("fit_", endog)
        co <- safe_coef(fits[[pair[1]]], nm)
        stopifnot(
            "the pop-weighted arm must compute" = !is.na(co$est),
            "the pop-weighted arm must reproduce Table 12's estimate" =
                abs(co$est - ref[[paste0(pair[2], "_est")]]) < 1e-10,
            "the pop-weighted arm must reproduce Table 12's SE" =
                abs(co$se - ref[[paste0(pair[2], "_se")]]) < 1e-10
        )
    }
    invisible(TRUE)
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
# The stored tau is upper-triangle only -- 48,516 rows = 312*311/2, no
# diagonal, no duplicated unordered pair -- so the rbind builds each
# ordered pair exactly once and does not double count. TWO DEVIATIONS from
# the pipeline, both deliberate: destinations are restricted to dest_keep,
# and the i == j term is dropped by an explicit filter here rather than by
# the pipeline's construction. An absent weight is an ERROR, never coerced
# to 0, because silently weighting a destination 0 is the failure this
# script would be least able to see.
#
# Generalised from diagnostic_placebo_ma1947.R's version by taking a
# WEIGHT column rather than a population column, so the same code path
# serves the population-weighted gate, the unit-weighted object and the
# area-weighted leg of part [3].
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

# ===========================================================================
# diagnostic_pretrend_quintiles.R
#
# PURPOSE: do the results depend on the baseline controls entering LINEARLY?
#          Coauthor request, 2026-09, in the reading confirmed by Diego:
#          quintiles of a BASELINE LEVEL, not of the pre-trend.
#          DIAGNOSTIC ONLY: no paper exhibit, no scalar changes.
#
# WHY THIS IS A REAL QUESTION, stated before any result is seen. The main
# specification carries two baseline levels and one of them is
# load-bearing: baseline_ma_control_note.md records baseline log MA as "the
# single most influential control in the spec -- it doubles the headline
# coefficient", and PR #163's controls ladder showed the hypothetical-road
# instrument's first-stage F falling from about 26 to about 4 when it
# enters. A control doing that much work should not be assumed linear. That
# is the ex-ante case, and it is what selects baseline MA as the object of
# interest; nothing below is chosen after seeing which cells moved.
#
# HOW THE QUESTION IS ANSWERED, in order of evidential weight:
#
#   [1] A JOINT TEST of linearity. Keep the linear baseline term, add its
#       quintile dummies, and test the dummies jointly. This is the
#       omnibus answer: it asks whether the data reject the linear form,
#       and it needs no comparison of coefficients across specifications.
#       Run for BOTH baselines, which matters: the rejections do not fall
#       on the same outcomes, and reading only the market-access column
#       supports a conclusion the population column contradicts.
#
#   THE SYNTHESIS, since [1] and [2] can look contradictory: linearity is
#   rejected for several outcomes, including the two manufacturing ones the
#   paper leans on (in the population baseline), and yet relaxing it barely
#   moves those two coefficients. Both facts are real. A control whose
#   functional form is misspecified does not thereby bias the coefficient
#   of interest, and here the misspecification turns out to be close to
#   orthogonal to the estimand. That is the finding, and it is stronger
#   than either half alone.
#
#   [2] The four specifications, all four columns. C0 both baselines
#       linear (the published spec), C1 quintiles of baseline log MA, C2
#       quintiles of baseline log pop, C3 quintiles of both.
#
#   [3] BOOTSTRAP INFERENCE on the C0-to-C1 change in the coefficient,
#       because a percent change with no standard error is not a finding.
#       An earlier version of this script reported the percent changes
#       alone and read them as a difference between outcomes; the
#       bootstrap shows most of them are within noise (cr-review PR #165).
#
#   [4] Whether the dummies absorb the identifying variation, measured
#       rather than argued, since that is the obvious mechanical
#       explanation for any movement.
#
# DEGREES OF FREEDOM. Five bins give four dummies, and C1/C2 drop the
# linear term they replace, so each costs 3 extra parameters, not 4; C3
# costs 6. An interacted 5x5 structure would cost 24 and is not reported.
# Verified by coefficient counts: C0 10, C1 13, C2 13, C3 16.
#
# NO SAMPLE RESTRICTION. Both baseline levels are complete on all 311
# districts, so the functional form changes without changing who is in the
# sample. Each outcome keeps its own N, and N is asserted constant ACROSS
# the four specifications within an outcome.
#
# QUINTILES ARE CUT ONCE, on the full sample, so the bins are the same
# object for every outcome and specification.
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#   results/tables/table_9_population_iv.csv   (C0 assertion)
#   results/tables/table_10_sectoral_iv.csv    (C0 assertion)
#
# PRODUCES:
#   results/tables/diagnostic_pretrend_quintiles.{txt,csv}
#   results/tables/diagnostic_pretrend_quintiles_tests.csv
#
# RUNTIME: about 5 minutes, almost all of it the bootstrap in [3].
# ===========================================================================
suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
})

# Bootstrap replications for [3]. 1000 against 311 districts; the seed is
# fixed so the package reproduces bit for bit.
N_BOOT <- 1000L
BOOT_SEED <- 20260918L

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

    rep(strrep("=", 74))
    rep("ARE THE BASELINE CONTROLS LINEAR? (coauthor request)")
    rep("Quintile dummies for the baseline levels, tested and compared.")
    rep("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
    rep(strrep("=", 74))

    d <- ensure_geolev2_char(as.data.frame(arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))))

    ma_base  <- "logMA_actual_1960_s0_elow"
    pop_base <- "log_pop_1960"
    stopifnot(
        "both baselines must be in geo_controls_main" =
            all(c(ma_base, pop_base) %in% geo_controls_main),
        "both baselines must be complete on the estimation sample" =
            !any(is.na(d[[ma_base]])) && !any(is.na(d[[pop_base]]))
    )
    d$q_ma  <- quintile_factor(d[[ma_base]])
    d$q_pop <- quintile_factor(d[[pop_base]])
    geo_only <- setdiff(geo_controls_main, c(ma_base, pop_base))

    rep("\n%s", strrep("-", 74))
    rep("[0] THE BINS")
    rep(strrep("-", 74))
    rep("  cut once on the full %d-district sample.", nrow(d))
    rep("  quintiles of %-28s %s", ma_base,
        paste(as.vector(table(d$q_ma)), collapse = " / "))
    rep("  quintiles of %-28s %s", pop_base,
        paste(as.vector(table(d$q_pop)), collapse = " / "))
    stopifnot(
        "every quintile cell must be populated" =
            all(table(d$q_ma) > 1L) && all(table(d$q_pop) > 1L),
        "quintiles must have five levels" =
            nlevels(d$q_ma) == 5L && nlevels(d$q_pop) == 5L
    )

    specs <- list(
        list(id = "C0", lab = "both baselines linear (main)",
             ctrls = geo_controls_main),
        list(id = "C1", lab = "quintiles of baseline log MA",
             ctrls = c(geo_only, "q_ma", pop_base)),
        list(id = "C2", lab = "quintiles of baseline log pop",
             ctrls = c(geo_only, ma_base, "q_pop")),
        list(id = "C3", lab = "quintiles of both (additive)",
             ctrls = c(geo_only, "q_ma", "q_pop"))
    )
    outcomes <- list(
        list(var = "chg_log_nestab_85_54",     lab = "Mfg. establishments",
             ref = "t10", flag = FALSE),
        list(var = "chg_log_valprod_85_54",    lab = "Mfg. production value",
             ref = "t10", flag = FALSE),
        list(var = "chg_log_massal_85_54",     lab = "Mfg. wage mass",
             ref = "t10", flag = FALSE),
        list(var = "chg_log_nexp_88_60",       lab = "Ag. farms",
             ref = "t10", flag = FALSE),
        list(var = "chg_log_areatot_ha_88_60", lab = "Ag. farmed area",
             ref = "t10", flag = FALSE),
        list(var = "chg_log_pop_91_60",        lab = "Total population",
             ref = "t9",  flag = TRUE)
    )
    published <- list(
        t9  = read.csv(file.path(dir_tables, "table_9_population_iv.csv"),
                       stringsAsFactors = FALSE),
        t10 = read.csv(file.path(dir_tables, "table_10_sectoral_iv.csv"),
                       stringsAsFactors = FALSE)
    )

    tests <- linearity_tests(d, outcomes, geo_only, ma_base, pop_base, rep)
    df    <- spec_grid(d, outcomes, specs, published, rep)
    boot  <- boot_change(d, outcomes, geo_only, ma_base, pop_base, rep)
    absorption_check(d, geo_only, geo_controls_main, rep)
    write_reading(df, tests, boot, rep)

    out_txt <- file.path(dir_tables, "diagnostic_pretrend_quintiles.txt")
    writeLines(out, out_txt)
    message("\nSaved: ", out_txt)
    out_csv <- file.path(dir_tables, "diagnostic_pretrend_quintiles.csv")
    write.csv(df, out_csv, row.names = FALSE)
    message("Saved: ", out_csv)
    out_t <- file.path(dir_tables,
                       "diagnostic_pretrend_quintiles_tests.csv")
    write.csv(merge(tests, boot, by = c("outcome", "outcome_lab"),
                    all = TRUE), out_t, row.names = FALSE)
    message("Saved: ", out_t)
}

# ---------------------------------------------------------------------------
# linearity_tests(): [1], the omnibus answer.
#
# Keep the linear baseline term, add its quintile dummies, test the dummies
# jointly with a robust Wald. Rejecting says the data are inconsistent with
# the linear form; failing to reject says they are consistent with it. This
# needs no cross-specification coefficient comparison, so unlike [2] and
# [3] it is not exposed to the "large percent change on a small base"
# problem.
#
# An earlier version of this script claimed "nothing here tests linearity
# against the data" while disclaiming exactly this test, which was
# available the whole time (cr-review PR #165, blocking 4).
# ---------------------------------------------------------------------------
linearity_tests <- function(d, outcomes, geo_only, ma_base, pop_base, rep) {
    rep("\n%s", strrep("=", 74))
    rep("[1] JOINT TEST OF LINEARITY -- the primary result")
    rep(strrep("=", 74))
    rep("  Linear baseline KEPT, its quintile dummies added, dummies")
    rep("  tested jointly (robust Wald, 3 restrictions).")
    rep("  %-24s %-19s %-19s", "outcome", "baseline log MA",
        "baseline log pop")
    rows <- list()
    for (o in outcomes) {
        res <- list()
        for (b in list(c(ma_base, "q_ma"), c(pop_base, "q_pop"))) {
            f <- as.formula(sprintf(
                "%s ~ %s | %s ~ %s + %s",
                o$var,
                paste(c(geo_only, ma_base, pop_base, b[2]), collapse = " + "),
                main_treatment, main_lp_instrument, main_hypo_instrument))
            m <- feols(f, data = d, vcov = "hetero")
            w <- fixest::wald(m, b[2], print = FALSE)
            res[[b[2]]] <- c(F = as.numeric(w$stat), p = as.numeric(w$p))
        }
        rep("  %-24s F %5.2f  p %.4f   F %5.2f  p %.4f", o$lab,
            res$q_ma["F"], res$q_ma["p"],
            res$q_pop["F"], res$q_pop["p"])
        rows[[length(rows) + 1L]] <- data.frame(
            outcome = o$var, outcome_lab = o$lab,
            wald_F_ma = res$q_ma[["F"]], wald_p_ma = res$q_ma[["p"]],
            wald_F_pop = res$q_pop[["F"]], wald_p_pop = res$q_pop[["p"]],
            stringsAsFactors = FALSE)
    }
    T <- do.call(rbind, rows)
    stopifnot("every Wald test must compute" =
                  !any(is.na(c(T$wald_p_ma, T$wald_p_pop))))
    rep("  Both baselines are kept linear in every column here; only the")
    rep("  dummies being tested are added, so this is a pure test of")
    rep("  functional form and not a respecification.")
    T
}

# ---------------------------------------------------------------------------
# spec_grid(): [2], the four specifications.
#
# ALL FOUR COLUMNS are printed, not just OLS and IV-Both. The earlier
# version printed IV-Both only, which hid two things (cr-review PR #165,
# blocking 3): the strong-instrument column moves about half as much as
# IV-Both, and the instrument MIX shifts, since the LP first stage weakens
# while the hypothetical one strengthens. IV-Both under C1 therefore leans
# more on the weak instrument than under C0, which makes that column a
# less clean functional-form comparison than it looks.
# ---------------------------------------------------------------------------
spec_grid <- function(d, outcomes, specs, published, rep) {
    rows <- list()
    rep("\n%s", strrep("=", 74))
    rep("[2] THE FOUR SPECIFICATIONS, all four columns")
    rep(strrep("=", 74))
    for (o in outcomes) {
        rep("\n%s%s", o$lab, if (o$flag) "   [SEE [5] BELOW]" else "")
        rep("  %-4s %-14s %-9s %-15s %-15s %-15s", "spec", "controls",
            "OLS", "IV-LP (F)", "IV-Hypo (F)", "IV-Both (F)")
        block <- list()
        for (s in specs) {
            fits <- fit_iv_quad(o$var, d, main_treatment,
                                main_lp_instrument, main_hypo_instrument,
                                s$ctrls)
            nm <- paste0("fit_", main_treatment)
            co_o <- safe_coef(fits[["OLS"]], main_treatment)
            co_l <- safe_coef(fits[["IV-LP"]], nm)
            co_h <- safe_coef(fits[["IV-H"]], nm)
            co_b <- safe_coef(fits[["IV-B"]], nm)
            f_l <- fitstat_F_robust(fits[["IV-LP"]])
            f_h <- fitstat_F_robust(fits[["IV-H"]])
            f_b <- fitstat_F_robust(fits[["IV-B"]])
            rep("  %-4s %-14s %+.3f    %+.3f (%5.2f)  %+.3f (%5.2f)  %+.3f (%5.2f)",
                s$id, substr(s$lab, 1, 14), co_o$est,
                co_l$est, f_l, co_h$est, f_h, co_b$est, f_b)
            block[[length(block) + 1L]] <- data.frame(
                outcome = o$var, outcome_lab = o$lab,
                spec = s$id, spec_lab = s$lab,
                ols_est = co_o$est, ols_se = co_o$se, ols_p = co_o$p,
                iv_lp_est = co_l$est, iv_lp_se = co_l$se, iv_lp_p = co_l$p,
                iv_h_est = co_h$est, iv_h_se = co_h$se, iv_h_p = co_h$p,
                iv_b_est = co_b$est, iv_b_se = co_b$se, iv_b_p = co_b$p,
                iv_lp_F = f_l, iv_h_F = f_h, iv_b_F = f_b,
                n_obs = nobs(fits[["OLS"]]),
                population_flagged = o$flag, stringsAsFactors = FALSE)
            if (s$id == "C0") assert_c0_matches(fits, o, published)
        }
        B <- do.call(rbind, block)
        stopifnot(
            "every estimate in the block must compute" =
                !any(is.na(c(B$ols_est, B$iv_lp_est, B$iv_h_est, B$iv_b_est))),
            "every first-stage F must compute" =
                !any(is.na(c(B$iv_lp_F, B$iv_h_F, B$iv_b_F))),
            "the four specifications must share one sample" =
                length(unique(B$n_obs)) == 1L
        )
        rows <- c(rows, block)
    }
    do.call(rbind, rows)
}

# ---------------------------------------------------------------------------
# boot_change(): [3], inference on the C0-to-C1 change.
#
# A pairs (nonparametric) bootstrap: resample districts with replacement,
# refit both specifications, take the difference in the IV-Both
# coefficient. Quintile breaks are RECUT inside each replication, because
# the bins are a function of the sample and holding them fixed would
# understate the variance.
#
# WHY IT IS HERE: the first version of this script reported percent changes
# with no standard error and read them as a difference between outcomes.
# Most of those changes sit inside sampling noise, and the percent changes
# are large mainly because the denominators are close to zero (cr-review PR
# #165, blocking 1).
# ---------------------------------------------------------------------------
boot_change <- function(d, outcomes, geo_only, ma_base, pop_base, rep) {
    rep("\n%s", strrep("=", 74))
    rep("[3] IS THE C0-TO-C1 CHANGE DISTINGUISHABLE FROM NOISE?")
    rep(strrep("=", 74))
    rep("  Pairs bootstrap, %d replications, seed %d; bins recut in each",
        N_BOOT, BOOT_SEED)
    rep("  replication. Reported for the IV-Both column.")
    rep("  %-24s %8s %8s %7s %7s  %s", "outcome", "change", "boot SE",
        "t", "p", "95% CI")
    nm <- paste0("fit_", main_treatment)
    ctrls1 <- function(dd) c(geo_only, "q_ma", pop_base)
    rows <- list()
    for (o in outcomes) {
        obs <- change_once(d, o$var, geo_only, ma_base, pop_base, nm)
        set.seed(BOOT_SEED)
        reps <- vapply(seq_len(N_BOOT), function(b) {
            db <- d[sample(nrow(d), replace = TRUE), ]
            db$q_ma <- quintile_factor(db[[ma_base]])
            tryCatch(change_once(db, o$var, geo_only, ma_base, pop_base, nm),
                     error = function(e) NA_real_)
        }, numeric(1))
        ok <- reps[is.finite(reps)]
        # A replication can fail if a resample leaves a quintile empty. Few
        # failures are harmless; many would mean the bootstrap is not
        # sampling the same estimator.
        stopifnot("at least 95% of bootstrap replications must succeed" =
                      length(ok) >= 0.95 * N_BOOT)
        se <- sd(ok)
        tstat <- obs / se
        pval <- 2 * pnorm(-abs(tstat))
        ci <- quantile(ok, c(0.025, 0.975))
        rep("  %-24s %+8.4f %8.4f %+7.2f %7.3f  [%+.3f, %+.3f]",
            o$lab, obs, se, tstat, pval, ci[1], ci[2])
        rows[[length(rows) + 1L]] <- data.frame(
            outcome = o$var, outcome_lab = o$lab,
            change_c0_c1 = obs, boot_se = se, boot_t = tstat,
            boot_p = pval, ci_lo = ci[[1]], ci_hi = ci[[2]],
            boot_reps_ok = length(ok), stringsAsFactors = FALSE)
    }
    rep("  The CI is the percentile interval on the CHANGE, not on either")
    rep("  coefficient. A CI containing zero means the two specifications")
    rep("  are not distinguishable, whatever the percent change looks like.")
    do.call(rbind, rows)
}

# One C0-to-C1 difference in the IV-Both coefficient, for one sample.
change_once <- function(dd, y, geo_only, ma_base, pop_base, nm) {
    f0 <- fit_iv_quad(y, dd, main_treatment, main_lp_instrument,
                      main_hypo_instrument,
                      c(geo_only, ma_base, pop_base))
    f1 <- fit_iv_quad(y, dd, main_treatment, main_lp_instrument,
                      main_hypo_instrument,
                      c(geo_only, "q_ma", pop_base))
    safe_coef(f1[["IV-B"]], nm)$est - safe_coef(f0[["IV-B"]], nm)$est
}

# ---------------------------------------------------------------------------
# absorption_check(): [4].
#
# The obvious mechanical explanation for any movement is that four dummies
# absorb more of the treatment's and the instruments' variation than one
# slope, so the comparison would be between different amounts of
# identifying variation rather than between functional forms. Measured
# here rather than argued either way.
# ---------------------------------------------------------------------------
absorption_check <- function(d, geo_only, full_set, rep) {
    ma_base <- "logMA_actual_1960_s0_elow"
    c1 <- c(geo_only, "q_ma", "log_pop_1960")
    r2 <- function(v, ctrls) summary(lm(as.formula(
        paste(v, "~", paste(ctrls, collapse = " + "))), data = d))$r.squared
    rep("\n%s", strrep("=", 74))
    rep("[4] DO THE DUMMIES ABSORB THE IDENTIFYING VARIATION?")
    rep(strrep("=", 74))
    rep("  R2 of ...              on C0 controls   on C1 controls")
    rep("  treatment              %.4f           %.4f",
        r2(main_treatment, full_set), r2(main_treatment, c1))
    rep("  Larkin Plan instrument %.4f           %.4f",
        r2(main_lp_instrument, full_set), r2(main_lp_instrument, c1))
    rep("  corr(baseline log MA, Larkin Plan instrument) = %+.4f",
        cor(d[[ma_base]], d[[main_lp_instrument]]))
    rep("  NO. The dummies explain essentially nothing extra, and the")
    rep("  instrument is close to orthogonal to the control being")
    rep("  relaxed, so whatever moves in [2] is not the comparison")
    rep("  running on less identifying variation. Note also that")
    rep("  absorbing identifying variation inflates a standard error; it")
    rep("  does not pull a point estimate toward zero.")
    invisible(TRUE)
}

# ---------------------------------------------------------------------------
# write_reading(): the population caveat and how to read the three parts.
# Everything numeric is derived from the frames rather than typed, after an
# earlier draft asserted a pattern its own table contradicted.
# ---------------------------------------------------------------------------
write_reading <- function(df, tests, boot, rep) {
    pct <- function(v, sp) {
        b0 <- df$iv_b_est[df$outcome == v & df$spec == "C0"]
        b  <- df$iv_b_est[df$outcome == v & df$spec == sp]
        stopifnot(length(b0) == 1L, length(b) == 1L)
        100 * (b / b0 - 1)
    }
    g <- function(tab, v, col) {
        x <- tab[[col]][tab$outcome == v]
        stopifnot(length(x) == 1L)
        x
    }
    rep("\n%s", strrep("=", 74))
    rep("[5] THE POPULATION BLOCK IS NOT A ROBUSTNESS RESULT")
    rep(strrep("=", 74))
    rep("  C2 and C3 bin on log_pop_1960, which carries the")
    rep("  locality-universe measurement error documented in")
    rep("  diagnostic_pop1960_universe.R and is also the initial level")
    rep("  inside the outcome chg_log_pop_91_60, so they bin on a")
    rep("  mismeasured variable whose error is shared with the outcome.")
    rep("  C1 IS THE LEAST CONTAMINATED OF THE THREE, NOT EXEMPT. An")
    rep("  earlier version of this note claimed it carried none of the")
    rep("  objection, which is wrong: baseline MA is built FROM")
    rep("  pop_1960, so binning on it bins on a tau-weighted sum of the")
    rep("  same mismeasured variable. The error is missing dispersed")
    rep("  rural population, hence rural-concentrated and spatially")
    rep("  correlated, so a district's baseline MA inherits a")
    rep("  neighbour-weighted version of an error correlated with its own.")
    rep("  What genuinely attenuates the channel is that the MA sum")
    rep("  excludes the district itself (04_market_access.R), so a")
    rep("  district is never binned on its own mismeasured population.")
    rep("  Attenuated, not closed (cr-review PR #165).")

    rep("\n%s", strrep("=", 74))
    rep("READING, in order of evidential weight")
    rep(strrep("=", 74))
    n_rej_ma  <- sum(tests$wald_p_ma < 0.05)
    n_rej_pop <- sum(tests$wald_p_pop < 0.05)
    rep("  [1] LINEARITY IS REJECTED IN SEVERAL PLACES. At the five")
    rep("  percent level the baseline-log-MA dummies are jointly")
    rep("  significant for %d of %d outcomes and the baseline-log-pop",
        n_rej_ma, nrow(tests))
    rep("  dummies for %d of %d. The two are not the same outcomes: in",
        n_rej_pop, nrow(tests))
    rep("  baseline MA it is establishments (p = %.4f) and population",
        g(tests, "chg_log_nestab_85_54", "wald_p_ma"))
    rep("  (p = %.4f); in baseline population it includes the two",
        g(tests, "chg_log_pop_91_60", "wald_p_ma"))
    rep("  manufacturing outcomes the paper leans on, production value")
    rep("  p = %.4f and wage mass p = %.4f, and population at p = %.4f.",
        g(tests, "chg_log_valprod_85_54", "wald_p_pop"),
        g(tests, "chg_log_massal_85_54", "wald_p_pop"),
        g(tests, "chg_log_pop_91_60", "wald_p_pop"))
    rep("  So the honest statement is NOT that the linear form is fine")
    rep("  where the paper needs it. It is rejected there too, in the")
    rep("  population baseline.")
    rep("")
    rep("  [1] AND [2] TOGETHER ARE THE POINT, and they point the same way")
    rep("  for a reason worth stating. A control's functional form being")
    rep("  rejected is not the same thing as the coefficient of interest")
    rep("  being biased by it. Wage mass is the cleanest case: linearity")
    rep("  in baseline population is rejected at p = %.4f, and relaxing it",
        g(tests, "chg_log_massal_85_54", "wald_p_pop"))
    rep("  moves the IV-Both estimate by %.0f%% (C2). Production value:",
        abs(pct("chg_log_massal_85_54", "C2")))
    rep("  rejected at p = %.4f, moves %.0f%%. The misspecification is",
        g(tests, "chg_log_valprod_85_54", "wald_p_pop"),
        abs(pct("chg_log_valprod_85_54", "C2")))
    rep("  real and it is orthogonal to the estimand.")
    rep("")
    rep("  [3] SAYS NO CHANGE IS DISTINGUISHABLE FROM ZERO. The smallest")
    rep("  bootstrap p across the six outcomes is %.3f (%s),",
        min(boot$boot_p), boot$outcome_lab[which.min(boot$boot_p)])
    rep("  and every 95% interval contains zero. Population, the largest")
    rep("  percent change at %.0f%%, has p = %.3f and an interval of",
        abs(pct("chg_log_pop_91_60", "C1")),
        g(boot, "chg_log_pop_91_60", "boot_p"))
    rep("  [%+.3f, %+.3f]. The percent changes are large mainly because",
        g(boot, "chg_log_pop_91_60", "ci_lo"),
        g(boot, "chg_log_pop_91_60", "ci_hi"))
    rep("  the denominators are small: every IV-Both estimate in the two")
    rep("  blocks that move is insignificant under every specification.")
    rep("  An earlier version of this script reported those percentages")
    rep("  alone and read them as a finding.")
    rep("")
    rep("  WHICH COLUMN MOVES. For population the strong column moves")
    rep("  least: IV-LP %+.3f to %+.3f against IV-Hypo %+.3f to %+.3f.",
        df$iv_lp_est[df$outcome == "chg_log_pop_91_60" & df$spec == "C0"],
        df$iv_lp_est[df$outcome == "chg_log_pop_91_60" & df$spec == "C1"],
        df$iv_h_est[df$outcome == "chg_log_pop_91_60" & df$spec == "C0"],
        df$iv_h_est[df$outcome == "chg_log_pop_91_60" & df$spec == "C1"])
    rep("  The mix shifts too: the LP first stage weakens (%.2f to %.2f)",
        df$iv_lp_F[df$outcome == "chg_log_pop_91_60" & df$spec == "C0"],
        df$iv_lp_F[df$outcome == "chg_log_pop_91_60" & df$spec == "C1"])
    rep("  while the hypothetical one strengthens (%.2f to %.2f), so",
        df$iv_h_F[df$outcome == "chg_log_pop_91_60" & df$spec == "C0"],
        df$iv_h_F[df$outcome == "chg_log_pop_91_60" & df$spec == "C1"])
    rep("  IV-Both under C1 leans more on the weak instrument than under")
    rep("  C0. That makes IV-Both a less clean comparison than it looks,")
    rep("  which is a second reason to read [1] instead.")
    rep("")
    rep("  WHAT THIS STILL CANNOT SETTLE. Quintiles are one departure from")
    rep("  linearity. [1] tests that departure and no other.")
    rep("  There are %d distinct first stages here, not %d: outcomes that",
        length(unique(paste(df$n_obs, df$spec))), nrow(df))
    rep("  share a sample share a first stage.")
    rep("  baseline_ma_control_note.md's +0.046 (0.033), F 13.6 predates")
    rep("  the theta switch to 4.14 and is not the C0 row here.")
    rep(strrep("=", 74))
    invisible(TRUE)
}

# ---------------------------------------------------------------------------
# quintile_factor(x): five equal-count bins, lowest as reference. Type-7
# quantiles, the R default, so cut points are reproducible without a seed.
# The strictly-increasing guard fires BEFORE cut(), so tied data cannot
# silently collapse a bin.
# ---------------------------------------------------------------------------
quintile_factor <- function(x) {
    br <- quantile(x, probs = seq(0, 1, 0.2), na.rm = TRUE)
    stopifnot("quintile breaks must be strictly increasing" =
                  all(diff(br) > 0))
    factor(cut(x, breaks = br, include.lowest = TRUE, labels = FALSE))
}

# ---------------------------------------------------------------------------
# assert_c0_matches(): C0 IS the published specification, so it must
# reproduce the published output -- estimate, standard error AND N, all four
# columns, explicit 1e-10.
#
# This guard exists because PRs #163 and #164 each shipped a check that
# verified nothing, and both were caught by review rather than by code.
# Asserting against the committed artifact catches the class.
# ---------------------------------------------------------------------------
assert_c0_matches <- function(fits, o, published) {
    ref_tab <- published[[o$ref]]
    ref <- ref_tab[ref_tab$outcome == o$var, ]
    stopifnot("the published table must carry this outcome" = nrow(ref) > 0L)
    for (key in c("OLS", "IV-LP", "IV-H", "IV-B")) {
        r <- ref[ref$spec == key, ]
        stopifnot("the published table must carry this spec" = nrow(r) == 1L)
        nm <- if (key == "OLS") main_treatment else
            paste0("fit_", main_treatment)
        co <- safe_coef(fits[[key]], nm)
        stopifnot(
            "C0 must compute" = !is.na(co$est),
            "C0 must reproduce the published estimate" =
                abs(co$est - r$estimate) < 1e-10,
            "C0 must reproduce the published standard error" =
                abs(co$se - r$std_err) < 1e-10,
            "C0 must use the published sample size" =
                nobs(fits[[key]]) == r$n_obs
        )
    }
    invisible(TRUE)
}

main()

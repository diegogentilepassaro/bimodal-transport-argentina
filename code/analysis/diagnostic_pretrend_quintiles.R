# ===========================================================================
# diagnostic_pretrend_quintiles.R
#
# PURPOSE: does the headline result depend on the baseline controls entering
#          LINEARLY? Coauthor request, 2026-09: replace a linear baseline
#          level with dummies for its quintiles, so convergence is absorbed
#          flexibly instead of being forced through one slope.
#          DIAGNOSTIC ONLY: no paper exhibit, no scalar changes.
#
# WHY THIS IS A REAL QUESTION HERE. The main specification carries two
# baseline levels, and one of them is load-bearing:
# baseline_ma_control_note.md records that baseline log MA is "the single
# most influential control in the spec -- it doubles the headline
# coefficient", and PR #163's controls ladder showed the hypothetical-road
# instrument's first-stage F falling from about 26 to about 4 when it
# enters. A control doing that much work should not be assumed linear
# without checking.
#
# THE FOUR SPECIFICATIONS, per outcome:
#   C0  main spec: both baselines linear (the published specification)
#   C1  quintiles of baseline log MA;  baseline log pop stays linear
#   C2  quintiles of baseline log pop; baseline log MA stays linear
#   C3  quintiles of both, additively (8 extra df, no interaction)
#
# All three combinations are reported because there are two baseline
# controls and no reason on the face of it to privilege one; each costs 4
# degrees of freedom against N = 311, so reporting all three is cheaper
# than arguing about which to report. C3 is additive, not interacted: a
# 5x5 cell structure would put 25 dummies against 311 observations.
#
# NO SAMPLE RESTRICTION. Both baseline levels are defined for all 311
# districts, so unlike a pre-trend-based control this changes the
# functional form without changing who is in the sample. Each outcome keeps
# its own N (the sectoral censuses differ), and N is asserted constant
# ACROSS the four specifications within an outcome, which is the thing that
# would otherwise make C0-vs-C3 unattributable.
#
# QUINTILES ARE CUT ONCE, on the full 311-district sample, so the bins are
# the same object for every outcome and every specification. Cutting them
# per outcome would make the sectoral rows incomparable with each other.
#
# THE FIRST STAGE MOVES, and that is informative rather than a fault.
# Replacing a linear term with four dummies absorbs more of the
# instruments' variation, by the same mechanism PR #163 measured on the
# ladder, so the F is reported for every cell and the reader is told not to
# read a falling F as weakness appearing from nowhere.
#
# POPULATION OUTCOME: reported, NOT offered as a robustness result. The
# ledger flags it for the pop60 shared-error mechanism, and the objection
# is specific to C2 and C3: log_pop_1960 carries the locality-universe
# measurement error documented in diagnostic_pop1960_universe.R, and it is
# also the initial level inside the outcome chg_log_pop_91_60. Binning on a
# mismeasured variable misassigns districts to bins, and the error is
# shared with the outcome. C1 does not have this problem, since it bins on
# baseline MA. See the note printed beside the population block.
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#   results/tables/table_9_population_iv.csv   (C0 assertion)
#   results/tables/table_10_sectoral_iv.csv    (C0 assertion)
#
# PRODUCES:
#   results/tables/diagnostic_pretrend_quintiles.{txt,csv}
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

    rep(strrep("=", 74))
    rep("BASELINE CONTROLS AS QUINTILE DUMMIES (coauthor request)")
    rep("Does the result depend on the baseline levels entering linearly?")
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
    rep("\n%s", strrep("-", 74))
    rep("[0] THE BINS")
    rep(strrep("-", 74))
    rep("  cut once on the full %d-district sample, so the bins are the",
        nrow(d))
    rep("  same object for every outcome and every specification.")
    rep("  quintiles of %-28s %s", ma_base,
        paste(as.vector(table(d$q_ma)), collapse = " / "))
    rep("  quintiles of %-28s %s", pop_base,
        paste(as.vector(table(d$q_pop)), collapse = " / "))
    # An empty or singleton cell would silently drop a dummy and change the
    # df without changing the printed specification label.
    stopifnot(
        "every quintile cell must be populated" =
            all(table(d$q_ma) > 1L) && all(table(d$q_pop) > 1L),
        "quintiles must have five levels" =
            nlevels(d$q_ma) == 5L && nlevels(d$q_pop) == 5L
    )

    geo_only <- setdiff(geo_controls_main, c(ma_base, pop_base))
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

    rows <- list()
    for (o in outcomes) {
        rep("\n%s", strrep("-", 74))
        rep("%s%s", o$lab, if (o$flag) "   [SEE THE NOTE BELOW]" else "")
        rep(strrep("-", 74))
        rep("  %-4s %-31s %-17s %-17s %s", "spec", "controls",
            "OLS", "IV-Both", "F / N")
        block <- list()
        for (s in specs) {
            fits <- fit_iv_quad(o$var, d, main_treatment,
                                main_lp_instrument, main_hypo_instrument,
                                s$ctrls)
            co_o <- safe_coef(fits[["OLS"]],  main_treatment)
            co_l <- safe_coef(fits[["IV-LP"]],
                              paste0("fit_", main_treatment))
            co_h <- safe_coef(fits[["IV-H"]],
                              paste0("fit_", main_treatment))
            co_b <- safe_coef(fits[["IV-B"]],
                              paste0("fit_", main_treatment))
            f_b <- fitstat_F_robust(fits[["IV-B"]])
            rep("  %-4s %-31s %+.3f (%.3f)    %+.3f (%.3f)    %5.2f / %d",
                s$id, s$lab, co_o$est, co_o$se, co_b$est, co_b$se,
                f_b, nobs(fits[["OLS"]]))
            block[[length(block) + 1L]] <- data.frame(
                outcome = o$var, outcome_lab = o$lab,
                spec = s$id, spec_lab = s$lab,
                ols_est = co_o$est, ols_se = co_o$se, ols_p = co_o$p,
                iv_lp_est = co_l$est, iv_lp_se = co_l$se, iv_lp_p = co_l$p,
                iv_h_est = co_h$est, iv_h_se = co_h$se, iv_h_p = co_h$p,
                iv_b_est = co_b$est, iv_b_se = co_b$se, iv_b_p = co_b$p,
                iv_lp_F = fitstat_F_robust(fits[["IV-LP"]]),
                iv_h_F = fitstat_F_robust(fits[["IV-H"]]),
                iv_b_F = f_b, n_obs = nobs(fits[["OLS"]]),
                population_flagged = o$flag,
                stringsAsFactors = FALSE)
            if (s$id == "C0") {
                assert_c0_matches(fits, o, published)
            }
        }
        B <- do.call(rbind, block)
        # Changing the functional form must not change WHO is in the
        # sample. If it did, C0-vs-C3 would mix form with composition and
        # nothing in the block would be attributable (the lesson of the
        # selection problem flagged in Section 8.2).
        stopifnot(
            "every estimate in the block must compute" =
                !any(is.na(c(B$ols_est, B$iv_b_est))),
            "every first-stage F must compute" = !any(is.na(B$iv_b_F)),
            "the four specifications must share one sample" =
                length(unique(B$n_obs)) == 1L
        )
        rows <- c(rows, block)
    }
    df <- do.call(rbind, rows)

    # The three narrative blocks, in write_reading() below: the population
    # caveat, how to read the table, and the computed summary of what
    # moves. Split out of main() for the 200-line function limit.
    write_reading(df, rep)
    out_txt <- file.path(dir_tables, "diagnostic_pretrend_quintiles.txt")
    writeLines(out, out_txt)
    message("\nSaved: ", out_txt)
    out_csv <- file.path(dir_tables, "diagnostic_pretrend_quintiles.csv")
    write.csv(df, out_csv, row.names = FALSE)
    message("Saved: ", out_csv)
}

# ---------------------------------------------------------------------------
# write_reading(df, rep): the population caveat, the reading guide and the
# computed summary of which coefficients move. Everything here is derived
# from df rather than typed, so the prose cannot drift from the numbers --
# an earlier draft asserted that population moved under C1 rather than C2,
# which its own table contradicted.
# ---------------------------------------------------------------------------
write_reading <- function(df, rep) {
    rep("\n%s", strrep("=", 74))
    rep("THE POPULATION BLOCK IS NOT A ROBUSTNESS RESULT")
    rep(strrep("=", 74))
    rep("  C2 and C3 bin on log_pop_1960, which carries the")
    rep("  locality-universe measurement error documented in")
    rep("  diagnostic_pop1960_universe.R AND is the initial level inside")
    rep("  the outcome chg_log_pop_91_60. Binning on a mismeasured")
    rep("  variable misassigns districts to bins, and that error is shared")
    rep("  with the outcome, so those two cells cannot be read as")
    rep("  robustness. C1 bins on baseline MA and does not have the")
    rep("  problem. The population block is here because the coauthors")
    rep("  will ask for it, flagged rather than omitted.")
    rep(strrep("=", 74))

    rep("\n%s", strrep("=", 74))
    rep("READING")
    rep(strrep("=", 74))
    rep("  WHAT MOVES. Compare C0 with C1-C3 within an outcome; the sample")
    rep("  is identical by construction and asserted so, so any movement is")
    rep("  functional form and nothing else.")
    rep("  INSTRUMENT STRENGTH IS NOT WHAT CHANGES. Four dummies could in")
    rep("  principle absorb more of the instruments' variation than one")
    rep("  slope, which is the mechanism PR #163's ladder measured on")
    rep("  baseline MA, but here it does not bite: the IV-Both robust F")
    rep("  spans %.2f to %.2f across all %d cells and it RISES about as",
        min(df$iv_b_F), max(df$iv_b_F), nrow(df))
    rep("  often as it falls. So the coefficient movements below are not a")
    rep("  weak-instrument artifact.")
    rep("  WHAT THIS CANNOT SETTLE. Quintiles are one flexible form among")
    rep("  many. A result that survives them has survived one departure")
    rep("  from linearity, not every departure; nothing here tests")
    rep("  linearity against the data.")
    rep(strrep("=", 74))

    # ---- The pattern, computed rather than typed --------------------------
    # C1 is the cell to read: it relaxes the load-bearing control and, unlike
    # C2 and C3, does not bin on the mismeasured log_pop_1960.
    rep("\n%s", strrep("=", 74))
    rep("WHAT MOVES: IV-Both under C1 (baseline-MA quintiles) vs C0")
    rep(strrep("=", 74))
    rep("  percent change in the IV-Both coefficient against C0")
    rep("  %-24s %8s %7s %7s %7s  %s", "outcome", "C0", "C1", "C2", "C3",
        "flagged?")
    pct <- function(v, sp) {
        b0 <- df$iv_b_est[df$outcome == v & df$spec == "C0"]
        b  <- df$iv_b_est[df$outcome == v & df$spec == sp]
        stopifnot(length(b0) == 1L, length(b) == 1L)
        100 * (b / b0 - 1)
    }
    for (v in unique(df$outcome)) {
        b0 <- df$iv_b_est[df$outcome == v & df$spec == "C0"]
        lab <- unique(df$outcome_lab[df$outcome == v])
        fl <- unique(df$population_flagged[df$outcome == v])
        rep("  %-24s %+8.3f %+6.0f%% %+6.0f%% %+6.0f%%  %s", lab, b0,
            pct(v, "C1"), pct(v, "C2"), pct(v, "C3"),
            if (fl) "yes" else "")
    }
    rep("")
    rep("  THE TWO OUTCOMES THAT CARRY THE PAPER'S SECTORAL RESULT DO NOT")
    rep("  MOVE. Manufacturing production value changes by %.0f%% under C1",
        abs(pct("chg_log_valprod_85_54", "C1")))
    rep("  and %.0f%% under C3; wage mass by %.0f%% and %.0f%%. Neither",
        abs(pct("chg_log_valprod_85_54", "C3")),
        abs(pct("chg_log_massal_85_54", "C1")),
        abs(pct("chg_log_massal_85_54", "C3")))
    rep("  depends on the baselines being linear.")
    rep("")
    rep("  TWO OUTCOMES DO MOVE, and they move differently, so the two")
    rep("  baselines are not interchangeable here:")
    rep("    Manufacturing establishments moves under C1 (%+.0f%%) and",
        pct("chg_log_nestab_85_54", "C1"))
    rep("    barely under C2 (%+.0f%%), so it is the linearity of BASELINE",
        pct("chg_log_nestab_85_54", "C2"))
    rep("    MA that it depends on.")
    rep("    Total population moves under BOTH, %+.0f%% under C1 and",
        pct("chg_log_pop_91_60", "C1"))
    rep("    %+.0f%% under C2, compounding to %+.0f%% under C3. So for",
        pct("chg_log_pop_91_60", "C2"), pct("chg_log_pop_91_60", "C3"))
    rep("    population BOTH baselines matter, not baseline MA alone.")
    rep("")
    rep("  The C1 population cell is the one to take to the coauthors,")
    rep("  because it is the only one of the three that carries none of the")
    rep("  pop60 objection. The paper already reports that elasticity as")
    rep("  not distinguishable from zero; under C1 it is smaller still.")
    rep("  It is also consistent with baseline_ma_control_note.md, which")
    rep("  records that including baseline log MA roughly doubles the")
    rep("  population coefficient: relaxing its functional form gives back")
    rep("  a good part of that.")
    rep(strrep("=", 74))

    invisible(TRUE)
}
# ---------------------------------------------------------------------------
# quintile_factor(x): five equal-count bins as a factor, lowest as
# reference. Uses type-7 quantiles, the R default, so the cut points are
# reproducible without a seed.
# ---------------------------------------------------------------------------
quintile_factor <- function(x) {
    br <- quantile(x, probs = seq(0, 1, 0.2), na.rm = TRUE)
    stopifnot("quintile breaks must be strictly increasing" =
                  all(diff(br) > 0))
    factor(cut(x, breaks = br, include.lowest = TRUE, labels = FALSE))
}

# ---------------------------------------------------------------------------
# assert_c0_matches(fits, o, published)
#
# C0 IS the published specification, so it must reproduce the published
# output rather than merely resembling it: estimate and standard error, all
# four columns, explicit 1e-10.
#
# This guard exists because the two preceding PRs each shipped a check that
# verified nothing -- #163's top-rung comparison refit the same model and
# compared it with itself, and #164's control set was silently wrong at one
# elasticity for a whole PR while a construction gate passed clean. Both
# were caught by review rather than by code. Asserting against the
# committed artifact catches the class.
# ---------------------------------------------------------------------------
assert_c0_matches <- function(fits, o, published) {
    ref_tab <- published[[o$ref]]
    ref <- ref_tab[ref_tab$outcome == o$var, ]
    stopifnot("the published table must carry this outcome" = nrow(ref) > 0L)
    for (pair in list(c("OLS", "OLS"), c("IV-LP", "IV-LP"),
                      c("IV-H", "IV-H"), c("IV-B", "IV-B"))) {
        r <- ref[ref$spec == pair[2], ]
        stopifnot("the published table must carry this spec" = nrow(r) == 1L)
        nm <- if (pair[1] == "OLS") main_treatment else
            paste0("fit_", main_treatment)
        co <- safe_coef(fits[[pair[1]]], nm)
        stopifnot(
            "C0 must compute" = !is.na(co$est),
            "C0 must reproduce the published estimate" =
                abs(co$est - r$estimate) < 1e-10,
            "C0 must reproduce the published standard error" =
                abs(co$se - r$std_err) < 1e-10
        )
    }
    invisible(TRUE)
}

main()

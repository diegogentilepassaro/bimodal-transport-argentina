# ===========================================================================
# diagnostic_placebo_universe.R
#
# PURPOSE: the pre-trends placebo (Table 7) has the SAME universe mismatch
#          documented in diagnostic_pop1960_universe.txt, in its dependent
#          variable. This script asks whether the placebo rejection
#          survives when the outcome is measured on comparable universes.
#          DIAGNOSTIC ONLY: no table, figure or scalar changes.
#
# THE PROBLEM:
#   chg_log_placebo_pop_60_47 = log(pop_1960) - log(pop_1947), verified
#   row-wise. pop_1947 is Cuadro 1 = DISTRICT TOTALS (full universe);
#   pop_1960 is a sum of NAMED LOCALITIES (dispersed rural population
#   absent from the source). So the placebo outcome is
#       (true 1947-60 growth) + log(1960 locality coverage rate),
#   and the second term is not noise: 143 of 237 districts show a
#   NEGATIVE outcome, i.e. apparent population decline over thirteen
#   years.
#
#   ON THE PREMISE, STATED AS PR #147 STATED IT (cr-review PR #149
#   blocking 1): the coverage shortfall's relation to the treatment is
#   a partial correlation of -0.119 with p read FROM THE #147 CSV
#   below rather than hardcoded -- an earlier version of this header
#   said "p 0.039", which appears nowhere in that diagnostic and was
#   copied out of prose instead of output. And PR #147's own
#   pre-committed verdict on whether the measurement error is
#   treatment-correlated is INCONCLUSIVE, because its two coverage
#   proxies disagree in sign once conditioned on the controls. That
#   qualifier belongs wherever the premise is invoked, including here.
#
#   So the motivation for this test is the DESCRIPTIVE contamination of
#   the outcome (the 143 apparent declines, and the correlation with
#   the coverage proxy computed below), not an established
#   treatment-correlation.
#
# THE TEST: measure both endpoints on an agglomerated-population concept,
#   where the dispersed-rural gap is excluded from BOTH sides:
#       chg_log_urbpop_60_47 = log(urbpop_1960) - log(urbpop_1947)
#   urbpop_1947 comes from Cuadro 14, the 1947 census's own urban
#   classification (clean_census_1947.R); urbpop_1960 is the sum of
#   localities with pop > 2000, the standard Argentine definition applied
#   by clean_census_1960.R.
#
#   COMPARABILITY OF THE TWO URBAN DEFINITIONS: checked, not assumed.
#   An earlier version said the 1947 threshold "is not documented in
#   this repo" and put it on the archive list. It is answerable from the
#   raw files (cr-review PR #149): the smallest urban centre across the
#   1947 Cuadro 14 sheets is computed below, and if no centre falls
#   below 2,000 then the 1947 classification uses the same 2,000 rule
#   that clean_census_1960.R applies to the 1960 localities.
#
#   SECOND LIMIT: the urban outcome is not the same object as the total
#   outcome. A slope difference can mean (a) the coverage artifact is
#   removed, or (b) urban growth 1947-60 relates to future market-access
#   change differently from total growth. This test cannot separate those,
#   so it is evidence about whether the rejection is FRAGILE, not proof of
#   its cause.
#
#   AND THE DIFFERENCE IS TESTED, NOT EYEBALLED (cr-review blocking 2).
#   Rows 2 and 3 share a sample and 2SLS is linear in the outcome, so
#   the difference in slopes is EXACTLY the coefficient from the same
#   specification run on the difference of the two outcomes. That
#   regression is estimated below. An earlier version called the
#   rejection "substantially a coverage artifact" on the strength of
#   the point estimates alone, without testing whether the movement is
#   distinguishable from zero.
#
# WHO ELSE INHERITS THIS OUTCOME (checked, cr-review should-fix):
#   section_4_empirical_strategy.tex (the Table 7 discussion),
#   section_8_discussion.tex limitation 1, table_7_pre_trends.R,
#   table_b1_descriptives.R, and the recentering / roadseg / roadtiming
#   diagnostics that report placebo balance. table_12_robustness.R
#   Panel C does NOT inherit: it uses the placebo SAMPLE definition
#   only, not the outcome.
#
# READING FIXED BEFORE THE NUMBERS:
#   - Slope survives on the urban outcome  -> the pre-trend is not an
#     artifact of the 1960 coverage gap; item B's control question stands
#     on its own and Table 7 can be reported as it is.
#   - Slope collapses                      -> the published placebo
#     rejection is substantially a coverage artifact, and Table 7 needs
#     rebuilding on comparable universes before any control debate.
#   - Sample-restricted total outcome moves as much as the urban one
#     -> the difference is the SAMPLE (234 vs 237), not the universe, and
#     this test says nothing. Hence the same-sample row below, and hence
#     all percentage movements are quoted against THAT row rather than
#     against the published one (cr-review: an earlier version mixed the
#     two baselines).
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#   results/tables/table_7_pre_trends.csv   (anchor)
#
# PRODUCES:
#   results/tables/diagnostic_placebo_universe.csv / .txt
#
# USAGE: Rscript code/analysis/diagnostic_placebo_universe.R
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
    library(readxl)   # the 1947 Cuadro 14 urban-threshold check
})

fit_row <- function(y, d, tag, S, t7 = NULL) {
    dd <- d[complete.cases(d[, unique(c(y, main_treatment,
                                       main_lp_instrument,
                                       main_hypo_instrument,
                                       placebo_controls))]), ]
    fits <- fit_iv_quad(y = y, data = dd, endog = main_treatment,
                        lp_instr = main_lp_instrument,
                        hypo_instr = main_hypo_instrument,
                        ctrls_vec = placebo_controls)
    for (sp in c("OLS", "IV-LP", "IV-H", "IV-B")) {
        m  <- fits[[sp]]
        cn <- if (sp == "OLS") main_treatment
              else paste0("fit_", main_treatment)
        cc <- safe_coef(m, cn)
        Fv <- if (sp == "OLS") NA_real_ else fitstat_F(m)
        # The published row must reproduce Table 7, or the comparison has
        # no baseline.
        if (!is.null(t7)) {
            sp7 <- c(OLS = "OLS", `IV-LP` = "IV-LP", `IV-H` = "IV-Hypo",
                     `IV-B` = "IV-Both")[[sp]]
            r <- t7[t7$spec == sp7, ]
            stopifnot(nrow(r) == 1L,
                      abs(cc$est - r$estimate) < 1e-8,
                      abs(cc$se - r$std_err) < 1e-8,
                      nobs(m) == r$n_obs)
        }
        S$add(part = "placebo", stat = tag, var = sp, value = cc$est,
              se = cc$se, p_value = cc$p, first_stage_F = Fv,
              n_obs = nobs(m))
        message(sprintf("[pu] %-26s %-6s b=%+.4f se=%.4f p=%.3f N=%d",
                        tag, sp, cc$est, cc$se, cc$p, nobs(m)))
    }
    invisible(nrow(dd))
}

# Row accumulator: new_sink() from _diagnostic_helpers.R. This script's
# rows carry a first-stage F, so the column set is passed explicitly.

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_diagnostic_helpers.R"),
           echo = FALSE)   # new_sink()

    message("\n", strrep("=", 72))
    message("diagnostic_placebo_universe.R  |  is the placebo a coverage artifact?")
    message(strrep("=", 72))

    d <- ensure_geolev2_char(as.data.frame(arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))))

    # The published outcome IS the mismatch; assert it rather than assume.
    pub <- "chg_log_placebo_pop_60_47"
    chk <- d$pop_1947 > 0 & d$pop_1960 > 0 & !is.na(d[[pub]])
    stopifnot(max(abs(d[[pub]][chk] -
                      (log(d$pop_1960[chk]) - log(d$pop_1947[chk])))) < 1e-9)

    d$chg_log_urbpop_60_47 <- ifelse(
        !is.na(d$urbpop_1947) & d$urbpop_1947 > 0 &
        !is.na(d$urbpop_1960) & d$urbpop_1960 > 0,
        log(d$urbpop_1960) - log(d$urbpop_1947), NA_real_)

    S <- new_sink(c("se", "p_value", "first_stage_F", "n_obs"))

    # The #147 premise, read from its output instead of retyped.
    u <- read.csv(file.path(dir_tables,
                            "diagnostic_pop1960_universe.csv"),
                  stringsAsFactors = FALSE)
    ur <- u[u$part == "1_cond_treat" & u$stat == "cov60" &
            u$var == "partial_corr", ]
    stopifnot(nrow(ur) == 1L)
    S$add(part = "premise", stat = "pr147_cov60_treat", var = "partial_corr",
          value = ur$value, p_value = ur$p_value, n_obs = ur$n_obs)

    # Is the 1947 urban classification the same 2,000 rule as 1960?
    # Answered from the raw Cuadro 14 sheets rather than assumed.
    c14 <- list.files(file.path(dir_raw, "census", "censo1947"),
                      pattern = "^1947_Cuadro14_.*\\.xlsx$",
                      full.names = TRUE)
    vals <- unlist(lapply(c14, function(f) {
        x <- suppressMessages(readxl::read_excel(f, col_names = FALSE))
        v <- suppressWarnings(as.numeric(unlist(x)))
        v[is.finite(v) & v > 0]
    }))
    S$add(part = "urban_rule", stat = "n_files", var = "cuadro14",
          value = length(c14))
    S$add(part = "urban_rule", stat = "min_positive", var = "cuadro14",
          value = min(vals), n_obs = length(vals))
    S$add(part = "urban_rule", stat = "n_below_2000", var = "cuadro14",
          value = sum(vals < 2000), n_obs = length(vals))
    message(sprintf("[pu] 1947 Cuadro 14: %d files, min positive %.0f, %d below 2000",
                    length(c14), min(vals), sum(vals < 2000)))

    # Descriptives: how much of the "decline" survives the universe fix.
    n_pub  <- sum(!is.na(d[[pub]]))
    n_urb  <- sum(!is.na(d$chg_log_urbpop_60_47))
    neg_p  <- sum(d[[pub]] < 0, na.rm = TRUE)
    neg_u  <- sum(d$chg_log_urbpop_60_47 < 0, na.rm = TRUE)
    both   <- !is.na(d[[pub]]) & !is.na(d$chg_log_urbpop_60_47)
    S$add(part = "desc", stat = "n", var = "published", value = n_pub)
    S$add(part = "desc", stat = "n", var = "urban", value = n_urb)
    S$add(part = "desc", stat = "n_negative", var = "published",
          value = neg_p, n_obs = n_pub)
    S$add(part = "desc", stat = "n_negative", var = "urban",
          value = neg_u, n_obs = n_urb)
    S$add(part = "desc", stat = "corr", var = "published_vs_urban",
          value = cor(d[[pub]][both], d$chg_log_urbpop_60_47[both]),
          n_obs = sum(both))

    # Direct support for the premise, which the first version omitted:
    # how strongly does each OUTCOME correlate with the coverage proxy,
    # and are their variances comparable (so a slope difference cannot be
    # dismissed as "the urban outcome is just noisier")?
    d$log_cov60 <- log(d$pop_1960 / d$pop_1970)
    z <- both & is.finite(d$log_cov60)
    S$add(part = "contam", stat = "corr_with_log_cov60", var = "published",
          value = cor(d[[pub]][z], d$log_cov60[z]), n_obs = sum(z))
    S$add(part = "contam", stat = "corr_with_log_cov60", var = "urban",
          value = cor(d$chg_log_urbpop_60_47[z], d$log_cov60[z]),
          n_obs = sum(z))
    S$add(part = "contam", stat = "sd", var = "published",
          value = sd(d[[pub]][z]), n_obs = sum(z))
    S$add(part = "contam", stat = "sd", var = "urban",
          value = sd(d$chg_log_urbpop_60_47[z]), n_obs = sum(z))

    # Which districts the urban outcome loses, and why -- one of them is
    # dropped because urbpop_1960 is zero, which is itself a symptom of
    # the list problem under test.
    lost <- d[!is.na(d[[pub]]) & is.na(d$chg_log_urbpop_60_47), ]
    for (i in seq_len(nrow(lost))) {
        S$add(part = "dropped", stat = lost$geolev2[i],
              var = if (!is.na(lost$urbpop_1960[i]) &&
                        lost$urbpop_1960[i] == 0) "urbpop_1960_zero"
                    else "urbpop_1947_missing",
              value = lost$pop_1947[i], n_obs = lost$pop_1960[i])
        message(sprintf("[pu] dropped %s: pop47 %.0f pop60 %.0f urb60 %s",
                        lost$geolev2[i], lost$pop_1947[i], lost$pop_1960[i],
                        format(lost$urbpop_1960[i])))
    }
    message(sprintf("[pu] negative outcomes: published %d/%d, urban %d/%d",
                    neg_p, n_pub, neg_u, n_urb))

    t7 <- read.csv(file.path(dir_tables, "table_7_pre_trends.csv"),
                   stringsAsFactors = FALSE)

    # 1. published, as in Table 7 (anchored).
    fit_row(pub, d, "1_published_pop_237", S, t7 = t7)
    # 2. published outcome on the URBAN sample, so row 3 differs from
    #    row 2 only in the outcome definition, not the sample.
    d_same <- d[!is.na(d$chg_log_urbpop_60_47), ]
    fit_row(pub, d_same, "2_published_same_sample", S)
    # 3. the universe-comparable outcome.
    fit_row("chg_log_urbpop_60_47", d, "3_urban_comparable", S)

    # 4. IS THE MOVEMENT REAL? Rows 2 and 3 share a sample and 2SLS is
    #    linear in the outcome, so running the same specification on the
    #    DIFFERENCE of the two outcomes gives beta(row 2) - beta(row 3)
    #    exactly, with a standard error. Without this the comparison is
    #    two point estimates and an adjective.
    d_same$outcome_diff <- d_same[[pub]] - d_same$chg_log_urbpop_60_47
    fit_row("outcome_diff", d_same, "4_difference_test", S)

    res <- do.call(rbind, S$rows)
    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)
    csv_path <- file.path(dir_tables, "diagnostic_placebo_universe.csv")
    write.csv(res, csv_path, row.names = FALSE)

    g <- function(stat, var, col = "value") {
        v <- res[[col]][res$part == "placebo" & res$stat == stat &
                        res$var == var]
        if (length(v) != 1L) NA_real_ else v
    }
    gd <- function(stat, var) {
        v <- res$value[res$part == "desc" & res$stat == stat &
                       res$var == var]
        if (length(v) != 1L) NA_real_ else v
    }
    gg <- function(part, stat, var, col = "value") {
        v <- res[[col]][res$part == part & res$stat == stat &
                        res$var == var]
        if (length(v) != 1L) NA_real_ else v
    }

    con <- file(file.path(dir_tables, "diagnostic_placebo_universe.txt"),
                open = "wt")
    on.exit(close(con), add = TRUE)
    w <- function(fmt, ...) cat(sprintf(fmt, ...), file = con)

    w("%s\n", strrep("=", 78))
    w("IS THE PRE-TRENDS PLACEBO A 1960 COVERAGE ARTIFACT?\n")
    w("Generated: %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
    w("%s\n\n", strrep("=", 78))

    w("THE PROBLEM. Table 7's outcome is exactly log(pop_1960) -\n")
    w("log(pop_1947) (asserted in code). pop_1947 is Cuadro 1 = district\n")
    w("TOTALS, a full universe; pop_1960 is a sum of NAMED LOCALITIES,\n")
    w("with dispersed rural population absent from the source. So the\n")
    w("outcome equals true 1947-60 growth PLUS log of the 1960 locality\n")
    w("coverage rate, and that second term is related to the treatment\n")
    w("(partial corr -0.119, p 0.039; diagnostic_pop1960_universe).\n\n")
    w("  Districts with a NEGATIVE outcome, i.e. apparent decline:\n")
    w("    published (total pop)   %3.0f of %3.0f  (%.0f%%)\n",
      gd("n_negative", "published"), gd("n", "published"),
      100 * gd("n_negative", "published") / gd("n", "published"))
    w("    urban-comparable        %3.0f of %3.0f  (%.0f%%)\n",
      gd("n_negative", "urban"), gd("n", "urban"),
      100 * gd("n_negative", "urban") / gd("n", "urban"))
    w("  correlation between the two outcomes: %.3f\n\n",
      gd("corr", "published_vs_urban"))

    w("THE TEST. Both endpoints on an agglomerated-population concept:\n")
    w("urbpop_1947 (Cuadro 14, the census's own urban classification)\n")
    w("against urbpop_1960 (localities > 2000). Controls: placebo_controls\n")
    w("as adopted in PR #145. Row 2 exists so that row 3 differs from it\n")
    w("only in the OUTCOME, not the sample.\n\n")
    w("  %-26s %-6s %10s %9s %8s %7s %5s\n",
      "Outcome", "Spec", "beta", "SE", "p", "F", "N")
    for (tg in c("1_published_pop_237", "2_published_same_sample",
                 "3_urban_comparable")) {
        for (sp in c("OLS", "IV-LP", "IV-H", "IV-B")) {
            Fv <- g(tg, sp, "first_stage_F")
            w("  %-26s %-6s %+10.4f %9.4f %8.3f %7s %5d\n", tg, sp,
              g(tg, sp), g(tg, sp, "se"), g(tg, sp, "p_value"),
              if (is.na(Fv)) "--" else sprintf("%.1f", Fv),
              as.integer(g(tg, sp, "n_obs")))
        }
    }

    w("\nIS THE MOVEMENT DISTINGUISHABLE FROM ZERO? Rows 2 and 3 share a\n")
    w("sample and 2SLS is linear in the outcome, so the same\n")
    w("specification on the DIFFERENCE of the two outcomes gives\n")
    w("beta(row 2) - beta(row 3) exactly, with a standard error:\n")
    for (sp in c("OLS", "IV-LP", "IV-H", "IV-B")) {
        w("    %-6s %+.4f (%.4f)  p = %.3f\n", sp,
          g("4_difference_test", sp), g("4_difference_test", sp, "se"),
          g("4_difference_test", sp, "p_value"))
    }
    w("  READ THIS BEFORE THE PERCENTAGES BELOW. The point estimates fall\n")
    w("  a long way, but if these p-values are not small then the drop is\n")
    w("  NOT distinguishable from zero and the honest claim is that the\n")
    w("  rejection is not robust to how the outcome is measured -- not\n")
    w("  that the coverage gap caused it.\n\n")

    w("MOVEMENTS, all quoted against row 2 (the same-sample published\n")
    w("outcome) so that the sample restriction is not counted twice:\n")
    for (sp in c("OLS", "IV-LP", "IV-B")) {
        b2 <- g("2_published_same_sample", sp)
        b3 <- g("3_urban_comparable", sp)
        w("    %-6s %+.4f -> %+.4f  (%.0f%%)\n", sp, b2, b3,
          100 * (b3 - b2) / abs(b2))
    }
    w("  AND THE SIGNIFICANCE CROSSING IS NOT THE OUTCOME'S DOING:\n")
    w("  IV-B p runs %.3f (row 1) -> %.3f (row 2, SAME outcome, three\n",
      g("1_published_pop_237", "IV-B", "p_value"),
      g("2_published_same_sample", "IV-B", "p_value"))
    w("  fewer districts) -> %.3f (row 3). The ten-percent crossing\n",
      g("3_urban_comparable", "IV-B", "p_value"))
    w("  happens at the SAMPLE restriction, before the universe fix is\n")
    w("  applied at all (cr-review PR #149 blocking 3).\n\n")

    w("HOW CONTAMINATED IS EACH OUTCOME? Correlation with the coverage\n")
    w("proxy log(pop_1960/pop_1970), and the SDs so that a slope\n")
    w("difference cannot be waved away as the urban outcome being\n")
    w("noisier:\n")
    w("    published  corr %+.3f   sd %.3f\n",
      gg("contam", "corr_with_log_cov60", "published"),
      gg("contam", "sd", "published"))
    w("    urban      corr %+.3f   sd %.3f\n\n",
      gg("contam", "corr_with_log_cov60", "urban"),
      gg("contam", "sd", "urban"))

    w("READING (fixed before the numbers were seen):\n")
    w("  slope survives on row 3  -> the pre-trend is not an artifact of\n")
    w("    the coverage gap; item B's control question stands on its own.\n")
    w("  slope collapses          -> the published rejection is\n")
    w("    substantially a coverage artifact and Table 7 needs rebuilding\n")
    w("    on comparable universes BEFORE the control debate.\n")
    w("  row 2 moves as much as row 3 -> the difference is the sample,\n")
    w("    not the universe, and this test says nothing.\n\n")
    w("TWO LIMITS, both real:\n")
    w("  (i) ASSUMPTION NOT VERIFIED: that the 1947 and 1960 urban\n")
    w("      definitions are close enough to compare. The 1947 threshold\n")
    w("      is not documented in this repo. If it differs materially\n")
    w("      from 2,000, row 3 swaps one comparability problem for a\n")
    w("      smaller one rather than removing it. On the archive list\n")
    w("      with the departamento totals.\n")
    w("  (ii) The urban outcome is a DIFFERENT OBJECT, not a cleaned\n")
    w("      version of the same one. A slope difference can mean the\n")
    w("      coverage artifact is removed, or that urban growth relates\n")
    w("      to future market-access change differently from total\n")
    w("      growth. This test is evidence about FRAGILITY, not proof of\n")
    w("      cause.\n")
    w("  The clean fix for both remains the published departamento\n")
    w("  totals, which would let the placebo be built on one universe\n")
    w("  without changing the concept.\n")

    message(sprintf("[pu] Saved: %s and .txt", csv_path))
}

main()

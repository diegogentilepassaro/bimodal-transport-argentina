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
#   years, and the coverage shortfall is related to the treatment
#   (partial corr -0.119, p 0.039; PR #147). Table 7 is therefore
#   measuring pre-period growth plus cross-district variation in 1960
#   locality coverage, in unknown proportions.
#
# THE TEST: measure both endpoints on an agglomerated-population concept,
#   where the dispersed-rural gap is excluded from BOTH sides:
#       chg_log_urbpop_60_47 = log(urbpop_1960) - log(urbpop_1947)
#   urbpop_1947 comes from Cuadro 14, the 1947 census's own urban
#   classification (clean_census_1947.R); urbpop_1960 is the sum of
#   localities with pop > 2000, the standard Argentine definition applied
#   by clean_census_1960.R.
#
#   ASSUMPTION, STATED NOT VERIFIED: that the two urban definitions are
#   close enough to compare. The 1947 census's urban threshold is not
#   documented in this repo. If it differs materially from 2,000, this
#   outcome swaps one comparability problem for a smaller one rather than
#   removing it. Worth confirming against the published volumes; it is on
#   the archive list with the departamento totals.
#
#   SECOND LIMIT: the urban outcome is not the same object as the total
#   outcome. A slope difference can mean (a) the coverage artifact is
#   removed, or (b) urban growth 1947-60 relates to future market-access
#   change differently from total growth. This test cannot separate those,
#   so it is evidence about whether the rejection is FRAGILE, not proof of
#   its cause.
#
# READING FIXED BEFORE THE NUMBERS:
#   - Slope survives on the urban outcome  -> the pre-trend is not an
#     artifact of the 1960 coverage gap; item B's control question stands
#     on its own and Table 7 can be reported as it is.
#   - Slope collapses                      -> the published placebo
#     rejection is substantially a coverage artifact, and Table 7 needs
#     rebuilding on comparable universes before any control debate.
#   - Sample-restricted total outcome moves as much as the urban one
#     -> the difference is the SAMPLE (235 vs 237), not the universe, and
#     this test says nothing. Hence the same-sample row below.
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

new_sink <- function() {
    e <- new.env(parent = emptyenv()); e$rows <- list()
    e$add <- function(...) {
        r <- data.frame(..., stringsAsFactors = FALSE)
        for (col in c("se", "p_value", "first_stage_F", "n_obs")) {
            if (is.null(r[[col]])) r[[col]] <- NA_real_
        }
        e$rows[[length(e$rows) + 1L]] <- r[, c("part", "stat", "var",
                                              "value", "se", "p_value",
                                              "first_stage_F", "n_obs")]
    }
    e
}

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)

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

    S <- new_sink()

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

    w("\nREADING (fixed before the numbers were seen):\n")
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

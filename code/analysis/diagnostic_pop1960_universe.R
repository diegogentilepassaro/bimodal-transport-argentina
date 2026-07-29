# ===========================================================================
# diagnostic_pop1960_universe.R
#
# PURPOSE: pop_1960 and the 1970-2010 IPUMS population variables are drawn
#          from DIFFERENT UNIVERSES. This diagnostic documents that and
#          sizes what it does -- and, after the cr-review of PR #147, is
#          explicit about which of those things it can and cannot settle.
#          DIAGNOSTIC ONLY: no table, figure or scalar changes.
#
# THE MISMATCH:
#   data/raw/census/censo1960/1c1960_*.xlsx has three columns --
#   provincia, distrito, pop -- ONE ROW PER LOCALITY, no locality name, no
#   rural-dispersed line, no urban/rural flag. clean_census_1960.R derives
#   urban/rural from the pop > 2000 rule, so pop_1960 is the sum of named
#   localities in a district, not the district's population. Dispersed
#   rural population is absent from the source and cannot be in it.
#   pop_1970 and later, from IPUMS microdata, do include it. So
#   chg_log_pop_91_60 divides a full-population 1991 by a
#   locality-universe 1960.
#
# EVIDENCE FOR THE DIAGNOSIS (Part 0), in-repo only:
#   (a) implied 1960 urban share;
#   (b) implied 1960->1970 growth, shown BESIDE the all-IPUMS 1970->1980
#       and 1980->1991 decades so the reader can see how much of (b) is
#       specific to the 1960 source rather than to Argentine growth
#       (cr-review: 1970-80 is itself 1.38x, so (b) alone is weak);
#   (c) THE DECISIVE ONE: pop_1947 comes from Cuadro 1 of the 1947 census,
#       which is DISTRICT TOTALS (clean_census_1947.R), i.e. a
#       full-universe source. Comparing it to pop_1960 on the overlapping
#       districts needs no external benchmark and no judgment about
#       plausible growth: population cannot fall in most of the country
#       over 1947-1960.
#   Part 0 previously leaned on the assertion that (a) and (b) are "not
#   credible for Argentina", an uncited external judgment. (c) replaces it.
#
# WHAT PART 1 CAN AND CANNOT ESTABLISH:
#   The question is whether the measurement error is correlated with the
#   INSTRUMENTS, since that is what would carry it into the IV estimates.
#   Raw correlations cannot answer it: the relevant condition is
#   orthogonality AFTER partialling out the controls actually used, and
#   geo_controls_main contains log_pop_1960 and dist_to_BA_std, both
#   related to the proxies. So the conditional tests are the evidence and
#   the raw correlations are reported only as description (cr-review
#   PR #147 blocking 1).
#
#   TWO PROXIES, and a stopping rule fixed in advance: c_i =
#   pop_1960/pop_1970 is contaminated by genuine 1960-70 growth;
#   urbshr_1960 is growth-free but measures something adjacent. If the two
#   disagree in sign against the treatment CONDITIONAL ON CONTROLS, this
#   part is inconclusive and says so. The first version of this script
#   wrote that rule and then only computed the raw version, which could
#   not trigger it. The conditional verdict is computed and printed below.
#
#   THE c_i CONTROL IS A BAD CONTROL, on two counts, and the result is
#   reported as descriptive rather than as a measurement-error correction:
#     - Mechanically, log(c_i) = -(log pop_1970 - log pop_1960), so
#       conditioning on it removes the whole 1960-70 component of the
#       outcome -- genuine growth included, not just coverage error. The
#       identity is asserted below.
#     - pop_1970 is realised INSIDE the outcome window, after part of the
#       1960-86 treatment, so conditioning on it breaks the conditional
#       exclusion restriction for the IV columns.
#
#   AND PART 1'S CONTROL AND PART 2'S WINDOW ARE THE SAME ADJUSTMENT, not
#   independent angles: chg_log_pop_91_60 = -log(c_i) + chg_log_pop_91_70,
#   so adding c_i is the window change with the coefficient free instead
#   of imposed at -1. The free coefficient is estimated and reported so
#   the reader can see how close to -1 it lands.
#
# PART 2 -- outcomes on one universe.
#   Population 1970->1991 (both IPUMS) and, since urbpop_1970 is missing
#   from the extract but urbpop_1980 is not, urban share 1980->1991. An
#   earlier version claimed the urban share could not be made
#   universe-consistent at all; that was wrong (cr-review blocking 5).
#   MISALIGNMENT, stated because it is an alternative explanation for
#   whatever the shorter windows show: the treatment is the 1960-86
#   market-access change, so a 1970 or 1980 baseline is already partly
#   treated and any first-decade response is differenced away. No aligned
#   treatment exists -- there is no 1970 rail vintage and hence no
#   chg_logMA_86_70.
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#   data/derived/base/census_1960/census_1960_ipums.parquet
#   results/tables/table_9_population_iv.csv   (anchor)
#
# PRODUCES:
#   results/tables/diagnostic_pop1960_universe.csv / .txt
#
# USAGE: Rscript code/analysis/diagnostic_pop1960_universe.R
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
})

# Row accumulator shared by all parts: new_sink() from
# _diagnostic_helpers.R, with this script's column set (no first-stage F).

# ---------------------------------------------------------------------------
# Part 0: the mismatch, from in-repo quantities only.
# ---------------------------------------------------------------------------
describe_universes <- function(c60, d) {
    caba <- c60$pop[c60$geolev2 == "32002001"]
    stopifnot(length(caba) == 1L)
    n47 <- is.finite(d$pop_1947) & d$pop_1947 > 0 &
           is.finite(d$pop_1960) & d$pop_1960 > 0
    list(
        n_districts = nrow(c60), n_sample = nrow(d),
        tot60 = sum(c60$pop, na.rm = TRUE),
        urb60 = sum(c60$urbpop, na.rm = TRUE),
        rur60 = sum(c60$rur, na.rm = TRUE),
        caba = caba, non_caba = sum(c60$pop, na.rm = TRUE) - caba,
        urb_share = sum(c60$urbpop, na.rm = TRUE) / sum(c60$pop, na.rm = TRUE),
        p60 = sum(d$pop_1960, na.rm = TRUE),
        p70 = sum(d$pop_1970, na.rm = TRUE),
        p80 = sum(d$pop_1980, na.rm = TRUE),
        p91 = sum(d$pop_1991, na.rm = TRUE),
        # 1947 comparison: full-universe district totals (Cuadro 1).
        n47 = sum(n47),
        n47_falling = sum(d$pop_1960[n47] < d$pop_1947[n47]),
        r47_60 = sum(d$pop_1960[n47]) / sum(d$pop_1947[n47]),
        r60_70 = sum(d$pop_1970[n47]) / sum(d$pop_1960[n47])
    )
}

# ---------------------------------------------------------------------------
# Control-conditional instrument tests: the statistic Part 1 actually
# needs. For each proxy, regress it on both instruments plus the control
# set the paper uses, HC1, and report each instrument's coefficient and
# p-value plus the joint Wald test.
# ---------------------------------------------------------------------------
cond_tests <- function(d, S) {
    proxies <- list(
        list(tag = "growth_gap_70_60", v = "growth_gap_70_60"),
        list(tag = "cov60",            v = "cov60"),
        list(tag = "urbshr_1960",      v = "urbshr_1960")
    )
    instr <- c(main_lp_instrument, main_hypo_instrument)
    for (p in proxies) {
        f <- as.formula(paste(p$v, "~",
                              paste(c(instr, geo_controls_main),
                                    collapse = " + ")))
        m <- feols(f, data = d, vcov = "hetero")
        for (iv in instr) {
            cc <- safe_coef(m, iv)
            S$add(part = "1_cond_instr", stat = p$tag, var = iv,
                  value = cc$est, se = cc$se, p_value = cc$p,
                  n_obs = nobs(m))
        }
        w <- tryCatch(fixest::wald(m, keep = instr, print = FALSE),
                      error = function(e) NULL)
        S$add(part = "1_cond_instr", stat = p$tag, var = "joint_F",
              value = if (is.null(w)) NA_real_ else unname(w$stat),
              p_value = if (is.null(w)) NA_real_ else unname(w$p),
              n_obs = nobs(m))
        message(sprintf("[u1] %-18s LP p=%.3f  H p=%.3f  joint p=%s",
                        p$tag, safe_coef(m, instr[1])$p,
                        safe_coef(m, instr[2])$p,
                        if (is.null(w)) "NA" else sprintf("%.3f", w$p)))

        # Conditional correlation with the treatment: residualise both on
        # the controls, then correlate. This is the quantity whose SIGN
        # the stopping rule below compares across the two proxies.
        rp <- residuals(feols(as.formula(paste(p$v, "~",
                        paste(geo_controls_main, collapse = " + "))),
                        data = d))
        rt <- residuals(feols(as.formula(paste(main_treatment, "~",
                        paste(geo_controls_main, collapse = " + "))),
                        data = d))
        mt <- feols(rp ~ rt, data = data.frame(rp = rp, rt = rt),
                    vcov = "hetero")
        cc <- safe_coef(mt, "rt")
        S$add(part = "1_cond_treat", stat = p$tag, var = "partial_corr",
              value = cor(rp, rt), p_value = cc$p, n_obs = length(rp))
        message(sprintf("[u1] %-18s partial corr with treatment %+.3f (p=%.3f)",
                        p$tag, cor(rp, rt), cc$p))
    }
}

# ---------------------------------------------------------------------------
# Part 1: description, then the c_i-control sensitivity (all four specs).
# ---------------------------------------------------------------------------
run_part1 <- function(d, S) {
    ok <- is.finite(d$cov60) & d$cov60 > 0
    q <- quantile(d$cov60[ok], c(.05, .25, .50, .75, .95))
    for (i in seq_along(q)) {
        S$add(part = "1_coverage", stat = paste0("q", names(q)[i]),
              var = "cov60", value = unname(q[i]), n_obs = sum(ok))
    }

    # RAW correlations: description only. The conditional tests above are
    # the evidence; these are kept because they are what a reader computes
    # first, and because the gap between the two is itself the lesson.
    targets <- c(main_treatment, main_lp_instrument, main_hypo_instrument,
                 "chg_log_pop_91_60", "dist_to_BA_std", "urbshr_1960")
    for (proxy in c("cov60", "urbshr_1960")) {
        for (v in targets) {
            if (identical(proxy, v)) next
            z <- ok & is.finite(d[[v]]) & is.finite(d[[proxy]])
            S$add(part = "1_corr_raw", stat = proxy, var = v,
                  value = cor(d[[proxy]][z], d[[v]][z]), n_obs = sum(z))
        }
    }

    cond_tests(d, S)

    # The identity that makes Part 1 and Part 2 the same adjustment.
    idt <- max(abs(log(d$cov60) + d$growth_gap_70_60), na.rm = TRUE)
    stopifnot(idt < 1e-10)
    S$add(part = "1_identity", stat = "max_abs_dev", var = "log_cov_plus_gap",
          value = idt, n_obs = sum(is.finite(d$cov60)))

    y <- "chg_log_pop_91_60"
    for (spec in c("as_published", "plus_cov60")) {
        ctrls <- if (spec == "as_published") geo_controls_main
                 else c(geo_controls_main, "cov60")
        dd <- d[complete.cases(d[, unique(c(y, main_treatment,
                                            main_lp_instrument,
                                            main_hypo_instrument,
                                            ctrls))]), ]
        fits <- fit_iv_quad(y = y, data = dd, endog = main_treatment,
                            lp_instr = main_lp_instrument,
                            hypo_instr = main_hypo_instrument,
                            ctrls_vec = ctrls)
        # All four specs written, not two. The first version reported OLS
        # and IV-B only, and the omitted IV-H moved most (cr-review B4).
        for (sp in c("OLS", "IV-LP", "IV-H", "IV-B")) {
            m <- fits[[sp]]
            cn <- if (sp == "OLS") main_treatment
                  else paste0("fit_", main_treatment)
            cc <- safe_coef(m, cn)
            S$add(part = "1_beta", stat = spec, var = sp, value = cc$est,
                  se = cc$se, p_value = cc$p, n_obs = nobs(m))
            message(sprintf("[u1] %-12s %-6s b=%+.4f se=%.4f p=%.3f",
                            spec, sp, cc$est, cc$se, cc$p))
        }
    }

    # Free-coefficient version of the same adjustment: if the fitted
    # coefficient on log(c_i) is near -1, Part 1 and Part 2 are doing the
    # same thing and should not be described as independent.
    dd <- d[complete.cases(d[, unique(c(y, main_treatment,
                                        geo_controls_main, "log_cov60"))]), ]
    mfree <- feols(as.formula(paste(y, "~", main_treatment, "+ log_cov60 +",
                                    paste(geo_controls_main,
                                          collapse = " + "))),
                   data = dd, vcov = "hetero")
    for (v in c(main_treatment, "log_cov60")) {
        cc <- safe_coef(mfree, v)
        S$add(part = "1_free", stat = "ols_free_logcov", var = v,
              value = cc$est, se = cc$se, p_value = cc$p, n_obs = nobs(mfree))
    }
    message(sprintf("[u1] free coef on log(c_i) = %+.3f (imposed -1 by the window)",
                    safe_coef(mfree, "log_cov60")$est))
}

# ---------------------------------------------------------------------------
# Part 2: outcomes measured on one universe.
# ---------------------------------------------------------------------------
run_part2 <- function(d, t9, S) {
    outcomes <- list(
        list(tag = "pop_91_60_published",   y = "chg_log_pop_91_60",
             anchor = "chg_log_pop_91_60"),
        list(tag = "pop_91_70_consistent",  y = "chg_log_pop_91_70",
             anchor = NA),
        list(tag = "urbshr_91_60_published", y = "chg_urbshr_91_60",
             anchor = "chg_urbshr_91_60"),
        # urbpop_1970 is missing from the extract but urbpop_1980 is not,
        # so the urban share CAN be put on one universe -- just a decade
        # later (cr-review PR #147 blocking 5).
        list(tag = "urbshr_91_80_consistent", y = "chg_urbshr_91_80",
             anchor = NA)
    )
    # Non-vacuous: assert the column exists AND is empty, so dropping or
    # renaming it fails instead of passing silently.
    stopifnot("urbpop_1970" %in% names(d), all(is.na(d$urbpop_1970)),
              "urbpop_1980" %in% names(d), !any(is.na(d$urbpop_1980)))
    for (o in outcomes) {
        dd <- d[complete.cases(d[, unique(c(o$y, main_treatment,
                                            main_lp_instrument,
                                            main_hypo_instrument,
                                            geo_controls_main))]), ]
        fits <- fit_iv_quad(y = o$y, data = dd, endog = main_treatment,
                            lp_instr = main_lp_instrument,
                            hypo_instr = main_hypo_instrument,
                            ctrls_vec = geo_controls_main)
        for (sp in c("OLS", "IV-LP", "IV-H", "IV-B")) {
            m <- fits[[sp]]
            cn <- if (sp == "OLS") main_treatment
                  else paste0("fit_", main_treatment)
            cc <- safe_coef(m, cn)
            if (!is.na(o$anchor)) {
                # Table 9's CSV labels the columns OLS / IV-LP / IV-H /
                # IV-B, which is what sp already carries.
                r <- t9[t9$outcome == o$anchor & t9$spec == sp, ]
                stopifnot(nrow(r) == 1L,
                          abs(cc$est - r$estimate) < 1e-8,
                          abs(cc$se - r$std_err) < 1e-8,
                          nobs(m) == r$n_obs)
            }
            S$add(part = "2_window", stat = o$tag, var = sp, value = cc$est,
                  se = cc$se, p_value = cc$p, n_obs = nobs(m))
            message(sprintf("[u2] %-25s %-6s b=%+.4f se=%.4f p=%.3f N=%d",
                            o$tag, sp, cc$est, cc$se, cc$p, nobs(m)))
        }
        S$add(part = "2_firstF", stat = o$tag, var = "IV-H",
              value = fitstat_F(fits[["IV-H"]]), n_obs = nobs(fits[["IV-H"]]))
    }
}

write_report <- function(res, meta, path) {
    con <- file(path, open = "wt")
    on.exit(close(con), add = TRUE)   # never leave a truncated report
    w <- function(fmt, ...) cat(sprintf(fmt, ...), file = con)
    g <- function(part, stat, var, col = "value") {
        v <- res[[col]][res$part == part & res$stat == stat & res$var == var]
        if (length(v) != 1L) return(NA_real_)
        v
    }

    w("%s\n", strrep("=", 78))
    w("THE 1960 POPULATION UNIVERSE MISMATCH: WHAT IS AND IS NOT SETTLED\n")
    w("Generated: %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
    w("%s\n\n", strrep("=", 78))

    w("PART 0 - THE MISMATCH\n")
    w("  census_1960_ipums.parquet, %d districts:\n", meta$n_districts)
    w("    pop     %14s = urbpop %s + rur %s\n",
      format(meta$tot60, big.mark = ","),
      format(meta$urb60, big.mark = ","),
      format(meta$rur60, big.mark = ","))
    w("    Capital Federal %s, rest %s\n",
      format(meta$caba, big.mark = ","),
      format(meta$non_caba, big.mark = ","))
    w("  (a) implied 1960 urban share: %.1f%%\n", 100 * meta$urb_share)
    w("  (b) decadal growth, estimation sample (%d districts):\n",
      meta$n_sample)
    w("        1960->1970  %.3fx   <- crosses the universe boundary\n",
      meta$p70 / meta$p60)
    w("        1970->1980  %.3fx   all IPUMS\n", meta$p80 / meta$p70)
    w("        1980->1991  %.3fx   all IPUMS (11 years)\n",
      meta$p91 / meta$p80)
    w("      NOTE: 1970-80 is itself large, so (b) ALONE is weak evidence\n")
    w("      for the mismatch. It is shown with its comparators rather\n")
    w("      than on its own (cr-review PR #147).\n")
    w("  (c) DECISIVE, and needs no external benchmark: pop_1947 is\n")
    w("      Cuadro 1 of the 1947 census = DISTRICT TOTALS, a full\n")
    w("      universe (clean_census_1947.R). On the %d districts where\n",
      meta$n47)
    w("      both exist, pop_1960 < pop_1947 in %d of them (%.0f%%),\n",
      meta$n47_falling, 100 * meta$n47_falling / meta$n47)
    w("      and the aggregate ratio is %.3f over 1947-1960 against\n",
      meta$r47_60)
    w("      %.3f over 1960-1970 on the same districts. Population does\n",
      meta$r60_70)
    w("      not fall in most of a country over thirteen years: the 1960\n")
    w("      denominator is a smaller universe, not a smaller country.\n")
    w("  OPEN: the fix is the published departamento totals in the 1960\n")
    w("  volumes, which neither digitization captured.\n\n")

    w("PART 1 - IS THE ERROR CORRELATED WITH THE INSTRUMENTS?\n")
    w("  Coverage proxy c_i = pop_1960/pop_1970. Distribution:\n")
    for (s in c("q5%", "q25%", "q50%", "q75%", "q95%")) {
        w("    %-5s %.3f\n", s, g("1_coverage", s, "cov60"))
    }
    w("\n  RAW correlations (DESCRIPTION ONLY - see the conditional tests\n")
    w("  below, which are the evidence):\n")
    w("    %-28s %9s %9s\n", "target", "cov60", "urbshr60")
    for (v in unique(res$var[res$part == "1_corr_raw"])) {
        a <- g("1_corr_raw", "cov60", v)
        b <- g("1_corr_raw", "urbshr_1960", v)
        note <- if (identical(v, "chg_log_pop_91_60"))
            "  <- IDENTITY, not evidence: pop_1960 sits in both" else ""
        w("    %-28s %+9.3f %+9.3f%s\n", v, a, b, note)
    }

    w("\n  CONDITIONAL ON geo_controls_main (HC1) - the actual test.\n")
    w("  Orthogonality has to hold after partialling out the controls the\n")
    w("  regression uses, not in raw data.\n")
    w("    %-20s %10s %10s %10s\n", "proxy", "LP p", "hypo p", "joint p")
    for (tg in c("growth_gap_70_60", "cov60", "urbshr_1960")) {
        w("    %-20s %10.3f %10.3f %10.3f\n", tg,
          g("1_cond_instr", tg, main_lp_instrument, "p_value"),
          g("1_cond_instr", tg, main_hypo_instrument, "p_value"),
          g("1_cond_instr", tg, "joint_F", "p_value"))
    }
    w("\n    partial corr with the treatment (sign is what the stopping\n")
    w("    rule compares):\n")
    for (tg in c("growth_gap_70_60", "cov60", "urbshr_1960")) {
        w("      %-20s %+.3f (p=%.3f)\n", tg,
          g("1_cond_treat", tg, "partial_corr"),
          g("1_cond_treat", tg, "partial_corr", "p_value"))
    }
    s1 <- sign(g("1_cond_treat", "cov60", "partial_corr"))
    s2 <- sign(g("1_cond_treat", "urbshr_1960", "partial_corr"))
    w("\n  VERDICT (rule fixed before the numbers): the two proxies %s\n",
      if (identical(s1, s2)) "AGREE in sign" else "DISAGREE in sign")
    if (!identical(s1, s2)) {
        w("  => THIS PART IS INCONCLUSIVE on whether the measurement error\n")
        w("  is treatment-correlated. The proxies point in opposite\n")
        w("  directions once the controls are partialled out, so neither\n")
        w("  a reassuring nor an alarming reading is supported here.\n")
    } else {
        w("  => the direction they agree on may be reported, with the\n")
        w("  caveat that both proxies are imperfect.\n")
    }
    w("  What IS supported either way: the joint tests above say whether\n")
    w("  the instruments predict each proxy conditional on controls.\n")
    w("  Read those p-values, not the raw correlations.\n")

    w("\n  Headline population regression with c_i added as a control:\n")
    w("    %-6s %22s %22s\n", "spec", "as published", "+ c_i")
    for (sp in c("OLS", "IV-LP", "IV-H", "IV-B")) {
        w("    %-6s %+10.4f (%.4f) %+10.4f (%.4f)\n", sp,
          g("1_beta", "as_published", sp), g("1_beta", "as_published", sp, "se"),
          g("1_beta", "plus_cov60", sp), g("1_beta", "plus_cov60", sp, "se"))
    }
    w("  c_i IS A BAD CONTROL and this row is DESCRIPTIVE, not a\n")
    w("  measurement-error correction. Two reasons:\n")
    w("    (i) log(c_i) = -(log pop_1970 - log pop_1960) exactly (max abs\n")
    w("        deviation %.1e), so conditioning on it removes the whole\n",
      g("1_identity", "max_abs_dev", "log_cov_plus_gap"))
    w("        1960-70 component of the outcome, genuine growth included.\n")
    w("    (ii) pop_1970 is realised inside the outcome window, after part\n")
    w("        of the 1960-86 treatment, so conditioning on it breaks the\n")
    w("        conditional exclusion restriction for the IV columns.\n")
    w("  SAME ADJUSTMENT AS PART 2, not an independent angle: the free\n")
    w("  coefficient on log(c_i) is %+.3f, against the -1 that moving the\n",
      g("1_free", "ols_free_logcov", "log_cov60"))
    w("  outcome window imposes.\n\n")

    w("PART 2 - OUTCOMES ON ONE UNIVERSE\n")
    w("  %-25s %-6s %10s %9s %8s %6s\n",
      "Outcome / window", "Spec", "beta", "SE", "p", "N")
    for (tg in unique(res$stat[res$part == "2_window"])) {
        for (sp in c("OLS", "IV-LP", "IV-H", "IV-B")) {
            w("  %-25s %-6s %+10.4f %9.4f %8.3f %6d\n", tg, sp,
              g("2_window", tg, sp), g("2_window", tg, sp, "se"),
              g("2_window", tg, sp, "p_value"),
              as.integer(g("2_window", tg, sp, "n_obs")))
        }
        w("  %-25s IV-H first-stage F = %.1f\n", "", g("2_firstF", tg, "IV-H"))
    }
    w("\n  MISALIGNMENT, an alternative explanation for these rows: the\n")
    w("  treatment is the 1960-86 market-access change, so a 1970 or 1980\n")
    w("  baseline is ALREADY PARTLY TREATED and any first-decade response\n")
    w("  is differenced away. An aligned treatment does not exist -- there\n")
    w("  is no 1970 rail vintage and hence no chg_logMA_86_70. So a\n")
    w("  coefficient that falls on the shorter window is consistent with\n")
    w("  BOTH 'the 1960-91 estimate was inflated by the universe mismatch'\n")
    w("  AND 'the response happened mostly in the first decade'. This\n")
    w("  diagnostic does not separate them.\n")
    w("  Caveat throughout: the IV-H first-stage F values above are weak,\n")
    w("  so that column is the least informative of the four.\n")
    w("  On the urban share: the 1960 leg sits on the inflated locality\n")
    w("  denominator, and the 1980->1991 row is the universe-consistent\n")
    w("  comparison. No ranking of Table 9's four outcomes by\n")
    w("  contamination is offered here -- the rural panel would need its\n")
    w("  own analysis (rur_1960 is only %s sample-wide).\n",
      format(meta$rur60, big.mark = ","))
}

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_diagnostic_helpers.R"),
           echo = FALSE)   # new_sink()

    message("\n", strrep("=", 72))
    message("diagnostic_pop1960_universe.R  |  1960 universe mismatch")
    message(strrep("=", 72))

    d <- ensure_geolev2_char(as.data.frame(arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))))
    # Read the census file directly rather than through
    # _diagnostic_helpers.R::load_1960_pop(), which returns geolev2 and pop
    # only; Part 0 needs urbpop and rur as well.
    c60 <- ensure_geolev2_char(as.data.frame(arrow::read_parquet(
        file.path(dir_derived_census1960, "census_1960_ipums.parquet"))))
    stopifnot(nrow(d) == n_districts - 1L, nrow(c60) == n_districts)

    meta <- describe_universes(c60, d)
    message(sprintf(
        "[u0] urban share %.1f%%; 1947->60 ratio %.3f; %d of %d shrink",
        100 * meta$urb_share, meta$r47_60, meta$n47_falling, meta$n47))

    d$cov60 <- ifelse(is.finite(d$pop_1970) & d$pop_1970 > 0,
                      d$pop_1960 / d$pop_1970, NA_real_)
    d$log_cov60 <- log(d$cov60)
    d$growth_gap_70_60 <- log(d$pop_1970) - log(d$pop_1960)
    d$chg_log_pop_91_70 <- log(d$pop_1991) - log(d$pop_1970)
    d$urbshr_1980 <- d$urbpop_1980 / d$pop_1980
    d$chg_urbshr_91_80 <- d$urbshr_1991 - d$urbshr_1980

    t9 <- read.csv(file.path(dir_tables, "table_9_population_iv.csv"),
                   stringsAsFactors = FALSE)

    S <- new_sink()
    run_part1(d, S)
    run_part2(d, t9, S)
    res <- do.call(rbind, S$rows)

    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)
    csv_path <- file.path(dir_tables, "diagnostic_pop1960_universe.csv")
    write.csv(res, csv_path, row.names = FALSE)
    write_report(res, meta,
                 file.path(dir_tables, "diagnostic_pop1960_universe.txt"))
    message(sprintf("[u] Saved: %s and .txt", csv_path))
}

main()

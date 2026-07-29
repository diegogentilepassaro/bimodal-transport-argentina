# ===========================================================================
# diagnostic_pop1960_universe.R
#
# PURPOSE: the 1960 population variable and the 1970-2010 IPUMS population
#          variables are drawn from DIFFERENT UNIVERSES, and this
#          diagnostic sizes what that does to the headline results.
#          DIAGNOSTIC ONLY: no table, figure or scalar changes.
#
# THE MISMATCH (established 2026-07-29 while reviewing PR #146):
#   data/raw/census/censo1960/1c1960_*.xlsx has three columns --
#   provincia, distrito, pop -- with ONE ROW PER LOCALITY and no locality
#   name, no rural-dispersed line and no urban/rural flag.
#   clean_census_1960.R therefore derives urban/rural from the pop > 2000
#   rule, and pop_1960 is the sum of NAMED LOCALITIES in a district, not
#   the district's population. Dispersed rural population (poblacion
#   rural dispersa) is absent from the source and so cannot be in it.
#   pop_1970 and later come from IPUMS microdata and DO include it.
#
#   Two internal facts that corroborate this without any external source:
#     (a) implied 1960 urban share = urbpop_1960 / pop_1960, which comes
#         out far above any plausible 1960 figure for Argentina;
#     (b) implied 1960->1970 growth = pop_1970 / pop_1960, which comes
#         out far above any plausible decadal growth rate.
#   Both are computed and printed below rather than asserted.
#
#   EXTERNAL BENCHMARK NOT USED AS EVIDENCE HERE: the published 1960
#   census national total and urban share. Quoting them from memory is
#   how the first pass of this analysis went wrong (see the correction on
#   PR #146), so this script reports only in-repo quantities and flags
#   the archive check as an open item.
#
# WHY IT MATTERS: chg_log_pop_91_60 = log pop_1991 - log pop_1960 mixes
#   the two universes, and the 1960 shortfall is LARGEST where the
#   dispersed-rural share was largest -- rural districts, which is where
#   the rail closures landed. So the measurement error is plausibly
#   correlated with the treatment. Same mismatch reaches urbshr_1960,
#   log_pop_1960 (a control) and the MA population weights, where it
#   under-weights rural destinations relative to Capital Federal.
#
# PART 1 -- is the error treatment-correlated?
#   Coverage proxy c_i = pop_1960 / pop_1970, plus correlations with the
#   treatment, both instruments and two independent proxies, and a direct
#   sensitivity: the headline IV with c_i as an added control.
#
#   HONEST LIMIT, stated before the numbers: c_i is contaminated by
#   genuine 1960-70 growth. A low c_i can mean "high dispersed-rural
#   share" OR "fast-growing district". It is a noisy proxy, not a
#   measurement. urbshr_1960 is reported alongside as a growth-free
#   second proxy; agreement between the two is the signal, and if they
#   disagree this diagnostic is inconclusive and says so.
#
# PART 2 -- universe-consistent robustness.
#   Rerun the headline population regression with the outcome measured
#   1970->1991, both endpoints from IPUMS. Costs a decade of the window
#   and removes the mismatch entirely. Precedent: every outcome in
#   Table 11 is 1970-91 for exactly this reason. Same for the urban
#   share, which has the same defect.
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

# ---------------------------------------------------------------------------
# Part 0: size the mismatch from in-repo quantities only.
# ---------------------------------------------------------------------------
describe_universes <- function(c60, d) {
    tot60 <- sum(c60$pop, na.rm = TRUE)
    urb60 <- sum(c60$urbpop, na.rm = TRUE)
    rur60 <- sum(c60$rur, na.rm = TRUE)
    caba  <- c60$pop[c60$geolev2 == "32002001"]
    stopifnot(length(caba) == 1L)
    list(
        n_districts = nrow(c60),
        tot60 = tot60, urb60 = urb60, rur60 = rur60, caba = caba,
        non_caba = tot60 - caba,
        urb_share = urb60 / tot60,
        # Estimation-sample sums (311 districts, CABA already dropped).
        est_pop60 = sum(d$pop_1960, na.rm = TRUE),
        est_pop70 = sum(d$pop_1970, na.rm = TRUE),
        est_pop91 = sum(d$pop_1991, na.rm = TRUE)
    )
}

# ---------------------------------------------------------------------------
# Part 1: is the coverage shortfall related to treatment or instruments?
# ---------------------------------------------------------------------------
run_part1 <- function(d) {
    rows <- list()
    add <- function(...) rows[[length(rows) + 1L]] <<-
        data.frame(..., stringsAsFactors = FALSE)

    ok <- is.finite(d$cov60) & d$cov60 > 0
    q <- quantile(d$cov60[ok], c(.05, .25, .50, .75, .95))
    for (i in seq_along(q)) {
        add(part = "1_coverage", stat = paste0("q", names(q)[i]),
            var = "cov60", value = unname(q[i]), n_obs = sum(ok))
    }

    # Correlations against the two proxies. cov60 is the growth-contaminated
    # one; urbshr_1960 is growth-free but measures a different thing, so
    # both are reported and neither is treated as decisive alone.
    targets <- c(main_treatment, main_lp_instrument, main_hypo_instrument,
                 "chg_log_pop_91_60", "dist_to_BA_std", "urbshr_1960")
    for (proxy in c("cov60", "urbshr_1960")) {
        for (v in targets) {
            if (identical(proxy, v)) next
            z <- ok & is.finite(d[[v]]) & is.finite(d[[proxy]])
            r <- cor(d[[proxy]][z], d[[v]][z])
            add(part = "1_corr", stat = proxy, var = v, value = r,
                n_obs = sum(z))
            message(sprintf("[u1] corr(%-12s, %-26s) = %+.3f  N=%d",
                            proxy, v, r, sum(z)))
        }
    }

    # Direct sensitivity: does adding the coverage proxy as a control move
    # the headline coefficient? This is the question the correlations only
    # gesture at.
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
        for (sp in c("OLS", "IV-B")) {
            m <- fits[[sp]]
            cn <- if (sp == "OLS") main_treatment
                  else paste0("fit_", main_treatment)
            cc <- safe_coef(m, cn)
            add(part = "1_beta", stat = spec, var = sp, value = cc$est,
                n_obs = nobs(m), se = cc$se, p_value = cc$p)
            message(sprintf("[u1] %-12s %-5s b=%+.4f se=%.4f p=%.3f N=%d",
                            spec, sp, cc$est, cc$se, cc$p, nobs(m)))
        }
    }
    do.call(rbind, lapply(rows, function(r) {
        for (col in c("se", "p_value")) if (is.null(r[[col]])) r[[col]] <- NA_real_
        r[, c("part", "stat", "var", "value", "se", "p_value", "n_obs")]
    }))
}

# ---------------------------------------------------------------------------
# Part 2: outcomes measured on one universe (1970 -> 1991, both IPUMS).
# ---------------------------------------------------------------------------
run_part2 <- function(d, t9) {
    rows <- list()
    add <- function(...) rows[[length(rows) + 1L]] <<-
        data.frame(..., stringsAsFactors = FALSE)

    outcomes <- list(
        list(tag = "pop_91_60_published", y = "chg_log_pop_91_60",
             anchor = "chg_log_pop_91_60"),
        list(tag = "pop_91_70_consistent", y = "chg_log_pop_91_70", anchor = NA),
        list(tag = "urbshr_91_60_published", y = "chg_urbshr_91_60",
             anchor = "chg_urbshr_91_60")
        # NO urbshr_91_70 ROW. urbpop_1970 is entirely NA in the IPUMS
        # extract (0 of 311 non-missing), so no 1970 urban share can be
        # formed and the urban-share outcome CANNOT be put on a
        # consistent universe with the data in hand. Asserted below so
        # this stops being true silently if the extract is refreshed.
    )
    stopifnot(all(is.na(d$urbpop_1970)))
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
            # The as-published rows must reproduce Table 9, so a drifting
            # baseline fails here rather than producing a difference that
            # looks like a finding.
            if (!is.na(o$anchor)) {
                # Table 9's CSV labels the hypothetical-instrument column
                # "IV-H"; Table 7's uses "IV-Hypo". Anchor to the label
                # this file actually carries.
                sp9 <- sp
                r <- t9[t9$outcome == o$anchor & t9$spec == sp9, ]
                stopifnot(nrow(r) == 1L,
                          abs(cc$est - r$estimate) < 1e-8,
                          abs(cc$se - r$std_err) < 1e-8,
                          nobs(m) == r$n_obs)
            }
            add(part = "2_window", stat = o$tag, var = sp, value = cc$est,
                se = cc$se, p_value = cc$p, n_obs = nobs(m))
            message(sprintf("[u2] %-24s %-6s b=%+.4f se=%.4f p=%.3f N=%d",
                            o$tag, sp, cc$est, cc$se, cc$p, nobs(m)))
        }
    }
    do.call(rbind, rows)
}

write_report <- function(res, meta, path) {
    g <- function(part, stat, var) {
        v <- res$value[res$part == part & res$stat == stat & res$var == var]
        stopifnot(length(v) == 1L); v
    }
    gse <- function(part, stat, var) {
        v <- res$se[res$part == part & res$stat == stat & res$var == var]
        stopifnot(length(v) == 1L); v
    }
    gp <- function(part, stat, var) {
        v <- res$p_value[res$part == part & res$stat == stat & res$var == var]
        stopifnot(length(v) == 1L); v
    }

    sink(path)
    cat(strrep("=", 78), "\n")
    cat("THE 1960 POPULATION UNIVERSE MISMATCH AND WHAT IT DOES\n")
    cat(sprintf("Generated: %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
    cat(strrep("=", 78), "\n\n")

    cat("PART 0 - THE MISMATCH, FROM IN-REPO QUANTITIES ONLY\n")
    cat(sprintf("  census_1960_ipums.parquet: %d districts\n",
                meta$n_districts))
    cat(sprintf("    pop      %14s\n", format(meta$tot60, big.mark = ",")))
    cat(sprintf("    urbpop   %14s   (localities > 2000)\n",
                format(meta$urb60, big.mark = ",")))
    cat(sprintf("    rur      %14s   (localities <= 2000)\n",
                format(meta$rur60, big.mark = ",")))
    cat(sprintf("    of which Capital Federal %s, rest %s\n",
                format(meta$caba, big.mark = ","),
                format(meta$non_caba, big.mark = ",")))
    cat(sprintf("  implied 1960 urban share: %.1f%%\n",
                100 * meta$urb_share))
    cat(sprintf("  estimation sample (311 districts, CABA dropped):\n"))
    cat(sprintf("    pop_1960 %14s\n", format(meta$est_pop60, big.mark = ",")))
    cat(sprintf("    pop_1970 %14s\n", format(meta$est_pop70, big.mark = ",")))
    cat(sprintf("    pop_1991 %14s\n", format(meta$est_pop91, big.mark = ",")))
    cat(sprintf("  implied 1960->1970 growth: %.2fx over one decade (%.1f%%/yr)\n",
                meta$est_pop70 / meta$est_pop60,
                100 * ((meta$est_pop70 / meta$est_pop60)^(1/10) - 1)))
    cat(sprintf("  implied 1960->1991 growth: %.2fx\n",
                meta$est_pop91 / meta$est_pop60))
    cat("  READ THIS AS: a decadal growth rate and an urban share of that\n")
    cat("  size are not credible for Argentina in 1960. Both are what you\n")
    cat("  get when a locality-list denominator meets a full-population\n")
    cat("  numerator. The source has no dispersed-rural rows to include,\n")
    cat("  so this is a property of the data, not a coding error.\n")
    cat("  OPEN: confirm against the published departamento totals in the\n")
    cat("  1960 volumes, which neither digitization captured.\n\n")

    cat("PART 1 - IS THE ERROR TREATMENT-CORRELATED?\n")
    cat("  Coverage proxy c_i = pop_1960 / pop_1970. Distribution:\n")
    for (s in c("q5%", "q25%", "q50%", "q75%", "q95%")) {
        cat(sprintf("    %-5s %.3f\n", s, g("1_coverage", s, "cov60")))
    }
    cat("\n  Correlations (two proxies; agreement is the signal):\n")
    cat(sprintf("    %-28s %9s %9s\n", "target", "cov60", "urbshr60"))
    for (v in unique(res$var[res$part == "1_corr"])) {
        a <- res$value[res$part == "1_corr" & res$stat == "cov60" &
                       res$var == v]
        b <- res$value[res$part == "1_corr" & res$stat == "urbshr_1960" &
                       res$var == v]
        cat(sprintf("    %-28s %+9.3f %+9.3f\n", v,
                    if (length(a)) a else NA_real_,
                    if (length(b)) b else NA_real_))
    }
    cat("\n  Headline population IV, with and without the coverage proxy\n")
    cat("  as a control (the direct sensitivity):\n")
    for (sp in c("OLS", "IV-B")) {
        cat(sprintf("    %-5s as published %+.4f (%.4f) p=%.3f | + c_i %+.4f (%.4f) p=%.3f\n",
                    sp,
                    g("1_beta", "as_published", sp),
                    gse("1_beta", "as_published", sp),
                    gp("1_beta", "as_published", sp),
                    g("1_beta", "plus_cov60", sp),
                    gse("1_beta", "plus_cov60", sp),
                    gp("1_beta", "plus_cov60", sp)))
    }
    cat("\n  LIMIT (fixed before the numbers were seen): c_i is\n")
    cat("  contaminated by genuine 1960-70 growth, so a low value can mean\n")
    cat("  high dispersed-rural share OR fast growth. If the two proxies\n")
    cat("  disagree in sign against the treatment or the instruments, this\n")
    cat("  part is inconclusive and must be reported as such.\n\n")

    cat("PART 2 - OUTCOMES ON ONE UNIVERSE (1970->1991, both IPUMS)\n")
    cat(sprintf("  %-26s %-6s %10s %9s %8s %6s\n",
                "Outcome / window", "Spec", "beta", "SE", "p", "N"))
    for (tg in unique(res$stat[res$part == "2_window"])) {
        for (sp in c("OLS", "IV-LP", "IV-H", "IV-B")) {
            cat(sprintf("  %-26s %-6s %+10.4f %9.4f %8.3f %6d\n", tg, sp,
                        g("2_window", tg, sp), gse("2_window", tg, sp),
                        gp("2_window", tg, sp),
                        as.integer(res$n_obs[res$part == "2_window" &
                                             res$stat == tg &
                                             res$var == sp])))
        }
    }
    cat("\n  The 1970-91 rows remove the mismatch and cost a decade of the\n")
    cat("  window; the treatment is unchanged (1960-86 market access). If\n")
    cat("  the coefficient survives here, the mismatch is a documentation\n")
    cat("  and data-request item. If it does not, it is a results item.\n")
    cat("  Precedent for the shorter window: every outcome in Table 11 is\n")
    cat("  measured 1970-91 because IPUMS has no earlier microdata.\n\n")
    cat("  NOT FIXABLE THE SAME WAY: the urban-share outcome. urbpop_1970\n")
    cat("  is entirely missing from the IPUMS extract (0 of 311), so no\n")
    cat("  1970 urban share can be formed and chg_urbshr_91_60 keeps the\n")
    cat("  mismatch with no in-repo remedy. Its 1960 leg also sits on the\n")
    cat("  inflated locality-universe denominator (implied urban share\n")
    cat("  above). Treat that outcome as the weakest of the four in\n")
    cat("  Table 9 until a 1970 urban indicator is obtained.\n")
    sink()
}

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)

    message("\n", strrep("=", 72))
    message("diagnostic_pop1960_universe.R  |  1960 universe mismatch")
    message(strrep("=", 72))

    d <- ensure_geolev2_char(as.data.frame(arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))))
    c60 <- ensure_geolev2_char(as.data.frame(arrow::read_parquet(
        file.path(dir_derived_base, "census_1960",
                  "census_1960_ipums.parquet"))))
    stopifnot(nrow(d) == 311L, nrow(c60) == 312L)

    meta <- describe_universes(c60, d)
    message(sprintf("[u0] 1960 total %s (CABA %s); implied urban share %.1f%%",
                    format(meta$tot60, big.mark = ","),
                    format(meta$caba, big.mark = ","),
                    100 * meta$urb_share))
    message(sprintf("[u0] implied 1960->1970 growth %.2fx over a decade",
                    meta$est_pop70 / meta$est_pop60))

    # Coverage proxy and the universe-consistent outcomes.
    d$cov60 <- ifelse(is.finite(d$pop_1970) & d$pop_1970 > 0,
                      d$pop_1960 / d$pop_1970, NA_real_)
    d$chg_log_pop_91_70 <- ifelse(
        is.finite(d$pop_1991) & d$pop_1991 > 0 &
        is.finite(d$pop_1970) & d$pop_1970 > 0,
        log(d$pop_1991) - log(d$pop_1970), NA_real_)
    d$urbshr_1970 <- ifelse(is.finite(d$pop_1970) & d$pop_1970 > 0,
                            d$urbpop_1970 / d$pop_1970, NA_real_)
    d$chg_urbshr_91_70 <- d$urbshr_1991 - d$urbshr_1970

    t9 <- read.csv(file.path(dir_tables, "table_9_population_iv.csv"),
                   stringsAsFactors = FALSE)

    res <- rbind(run_part1(d), run_part2(d, t9))
    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)
    csv_path <- file.path(dir_tables, "diagnostic_pop1960_universe.csv")
    write.csv(res, csv_path, row.names = FALSE)
    write_report(res, meta,
                 file.path(dir_tables, "diagnostic_pop1960_universe.txt"))
    message(sprintf("[u] Saved: %s and .txt", csv_path))
}

main()

# ===========================================================================
# diagnostic_modern_iv.R
#
# PURPOSE: Modern weak-IV inference check (Cote reading note #35).
#          For the paper's headline IV cells, reports side by side:
#
#   (1) F_classical — the non-robust first-stage F the paper currently
#       quotes (fixest type = "ivf" via fitstat_F; kept for continuity).
#   (2) F_robust — heteroskedasticity-robust first-stage Wald F
#       (fixest type = "ivwald" on the HC1-fitted model).
#   (3) F_eff — Montiel Olea & Pflueger (2013) effective F. Following
#       Pflueger & Wang (2015), included exogenous controls are
#       partialled out of the treatment and the instruments first; then
#           F_eff = pi' Q_zz pi / tr(Sigma Q_zz),
#       with pi the first-stage instrument coefficients, Sigma their
#       HC1 vcov block, Q_zz = Z~'Z~ on the residualized instruments.
#       Cross-checked against the ivDiag reference implementation
#       (Lal, Lockhart, Xu & Zu 2023, R/effF.R) with ONE DELIBERATE
#       DIVERGENCE: ivDiag builds Q_zz from the instruments WITHOUT
#       residualizing them on the included controls; we residualize,
#       per the MOP definition and Stata's weakivtest convention.
#       Verified consequence on the total-pop IV-B cell: residualized
#       13.11 (ours) vs ivDiag-style 14.39 — so a future cross-check
#       against ivDiag will differ BY DESIGN, not by bug (cr-review
#       PR #131 SF2). For a SINGLE instrument F_eff = pi^2/Sigma = the
#       robust first-stage t^2 (Q_zz cancels algebraically), so the
#       K=1 cells anchor pi and Sigma against fixest's independent
#       ivwald — but NOT the Q_zz residualization convention, which
#       only matters at K=2. That convention was validated separately:
#       the PR #131 review rebuilt the K=2 total-pop F_eff through an
#       independent path (full-design lm + hand-built HC1 sandwich)
#       and matched to all printed digits (13.1136367247).
#   (4) Conventional 95% CI (2SLS point estimate +/- 1.96 HC1 SE).
#   (5) Anderson-Rubin 95% confidence set by robust test inversion:
#       for each beta0 on a grid, regress (Y~ - beta0 D~) on Z~ (all
#       residualized on controls) and compute the HC1 joint Wald F of
#       the instruments; the set collects beta0 with p >= 0.05.
#       Set shapes handled as in ivDiag R/AR_test.R: bounded interval,
#       half-lines, disjoint union (-inf,a] U [b,inf), whole line,
#       empty. (Like ivDiag, the shape logic assumes at most one
#       accepted run, which is what AR acceptance regions — ratios of
#       quadratics in beta0 — can produce.) Grid: beta_hat +/- 3 SE in
#       0.02 SE steps, tails to +/- 25 SE at 0.05 SE spacing;
#       endpoint acceptance => unbounded classification.
#
# CELLS (the paper's headline IV surface, 27 cells since PR #158):
#   every Table 9 and Table 10 outcome x {IV-LP, IV-H, IV-B} -- the same
#   grid whose AR sets the tables now print, so this file is the
#   validated record for every cell a reader sees.
#
# WHAT THIS DOES NOT DO: MOP critical values for the 2-instrument
#   IV-B cells (they require the Patnaik-approximation simulation);
#   for K = 1, F_eff = F_robust and conventional benchmarks apply.
#   MOP critical values for the K=2 cells live in
#   diagnostic_mop_critical.R (PR #136), not here.
#
# NO LONGER DIAGNOSTIC-ONLY (PR #155): eff_F() and fitstat_F_robust()
#   moved to _iv_helpers.R and Tables 8, 9 and 10 now report the effective
#   F from them. This script remains the validation of that implementation
#   (the ivDiag cross-check above) and the home of the AR sets.
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/diagnostic_modern_iv.txt
#   results/tables/diagnostic_modern_iv.csv
# ===========================================================================
suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
})

# ar_p() and ar_invert() moved to _iv_helpers.R in PR #158, so that
# Tables 9 and 10 report the AR set from the implementation this
# diagnostic validated rather than a second copy. The former file-level
# AR_ALPHA constant became ar_invert()'s `alpha` argument; this script
# takes the default 0.05, which is what AR_ALPHA was.

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)

    est <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))
    est <- ensure_geolev2_char(est)

    lp_instr   <- "chg_logMA_stu_s0_elow"
    hypo_instr <- main_hypo_instrument
    endog      <- "chg_logMA_86_60_s0_elow"

    # All three IV specs for every outcome, 27 cells. Was 11 (IV-LP/IV-H
    # only for total population): PR #158 put an AR row in Tables 9-10 for
    # ALL THREE IV columns, which meant 16 of the table's AR cells --
    # including every unbounded IV-Hypo set the Section 5.1 prose leans
    # on -- existed only in the tables and not in this validated record.
    # The grid covers them all now (cr-review PR #158).
    outcomes_all <- list(
        c("chg_log_pop_91_60",        "T9 total pop"),
        c("chg_log_urbpop_91_60",     "T9 urban pop"),
        c("chg_log_rur_91_60",        "T9 rural pop"),
        c("chg_urbshr_91_60",         "T9 urban share"),
        c("chg_log_valprod_85_54",    "T10 mfg value"),
        c("chg_log_massal_85_54",     "T10 wage mass"),
        c("chg_log_nestab_85_54",     "T10 establish."),
        c("chg_log_nexp_88_60",       "T10 farms"),
        c("chg_log_areatot_ha_88_60", "T10 farmed area")
    )
    cells <- do.call(c, lapply(outcomes_all, function(o) {
        lapply(c("IV-LP", "IV-H", "IV-B"), function(sp) {
            list(y = o[1], lab = o[2], spec = sp)
        })
    }))

    rows <- list()
    for (cell in cells) {
        instrs <- switch(cell$spec,
                         "IV-LP" = lp_instr,
                         "IV-H"  = hypo_instr,
                         "IV-B"  = c(lp_instr, hypo_instr))
        rows[[length(rows) + 1L]] <-
            run_cell(cell, est, endog, instrs, lp_instr, hypo_instr)
    }
    df <- do.call(rbind, rows)
    write_outputs(df)
}

# ---------------------------------------------------------------------------
# One cell: fixest fits + hand-rolled effective F + AR inversion
# ---------------------------------------------------------------------------
run_cell <- function(cell, est, endog, instrs, lp_instr, hypo_instr) {
    # Complete-case sample on every variable used, so the hand-rolled
    # pieces and the fixest fits see the identical sample.
    vars <- c(cell$y, endog, lp_instr, hypo_instr, geo_controls_main)
    d <- est[complete.cases(est[, vars]), vars]

    fits <- fit_iv_quad(
        y          = cell$y,
        data       = d,
        endog      = endog,
        lp_instr   = lp_instr,
        hypo_instr = hypo_instr,
        ctrls_vec  = geo_controls_main
    )
    m <- fits[[cell$spec]]
    stopifnot(nobs(m) == nrow(d))
    co <- safe_coef(m, paste0("fit_", endog))

    # Residualize on controls + intercept (Frisch-Waugh / Pflueger-Wang)
    X <- as.matrix(cbind(1, d[, geo_controls_main]))
    r <- function(v) as.numeric(qr.resid(qr(X), v))
    Dt <- r(d[[endog]])
    Zt <- sapply(instrs, function(z) r(d[[z]]))
    Zt <- matrix(Zt, ncol = length(instrs))
    Yt <- r(d[[cell$y]])
    n  <- nrow(d)

    ef <- eff_F(Dt, Zt, n_ctrl = ncol(X))
    ar <- ar_invert(Yt, Dt, Zt, n_ctrl = ncol(X),
                    beta_hat = co$est, se_hat = co$se)

    data.frame(
        outcome     = cell$y,
        label       = cell$lab,
        spec        = cell$spec,
        n_obs       = n,
        beta        = co$est,
        se          = co$se,
        ci_lo       = co$est - qnorm(0.975) * co$se,
        ci_hi       = co$est + qnorm(0.975) * co$se,
        F_classical = fitstat_F(m),
        F_robust    = fitstat_F_robust(m),
        F_eff       = ef,
        ar_p_at_0   = ar$p0,
        ar_set      = ar$print,
        ar_bounded  = ar$bounded,
        stringsAsFactors = FALSE
    )
}

# ---------------------------------------------------------------------------
# eff_F() and fitstat_F_robust() moved to _iv_helpers.R in PR #155, so that
# Tables 8-10 report the effective F from the same implementation this
# diagnostic validated rather than a second copy of it. Sourced in main().

# ---------------------------------------------------------------------------
# AR test inversion (robust). Returns p at beta = 0, the 95% set as a
# print string, and a boundedness flag. Set-shape logic as in ivDiag.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Report + CSV
# ---------------------------------------------------------------------------
write_outputs <- function(df) {
    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)
    csv_path <- file.path(dir_tables, "diagnostic_modern_iv.csv")
    write.csv(df, csv_path, row.names = FALSE)

    txt_path <- file.path(dir_tables, "diagnostic_modern_iv.txt")
    con <- file(txt_path, open = "wt")
    wline <- function(...) { line <- sprintf(...); cat(line, "\n")
                             cat(line, "\n", file = con) }
    wline("%s", strrep("=", 84))
    wline("MODERN WEAK-IV INFERENCE CHECK (Cote reading note #35)")
    wline("F_classical = the paper's current (non-robust) first-stage F;")
    wline("F_robust = HC1 first-stage Wald F (fixest ivwald);")
    wline("F_eff = Montiel Olea-Pflueger effective F (controls partialled")
    wline("out; = F_robust up to dof conventions when K = 1, distinct for")
    wline("the 2-instrument IV-B cells). AR set = 95%% Anderson-Rubin")
    wline("confidence set by robust test inversion. Diagnostic only.")
    wline("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
    wline("%s", strrep("=", 84))
    wline("")
    wline("%-15s %-6s %4s %-16s %7s %7s %7s  %-26s",
          "Cell", "Spec", "N", "beta (SE)", "F_cls", "F_rob", "F_eff",
          "AR 95% set")
    for (i in seq_len(nrow(df))) {
        r <- df[i, ]
        wline("%-15s %-6s %4d %-16s %7.1f %7.1f %7.1f  %-26s",
              r$label, r$spec, r$n_obs,
              sprintf("%+.3f (%.3f)", r$beta, r$se),
              r$F_classical, r$F_robust, r$F_eff, r$ar_set)
    }
    # Computed reading notes
    k1 <- df[df$spec != "IV-B", ]
    reldev <- max(abs(k1$F_eff - k1$F_robust) / k1$F_robust)
    ivb <- df[df$spec == "IV-B", ]
    n_unbounded <- sum(!df$ar_bounded)
    wline("")
    wline("Reading notes:")
    wline("- Single-instrument anchor: F_eff vs fixest's F_robust agree to")
    wline("  a max relative deviation of %.2f%% across the K=1 cells (dof",
          100 * reldev)
    wline("  conventions only), validating the hand-rolled formula.")
    wline("- IV-B cells: F_eff between %.1f and %.1f vs robust Wald %.1f",
          min(ivb$F_eff), max(ivb$F_eff), min(ivb$F_robust))
    wline("  to %.1f. MOP critical values for K=2 are NOT computed here",
          max(ivb$F_robust))
    wline("  (Patnaik approximation); for K=1 conventional benchmarks")
    wline("  apply to F_eff directly.")
    wline("- AR sets: %d of %d cells are unbounded or otherwise",
          n_unbounded, nrow(df))
    wline("  non-standard (the informative weak-IV outcome); see the")
    wline("  set column. Bounded AR sets close to the Wald CIs indicate")
    wline("  the conventional inference is not weak-IV fragile there.")
    wline("")
    wline("Full columns (CIs, AR p at beta=0): diagnostic_modern_iv.csv")
    close(con)
    message("\nSaved: ", txt_path)
    message("Saved: ", csv_path)
}

main()

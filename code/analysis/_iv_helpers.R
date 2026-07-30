# ===========================================================================
# _iv_helpers.R
#
# PURPOSE: Shared helpers for the analysis tables that follow the
#          four-column IV specification template:
#            (1) OLS
#            (2) IV-LP    (Larkin-Plan instrument)
#            (3) IV-Hypo  (hypothetical-road instrument)
#            (4) IV-Both  (both instruments)
#
# Used by: table_6_pre_balance.R, table_7_pre_trends.R,
#          table_9_population.R, table_10_sectoral.R, and any future
#          table that consumes the same 4-spec grid.
#
# Helpers exported:
#   fit_iv_quad(y, data, endog, lp_instr, hypo_instr, ctrls_vec)
#       Fits the 4 specifications for outcome y and returns a named
#       list with keys "OLS", "IV-LP", "IV-H", "IV-B". Uses HC1
#       (heteroskedasticity-robust) standard errors.
#
#   safe_coef(model, cname)
#       Returns a named list (est, se, t, p) for the coefficient
#       cname, or all NA if the coefficient is absent. For IV columns
#       the coefficient on the instrumented regressor is prefixed
#       "fit_" by fixest.
#
#   fitstat_F(iv_model)
#       Returns the first-stage F for the excluded instrument(s),
#       fixest type = "ivf": the IID F, NOT the robust Wald
#       ("ivwald") — the distinction matters when quoting against
#       weak-IV thresholds (cr-review PR #123; see also the open
#       modern-IV ledger item, Cote note #35).
#       Defensive across fixest versions (handles both the list-of-
#       stats and the simplified return shapes).
#
#   fitstat_F_all(iv_model)
#       Named vector of per-endogenous-regressor first-stage Wald Fs
#       (keys "ivf1::<endog_var>"). For models with >1 endogenous
#       regressor, where fitstat_F reports only the first.
#
# USAGE:
#   source(file.path(dir_code, "analysis", "_iv_helpers.R"))
#   models <- fit_iv_quad(y = "chg_log_pop_91_60",
#                         data = d,
#                         endog = "chg_logMA_86_60_s0_elow",
#                         lp_instr = "chg_logMA_stu_s0_elow",
#                         hypo_instr = main_hypo_instrument,
#                         ctrls_vec = geo_controls_main)
#   # models is list(OLS=, "IV-LP"=, "IV-H"=, "IV-B"=)
# ===========================================================================

suppressPackageStartupMessages({
    library(fixest)
})

fit_iv_quad <- function(y, data, endog, lp_instr, hypo_instr, ctrls_vec) {
    ctrls_expr <- paste(ctrls_vec, collapse = " + ")

    f_ols <- as.formula(sprintf("%s ~ %s + %s", y, endog, ctrls_expr))
    m_ols <- feols(f_ols, data = data, vcov = "hetero")

    f_iv_lp <- as.formula(sprintf(
        "%s ~ %s | %s ~ %s",
        y, ctrls_expr, endog, lp_instr))
    m_iv_lp <- feols(f_iv_lp, data = data, vcov = "hetero")

    f_iv_h <- as.formula(sprintf(
        "%s ~ %s | %s ~ %s",
        y, ctrls_expr, endog, hypo_instr))
    m_iv_h <- feols(f_iv_h, data = data, vcov = "hetero")

    f_iv_b <- as.formula(sprintf(
        "%s ~ %s | %s ~ %s + %s",
        y, ctrls_expr, endog, lp_instr, hypo_instr))
    m_iv_b <- feols(f_iv_b, data = data, vcov = "hetero")

    list(
        "OLS"   = m_ols,
        "IV-LP" = m_iv_lp,
        "IV-H"  = m_iv_h,
        "IV-B"  = m_iv_b
    )
}

safe_coef <- function(model, cname) {
    co <- summary(model)$coeftable
    if (!(cname %in% rownames(co))) {
        return(list(est = NA_real_, se = NA_real_,
                    t = NA_real_, p = NA_real_))
    }
    list(est = co[cname, 1], se = co[cname, 2],
         t = co[cname, 3], p = co[cname, 4])
}

fitstat_F <- function(iv_model) {
    fs <- fitstat(iv_model, type = "ivf")
    if (is.list(fs) && !is.null(fs[[1]]$stat)) {
        return(as.numeric(fs[[1]]$stat))
    }
    fs2 <- fitstat(iv_model, type = "ivf", simplify = TRUE)
    if (is.list(fs2) && !is.null(fs2$stat)) return(as.numeric(fs2$stat))
    NA_real_
}

# Per-endogenous-regressor first-stage Wald Fs. fitstat_F above returns
# only the first; with >1 endogenous regressor (e.g. an instrumented
# interaction, diagnostic_heterogeneity.R) each one has its own first
# stage. Returns a named vector keyed "ivf1::<endog_var>" so callers can
# index by name instead of relying on formula position.
fitstat_F_all <- function(iv_model) {
    fs <- tryCatch(fitstat(iv_model, type = "ivf"), error = function(e) NULL)
    if (is.null(fs)) return(c(none = NA_real_))
    vapply(fs, function(x) {
        if (is.list(x) && !is.null(x$stat)) as.numeric(x$stat) else NA_real_
    }, numeric(1))
}

# Insert a \label{...} right after the FIRST \caption{...} in a tex string.
# Used by multi-panel tables (e.g. Tables 9, 10, 11) so that paper-side
# \ref{tab:foo} resolves to the first panel rather than going undefined.
inject_first_label <- function(tex_text, label) {
    pattern  <- "(\\\\caption\\{[^}]*\\})"
    replace  <- sprintf("\\1\n\\\\label{%s}", label)
    sub(pattern, replace, tex_text, perl = TRUE)
}

# Append a reader-visible note inside a table float, immediately before
# the LAST \end{table} in a tex string. Notes carried only as LaTeX "%"
# comments in the generated file are invisible in the compiled PDF
# (cr-review PR #141): anything a reader needs in order to interpret a
# row has to go through this helper. Each panel of a multi-panel table
# is its own float, so the note is applied per panel to keep every float
# self-contained.
# LAYOUT, and why it is not just \vspace + text (cr-review PR #157). The
# first version emitted "\vspace{0.4em}" followed by the note with no
# \par, so the note shared a paragraph with \begin{tabular}[t]. Inside a
# \centering float against a top-aligned tabular, that put the note's
# first line to the RIGHT of the table and above the column headers, with
# the rest wrapping underneath. LaTeX issues no warning for it and the
# PDF text layer extracts in the right order, so neither the compile check
# nor a pdftotext grep catches it -- it was found by rendering the page to
# an image. The \par closes the tabular's paragraph and the minipage gives
# the note its own full-width, left-ragged block.
add_table_note <- function(tex_text, note) {
    marker <- "\\end{table}"
    pos <- max(gregexpr(marker, tex_text, fixed = TRUE)[[1]])
    if (pos < 0) return(tex_text)
    block <- sprintf(paste0(
        "\\par\\vspace{0.4em}\n",
        "\\begin{minipage}{\\linewidth}\\raggedright\n",
        "{\\footnotesize \\textit{Notes:} %s}\n",
        "\\end{minipage}\n"
    ), note)
    paste0(substr(tex_text, 1, pos - 1), block,
           substr(tex_text, pos, nchar(tex_text)))
}

# ---------------------------------------------------------------------------
# sargan_p(iv_model, k_instr)
#
# Classical (homoskedastic) Sargan overidentification p-value. Returns
# NA for a just-identified fit (k_instr < 2), where the test does not
# exist. Defensive about fitstat's return shape across fixest versions,
# like fitstat_F above.
#
# CAVEAT for callers: this statistic assumes homoskedasticity, while
# every specification in this project uses HC1. Quote the
# identification-robust counterpart (J from the minimized
# Anderson-Rubin statistic, computed in
# code/analysis/diagnostic_modern_iv_table11.R) as the primary
# evidence and this one for comparability with the classical
# literature.
# ---------------------------------------------------------------------------
sargan_p <- function(iv_model, k_instr) {
    if (k_instr < 2L) return(NA_real_)
    s <- tryCatch(fitstat(iv_model, type = "sargan"),
                  error = function(e) NULL)
    if (is.list(s) && is.list(s$sargan) && !is.null(s$sargan$p)) {
        return(as.numeric(s$sargan$p))
    }
    NA_real_
}

# ---------------------------------------------------------------------------
# swap_pop_baseline_1947(ctrls)
#
# Replace the 1960 population baseline with the 1947 one, in place, keeping
# every other control the caller passed.
#
# WHY THIS EXISTS. The pre-trends placebo's outcome is
# log(pop_1960) - log(pop_1947), so log_pop_1960 is the TERMINAL level of
# the window being tested: conditioning on it conditions on a component of
# the outcome. log_pop_1947 is the INITIAL level, an ordinary convergence
# control. config.R encodes that swap once as placebo_controls, for Table 7.
#
# WHY A TRANSFORM RATHER THAN placebo_controls DIRECTLY. The diagnostics
# that estimate the placebo outcome do not use geo_controls_main verbatim:
# diagnostic_recentering_treatments.R substitutes a sector-consistent
# baseline logMA, diagnostic_recentering_controls.R adds mu, rail baselines,
# a lat/lon quadratic and fixed effects across its C0-C6 ladder, and others
# add mu alone. Substituting placebo_controls wholesale would silently throw
# those modifications away. This swaps ONLY the population baseline and
# leaves the rest of each script's set intact.
#
# INVARIANT, asserted below: applied to geo_controls_main this reproduces
# placebo_controls exactly, so the diagnostics and Table 7 cannot drift
# apart. Both are built by the same setdiff-then-append construction.
# ---------------------------------------------------------------------------
swap_pop_baseline_1947 <- function(ctrls) {
    stopifnot(is.character(ctrls), "log_pop_1960" %in% ctrls,
              !("log_pop_1947" %in% ctrls))
    c(setdiff(ctrls, "log_pop_1960"), "log_pop_1947")
}

if (exists("geo_controls_main") && exists("placebo_controls")) {
    stopifnot(
        "swap_pop_baseline_1947(geo_controls_main) must equal placebo_controls" =
            identical(swap_pop_baseline_1947(geo_controls_main),
                      placebo_controls)
    )
}

# ---------------------------------------------------------------------------
# Robust first-stage Wald F from fixest (type = "ivwald"), defensive across
# fixest versions like fitstat_F above.
#
# Moved here from diagnostic_modern_iv.R (PR #155) so the paper's tables can
# report the same statistics the diagnostic does, computed by the same code
# rather than read out of the diagnostic's CSV. Coupling a table to a
# diagnostic's output would make table order depend on diagnostic order in
# main.R, which is exactly the kind of hidden dependency the AEA checklist
# warns about.
# ---------------------------------------------------------------------------
fitstat_F_robust <- function(iv_model) {
    fs <- tryCatch(fitstat(iv_model, type = "ivwald"),
                   error = function(e) NULL)
    if (is.list(fs) && !is.null(fs[[1]]$stat)) return(as.numeric(fs[[1]]$stat))
    fs2 <- tryCatch(fitstat(iv_model, type = "ivwald", simplify = TRUE),
                    error = function(e) NULL)
    if (is.list(fs2) && !is.null(fs2$stat)) return(as.numeric(fs2$stat))
    NA_real_
}

# ---------------------------------------------------------------------------
# eff_F(Dt, Zt, n_ctrl): Montiel Olea & Pflueger (2013) effective F.
#
#   F_eff = pi' Q_zz pi / tr(Sigma Q_zz)
#
# with pi the first-stage instrument coefficients, Sigma their HC1 vcov
# block and Q_zz = Z~'Z~. Following Pflueger & Wang (2015), the included
# exogenous controls must ALREADY be partialled out of both Dt and Zt by the
# caller (see eff_F_from_fit below, which does it).
#
# Cross-checked against the ivDiag reference implementation (Lal, Lockhart,
# Xu & Zu 2023, R/effF.R) with ONE DELIBERATE DIVERGENCE: ivDiag builds Q_zz
# from instruments WITHOUT residualizing them on the controls; we
# residualize, per the MOP definition and Stata's weakivtest convention.
# Verified consequence on the total-pop IV-B cell: 13.11 (ours) vs 14.39
# (ivDiag-style), so a future cross-check will differ BY DESIGN, not by bug.
#
# For a SINGLE instrument F_eff = pi^2 / Sigma, i.e. the robust first-stage
# Wald F, and the conventional benchmarks apply.
#
# Moved here from diagnostic_modern_iv.R (PR #155).
# ---------------------------------------------------------------------------
eff_F <- function(Dt, Zt, n_ctrl) {
    n <- length(Dt)
    k <- ncol(Zt)
    qz  <- qr(Zt)
    pi_ <- qr.coef(qz, Dt)
    e   <- qr.resid(qz, Dt)
    # HC1 with dof matching the full first stage (controls + instruments)
    ZZ    <- crossprod(Zt)
    ZZinv <- solve(ZZ)
    meat  <- crossprod(Zt * e, Zt * e)
    hc1   <- n / (n - n_ctrl - k)
    Sigma <- hc1 * ZZinv %*% meat %*% ZZinv
    as.numeric(t(pi_) %*% ZZ %*% pi_ / sum(diag(Sigma %*% ZZ)))
}

# ---------------------------------------------------------------------------
# eff_F_from_fit(data, endog, instrs, ctrls_vec): the effective F for one
# (treatment, instrument set, control set) triple, doing the
# Frisch-Waugh-Lovell residualization eff_F() requires.
#
# `data` must already be the ESTIMATION SAMPLE -- complete cases on every
# variable the corresponding IV fit used. Passing a wider frame silently
# computes the statistic on a different sample from the coefficient it sits
# beside in a table, so this asserts completeness rather than trusting it.
# ---------------------------------------------------------------------------
eff_F_from_fit <- function(data, endog, instrs, ctrls_vec) {
    vars <- c(endog, instrs, ctrls_vec)
    stopifnot(all(vars %in% names(data)))
    stopifnot("eff_F_from_fit(): data must be the estimation sample" =
                  all(complete.cases(data[, vars])))
    X <- as.matrix(cbind(1, data[, ctrls_vec]))
    qx <- qr(X)
    resid_on_ctrls <- function(v) as.numeric(qr.resid(qx, v))
    Dt <- resid_on_ctrls(data[[endog]])
    Zt <- matrix(sapply(instrs, function(z) resid_on_ctrls(data[[z]])),
                 ncol = length(instrs))
    eff_F(Dt, Zt, n_ctrl = ncol(X))
}

# ---------------------------------------------------------------------------
# f_rows_note(classical_row_is_robust): the shared sentences explaining the
# two first-stage F rows that Tables 8, 9 and 10 all carry.
#
# WHY SHARED (cr-review PR #157). Three tables were carrying near-identical
# versions of this paragraph, and they had already drifted: Table 10's copy
# said "with a single instrument the two coincide by construction", which is
# true of Table 8 -- whose upper row is a ROBUST Wald F -- and FALSE of
# Table 10, whose upper row is the CLASSICAL F (7.0 vs 4.4 in Panel A
# column 3). One string with the one genuine difference as an argument.
#
# classical_row_is_robust: TRUE for Table 8, whose "First-stage F" is a
#   squared robust t / robust Wald statistic; FALSE for Tables 9 and 10,
#   whose row comes from fitstat_F() and assumes homoskedasticity. This is
#   a real difference between the tables, deliberately not harmonised
#   (agenda item C), so it is stated rather than papered over.
# ---------------------------------------------------------------------------
f_rows_note <- function(classical_row_is_robust) {
    upper <- if (classical_row_is_robust) {
        paste(
            "``First-stage $F$'' is heteroskedasticity-robust: a squared",
            "robust $t$ in the single-instrument columns and a robust Wald",
            "statistic in the two-instrument column."
        )
    } else {
        paste(
            "``First-stage $F$'' is the Wald statistic for the excluded",
            "instrument(s) in that column, computed under homoskedasticity;",
            "note that the same row in Table~\\ref{tab:first_stage} is",
            "instead heteroskedasticity-robust, so the two tables' values",
            "are not the same statistic."
        )
    }
    coincide <- if (classical_row_is_robust) {
        paste(
            "With one instrument the two rows coincide by construction,",
            "which the single-instrument columns confirm; they differ only",
            "where two instruments are used."
        )
    } else {
        paste(
            "The effective $F$ equals the robust Wald $F$ by construction",
            "when there is one instrument, so in the single-instrument",
            "columns the two rows differ only in whether the variance is",
            "estimated under homoskedasticity -- and they are judged",
            "against different critical values regardless."
        )
    }
    paste(
        upper,
        "``Effective $F$ (MOP)'' is the Montiel Olea and Pflueger (2013)",
        "effective $F$, computed with the included controls partialled out",
        "of the treatment and the instruments. It is the one to read: these",
        "specifications use HC1, and with two instruments the classical $F$",
        "has no defined critical value.",
        coincide,
        "The Montiel Olea--Pflueger critical values these should be judged",
        "against are discussed in Section~\\ref{sec:first_stage}."
        # The AR pointer that used to end this string is gone (PR #158):
        # Tables 9 and 10 now carry the AR set as a row of their own,
        # explained by ar_row_note(), so pointing readers at a diagnostic
        # file for something one row up would be a contradiction. Table 8
        # has no outcome and hence no AR row, and its note has no reason
        # to mention AR at all.
    )
}

# ---------------------------------------------------------------------------
# ANDERSON-RUBIN INFERENCE. ar_p() and ar_invert() moved here from
# diagnostic_modern_iv.R in PR #158, for the same reason eff_F() was moved
# in PR #155: Tables 9 and 10 now report the AR set, and they should compute
# it with the implementation that diagnostic validated rather than a second
# copy of the algebra or a read of the diagnostic's CSV.
#
# WHY THE PAPER REPORTS THIS AT ALL. The effective F is judged against a
# Montiel Olea-Pflueger critical value that depends on an estimated bound on
# worst-case bias. For the two-instrument column that bound does real work:
# the exact critical value is 12.90 while the conservative (B = 1) version is
# 33.26, so "the joint spec is strong" holds under the exact computation and
# not under the conservative one. AR inference needs NO strength threshold of
# any kind -- it is valid whatever the first stage looks like -- so it is the
# statement that does not depend on that step.
#
# ar_p(beta0, ...): p-value of the AR test of H0: beta = beta0, from the
#   heteroskedasticity-robust Wald statistic on the instrument coefficients
#   in the regression of (Yt - beta0 * Dt) on Zt. Yt, Dt and Zt must already
#   be residualized on the controls.
# ---------------------------------------------------------------------------
ar_p <- function(beta0, Yt, Dt, Zt, n_ctrl) {
    u  <- Yt - beta0 * Dt
    n  <- length(u)
    k  <- ncol(Zt)
    qz <- qr(Zt)
    g  <- qr.coef(qz, u)
    e  <- qr.resid(qz, u)
    ZZinv <- solve(crossprod(Zt))
    meat  <- crossprod(Zt * e, Zt * e)
    df2   <- n - n_ctrl - k
    vcv   <- (n / df2) * ZZinv %*% meat %*% ZZinv
    Fst   <- as.numeric(t(g) %*% solve(vcv) %*% g) / k
    pf(Fst, k, df2, lower.tail = FALSE)
}

# ---------------------------------------------------------------------------
# ar_invert(): the AR confidence set, by inverting ar_p() over a grid, plus
# the p-value at zero. Returns list(print, bounded, p0).
#
# The grid is dense near beta_hat and sparse in the tails, and the set shape
# is read off whether the endpoints are accepted -- so an unbounded or
# disjoint set is reported as such rather than silently truncated to the
# grid. `bounded` is FALSE for those cases and callers should check it before
# printing a set as an interval.
#
# alpha is a parameter rather than a file-level constant (it was AR_ALPHA in
# diagnostic_modern_iv.R) so that a caller reporting a 95% set and a caller
# reporting something else cannot silently disagree.
# ---------------------------------------------------------------------------
ar_invert <- function(Yt, Dt, Zt, n_ctrl, beta_hat, se_hat, alpha = 0.05) {
    grid <- sort(unique(c(
        seq(beta_hat - 25 * se_hat, beta_hat - 3.1 * se_hat,
            by = 0.05 * se_hat),
        seq(beta_hat - 3 * se_hat, beta_hat + 3 * se_hat,
            by = 0.02 * se_hat),
        seq(beta_hat + 3.1 * se_hat, beta_hat + 25 * se_hat,
            by = 0.05 * se_hat)
    )))
    acc <- vapply(grid, function(b) {
        ar_p(b, Yt, Dt, Zt, n_ctrl) >= alpha
    }, logical(1))
    ng <- length(grid)
    fmt <- function(x) sprintf("%.3f", x)
    out <- if (all(acc)) {
        list(print = "(-Inf, Inf)", bounded = FALSE)
    } else if (!any(acc)) {
        list(print = "empty", bounded = FALSE)
    } else if (!acc[1] && !acc[ng]) {
        b <- range(grid[acc])
        list(print = sprintf("[%s, %s]", fmt(b[1]), fmt(b[2])), bounded = TRUE)
    } else if (acc[1] && acc[ng]) {
        b <- range(grid[!acc])
        list(print = sprintf("(-Inf, %s] U [%s, Inf)", fmt(b[1]), fmt(b[2])),
             bounded = FALSE)
    } else if (acc[1]) {
        b <- max(grid[acc])
        list(print = sprintf("(-Inf, %s]", fmt(b)), bounded = FALSE)
    } else {
        b <- min(grid[acc])
        list(print = sprintf("[%s, Inf)", fmt(b)), bounded = FALSE)
    }
    out$p0 <- ar_p(0, Yt, Dt, Zt, n_ctrl)
    out
}

# ---------------------------------------------------------------------------
# ar_from_fit(data, y, endog, instrs, ctrls_vec, beta_hat, se_hat): the AR
# set for one cell, doing the Frisch-Waugh-Lovell residualization that
# ar_p()/ar_invert() require. Same contract as eff_F_from_fit(): `data` must
# already be the estimation sample, asserted rather than trusted, because a
# statistic computed on a different sample from the coefficient beside it in
# a table is a silent error.
#
# Returns the list from ar_invert(), with $print ready for a table cell in
# LaTeX math ("$[0.035, 0.678]$" style formatting is the caller's job).
# ---------------------------------------------------------------------------
ar_from_fit <- function(data, y, endog, instrs, ctrls_vec,
                        beta_hat, se_hat, alpha = 0.05) {
    # safe_coef() returns all-NA on a missing coefficient, which would
    # crash seq() inside ar_invert() with a cryptic "'from' must be a
    # finite number"; se_hat == 0 would silently degenerate the grid to a
    # point. Fail here with a named error instead (cr-review PR #158).
    stopifnot("ar_from_fit(): beta_hat/se_hat must be finite, se_hat > 0" =
                  is.finite(beta_hat) && is.finite(se_hat) && se_hat > 0)
    vars <- c(y, endog, instrs, ctrls_vec)
    stopifnot(all(vars %in% names(data)))
    stopifnot("ar_from_fit(): data must be the estimation sample" =
                  all(complete.cases(data[, vars])))
    X <- as.matrix(cbind(1, data[, ctrls_vec]))
    qx <- qr(X)
    r <- function(v) as.numeric(qr.resid(qx, v))
    Zt <- matrix(sapply(instrs, function(z) r(data[[z]])),
                 ncol = length(instrs))
    ar_invert(Yt = r(data[[y]]), Dt = r(data[[endog]]), Zt = Zt,
              n_ctrl = ncol(X), beta_hat = beta_hat, se_hat = se_hat,
              alpha = alpha)
}

# ---------------------------------------------------------------------------
# ar_cell(ar): format an ar_invert() result for a LaTeX table cell.
#
# Bounded sets print as an interval. UNBOUNDED OR DISJOINT SETS ARE NOT
# SILENTLY PRINTED AS INTERVALS -- ar_invert() already reports their true
# shape, and squeezing "(-Inf, Inf)" or a two-piece union into a cell that
# looks like a confidence interval would misrepresent the inference. Those
# print verbatim, which is wide and ugly on purpose: it should be noticed.
# ---------------------------------------------------------------------------
ar_cell <- function(ar) {
    if (isTRUE(ar$bounded)) {
        # ar_invert() already formats a bounded set as "[a, b]".
        return(ar$print)
    }
    # Unbounded / disjoint / empty: print the true shape, with the symbols
    # LaTeX-safe. No attempt to make it look like an interval.
    s <- gsub("-Inf", "$-\\\\infty$", ar$print, fixed = FALSE)
    s <- gsub("Inf",  "$\\\\infty$",  s)
    gsub(" U ", " $\\\\cup$ ", s)
}

# ---------------------------------------------------------------------------
# ar_row_note(): the sentences explaining the "AR 95% set" row, for the
# tables that carry one (Tables 9 and 10; Table 8 has no outcome and so no
# AR row). Shared for the same reason f_rows_note() is -- three copies of a
# paragraph is how the drift in PR #157 happened.
#
# The last sentence is the one that earns the row's space: an unbounded set
# is not a formatting artifact, it is the finding. On four of the
# IV-Hypo cells the set is the whole line, which says "this instrument
# cannot bound the parameter" far more legibly than an F of 4.3 does.
# ---------------------------------------------------------------------------
ar_row_note <- function() {
    paste(
        "``AR 95\\% set'' is the Anderson--Rubin confidence set, obtained by",
        "inverting the identification-robust AR test over a grid. Unlike the",
        "reported standard errors and unlike either $F$ row, it requires no",
        "assumption about first-stage strength and remains valid however weak",
        "the instruments are, so it is the inference that does not depend on",
        "a first-stage strength threshold. Where the set is reported as",
        "$(-\\infty, \\infty)$ it is genuinely unbounded: the data place no",
        "finite bound on the coefficient under that instrument, which is a",
        "sharper statement of weakness than the $F$ statistics give."
    )
}

# ---------------------------------------------------------------------------
# MONTIEL OLEA-PFLUEGER MACHINERY. B_of_W(), patnaik_cv() and mop_check()
# moved here from four diagnostics (PR #159; flagged by the PR #137/#140
# reviews and ledgered since). The copies had already drifted in two ways,
# both reconciled here:
#   - patnaik_cv returned a bare cv in three copies and list(cv, k_eff) in
#     diagnostic_mop_critical.R, so the k_eff columns were silently absent
#     from the other scripts' outputs. The canonical version returns the
#     list; callers that want only the cv take $cv.
#   - three copies had dropped the beta -> +/-Inf derivation comment in
#     B_of_W. The commented version is the canonical one.
#
# B_of_W(W1, W2, W12): the exact Nagar-bias bound B for the MOP effective-F
# test, from the three W matrices (HC1-scaled, instrument-whitened).
# ---------------------------------------------------------------------------
B_of_W <- function(W1, W2, W12) {
    trW2 <- sum(diag(W2))
    ratio_at <- function(b) {
        S12 <- W12 - b * W2
        S1  <- W1 - b * (W12 + t(W12)) + b^2 * W2
        sym <- (S12 + t(S12)) / 2
        ev  <- eigen(sym, symmetric = TRUE, only.values = TRUE)$values
        num <- max(abs(sum(diag(S12)) - 2 * min(ev)),
                   abs(sum(diag(S12)) - 2 * max(ev))) / trW2
        den <- sqrt(max(sum(diag(S1)), .Machine$double.eps) / trW2)
        num / den
    }
    # beta -> +/-Inf limit: S12 ~ -b W2 and S1 ~ b^2 W2, so |n|/BM tends
    # to max over eigenvalue extremes of |tr(W2) - 2 lambda(W2)| / trW2
    # (the |b| factors cancel between numerator and denominator):
    evW2 <- eigen((W2 + t(W2)) / 2, symmetric = TRUE, only.values = TRUE)$values
    lim  <- max(abs(sum(diag(W2)) - 2 * min(evW2)),
                abs(sum(diag(W2)) - 2 * max(evW2))) / trW2
    grid <- tan(seq(-pi / 2 + 1e-3, pi / 2 - 1e-3, length.out = 2001))
    vals <- vapply(grid, ratio_at, numeric(1))
    i    <- which.max(vals)
    lo   <- grid[max(1, i - 1)]
    hi   <- grid[min(length(grid), i + 1)]
    ref  <- optimize(ratio_at, lower = lo, upper = hi, maximum = TRUE)
    max(vals[i], ref$objective, lim)
}

# ---------------------------------------------------------------------------
# patnaik_cv(W2, d_tau, alpha = 0.05): the Patnaik-approximation critical
# value for the effective F at noncentrality d_tau, WITH its effective
# degrees of freedom. Returns list(cv, k_eff) always -- the return-shape
# drift where three copies gave a bare cv is what silently dropped k_eff
# from their outputs.
# ---------------------------------------------------------------------------
patnaik_cv <- function(W2, d_tau, alpha = 0.05) {
    trW2  <- sum(diag(W2))
    trW22 <- sum(diag(crossprod(W2)))
    lmax  <- max(eigen((W2 + t(W2)) / 2, symmetric = TRUE,
                       only.values = TRUE)$values)
    k_eff <- trW2^2 * (1 + 2 * d_tau) /
             (trW22 + 2 * d_tau * trW2 * lmax)
    cv <- qchisq(1 - alpha, df = k_eff, ncp = k_eff * d_tau) / k_eff
    list(cv = cv, k_eff = k_eff)
}

# ---------------------------------------------------------------------------
# mop_check(m, yvar, endog, instrs, ctrls): effective F plus the exact-B
# MOP critical values at 10% and 20% bias tolerance, for one cell.
# Self-contained: residualizes on the controls itself (its own
# complete-case subset, asserted implicitly by the algebra using one `d`).
# Returns list(F_eff, cv10, cv20).
# ---------------------------------------------------------------------------
mop_check <- function(m, yvar, endog, instrs, ctrls) {
    vars <- c(yvar, endog, instrs, ctrls)
    d <- m[complete.cases(m[, vars]), vars]
    X <- as.matrix(cbind(1, d[, ctrls]))
    r <- function(v) as.numeric(qr.resid(qr(X), v))
    Dt <- r(d[[endog]])
    Yt <- r(d[[yvar]])
    Zt <- sapply(instrs, function(z) r(d[[z]]))
    Zt <- matrix(Zt, ncol = length(instrs))
    n <- nrow(d); k <- ncol(Zt)
    qz <- qr(Zt)
    pi_ <- qr.coef(qz, Dt)
    v2 <- qr.resid(qz, Dt)
    v1 <- qr.resid(qz, Yt)
    hc1 <- n / (n - ncol(X) - k)
    Q   <- crossprod(Zt)
    eQ  <- eigen(Q, symmetric = TRUE)
    Qih <- eQ$vectors %*% diag(1 / sqrt(eQ$values), k) %*% t(eQ$vectors)
    meat <- function(a, b) hc1 * crossprod(Zt * a, Zt * b)
    W1  <- Qih %*% meat(v1, v1) %*% Qih
    W2  <- Qih %*% meat(v2, v2) %*% Qih
    W12 <- Qih %*% meat(v1, v2) %*% Qih
    F_eff <- as.numeric(t(pi_) %*% Q %*% pi_ / sum(diag(W2)))
    B <- B_of_W(W1, W2, W12)
    list(F_eff = F_eff,
         cv10 = patnaik_cv(W2, B / 0.10)$cv,
         cv20 = patnaik_cv(W2, B / 0.20)$cv)
}

# ---------------------------------------------------------------------------
# cell_frame(data, vars, fit): the complete-case frame for ONE table cell,
# asserted against the fit it will sit beside.
#
# WHY PER CELL (deferred from the PR #155/#158 reviews, closed in PR #159):
# fit_iv_quad NA-drops per model, so each column of a table can in
# principle have its own sample. The previous pattern built one frame on
# BOTH instruments and asserted only against the IV-B fit -- so if an
# instrument ever acquired missingness, the IV-LP effective F and AR set
# would be computed on the wrong rows with no error anywhere. This asserts
# nrow == nobs(fit) for the fit actually being annotated. Today all four
# columns have identical samples for every outcome, so switching to
# per-cell frames changes no value; the assert is what changes.
# ---------------------------------------------------------------------------
cell_frame <- function(data, vars, fit) {
    dd <- as.data.frame(data)
    stopifnot(all(vars %in% names(dd)))
    cc <- dd[complete.cases(dd[, vars]), ]
    # stats::nobs, not fixest::nobs -- nobs is a stats generic that fixest
    # provides a method for, not an exported fixest object.
    stopifnot("cell_frame(): frame does not match the fit's sample" =
                  nrow(cc) == stats::nobs(fit))
    cc
}

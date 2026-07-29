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
add_table_note <- function(tex_text, note) {
    marker <- "\\end{table}"
    pos <- max(gregexpr(marker, tex_text, fixed = TRUE)[[1]])
    if (pos < 0) return(tex_text)
    block <- sprintf(
        "\\vspace{0.4em}\n{\\footnotesize \\textit{Notes:} %s}\n", note
    )
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

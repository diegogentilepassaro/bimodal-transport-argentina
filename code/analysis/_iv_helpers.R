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

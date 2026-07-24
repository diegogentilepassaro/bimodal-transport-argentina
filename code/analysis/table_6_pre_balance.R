# ===========================================================================
# table_6_pre_balance.R
#
# PURPOSE: Paper Table 6 — pre-period balance. Tests whether the
#          instruments (LP discontinuity, LCP-MST hypothetical roads)
#          correlate with baseline district characteristics after
#          partialling out baseline MA and pop. A significant
#          coefficient would threaten the exclusion restriction.
#
# DEP VARS (one regression each):
#   log(pop_1960)          Baseline log population
#   urbshr_1960            Urban share in 1960
#   log(area_km2)          Log district area
#   elev_mean_std          Standardized mean elevation
#   rugged_mea_std         Standardized ruggedness
#   wheat_std              Standardized wheat suitability
#   preCal_std             Standardized pre-1500 caloric potential
#   postCal_std            Standardized post-1500 caloric potential
#   dist_to_BA_std         Standardized distance to Buenos Aires
#
# REGRESSORS: the two instruments,
#   chg_logMA_stu_s0_elow, chg_logMA_lcp_mst_s0_elow,
# plus baseline log MA and baseline log pop (only the latter is
# partialled out via the instrument coefficients; area and the six
# geo controls are themselves outcomes here).
#
# COLUMNS:
#   (1) LP only      — single instrument
#   (2) Hypo only    — single instrument
#   (3) Both         — joint
#
# We report only the instrument coefficients (and SEs). Baseline-MA
# and baseline-pop controls are partialled out.
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/table_6_pre_balance.tex
#   results/tables/table_6_pre_balance.csv
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
    library(modelsummary)
})

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)
    options(modelsummary_factory_latex = "kableExtra")
    options(modelsummary_format_numeric_latex = "plain")

    HYPO_INSTRUMENT <- main_hypo_instrument

    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)

    d <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet")
    )
    # Derived outcomes that aren't in the panel yet
    d$log_area_km2 <- log(d$area_km2)

    outcomes <- list(
        list(var = "log_pop_1960",
             label = "Log pop, 1960"),
        list(var = "urbshr_1960",
             label = "Urban share, 1960"),
        list(var = "log_area_km2",
             label = "Log area (km$^2$)"),
        list(var = "elev_mean_std",
             label = "Elevation (std)"),
        list(var = "rugged_mea_std",
             label = "Ruggedness (std)"),
        list(var = "wheat_std",
             label = "Wheat suitability (std)"),
        list(var = "preCal_std",
             label = "Caloric pot.\\ pre-1500 (std)"),
        list(var = "postCal_std",
             label = "Caloric pot.\\ post-1500 (std)"),
        list(var = "dist_to_BA_std",
             label = "Distance to B.A. (std)")
    )

    # Build 3 specs × 9 outcomes = 27 regressions.
    # Only report the instrument coefficient (and SE) for each outcome.
    #
    # For each outcome, we partial out the six geographic controls that
    # appear in Tables 8 and 9 (elevation, ruggedness, wheat, pre-caloric,
    # post-caloric, distance to BA), as well as baseline log MA and log
    # pop — but we do NOT partial out the outcome itself. So for
    # outcome = "elev_mean_std", the partialled-out controls are the
    # other five geo controls + baseline log MA + log pop; for outcome
    # = "log_pop_1960", the partialled-out controls are all six geo +
    # baseline log MA. The self-exclusion is explicit in the setdiff
    # below over the FULL control set (cr-review PR #116 blocking item:
    # previously log_pop_1960 stayed on the RHS for its own row and
    # fixest silently dropped the duplicated response — numbers
    # identical, but the specification should not rely on that).
    geo_all <- c("elev_mean_std", "rugged_mea_std", "wheat_std",
                 "preCal_std", "postCal_std", "dist_to_BA_std")

    rows_out <- list()

    message("\n[t6] Pre-balance regressions (instrument coefs only):")
    message(sprintf("%-30s  %-18s %-18s %-18s",
                    "Outcome", "LP only", "Hypo only", "Both (LP|Hypo)"))

    for (out in outcomes) {
        y <- out$var
        if (!(y %in% names(d))) next

        # Partialling set: baseline log MA + log pop + six geo controls,
        # minus any control that is itself the outcome.
        partials <- setdiff(c("logMA_actual_1960_s0_elow", "log_pop_1960",
                              geo_all), y)
        partials_expr <- paste(partials, collapse = " + ")

        # Column (1): just LP as regressor
        f1 <- as.formula(sprintf(
            "%s ~ chg_logMA_stu_s0_elow + %s",
            y, partials_expr
        ))
        m1 <- feols(f1, data = d, vcov = "hetero")

        # Column (2): just Hypo
        f2 <- as.formula(sprintf(
            "%s ~ %s + %s",
            y, HYPO_INSTRUMENT, partials_expr
        ))
        m2 <- feols(f2, data = d, vcov = "hetero")

        # Column (3): Both
        f3 <- as.formula(sprintf(
            "%s ~ chg_logMA_stu_s0_elow + %s + %s",
            y, HYPO_INSTRUMENT, partials_expr
        ))
        m3 <- feols(f3, data = d, vcov = "hetero")

        # Extract LP and Hypo coefficients per spec
        b_lp_1  <- safe_coef(m1, "chg_logMA_stu_s0_elow")
        b_h_2   <- safe_coef(m2, HYPO_INSTRUMENT)
        b_lp_3  <- safe_coef(m3, "chg_logMA_stu_s0_elow")
        b_h_3   <- safe_coef(m3, HYPO_INSTRUMENT)

        rows_out[[length(rows_out) + 1L]] <- data.frame(
            outcome = y,
            outcome_label = out$label,
            lp_only_coef   = b_lp_1$est,
            lp_only_se     = b_lp_1$se,
            lp_only_p      = b_lp_1$p,
            hypo_only_coef = b_h_2$est,
            hypo_only_se   = b_h_2$se,
            hypo_only_p    = b_h_2$p,
            both_lp_coef   = b_lp_3$est,
            both_lp_se     = b_lp_3$se,
            both_lp_p      = b_lp_3$p,
            both_h_coef    = b_h_3$est,
            both_h_se      = b_h_3$se,
            both_h_p       = b_h_3$p,
            n_obs          = nobs(m3),
            stringsAsFactors = FALSE
        )

        message(sprintf("%-30s  %-18s %-18s %s | %s",
                        y,
                        format_be_se(b_lp_1),
                        format_be_se(b_h_2),
                        format_be_se(b_lp_3),
                        format_be_se(b_h_3)))
    }

    df <- do.call(rbind, rows_out)

    # CSV
    out_csv <- file.path(dir_tables, "table_6_pre_balance.csv")
    write.csv(df, out_csv, row.names = FALSE)
    message("\nSaved: ", out_csv)

    # LaTeX: one row per outcome with LP / Hypo / Both coefficients
    tex_lines <- c(
        "% Table 6: Pre-period balance.",
        "% Generated by code/analysis/table_6_pre_balance.R.",
        "%",
        "% Each row is a separate regression of a baseline district",
        "% characteristic on the instrument(s). Columns report the",
        "% instrument coefficient(s) with robust (HC1) SE in parentheses.",
        "% Significance: * p<0.10, ** p<0.05, *** p<0.01.",
        "% All regressions partial out baseline log MA and baseline log pop.",
        "",
        "\\begin{table}[htbp]",
        "\\centering",
        sprintf("\\caption{Pre-period balance on the two instruments}"),
        "\\label{tab:pre_balance}",
        "\\small",
        "\\begin{tabular}{lcccc}",
        "\\toprule",
        "& (1) & (2) & \\multicolumn{2}{c}{(3) Both} \\\\",
        "\\cmidrule(lr){4-5}",
        "Outcome & LP only & Hypo only & LP & Hypo \\\\",
        "\\midrule"
    )

    for (i in seq_len(nrow(df))) {
        r <- df[i, ]
        tex_lines <- c(tex_lines,
            sprintf(
                "%s & %s & %s & %s & %s \\\\",
                outcomes[[i]]$label,
                fmt_cell(r$lp_only_coef,  r$lp_only_se,  r$lp_only_p),
                fmt_cell(r$hypo_only_coef, r$hypo_only_se, r$hypo_only_p),
                fmt_cell(r$both_lp_coef,  r$both_lp_se,  r$both_lp_p),
                fmt_cell(r$both_h_coef,   r$both_h_se,   r$both_h_p)
            )
        )
    }

    tex_lines <- c(tex_lines,
        "\\midrule"
    )
    # N is supposed to be the same across all outcomes (they all share the
    # same estimation sample: baseline log MA + log pop + six geo controls
    # non-missing). Assert it before emitting a single Observations row,
    # so silent N disagreement in a future run becomes a loud error.
    stopifnot(length(unique(df$n_obs)) == 1L)
    n_common <- df$n_obs[1]
    tex_lines <- c(tex_lines,
        sprintf("Observations & %d & %d & \\multicolumn{2}{c}{%d} \\\\",
                n_common, n_common, n_common),
        "\\bottomrule",
        "\\end{tabular}",
        "",
        "\\footnotesize",
        paste0("\\emph{Notes}: Each row is one regression. The dependent ",
               "variable is the row label; the regressors are the ",
               "instrument(s) listed in the column headers plus baseline ",
               "log MA (1960), baseline log pop (1960), and the six ",
               "geographic controls of the main specification as ",
               "partialled-out controls. When the row's characteristic is ",
               "itself one of these controls, it is excluded from the ",
               "control set rather than partialled out against itself. ",
               "Robust (HC1) SE in parentheses. ",
               "$^{*}p<0.10,\\;^{**}p<0.05,\\;^{***}p<0.01$."),
        "\\end{table}"
    )

    out_tex <- file.path(dir_tables, "table_6_pre_balance.tex")
    writeLines(tex_lines, out_tex)
    message("Saved: ", out_tex)
}

# ---------------------------------------------------------------------------
# Helpers (table-local: display formatters)
# ---------------------------------------------------------------------------
format_be_se <- function(co) {
    if (is.na(co$est)) return("        NA        ")
    stars <- ifelse(co$p < 0.01, "***",
            ifelse(co$p < 0.05, "**",
            ifelse(co$p < 0.10, "*", "")))
    sprintf("%+6.3f%-3s (%.3f)", co$est, stars, co$se)
}

fmt_cell <- function(coef, se, p) {
    if (is.na(coef)) return(" ")
    stars <- ifelse(p < 0.01, "$^{***}$",
            ifelse(p < 0.05, "$^{**}$",
            ifelse(p < 0.10, "$^{*}$", "")))
    sprintf(
        "\\begin{tabular}{@{}c@{}} %.3f%s \\\\ (%.3f) \\end{tabular}",
        coef, stars, se
    )
}

main()

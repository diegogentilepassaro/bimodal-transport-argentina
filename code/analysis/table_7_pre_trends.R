# ===========================================================================
# table_7_pre_trends.R
#
# PURPOSE: Paper Table 7 — pre-trends placebo. Tests whether the
#          1960–1986 market-access change predicts pre-reform
#          population growth (1947–1960). If the instruments (or the
#          treatment) correlate with pre-reform trends, the
#          identification strategy is in trouble.
#
# DEP VAR: chg_log_placebo_pop_60_47 (log change in district
#          population between 1947 and 1960).
#
# REGRESSOR: chg_logMA_86_60_s0_elow (the main post-reform treatment).
#
# COLUMNS (same structure as Table 9):
#   (1) OLS       — direct regression of placebo outcome on treatment
#   (2) IV-LP     — instrument the treatment with LP
#   (3) IV-Hypo   — instrument with LCP-MST
#   (4) IV-Both   — instrument with both
#
# Interpretation:
#   - If the coefficient is near zero and insignificant, post-reform
#     infrastructure changes did NOT predict pre-reform population
#     growth. This supports a causal reading of the post-reform
#     relationship in Table 9.
#   - If the coefficient is significant, there's a pre-existing trend
#     correlated with the eventual infrastructure changes, which
#     threatens the causal interpretation.
#
# CONTROLS: Same as Table 9.
#
# SAMPLE: 235 districts (the subset for which the 1947 census provides
#         comparable population data — see Section 3.2).
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/table_7_pre_trends.{tex,csv}
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
    library(modelsummary)
})

HYPO_INSTRUMENT <- "chg_logMA_lcp_mst_s0_elow"

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    options(modelsummary_factory_latex = "kableExtra")
    options(modelsummary_format_numeric_latex = "plain")

    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)

    d <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet")
    )

    y <- "chg_log_placebo_pop_60_47"
    geo_controls_expr <- paste(c(
        "elev_mean_std", "rugged_mea_std", "wheat_std",
        "preCal_std", "postCal_std", "dist_to_BA_std",
        "logMA_actual_1960_s0_elow", "log_pop_1960"
    ), collapse = " + ")

    # (1) OLS
    f_ols <- as.formula(sprintf("%s ~ chg_logMA_86_60_s0_elow + %s",
                                y, geo_controls_expr))
    m_ols <- feols(f_ols, data = d, vcov = "hetero")

    # (2) IV-LP
    f_iv_lp <- as.formula(sprintf(
        "%s ~ %s | chg_logMA_86_60_s0_elow ~ chg_logMA_stu_s0_elow",
        y, geo_controls_expr
    ))
    m_iv_lp <- feols(f_iv_lp, data = d, vcov = "hetero")

    # (3) IV-Hypo
    f_iv_h <- as.formula(sprintf(
        "%s ~ %s | chg_logMA_86_60_s0_elow ~ %s",
        y, geo_controls_expr, HYPO_INSTRUMENT
    ))
    m_iv_h <- feols(f_iv_h, data = d, vcov = "hetero")

    # (4) IV-Both
    f_iv_b <- as.formula(sprintf(paste(
        "%s ~ %s | chg_logMA_86_60_s0_elow ~",
        "chg_logMA_stu_s0_elow + %s"
    ), y, geo_controls_expr, HYPO_INSTRUMENT))
    m_iv_b <- feols(f_iv_b, data = d, vcov = "hetero")

    # First-stage F-stats per IV spec
    fs_lp <- fitstat_F(m_iv_lp)
    fs_h  <- fitstat_F(m_iv_h)
    fs_b  <- fitstat_F(m_iv_b)

    message("\n[t7] Pre-trends placebo on Δlog(pop_60_47):")
    message(sprintf("%-12s  %-20s  N = %d", "OLS",
                    format_co(safe_coef(m_ols, "chg_logMA_86_60_s0_elow")),
                    nobs(m_ols)))
    message(sprintf("%-12s  %-20s  F = %.1f", "IV-LP",
                    format_co(safe_coef(m_iv_lp,
                        "fit_chg_logMA_86_60_s0_elow")),
                    fs_lp))
    message(sprintf("%-12s  %-20s  F = %.1f", "IV-Hypo",
                    format_co(safe_coef(m_iv_h,
                        "fit_chg_logMA_86_60_s0_elow")),
                    fs_h))
    message(sprintf("%-12s  %-20s  F = %.1f", "IV-Both",
                    format_co(safe_coef(m_iv_b,
                        "fit_chg_logMA_86_60_s0_elow")),
                    fs_b))

    # --- Build LaTeX ------------------------------------------------------
    models <- list(
        "(1) OLS"      = m_ols,
        "(2) IV-LP"    = m_iv_lp,
        "(3) IV-Hypo"  = m_iv_h,
        "(4) IV-Both"  = m_iv_b
    )

    coef_map <- c(
        "chg_logMA_86_60_s0_elow"     = "$\\Delta \\ln \\mathrm{MA}^{\\mathrm{full}}$",
        "fit_chg_logMA_86_60_s0_elow" = "$\\Delta \\ln \\mathrm{MA}^{\\mathrm{full}}$"
    )

    gof_custom <- list(
        list("raw" = "nobs", "clean" = "Observations", "fmt" = 0)
    )

    add_rows <- tibble::tibble(
        ` `           = "First-stage $F$",
        `(1) OLS`     = "---",
        `(2) IV-LP`   = sprintf("%.1f", fs_lp),
        `(3) IV-Hypo` = sprintf("%.1f", fs_h),
        `(4) IV-Both` = sprintf("%.1f", fs_b)
    )

    tbl <- modelsummary(
        models,
        output   = "latex",
        coef_map = coef_map,
        gof_map  = gof_custom,
        stars    = c("*" = .1, "**" = .05, "***" = .01),
        escape   = FALSE,
        add_rows = add_rows,
        title    = "Pre-trends placebo: does $\\Delta \\ln \\mathrm{MA}^{\\mathrm{full}}$ predict 1947--1960 population growth?"
    )

    # Notes are appended as raw LaTeX before \end{table}, because
    # modelsummary's note-escaping mangles backslashes. Use plain
    # \footnotesize text instead of threeparttable to keep the
    # dependency surface minimal (matches Table 9 which has no notes).
    notes_tex <- paste(c(
        "\\vspace{0.5em}",
        "{\\footnotesize \\textit{Notes:}",
        "Dependent variable: $\\Delta \\ln(\\mathrm{pop}_{1960}/\\mathrm{pop}_{1947})$.",
        "Robust (HC1) SE in parentheses.",
        "All columns include baseline log MA (1960), baseline log pop (1960),",
        "and the six standardized geographic controls.",
        "A near-zero and insignificant coefficient is evidence that",
        "post-reform $\\Delta \\ln \\mathrm{MA}$ is not picking up pre-reform trends.",
        "$^{*}p<0.10,\\;^{**}p<0.05,\\;^{***}p<0.01$.}"
    ), collapse = "\n")

    # Inject the notes just before \end{table}. Use gsub with fixed = TRUE
    # on the replacement to avoid backreference interpretation of backslashes.
    tbl_txt <- as.character(tbl)
    end_marker <- "\\end{table}"
    tbl_txt <- sub(end_marker, paste0(notes_tex, "\n", end_marker),
                   tbl_txt, fixed = TRUE)

    out_tex <- file.path(dir_tables, "table_7_pre_trends.tex")
    writeLines(c(
        "% Table 7: Pre-trends placebo.",
        "% Generated by code/analysis/table_7_pre_trends.R.",
        "%",
        "% Sample is 235 districts where the 1947 census provides",
        "% comparable population data (see Section 3.2).",
        "",
        tbl_txt
    ), out_tex)
    message("Saved: ", out_tex)

    # CSV
    csv_rows <- list()
    for (spec_name in c("OLS", "IV-LP", "IV-Hypo", "IV-Both")) {
        m <- models[[paste0("(", match(spec_name,
                                       c("OLS","IV-LP","IV-Hypo","IV-Both")),
                           ") ", spec_name)]]
        coef_name <- if (spec_name == "OLS") "chg_logMA_86_60_s0_elow"
                     else "fit_chg_logMA_86_60_s0_elow"
        co <- safe_coef(m, coef_name)
        csv_rows[[length(csv_rows) + 1L]] <- data.frame(
            spec          = spec_name,
            estimate      = co$est,
            std_err       = co$se,
            t_value       = co$t,
            p_value       = co$p,
            n_obs         = nobs(m),
            first_stage_F = if (spec_name == "OLS") NA_real_ else
                            fitstat_F(m),
            stringsAsFactors = FALSE
        )
    }
    csv_df <- do.call(rbind, csv_rows)
    out_csv <- file.path(dir_tables, "table_7_pre_trends.csv")
    write.csv(csv_df, out_csv, row.names = FALSE)
    message("Saved: ", out_csv)
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
fitstat_F <- function(iv_model) {
    fs <- fitstat(iv_model, type = "ivf")
    if (is.list(fs) && !is.null(fs[[1]]$stat)) {
        return(as.numeric(fs[[1]]$stat))
    }
    fs2 <- fitstat(iv_model, type = "ivf", simplify = TRUE)
    if (is.list(fs2) && !is.null(fs2$stat)) return(as.numeric(fs2$stat))
    NA_real_
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

format_co <- function(co) {
    if (is.na(co$est)) return("NA")
    stars <- ifelse(co$p < 0.01, "***",
            ifelse(co$p < 0.05, "**",
            ifelse(co$p < 0.10, "*", "")))
    sprintf("%+6.3f%-3s (%.3f)", co$est, stars, co$se)
}

main()

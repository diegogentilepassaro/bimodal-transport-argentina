# ===========================================================================
# table_12_robustness.R
#
# PURPOSE: Paper Table 12 — robustness of the headline Table 9 result
#          (total population, chg_log_pop_91_60). Three panels:
#
#   Panel A: Alternative trade elasticity theta = 8.11 (main spec uses
#            theta = 4.55). All MA variables switch from _elow to _ehigh.
#
#   Panel B: Alternative hypothetical-road instruments. Main spec uses
#            chg_logMA_lcp_mst_s0_elow. Alternatives: euc_mst, lcp, euc.
#            Each row is a separate Just-IV-Hypo regression (one
#            instrument at a time); rows are labeled by the instrument
#            used. LP and Both columns reuse the main spec's LP
#            instrument paired with the alternative hypo.
#
#   Panel C: Sample robustness. Refit the main Table 9 specification on
#            the 235-district subsample where the 1947 placebo outcome
#            is defined (see Section 4.5). Tests whether the OLS-IV gap
#            or the coefficient level changes on that subsample.
#
# CONTROLS and SE: same as Tables 9/10 (geo_controls_main; HC1).
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/table_12_robustness.{tex,csv}
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

    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)

    d <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet")
    )

    # Baseline log-MA control is theta-specific. For Panel A we swap the
    # _elow control to _ehigh so the control variable matches the theta
    # used in the treatment and instruments.
    ctrls_elow <- geo_controls_main  # uses logMA_actual_1960_s0_elow
    ctrls_ehigh <- c(setdiff(geo_controls_main, "logMA_actual_1960_s0_elow"),
                     "logMA_actual_1960_s0_ehigh")

    rows <- list()

    # ----------------------------------------------------------------------
    # Panel A: alternative theta (8.11). Replicates Table 9 col 4 (main
    # outcome) with _ehigh MA vars.
    # ----------------------------------------------------------------------
    fits_A <- fit_iv_quad(
        y = "chg_log_pop_91_60", data = d,
        endog = "chg_logMA_86_60_s0_ehigh",
        lp_instr = "chg_logMA_stu_s0_ehigh",
        hypo_instr = "chg_logMA_lcp_mst_s0_ehigh",
        ctrls_vec = ctrls_ehigh
    )
    rows[[length(rows) + 1L]] <- build_row(
        panel = "A", label = "Alt. theta = 8.11",
        fits = fits_A, endog = "chg_logMA_86_60_s0_ehigh"
    )

    # ----------------------------------------------------------------------
    # Panel B: alternative hypothetical-road instruments. Each row is a
    # full IV-Both specification using the row's named hypo instrument.
    # ----------------------------------------------------------------------
    hypo_alts <- list(
        list(name = "chg_logMA_lcp_mst_s0_elow", label = "LCP-MST (main)"),
        list(name = "chg_logMA_euc_mst_s0_elow", label = "Euclidean-MST"),
        list(name = "chg_logMA_lcp_s0_elow",     label = "LCP (bilateral)"),
        list(name = "chg_logMA_euc_s0_elow",     label = "Euclidean (bilateral)")
    )
    for (h in hypo_alts) {
        fits_B <- fit_iv_quad(
            y = "chg_log_pop_91_60", data = d,
            endog = "chg_logMA_86_60_s0_elow",
            lp_instr = "chg_logMA_stu_s0_elow",
            hypo_instr = h$name,
            ctrls_vec = ctrls_elow
        )
        rows[[length(rows) + 1L]] <- build_row(
            panel = "B", label = sprintf("Hypo = %s", h$label),
            fits = fits_B, endog = "chg_logMA_86_60_s0_elow"
        )
    }

    # ----------------------------------------------------------------------
    # Panel C: subsample stability. Refit the main spec on the 235-
    # district subset where chg_log_placebo_pop_60_47 is defined (Table 7
    # sample).
    # ----------------------------------------------------------------------
    d_sub <- d[!is.na(d$chg_log_placebo_pop_60_47), ]
    fits_C <- fit_iv_quad(
        y = "chg_log_pop_91_60", data = d_sub,
        endog = "chg_logMA_86_60_s0_elow",
        lp_instr = "chg_logMA_stu_s0_elow",
        hypo_instr = main_hypo_instrument,
        ctrls_vec = ctrls_elow
    )
    rows[[length(rows) + 1L]] <- build_row(
        panel = "C", label = sprintf("Placebo subsample (N=%d)", nrow(d_sub)),
        fits = fits_C, endog = "chg_logMA_86_60_s0_elow"
    )

    # For comparison, also report the main-spec IV-Both on the full sample
    fits_main <- fit_iv_quad(
        y = "chg_log_pop_91_60", data = d,
        endog = "chg_logMA_86_60_s0_elow",
        lp_instr = "chg_logMA_stu_s0_elow",
        hypo_instr = main_hypo_instrument,
        ctrls_vec = ctrls_elow
    )
    rows[[length(rows) + 1L]] <- build_row(
        panel = "C", label = "Full sample (for reference)",
        fits = fits_main, endog = "chg_logMA_86_60_s0_elow"
    )

    # ----------------------------------------------------------------------
    # Assemble data frame and print summary
    # ----------------------------------------------------------------------
    df <- do.call(rbind, rows)

    message("\n[t12] Robustness: coefficient on ΔlogMA across variations")
    message(sprintf("%-1s  %-35s  %-13s %-13s %-13s %-13s  %-5s",
                    "P", "Variation", "OLS", "IV-LP", "IV-Hypo",
                    "IV-Both", "N"))
    for (i in seq_len(nrow(df))) {
        r <- df[i, ]
        message(sprintf("%-1s  %-35s  %s %s %s %s  %-5d",
                        r$panel, r$label,
                        fmt(r$ols_est,    r$ols_se,    r$ols_p),
                        fmt(r$iv_lp_est,  r$iv_lp_se,  r$iv_lp_p),
                        fmt(r$iv_h_est,   r$iv_h_se,   r$iv_h_p),
                        fmt(r$iv_b_est,   r$iv_b_se,   r$iv_b_p),
                        r$n_obs))
    }

    # ----------------------------------------------------------------------
    # LaTeX output: one table with panel headers
    # ----------------------------------------------------------------------
    tex_lines <- c(
        "% Table 12: Robustness of the main population result (chg_log_pop_91_60).",
        "% Generated by code/analysis/table_12_robustness.R.",
        "%",
        "% Panel A: alternative trade elasticity theta = 8.11 (main = 4.55).",
        "% Panel B: alternative hypothetical-road instruments.",
        "% Panel C: subsample stability (235-district placebo subset).",
        "%",
        "% Columns (1)-(4) are OLS / IV-LP / IV-Hypo / IV-Both. All specs",
        "% include baseline log MA, baseline log pop, and the six",
        "% standardized geographic controls. Robust (HC1) standard errors.",
        "",
        "\\begin{table}[htbp]",
        "\\centering",
        "\\caption{Robustness of the main population elasticity (outcome:",
        "$\\Delta \\ln \\mathrm{Pop}_{1960 \\to 1991}$)}",
        "\\label{tab:robustness}",
        "\\small",
        "\\begin{tabular}{llcccc}",
        "\\toprule",
        " & Variation & (1) OLS & (2) IV-LP & (3) IV-Hypo & (4) IV-Both \\\\",
        "\\midrule",
        "\\multicolumn{6}{l}{\\textit{Panel A: alternative trade elasticity}} \\\\"
    )

    for (i in seq_len(nrow(df))) {
        r <- df[i, ]
        if (r$panel == "B" && i > 1 && df$panel[i - 1] == "A") {
            tex_lines <- c(tex_lines,
                "\\midrule",
                "\\multicolumn{6}{l}{\\textit{Panel B: alternative hypothetical-road instruments}} \\\\"
            )
        }
        if (r$panel == "C" && i > 1 && df$panel[i - 1] == "B") {
            tex_lines <- c(tex_lines,
                "\\midrule",
                "\\multicolumn{6}{l}{\\textit{Panel C: sample robustness}} \\\\"
            )
        }
        tex_lines <- c(tex_lines,
            sprintf("%s & %s & %s & %s & %s & %s \\\\",
                    r$panel,
                    r$label,
                    tex_cell(r$ols_est,   r$ols_se,   r$ols_p),
                    tex_cell(r$iv_lp_est, r$iv_lp_se, r$iv_lp_p),
                    tex_cell(r$iv_h_est,  r$iv_h_se,  r$iv_h_p),
                    tex_cell(r$iv_b_est,  r$iv_b_se,  r$iv_b_p))
        )
    }

    tex_lines <- c(tex_lines,
        "\\bottomrule",
        "\\end{tabular}",
        "",
        "\\footnotesize",
        paste0("\\emph{Notes}: All regressions use ",
               "$\\Delta \\ln(\\mathrm{Pop}_{1991}/\\mathrm{Pop}_{1960})$ ",
               "as the outcome. Panel~A swaps the MA elasticity from ",
               "$\\theta = 4.55$ to $\\theta = 8.11$ (all MA variables, ",
               "treatment, instruments, and baseline control, switch to ",
               "\\texttt{\\_ehigh} variants). Panel~B holds $\\theta$ at ",
               "4.55 and replaces the LCP-MST hypothetical-road ",
               "instrument with one of three alternatives. Panel~C ",
               "refits the main specification on the 235-district ",
               "subset for which the 1947 placebo outcome is defined. ",
               "Robust (HC1) SE. Significance: ",
               "$^{*}p<0.10,\\;^{**}p<0.05,\\;^{***}p<0.01$."),
        "\\end{table}"
    )

    out_tex <- file.path(dir_tables, "table_12_robustness.tex")
    writeLines(tex_lines, out_tex)
    message("\nSaved: ", out_tex)

    out_csv <- file.path(dir_tables, "table_12_robustness.csv")
    write.csv(df, out_csv, row.names = FALSE)
    message("Saved: ", out_csv)
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Build one row of the results data frame from a fit_iv_quad() output
build_row <- function(panel, label, fits, endog) {
    co_ols <- safe_coef(fits[["OLS"]],   endog)
    co_lp  <- safe_coef(fits[["IV-LP"]], paste0("fit_", endog))
    co_h   <- safe_coef(fits[["IV-H"]],  paste0("fit_", endog))
    co_b   <- safe_coef(fits[["IV-B"]],  paste0("fit_", endog))

    data.frame(
        panel        = panel,
        label        = label,
        ols_est      = co_ols$est, ols_se   = co_ols$se,
        ols_p        = co_ols$p,
        iv_lp_est    = co_lp$est,  iv_lp_se = co_lp$se,
        iv_lp_p      = co_lp$p,
        iv_h_est     = co_h$est,   iv_h_se  = co_h$se,
        iv_h_p       = co_h$p,
        iv_b_est     = co_b$est,   iv_b_se  = co_b$se,
        iv_b_p       = co_b$p,
        iv_lp_F      = fitstat_F(fits[["IV-LP"]]),
        iv_h_F       = fitstat_F(fits[["IV-H"]]),
        iv_b_F       = fitstat_F(fits[["IV-B"]]),
        n_obs        = nobs(fits[["OLS"]]),
        stringsAsFactors = FALSE
    )
}

fmt <- function(est, se, p) {
    if (is.na(est)) return("     NA       ")
    stars <- ifelse(p < 0.01, "***",
            ifelse(p < 0.05, "**",
            ifelse(p < 0.10, "*", "")))
    sprintf("%+6.3f%-3s(%.3f)", est, stars, se)
}

tex_cell <- function(est, se, p) {
    if (is.na(est)) return(" ")
    stars <- ifelse(p < 0.01, "$^{***}$",
            ifelse(p < 0.05, "$^{**}$",
            ifelse(p < 0.10, "$^{*}$", "")))
    sprintf(
        "\\begin{tabular}{@{}c@{}} %.3f%s \\\\ (%.3f) \\end{tabular}",
        est, stars, se
    )
}

main()

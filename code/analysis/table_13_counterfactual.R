# ===========================================================================
# table_13_counterfactual.R
#
# PURPOSE: Paper Table 13 — counterfactual MA decomposition. Compares
#          the population elasticities under three different MA
#          measures:
#
#   Panel A. Full MA       — Δlog MA^full = Δlog MA on the network
#                            where BOTH rails and roads change (1960 → 1986).
#                            This is the Table 9 headline; reported here
#                            with single-IV columns for direct comparison.
#
#   Panel B. Only-rail MA  — Δlog MA on the counterfactual network where
#                            ONLY rails change. Roads are frozen at 1954.
#                            Isolates the rail-closure shock.
#
#   Panel C. Only-road MA  — Δlog MA on the counterfactual network where
#                            ONLY roads change. Rails are frozen at 1960.
#                            Isolates the road-expansion shock.
#
# IV PAIRING (one instrument per panel):
#   Panel A. Full MA:       both LP and Hypo instruments (joint, IV-Both)
#   Panel B. Only-rail MA:  LP instrument only (chg_logMA_stu_s0_elow).
#                           Both vary rails on the 1954 road network.
#   Panel C. Only-road MA:  Hypo instrument only (chg_logMA_lcp_mst_s0_elow).
#                           Both vary roads on the 1960 rail network.
#
#   We do NOT put Δlog MA^only_rail and Δlog MA^only_road in the same
#   regression. They are spatially correlated, and each isolates a
#   different shock. Compare magnitudes across panels, not within a
#   single equation. (See .kiro/tasks.md C5 and §6.3 caveats.)
#
# DEP VARS (four outcomes; same as Table 9):
#   chg_log_pop_91_60     Total population (log change)
#   chg_log_urbpop_91_60  Urban population (log change)
#   chg_log_rur_91_60     Rural population (log change)
#   chg_urbshr_91_60      Urban share (level change)
#
# CONTROLS: geo_controls_main from config.R (same as Tables 9, 10).
# SE: heteroskedasticity-robust (HC1).
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/table_13_counterfactual.{tex,csv}
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

    outcomes <- list(
        list(var = "chg_log_pop_91_60",
             label = "$\\Delta \\ln \\mathrm{Pop}$"),
        list(var = "chg_log_urbpop_91_60",
             label = "$\\Delta \\ln \\mathrm{Pop}^{\\mathrm{urban}}$"),
        list(var = "chg_log_rur_91_60",
             label = "$\\Delta \\ln \\mathrm{Pop}^{\\mathrm{rural}}$"),
        list(var = "chg_urbshr_91_60",
             label = "$\\Delta (\\mathrm{Urban\\ share})$")
    )

    # Panels: each is (treatment_var, instrument, IV-spec label)
    # We use fit_iv_quad's IV-LP for Panel B (single LP instrument) and
    # IV-H for Panel C (single hypo instrument). Panel A reports IV-Both
    # for direct comparability with Table 9.
    panels <- list(
        list(
            id          = "A",
            title       = "A. Full MA: $\\Delta \\ln \\mathrm{MA}^{\\mathrm{full}}$ (both modes change)",
            treatment   = "chg_logMA_86_60_s0_elow",
            iv_key      = "IV-B",
            iv_label    = "IV-Both (LP and Hypo)"
        ),
        list(
            id          = "B",
            title       = "B. Only-rail MA: $\\Delta \\ln \\mathrm{MA}^{\\mathrm{only\\_rail}}$ (roads frozen at 1954)",
            treatment   = "chg_logMA_only_rail_s0_elow",
            iv_key      = "IV-LP",
            iv_label    = "IV-LP (Larkin Plan)"
        ),
        list(
            id          = "C",
            title       = "C. Only-road MA: $\\Delta \\ln \\mathrm{MA}^{\\mathrm{only\\_road}}$ (rails frozen at 1960)",
            treatment   = "chg_logMA_only_road_s0_elow",
            iv_key      = "IV-H",
            iv_label    = "IV-Hypo (LCP-MST)"
        )
    )

    # Build all model fits: 3 panels × 4 outcomes × 2 specs (OLS + chosen IV)
    rows <- list()
    for (p in panels) {
        for (out in outcomes) {
            fits <- fit_iv_quad(
                y          = out$var,
                data       = d,
                endog      = p$treatment,
                lp_instr   = "chg_logMA_stu_s0_elow",
                hypo_instr = main_hypo_instrument,
                ctrls_vec  = geo_controls_main
            )
            m_ols <- fits[["OLS"]]
            m_iv  <- fits[[p$iv_key]]

            co_ols <- safe_coef(m_ols, p$treatment)
            co_iv  <- safe_coef(m_iv,  paste0("fit_", p$treatment))

            fs <- fitstat_F(m_iv)

            rows[[length(rows) + 1L]] <- data.frame(
                panel        = p$id,
                outcome      = out$var,
                outcome_lab  = out$label,
                ols_est      = co_ols$est, ols_se = co_ols$se, ols_p = co_ols$p,
                iv_est       = co_iv$est,  iv_se  = co_iv$se,  iv_p  = co_iv$p,
                iv_F         = fs,
                n_obs        = nobs(m_ols),
                stringsAsFactors = FALSE
            )
        }
    }
    df <- do.call(rbind, rows)

    # ----- Console summary -----
    message("\n[t13] Counterfactual decomposition — coefficient on Δlog MA")
    message(sprintf("%-1s  %-32s  %-15s %-15s %-5s %-5s",
                    "P", "Outcome", "OLS", "IV", "F", "N"))
    for (i in seq_len(nrow(df))) {
        r <- df[i, ]
        message(sprintf("%-1s  %-32s  %s %s %5.1f %-5d",
                        r$panel, r$outcome,
                        fmt(r$ols_est, r$ols_se, r$ols_p),
                        fmt(r$iv_est,  r$iv_se,  r$iv_p),
                        r$iv_F, r$n_obs))
    }

    # ----- LaTeX output -----
    tex_lines <- c(
        "% Table 13: Counterfactual MA decomposition (population outcomes).",
        "% Generated by code/analysis/table_13_counterfactual.R.",
        "%",
        "% Panel A: full MA (both modes change). IV: Both (LP and Hypo).",
        "% Panel B: only-rail MA (roads frozen at 1954). IV: LP.",
        "% Panel C: only-road MA (rails frozen at 1960). IV: Hypo (LCP-MST).",
        "%",
        "% Same outcomes and controls as Table 9. Robust (HC1) SE.",
        "% First-stage F reported per IV cell.",
        "",
        "\\begin{table}[htbp]",
        "\\centering",
        "\\caption{Counterfactual decomposition of the population effect}",
        "\\label{tab:counterfactual}",
        "\\small",
        "\\begin{tabular}{lcc}",
        "\\toprule",
        " & (1) OLS & (2) IV \\\\",
        "\\midrule"
    )

    for (p in panels) {
        tex_lines <- c(tex_lines,
            sprintf("\\multicolumn{3}{l}{\\textit{%s}} \\\\", p$title),
            sprintf("\\multicolumn{3}{l}{\\quad Instrument in column (2): %s} \\\\",
                    p$iv_label))

        for (out in outcomes) {
            r <- df[df$panel == p$id & df$outcome == out$var, ]
            if (nrow(r) != 1L) next
            tex_lines <- c(tex_lines,
                sprintf("\\quad %s & %s & %s \\\\",
                        out$label,
                        tex_cell(r$ols_est, r$ols_se, r$ols_p),
                        tex_cell(r$iv_est,  r$iv_se,  r$iv_p)))
        }

        # First-stage F row (only meaningful for column 2)
        f_strs <- vapply(outcomes, function(out) {
            r <- df[df$panel == p$id & df$outcome == out$var, ]
            if (nrow(r) != 1L) return("---")
            sprintf("%.1f", r$iv_F)
        }, character(1L))
        # Use the first F (population total) as a single representative,
        # since outcomes share the same instrument and similar samples.
        # Show all four if you want; here we list them inline.
        tex_lines <- c(tex_lines,
            sprintf("\\quad First-stage $F$ (by outcome) & --- & %s \\\\",
                    paste(f_strs, collapse = " / ")),
            sprintf("\\quad Observations & %d & %d \\\\",
                    df$n_obs[df$panel == p$id & df$outcome == "chg_log_pop_91_60"],
                    df$n_obs[df$panel == p$id & df$outcome == "chg_log_pop_91_60"]),
            "\\midrule"
        )
    }

    tex_lines <- c(tex_lines[-length(tex_lines)],  # drop trailing \midrule
        "\\bottomrule",
        "\\end{tabular}",
        "",
        "\\footnotesize",
        paste0("\\emph{Notes}: Each cell is one regression of the row outcome ",
               "on the panel's $\\Delta \\ln \\mathrm{MA}$ measure. Column~(1) ",
               "is OLS; column~(2) instruments the MA measure with the panel-",
               "specific instrument noted in italics. Panel~A reports the joint ",
               "IV-Both (replicating Table~\\ref{tab:population_iv}). Panel~B ",
               "uses the Larkin Plan instrument; Panel~C uses the LCP-MST ",
               "hypothetical-road instrument. Each panel's first-stage $F$ row ",
               "lists $F$ for the four outcomes in the same order as the ",
               "outcome rows. All regressions include baseline log MA (1960), ",
               "baseline log pop (1960), and the six standardized geographic ",
               "controls. Robust (HC1) standard errors. ",
               "$^{*}p<0.10,\\;^{**}p<0.05,\\;^{***}p<0.01$."),
        "\\end{table}"
    )

    out_tex <- file.path(dir_tables, "table_13_counterfactual.tex")
    writeLines(tex_lines, out_tex)
    message("\nSaved: ", out_tex)

    out_csv <- file.path(dir_tables, "table_13_counterfactual.csv")
    write.csv(df, out_csv, row.names = FALSE)
    message("Saved: ", out_csv)
}

# ---------------------------------------------------------------------------
# Helpers (table-local: console-print and tex-cell formatters)
# ---------------------------------------------------------------------------
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

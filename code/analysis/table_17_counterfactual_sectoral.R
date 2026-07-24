# ===========================================================================
# table_17_counterfactual_sectoral.R
#
# PURPOSE: Paper Table 17 — counterfactual MA decomposition for the
#          SECTORAL outcomes (Cote reading notes #40/#45, email
#          2026-07-24: "población no es donde le estamos pegando;
#          manufacturas sí" — Table 13 decomposes only the population
#          outcomes). Mirrors table_13_counterfactual.R exactly:
#
#   Panel A. Full MA       — Δlog MA^full (both modes change);
#                            IV-Both, replicating the Table 10 spec.
#   Panel B. Only-rail MA  — roads frozen at 1954; IV = Larkin Plan.
#                            The panel Cote's note #40 asks about: if
#                            manufacturing loads on the rail-loss
#                            channel, that is evidence toward the
#                            de-industrialization reading.
#   Panel C. Only-road MA  — rails frozen at 1960; IV = LCP-MST hypo.
#
#   As in Table 13, the two counterfactual measures are NEVER in the
#   same regression (spatially correlated; compare across panels).
#
# DEP VARS (five outcomes; same as Table 10):
#   chg_log_valprod_85_54     mfg value of production
#   chg_log_massal_85_54      mfg wage mass
#   chg_log_nestab_85_54      mfg establishments
#   chg_log_nexp_88_60        agriculture: number of farms
#   chg_log_areatot_ha_88_60  agriculture: farmed area
#
# CONTROLS: geo_controls_main (same as Tables 9/10/13). SE: HC1.
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/table_17_counterfactual_sectoral.{tex,csv}
# ===========================================================================
suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
})

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)
    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)

    d <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet")
    )

    outcomes <- list(
        list(var = "chg_log_valprod_85_54",
             label = "$\\Delta \\ln (\\mathrm{Value\\ of\\ production})$"),
        list(var = "chg_log_massal_85_54",
             label = "$\\Delta \\ln (\\mathrm{Wage\\ mass})$"),
        list(var = "chg_log_nestab_85_54",
             label = "$\\Delta \\ln (\\mathrm{Establishments})$"),
        list(var = "chg_log_nexp_88_60",
             label = "$\\Delta \\ln (\\mathrm{Farms})$"),
        list(var = "chg_log_areatot_ha_88_60",
             label = "$\\Delta \\ln (\\mathrm{Farmed\\ area})$")
    )

    panels <- list(
        list(
            id        = "A",
            title     = "A. Full MA: $\\Delta \\ln \\mathrm{MA}^{\\mathrm{full}}$ (both modes change)",
            treatment = "chg_logMA_86_60_s0_elow",
            iv_key    = "IV-B",
            iv_label  = "IV-Both (LP and Hypo)"
        ),
        list(
            id        = "B",
            title     = "B. Only-rail MA: $\\Delta \\ln \\mathrm{MA}^{\\mathrm{only\\_rail}}$ (roads frozen at 1954)",
            treatment = "chg_logMA_only_rail_s0_elow",
            iv_key    = "IV-LP",
            iv_label  = "IV-LP (Larkin Plan)"
        ),
        list(
            id        = "C",
            title     = "C. Only-road MA: $\\Delta \\ln \\mathrm{MA}^{\\mathrm{only\\_road}}$ (rails frozen at 1960)",
            treatment = "chg_logMA_only_road_s0_elow",
            iv_key    = "IV-H",
            iv_label  = "IV-Hypo (LCP-MST)"
        )
    )

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
                panel       = p$id,
                outcome     = out$var,
                outcome_lab = out$label,
                ols_est = co_ols$est, ols_se = co_ols$se, ols_p = co_ols$p,
                iv_est  = co_iv$est,  iv_se  = co_iv$se,  iv_p  = co_iv$p,
                iv_F    = fs,
                n_obs   = nobs(m_ols),
                stringsAsFactors = FALSE
            )
        }
    }
    df <- do.call(rbind, rows)

    message("\n[t17] Sectoral counterfactual decomposition")
    message(sprintf("%-1s  %-28s  %-16s %-16s %-5s %-5s",
                    "P", "Outcome", "OLS", "IV", "F", "N"))
    for (i in seq_len(nrow(df))) {
        r <- df[i, ]
        message(sprintf("%-1s  %-28s  %s %s %5.1f %-5d",
                        r$panel, r$outcome,
                        fmt(r$ols_est, r$ols_se, r$ols_p),
                        fmt(r$iv_est,  r$iv_se,  r$iv_p),
                        r$iv_F, r$n_obs))
    }

    tex_lines <- c(
        "% Table 17: Counterfactual MA decomposition (sectoral outcomes).",
        "% Generated by code/analysis/table_17_counterfactual_sectoral.R.",
        "% Mirrors Table 13's panel structure for the Table 10 outcomes",
        "% (Cote reading notes #40/#45).",
        "",
        "\\begin{table}[htbp]",
        "\\centering",
        "\\caption{Counterfactual decomposition of the sectoral effects}",
        "\\label{tab:counterfactual_sectoral}",
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
        f_strs <- vapply(outcomes, function(out) {
            r <- df[df$panel == p$id & df$outcome == out$var, ]
            if (nrow(r) != 1L) return("---")
            sprintf("%.1f", r$iv_F)
        }, character(1L))
        n_strs <- vapply(outcomes, function(out) {
            r <- df[df$panel == p$id & df$outcome == out$var, ]
            if (nrow(r) != 1L) return("---")
            as.character(r$n_obs)
        }, character(1L))
        tex_lines <- c(tex_lines,
            sprintf("\\quad First-stage $F$ (by outcome) & --- & %s \\\\",
                    paste(f_strs, collapse = " / ")),
            sprintf("\\quad Observations (by outcome) & --- & %s \\\\",
                    paste(n_strs, collapse = " / ")),
            "\\midrule"
        )
    }

    tex_lines <- c(tex_lines[-length(tex_lines)],
        "\\bottomrule",
        "\\end{tabular}",
        "",
        "\\footnotesize",
        paste0("\\emph{Notes}: Each cell is one regression of the row outcome ",
               "on the panel's $\\Delta \\ln \\mathrm{MA}$ measure, mirroring ",
               "Table~\\ref{tab:counterfactual} for the sectoral outcomes of ",
               "Table~\\ref{tab:sectoral_iv}. Column~(1) is OLS; column~(2) ",
               "instruments the MA measure with the panel-specific instrument ",
               "noted in italics. The two counterfactual measures are never ",
               "in the same regression. Each panel's first-stage $F$ and ",
               "observation rows list the five outcomes in row order. All ",
               "regressions include baseline log MA (1960), baseline log pop ",
               "(1960), and the six standardized geographic controls. Robust ",
               "(HC1) standard errors. ",
               "$^{*}p<0.10,\\;^{**}p<0.05,\\;^{***}p<0.01$."),
        "\\end{table}"
    )

    out_tex <- file.path(dir_tables, "table_17_counterfactual_sectoral.tex")
    writeLines(tex_lines, out_tex)
    message("\nSaved: ", out_tex)

    out_csv <- file.path(dir_tables, "table_17_counterfactual_sectoral.csv")
    write.csv(df, out_csv, row.names = FALSE)
    message("Saved: ", out_csv)
}

# ---------------------------------------------------------------------------
# Helpers (table-local: console-print and tex-cell formatters, as Table 13)
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

# ===========================================================================
# table_16_sector_matched.R
#
# PURPOSE: Paper table (C3, sector-matched market access) — the five
#          sectoral outcomes of Table 10 re-estimated with the MA measure
#          constructed under the cargo-density cost schedule that matches
#          each sector's cargo profile (config.R sector mapping, from
#          Baumgartner-Palazzo Table II):
#
#            manufacturing outcomes <- s2 (low density, 100 t/day:
#                                          small-batch goods, road-favouring)
#            agricultural outcomes  <- s1 (high density, 1,000 t/day:
#                                          bulk grain, rail-favouring)
#
#          For each outcome, every MA object switches to the matched
#          schedule: treatment (chg_logMA_86_60_sX_elow), both
#          instruments (chg_logMA_stu_sX_elow, chg_logMA_lcp_mst_sX_elow),
#          and the baseline log-MA control (logMA_actual_1960_sX_elow).
#          Other controls, theta (= 4.55), and HC1 SE identical to
#          Table 10, whose baseline-schedule (s0) estimates are the
#          comparison the prose quotes.
#
# DESIGN DECISION (documented for Cote, 2026-07-17): Table 10 (s0)
#          remains the headline sectoral exhibit; this table is the
#          matched companion, not a replacement. Rationale: s0 keeps all
#          outcomes on one common treatment (comparable magnitudes),
#          while the matched schedules answer whether the sectoral
#          contrast strengthens when each sector faces its own cost
#          structure — the scale-economies question of Section 2.4.
#
# COLUMNS: (1) OLS  (2) IV-LP  (3) IV-Hypo  (4) IV-Both, as Table 10.
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/table_16_sector_matched.{tex,csv}
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
        list(var = "chg_log_nestab_85_54",  s = "s2", panel = "A",
             label = "Mfg.\\ establishments"),
        list(var = "chg_log_valprod_85_54", s = "s2", panel = "A",
             label = "Mfg.\\ production value"),
        list(var = "chg_log_massal_85_54",  s = "s2", panel = "A",
             label = "Mfg.\\ wage mass"),
        list(var = "chg_log_nexp_88_60",    s = "s1", panel = "B",
             label = "Ag.\\ farms"),
        list(var = "chg_log_areatot_ha_88_60", s = "s1", panel = "B",
             label = "Ag.\\ farmed area")
    )

    stopifnot("logMA_actual_1960_s0_elow" %in% geo_controls_main)

    rows <- list()
    for (out in outcomes) {
        s <- out$s
        endog  <- sprintf("chg_logMA_86_60_%s_elow", s)
        lp     <- sprintf("chg_logMA_stu_%s_elow", s)
        hypo   <- sprintf("chg_logMA_lcp_mst_%s_elow", s)
        ma_ctl <- sprintf("logMA_actual_1960_%s_elow", s)
        ctrls  <- c(setdiff(geo_controls_main, "logMA_actual_1960_s0_elow"),
                    ma_ctl)
        stopifnot(all(c(endog, lp, hypo, ma_ctl, out$var) %in% names(d)))

        fits <- fit_iv_quad(
            y = out$var, data = d,
            endog = endog, lp_instr = lp, hypo_instr = hypo,
            ctrls_vec = ctrls
        )
        co_ols <- safe_coef(fits[["OLS"]],   endog)
        co_lp  <- safe_coef(fits[["IV-LP"]], paste0("fit_", endog))
        co_h   <- safe_coef(fits[["IV-H"]],  paste0("fit_", endog))
        co_b   <- safe_coef(fits[["IV-B"]],  paste0("fit_", endog))

        rows[[length(rows) + 1L]] <- data.frame(
            panel     = out$panel,
            outcome   = out$var,
            schedule  = s,
            label     = out$label,
            ols_est   = co_ols$est, ols_se   = co_ols$se, ols_p = co_ols$p,
            iv_lp_est = co_lp$est,  iv_lp_se = co_lp$se,  iv_lp_p = co_lp$p,
            iv_h_est  = co_h$est,   iv_h_se  = co_h$se,   iv_h_p  = co_h$p,
            iv_b_est  = co_b$est,   iv_b_se  = co_b$se,   iv_b_p  = co_b$p,
            iv_lp_F   = fitstat_F(fits[["IV-LP"]]),
            iv_h_F    = fitstat_F(fits[["IV-H"]]),
            iv_b_F    = fitstat_F(fits[["IV-B"]]),
            n_obs     = nobs(fits[["OLS"]]),
            stringsAsFactors = FALSE
        )
    }
    df <- do.call(rbind, rows)

    message("\n[t16] Sector-matched MA: coefficient on matched dlogMA")
    for (i in seq_len(nrow(df))) {
        r <- df[i, ]
        message(sprintf(
            "  %-28s [%s] IV-B %+6.3f (%.3f) p=%.3f  F=%.1f  N=%d",
            r$outcome, r$schedule, r$iv_b_est, r$iv_b_se, r$iv_b_p,
            r$iv_b_F, r$n_obs))
    }

    # ----------------------------------------------------------------------
    # LaTeX (handmade panel layout, as tables 12/15)
    # ----------------------------------------------------------------------
    tex_cell <- function(est, se, p) {
        if (is.na(est)) return(" ")
        stars <- ifelse(p < 0.01, "$^{***}$",
                ifelse(p < 0.05, "$^{**}$",
                ifelse(p < 0.10, "$^{*}$", "")))
        sprintf(
            "\\begin{tabular}{@{}c@{}} %.3f%s \\\\ (%.3f) \\end{tabular}",
            est, stars, se)
    }

    tex_lines <- c(
        "% Table 16: sector-matched MA regressions (C3).",
        "% Generated by code/analysis/table_16_sector_matched.R.",
        "% Manufacturing outcomes use the low-density (road-favouring) MA;",
        "% agricultural outcomes use the high-density (rail-favouring) MA.",
        "% All MA objects (treatment, instruments, baseline control) switch.",
        "",
        "\\begin{table}[htbp]",
        "\\centering",
        "\\caption{Sectoral Outcomes Under Sector-Matched Cost Schedules}",
        "\\label{tab:sector_matched}",
        "\\small",
        "\\begin{tabular}{lcccc}",
        "\\toprule",
        "Outcome & (1) OLS & (2) IV-LP & (3) IV-Hypo & (4) IV-Both \\\\",
        "\\midrule",
        paste0("\\multicolumn{5}{l}{\\textit{Panel A: manufacturing ",
               "outcomes, low-density (100 t/day) MA}} \\\\"))
    for (i in seq_len(nrow(df))) {
        r <- df[i, ]
        if (r$panel == "B" && df$panel[max(i - 1, 1)] == "A") {
            tex_lines <- c(tex_lines,
                "\\midrule",
                paste0("\\multicolumn{5}{l}{\\textit{Panel B: agricultural ",
                       "outcomes, high-density (1{,}000 t/day) MA}} \\\\"))
        }
        tex_lines <- c(tex_lines,
            sprintf("%s & %s & %s & %s & %s \\\\",
                    r$label,
                    tex_cell(r$ols_est,   r$ols_se,   r$ols_p),
                    tex_cell(r$iv_lp_est, r$iv_lp_se, r$iv_lp_p),
                    tex_cell(r$iv_h_est,  r$iv_h_se,  r$iv_h_p),
                    tex_cell(r$iv_b_est,  r$iv_b_se,  r$iv_b_p)))
    }
    tex_lines <- c(tex_lines,
        "\\bottomrule",
        "\\end{tabular}",
        "",
        "\\footnotesize",
        paste0("\\emph{Notes}: Each row re-estimates the corresponding ",
               "Table~\\ref{tab:sectoral_iv} specification with every ",
               "market-access object --- treatment, both instruments, and ",
               "the baseline log-MA control --- constructed under the cost ",
               "schedule matched to the sector's cargo profile ",
               "(Table~\\ref{tab:cost_params}): low density for ",
               "manufacturing (small-batch goods), high density for ",
               "agriculture (bulk grain). Geographic controls, baseline ",
               "log population, and $\\theta = \\thetaLow{}$ are identical ",
               "to Table~\\ref{tab:sectoral_iv}. Sample sizes match ",
               "Table~\\ref{tab:sectoral_iv} row-for-row. Robust (HC1) SE. ",
               "Significance: ",
               "$^{*}p<0.10,\\;^{**}p<0.05,\\;^{***}p<0.01$."),
        "\\end{table}"
    )

    out_tex <- file.path(dir_tables, "table_16_sector_matched.tex")
    writeLines(tex_lines, out_tex)
    message("Saved: ", out_tex)

    out_csv <- file.path(dir_tables, "table_16_sector_matched.csv")
    write.csv(df, out_csv, row.names = FALSE)
    message("Saved: ", out_csv)
}

main()

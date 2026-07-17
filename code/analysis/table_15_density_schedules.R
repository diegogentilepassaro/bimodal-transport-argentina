# ===========================================================================
# table_15_density_schedules.R
#
# PURPOSE: Paper table — the headline population result estimated under
#          each of the three Baumgartner-Palazzo cargo-density cost
#          schedules (Section 3's Table of cost parameters):
#
#            s0  medium density (500 t/day)  — main specification
#            s1  high   density (1,000 t/day) — rail-favouring costs
#            s2  low    density (100 t/day)   — road-favouring costs
#
#          One row per schedule, columns OLS / IV-LP / IV-Hypo / IV-Both,
#          exactly the Table 9 estimator grid. For each row, ALL market-
#          access objects switch to the row's schedule: the treatment
#          (chg_logMA_86_60_sX_elow), both instruments
#          (chg_logMA_stu_sX_elow, chg_logMA_lcp_mst_sX_elow), and the
#          baseline log-MA control (logMA_actual_1960_sX_elow). Geographic
#          controls, baseline log population, theta (= 4.55), and HC1
#          standard errors are identical across rows.
#
#          The script also computes the cross-schedule correlations of
#          the treatment (d logMA under s1/s2 vs s0), which the paper
#          quotes when stating why medium density is the baseline.
#
# MOTIVATION (2026-07-17, Diego): the density schedules deserve
#          prominence, not a robustness footnote — at high density rail
#          is half the cost of road, at low density road wins, so the
#          extremes are mode-tilted by construction and the cross-
#          schedule comparison is economically meaningful (the
#          scale-economies channel of Section 2.4). Presentation follows
#          Donaldson-Hornbeck Table I: variants as rows of a main-text
#          exhibit next to the baseline.
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/table_15_density_schedules.{tex,csv}
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

    schedules <- list(
        list(s = "s0", label = "Medium density (500 t/day, baseline)"),
        list(s = "s1", label = "High density (1{,}000 t/day)"),
        list(s = "s2", label = "Low density (100 t/day)")
    )

    rows <- list()
    for (sch in schedules) {
        s <- sch$s
        endog  <- sprintf("chg_logMA_86_60_%s_elow", s)
        lp     <- sprintf("chg_logMA_stu_%s_elow", s)
        hypo   <- sprintf("chg_logMA_lcp_mst_%s_elow", s)
        ma_ctl <- sprintf("logMA_actual_1960_%s_elow", s)
        ctrls  <- c(setdiff(geo_controls_main, "logMA_actual_1960_s0_elow"),
                    ma_ctl)
        stopifnot(all(c(endog, lp, hypo, ma_ctl) %in% names(d)))

        fits <- fit_iv_quad(
            y = "chg_log_pop_91_60", data = d,
            endog = endog, lp_instr = lp, hypo_instr = hypo,
            ctrls_vec = ctrls
        )

        co_ols <- safe_coef(fits[["OLS"]],   endog)
        co_lp  <- safe_coef(fits[["IV-LP"]], paste0("fit_", endog))
        co_h   <- safe_coef(fits[["IV-H"]],  paste0("fit_", endog))
        co_b   <- safe_coef(fits[["IV-B"]],  paste0("fit_", endog))

        rows[[length(rows) + 1L]] <- data.frame(
            schedule  = s,
            label     = sch$label,
            ols_est   = co_ols$est, ols_se   = co_ols$se, ols_p = co_ols$p,
            iv_lp_est = co_lp$est,  iv_lp_se = co_lp$se,  iv_lp_p = co_lp$p,
            iv_h_est  = co_h$est,   iv_h_se  = co_h$se,   iv_h_p  = co_h$p,
            iv_b_est  = co_b$est,   iv_b_se  = co_b$se,   iv_b_p  = co_b$p,
            iv_lp_F   = fitstat_F(fits[["IV-LP"]]),
            iv_h_F    = fitstat_F(fits[["IV-H"]]),
            iv_b_F    = fitstat_F(fits[["IV-B"]]),
            # Treatment correlation with the baseline schedule (quoted in
            # Section 5's prose as the reason the grid is informative).
            corr_treat_s0 = cor(d[[endog]], d[["chg_logMA_86_60_s0_elow"]],
                                use = "complete.obs"),
            n_obs     = nobs(fits[["OLS"]]),
            stringsAsFactors = FALSE
        )
    }
    df <- do.call(rbind, rows)

    message("\n[t15] Population elasticity by cargo-density schedule")
    for (i in seq_len(nrow(df))) {
        r <- df[i, ]
        message(sprintf(
            "  %-38s IV-B %+6.3f (%.3f) p=%.3f  F=%.1f  corr(s0)=%.3f",
            r$label, r$iv_b_est, r$iv_b_se, r$iv_b_p, r$iv_b_F,
            r$corr_treat_s0))
    }

    # ----------------------------------------------------------------------
    # LaTeX
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
        "% Table 15: population elasticity across cargo-density cost schedules.",
        "% Generated by code/analysis/table_15_density_schedules.R.",
        "% One row per Baumgartner-Palazzo schedule; all MA objects (treatment,",
        "% both instruments, baseline log-MA control) switch with the row.",
        "",
        "\\begin{table}[htbp]",
        "\\centering",
        "\\caption{Population Elasticity Across Cargo-Density Cost Schedules",
        "(outcome: $\\Delta \\ln \\mathrm{Pop}_{1960 \\to 1991}$)}",
        "\\label{tab:density_schedules}",
        "\\small",
        "\\begin{tabular}{lcccc}",
        "\\toprule",
        "Cost schedule & (1) OLS & (2) IV-LP & (3) IV-Hypo & (4) IV-Both \\\\",
        "\\midrule"
    )
    for (i in seq_len(nrow(df))) {
        r <- df[i, ]
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
        paste0("\\emph{Notes}: Each row re-estimates the four Table~",
               "\\ref{tab:population_iv} specifications with every ",
               "market-access object --- the treatment, both instruments, ",
               "and the baseline log-MA control --- constructed under the ",
               "row's \\citet{baumgartnerpalazzo1969} cargo-density cost ",
               "schedule (Table~\\ref{tab:cost_params}). Geographic ",
               "controls, baseline log population, and $\\theta = ",
               "\\thetaLow{}$ are identical across rows. ",
               sprintf("$N = %d$. ", df$n_obs[1]),
               "Robust (HC1) SE. Significance: ",
               "$^{*}p<0.10,\\;^{**}p<0.05,\\;^{***}p<0.01$."),
        "\\end{table}"
    )

    out_tex <- file.path(dir_tables, "table_15_density_schedules.tex")
    writeLines(tex_lines, out_tex)
    message("Saved: ", out_tex)

    out_csv <- file.path(dir_tables, "table_15_density_schedules.csv")
    write.csv(df, out_csv, row.names = FALSE)
    message("Saved: ", out_csv)
}

main()

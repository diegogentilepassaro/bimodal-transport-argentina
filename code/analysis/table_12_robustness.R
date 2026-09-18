# ===========================================================================
# table_12_robustness.R
#
# PURPOSE: Paper Table 12 — robustness of the headline Table 9 result
#          (total population, chg_log_pop_91_60). Three panels:
#
#   Panel A: Alternative trade elasticity theta_high (main spec uses
#            theta_low; both from config.R). All MA variables switch
#            from _elow to _ehigh.
#
#   Panel B: Alternative hypothetical-road instruments. Main spec uses
#            chg_logMA_lcp_mst_s0_elow. Alternatives: euc_mst, lcp, euc.
#            Each row is a separate Just-IV-Hypo regression (one
#            instrument at a time); rows are labeled by the instrument
#            used. LP and Both columns reuse the main spec's LP
#            instrument paired with the alternative hypo.
#
#   Panel C: Sample robustness. Refit the main Table 9 specification on
#            the placebo subsample where the 1947 placebo outcome
#            is defined (see Section 4.5). Tests whether the OLS-IV gap
#            or the coefficient level changes on that subsample.
#
#   Panel D: Controls ladder. The control blocks enter one at a time,
#            ending at the main specification. Each instrumented cell
#            also carries that rung's robust first-stage F, because the
#            baseline log-MA control is part of the identification and
#            not a nuisance covariate, so the rungs change the first
#            stage too. See the block comment at Panel D.
#
# CONTROLS and SE: Panels A-C use geo_controls_main; Panel D varies the
# control set by construction. HC1 throughout.
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
    source(file.path(dir_code, "analysis", "_table_helpers.R"), echo = FALSE)
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
    # Panel A: alternative theta (theta_high). Replicates Table 9 col 4
    # (main outcome) with _ehigh MA vars.
    # ----------------------------------------------------------------------
    fits_A <- fit_iv_quad(
        y = "chg_log_pop_91_60", data = d,
        endog = "chg_logMA_86_60_s0_ehigh",
        lp_instr = "chg_logMA_stu_s0_ehigh",
        hypo_instr = "chg_logMA_lcp_mst_s0_ehigh",
        ctrls_vec = ctrls_ehigh
    )
    rows[[length(rows) + 1L]] <- build_row(
        panel = "A", label = sprintf("Alt. theta = %s", format(theta[["high"]])),
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
            endog = main_treatment,
            lp_instr = main_lp_instrument,
            hypo_instr = h$name,
            ctrls_vec = ctrls_elow
        )
        rows[[length(rows) + 1L]] <- build_row(
            panel = "B", label = sprintf("Hypo = %s", h$label),
            fits = fits_B, endog = main_treatment
        )
    }

    # ----------------------------------------------------------------------
    # Panel C: subsample stability. Refit the main spec on the subset
    # where chg_log_placebo_pop_60_47 is defined (Table 7 sample; the
    # count is computed, not hardcoded — 237 since issue #22).
    # ----------------------------------------------------------------------
    d_sub <- d[!is.na(d$chg_log_placebo_pop_60_47), ]
    n_sub <- nrow(d_sub)
    fits_C <- fit_iv_quad(
        y = "chg_log_pop_91_60", data = d_sub,
        endog = main_treatment,
        lp_instr = main_lp_instrument,
        hypo_instr = main_hypo_instrument,
        ctrls_vec = ctrls_elow
    )
    rows[[length(rows) + 1L]] <- build_row(
        panel = "C", label = sprintf("Placebo subsample (N=%d)", nrow(d_sub)),
        fits = fits_C, endog = main_treatment
    )

    # For comparison, also report the main-spec IV-Both on the full sample
    fits_main <- fit_iv_quad(
        y = "chg_log_pop_91_60", data = d,
        endog = main_treatment,
        lp_instr = main_lp_instrument,
        hypo_instr = main_hypo_instrument,
        ctrls_vec = ctrls_elow
    )
    rows[[length(rows) + 1L]] <- build_row(
        panel = "C", label = "Full sample (for reference)",
        fits = fits_main, endog = main_treatment
    )

    # ----------------------------------------------------------------------
    # Panel D: controls ladder (coauthor request, 2026-09). Adds the
    # control blocks one at a time so the sensitivity of the headline
    # estimate to the control set is on the page.
    #
    # READ THIS BEFORE READING THE PANEL. The rungs are NOT the same
    # estimator with fewer covariates; controls_ladder() in _iv_helpers.R
    # documents why. The short version is that the baseline log-MA term is
    # part of the identification, so removing it changes the first stage.
    # In this panel that is the largest single movement: the
    # hypothetical-road instrument's robust F is 26.35 at rung (2) and
    # 3.75 at rung (3). The weak-hypo fact the paper reports is therefore
    # a consequence of conditioning on baseline MA rather than a property
    # of the instrument on its own. Every rung prints its F so the reader
    # can see which of the two is moving.
    #
    # The rungs come from controls_ladder() in _iv_helpers.R so that this
    # panel and appendix Table B3 cannot drift apart.
    # ----------------------------------------------------------------------
    ladder <- controls_ladder(geo_controls_main)
    fits_D_last <- NULL
    for (rung in ladder) {
        fits_D <- fit_iv_quad(
            y = "chg_log_pop_91_60", data = d,
            endog = main_treatment,
            lp_instr = main_lp_instrument,
            hypo_instr = main_hypo_instrument,
            ctrls_vec = rung$ctrls
        )
        rows[[length(rows) + 1L]] <- build_row(
            panel = "D", label = rung$label,
            fits = fits_D, endog = main_treatment
        )
        fits_D_last <- fits_D
    }
    # The top rung IS the main specification, so it must reproduce the
    # Panel C reference row exactly. If it does not, the ladder is built
    # on a different control set than the paper's tables and every rung
    # below is uninterpretable. Machine precision, not display precision.
    for (key in c("OLS", "IV-LP", "IV-H", "IV-B")) {
        nm <- if (key == "OLS") main_treatment else paste0("fit_", main_treatment)
        stopifnot(
            "Panel D rung (4) must reproduce the main specification" =
                isTRUE(all.equal(
                    safe_coef(fits_D_last[[key]], nm)$est,
                    safe_coef(fits_main[[key]], nm)$est
                ))
        )
    }

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
        sprintf("%% Panel A: alternative trade elasticity theta = %s (main = %s).",
                format(theta[["high"]]), format(theta[["low"]])),
        "% Panel B: alternative hypothetical-road instruments.",
        "% Panel C: subsample stability (placebo subset; N computed).",
        "% Panel D: controls ladder; each IV cell carries its rung's",
        "%          robust first-stage F, because the rungs change the",
        "%          instrument as well as the control set.",
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
        if (r$panel == "D" && i > 1 && df$panel[i - 1] == "C") {
            tex_lines <- c(tex_lines,
                "\\midrule",
                paste0("\\multicolumn{6}{l}{\\textit{Panel D: controls ",
                       "ladder (first-stage $F$ in brackets)}} \\\\")
            )
        }
        # Panel D carries a third line per IV cell holding the first-stage
        # F, because the rungs change the instrument as well as the control
        # set. See the block comment on Panel D above.
        if (r$panel == "D") {
            tex_lines <- c(tex_lines,
                sprintf("%s & %s & %s & %s & %s & %s \\\\",
                        r$panel,
                        r$label,
                        tex_cell(r$ols_est, r$ols_se, r$ols_p),
                        tex_cell_F(r$iv_lp_est, r$iv_lp_se, r$iv_lp_p, r$iv_lp_F),
                        tex_cell_F(r$iv_h_est,  r$iv_h_se,  r$iv_h_p,  r$iv_h_F),
                        tex_cell_F(r$iv_b_est,  r$iv_b_se,  r$iv_b_p,  r$iv_b_F))
            )
            next
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
               sprintf("$\\theta = %s$ to $\\theta = %s$ (all MA variables, ",
                       format(theta[["low"]]), format(theta[["high"]])),
               "treatment, instruments, and baseline control, switch to ",
               "\\texttt{\\_ehigh} variants). Panel~B holds $\\theta$ at ",
               sprintf("%s and replaces the LCP-MST hypothetical-road ",
                       format(theta[["low"]])),
               "instrument with one of three alternatives. Panel~C ",
               sprintf("refits the main specification on the %d-district ",
                       n_sub),
               "subset for which the 1947 placebo outcome is defined. ",
               "Panel~D adds the control blocks one at a time, ending at ",
               "the specification used in Table~\\ref{tab:population_iv}; ",
               "the bracketed figure in each instrumented cell is that ",
               "rung's heteroskedasticity-robust first-stage $F$. The ",
               "rungs are not the same estimator with fewer covariates: ",
               "the baseline log market access term is the convergence ",
               "control that makes the instruments' variation comparable ",
               "across districts, so removing it changes the first stage ",
               "and not only the second. Movement between rungs~(2) ",
               "and~(3) therefore mixes sensitivity to the control set ",
               "with a change in instrument strength, which is why the ",
               "$F$ is shown on every rung. ",
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

# tex_cell_F(): tex_cell() plus a third line holding the first-stage F.
#
# Used by Panel D only. The shared tex_cell() in _table_helpers.R is left
# alone so that Panels A-C, and every other table that calls it, are
# unchanged byte for byte.
tex_cell_F <- function(est, se, p, F_stat) {
    if (is.na(est)) return(" ")
    sprintf(
        paste0("\\begin{tabular}{@{}c@{}} %.3f%s \\\\ (%.3f) \\\\ ",
               "{\\scriptsize [%.1f]} \\end{tabular}"),
        est, star_str(p, tex = TRUE), se, F_stat
    )
}

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
        iv_lp_F      = fitstat_F_robust(fits[["IV-LP"]]),
        iv_h_F       = fitstat_F_robust(fits[["IV-H"]]),
        iv_b_F       = fitstat_F_robust(fits[["IV-B"]]),
        n_obs        = nobs(fits[["OLS"]]),
        stringsAsFactors = FALSE
    )
}

main()

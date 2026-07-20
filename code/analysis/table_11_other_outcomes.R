# ===========================================================================
# table_11_other_outcomes.R
#
# PURPOSE: Paper Table 11 — IV regressions of education, migration, and
#          employment-status outcomes on the change in log market access.
#
# DEP VARS (four outcomes, all level changes in population shares,
# stored in the estimation sample by build_estimation_sample.R):
#   chg_college_91_70    College share in 1991 minus in 1970.
#   chg_secondary_91_70  Secondary-education share in 1991 minus in 1970.
#   chg_mig5_91_70       Share of residents who migrated in last 5 years,
#                        1991 minus 1970.
#   chg_empstat_emp_91_70  Employment rate (empstat==employed), 1991 minus
#                          1970. IPUMS EMPSTAT coding: 1=Employed,
#                          2=Unemployed, 3=Inactive/NILF. We use empstat_1.
#
# WINDOW CAVEAT:
#   The paper's main estimation window is 1960-1991. IPUMS does not publish
#   microdata for Argentina before 1970, so these four outcomes are defined
#   as 1970-1991 level changes, NOT 1960-1991. The regressor remains
#   chg_logMA_86_60_s0_elow (the 1960-1986 MA change) since infrastructure
#   restructuring predates these outcomes. This asymmetry should be
#   acknowledged in Section 5.4 prose.
#
# COLUMNS: same 4-column IV structure as Tables 9, 10.
#
# CONTROLS: geo_controls_main from config.R.
#
# SE: heteroskedasticity-robust (HC1).
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/table_11_other_outcomes_iv.{tex,csv}
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

    # Level-change outcomes (1970 -> 1991) are stored columns, built in
    # build_estimation_sample.R. Guard against a stale D.1 output.
    stopifnot(all(c("chg_college_91_70", "chg_secondary_91_70",
                    "chg_mig5_91_70", "chg_empstat_emp_91_70")
                  %in% names(d)))

    outcomes <- list(
        list(var = "chg_college_91_70",
             label = "$\\Delta$(College share)"),
        list(var = "chg_secondary_91_70",
             label = "$\\Delta$(Secondary share)"),
        list(var = "chg_mig5_91_70",
             label = "$\\Delta$(Recent-migration share)"),
        list(var = "chg_empstat_emp_91_70",
             label = "$\\Delta$(Employment rate)")
    )

    # Build 4 × 4 = 16 model fits
    all_models <- list()
    f_stats    <- list()
    for (out in outcomes) {
        y <- out$var
        fits <- fit_iv_quad(
            y = y, data = d,
            endog = "chg_logMA_86_60_s0_elow",
            lp_instr = "chg_logMA_stu_s0_elow",
            hypo_instr = main_hypo_instrument,
            ctrls_vec = geo_controls_main
        )
        for (spec in names(fits)) {
            all_models[[paste(y, spec, sep = "_")]] <- fits[[spec]]
        }
        f_stats[[y]] <- list(
            lp   = fitstat_F(fits[["IV-LP"]]),
            hypo = fitstat_F(fits[["IV-H"]]),
            both = fitstat_F(fits[["IV-B"]])
        )
    }

    # Console summary
    message("\n[t11] Coefficient on ΔlogMA across specifications:")
    message(sprintf("%-28s  %-13s %-13s %-13s %-13s  %-5s",
                    "Outcome", "OLS", "IV-LP", "IV-Hypo", "IV-Both", "N"))
    for (out in outcomes) {
        y <- out$var
        b_ols <- safe_coef(all_models[[paste(y, "OLS",   sep = "_")]],
                           "chg_logMA_86_60_s0_elow")
        b_lp  <- safe_coef(all_models[[paste(y, "IV-LP", sep = "_")]],
                           "fit_chg_logMA_86_60_s0_elow")
        b_h   <- safe_coef(all_models[[paste(y, "IV-H",  sep = "_")]],
                           "fit_chg_logMA_86_60_s0_elow")
        b_b   <- safe_coef(all_models[[paste(y, "IV-B",  sep = "_")]],
                           "fit_chg_logMA_86_60_s0_elow")
        n_ols <- nobs(all_models[[paste(y, "OLS", sep = "_")]])
        message(sprintf("%-28s  %-13s %-13s %-13s %-13s  %-5d",
                        y,
                        format_co(b_ols), format_co(b_lp),
                        format_co(b_h),   format_co(b_b),
                        n_ols))
    }

    # --- Build LaTeX ------------------------------------------------------
    coef_map <- c(
        "chg_logMA_86_60_s0_elow"     = "$\\Delta \\ln \\mathrm{MA}^{\\mathrm{full}}$",
        "fit_chg_logMA_86_60_s0_elow" = "$\\Delta \\ln \\mathrm{MA}^{\\mathrm{full}}$"
    )
    gof_custom <- list(
        list("raw" = "nobs", "clean" = "Observations", "fmt" = 0)
    )

    tex_chunks <- character()
    is_first_panel <- TRUE
    for (out in outcomes) {
        y <- out$var
        models_this <- list(
            "(1) OLS"      = all_models[[paste(y, "OLS",   sep = "_")]],
            "(2) IV-LP"    = all_models[[paste(y, "IV-LP", sep = "_")]],
            "(3) IV-Hypo"  = all_models[[paste(y, "IV-H",  sep = "_")]],
            "(4) IV-Both"  = all_models[[paste(y, "IV-B",  sep = "_")]]
        )
        fs <- f_stats[[y]]
        add_rows <- tibble::tibble(
            ` `           = "First-stage $F$",
            `(1) OLS`     = "---",
            `(2) IV-LP`   = sprintf("%.1f", fs$lp),
            `(3) IV-Hypo` = sprintf("%.1f", fs$hypo),
            `(4) IV-Both` = sprintf("%.1f", fs$both)
        )
        tbl <- modelsummary(
            models_this,
            output   = "latex",
            coef_map = coef_map,
            gof_map  = gof_custom,
            stars    = c("*" = .1, "**" = .05, "***" = .01),
            escape   = FALSE,
            add_rows = add_rows,
            title    = sprintf("Outcome: %s", out$label)
        )
        tbl_txt <- as.character(tbl)
        if (is_first_panel) {
            tbl_txt <- inject_first_label(tbl_txt, "tab:other_outcomes_iv")
            is_first_panel <- FALSE
        }
        tex_chunks <- c(tex_chunks, tbl_txt, "", "\\bigskip", "")
    }

    out_tex <- file.path(dir_tables, "table_11_other_outcomes_iv.tex")
    writeLines(c(
        "% Table 11: Other outcomes IV regressions (education, migration,",
        "%           employment rate).",
        "% Generated by code/analysis/table_11_other_outcomes.R.",
        "%",
        "% Outcomes are level changes in population shares between 1970 and",
        "% 1991 (not 1960-1991 -- IPUMS microdata for Argentina starts in",
        "% 1970). See Section 5.4 for window-asymmetry discussion.",
        "%",
        "% Each panel is one outcome. Columns (1)-(4) are OLS, IV-LP,",
        "% IV-Hypo, IV-Both. All specs include baseline log MA (1960),",
        "% baseline log pop (1960), and the six standardized geographic",
        "% controls. Robust (HC1) standard errors.",
        "",
        tex_chunks
    ), out_tex)
    message("\nSaved: ", out_tex)

    # --- CSV summary ------------------------------------------------------
    csv_rows <- list()
    for (out in outcomes) {
        y <- out$var
        for (spec in c("OLS", "IV-LP", "IV-H", "IV-B")) {
            m <- all_models[[paste(y, spec, sep = "_")]]
            coef_name <- if (spec == "OLS") "chg_logMA_86_60_s0_elow"
                         else "fit_chg_logMA_86_60_s0_elow"
            co <- safe_coef(m, coef_name)
            csv_rows[[length(csv_rows) + 1L]] <- data.frame(
                outcome  = y,
                spec     = spec,
                estimate = co$est,
                std_err  = co$se,
                t_value  = co$t,
                p_value  = co$p,
                n_obs    = nobs(m),
                first_stage_F = if (spec == "OLS") NA_real_ else fitstat_F(m),
                stringsAsFactors = FALSE
            )
        }
    }
    csv_df <- do.call(rbind, csv_rows)
    out_csv <- file.path(dir_tables, "table_11_other_outcomes_iv.csv")
    write.csv(csv_df, out_csv, row.names = FALSE)
    message("Saved: ", out_csv)
}

# ---------------------------------------------------------------------------
# Helpers (table-local: console-print formatter)
# ---------------------------------------------------------------------------
format_co <- function(co) {
    if (is.na(co$est)) return("     NA      ")
    stars <- ifelse(co$p < 0.01, "***",
            ifelse(co$p < 0.05, "**",
            ifelse(co$p < 0.10, "*", "")))
    sprintf("%+6.3f%-3s(%.3f)", co$est, stars, co$se)
}

main()

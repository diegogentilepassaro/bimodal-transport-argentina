# ===========================================================================
# table_9_population.R
#
# PURPOSE: Paper Table 9 — main IV regressions of population outcomes
#          on the change in log market access.
#
# DEP VARS (four outcomes):
#   chg_log_pop_91_60       Total population (log change)
#   chg_log_urbpop_91_60    Urban population (log change)
#   chg_log_rur_91_60       Rural population (log change)
#   chg_urbshr_91_60        Urban share (level change, not log)
#
# COLUMNS (four specifications per outcome):
#   (1) OLS                — no instrument
#   (2) IV-LP              — instrument: chg_logMA_stu_s0_elow
#   (3) IV-Hypo            — instrument: chg_logMA_lcp_mst_s0_elow
#   (4) IV-Both            — both instruments
#
# CONTROLS: same as Table 8 (baseline log MA, baseline log pop,
# six standardized geographic controls).
#
# SE: heteroskedasticity-robust (HC1).
#
# NOTE ON URBAN SHARE:
#   The urban/rural classification in IPUMS 1991 uses a different
#   geographic criterion than the 1960 digitized census. Districts
#   that were "fully urban" in 1960 (small villages with all pop
#   classified urban) often grew and became partly "rural" in 1991
#   under IPUMS's geographically defined rural boundary. The
#   urban-share result should be read with this measurement caveat;
#   a table note flags it.
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/table_9_population_iv.tex   (one combined LaTeX table)
#   results/tables/table_9_population_iv.csv   (convenience wide CSV)
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
    library(modelsummary)
})

# ---- Main hypo-road instrument ----------------------------------------------
# The paper's main specification uses the LCP-MST hypothetical network as
# the hypo-road instrument, sourced from config.R (main_hypo_instrument).
# Alternatives (euc_mst, lcp, euc) are reported in the robustness table.
# The panel already contains columns for all four variants.

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)

    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)

    # Booktabs output, no siunitx
    options(modelsummary_factory_latex = "kableExtra")
    options(modelsummary_format_numeric_latex = "plain")

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

    # Build 16 model fits (4 outcomes × 4 specifications)
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

        # Pull out IV first-stage F for reporting
        f_stats[[y]] <- list(
            lp   = fitstat_F(fits[["IV-LP"]]),
            hypo = fitstat_F(fits[["IV-H"]]),
            both = fitstat_F(fits[["IV-B"]])
        )
    }

    # --- Print beta on ΔlogMA across all 16 specifications to stdout ----
    message("\n[t9] Coefficient on ΔlogMA across specifications:")
    message(sprintf("%-30s  %-9s %-9s %-9s %-9s",
                    "Outcome", "OLS", "IV-LP", "IV-Hypo", "IV-Both"))
    for (out in outcomes) {
        y <- out$var
        b_ols <- get_ma_coef(all_models[[paste(y, "OLS",   sep = "_")]],
                             "chg_logMA_86_60_s0_elow")
        b_lp  <- get_ma_coef(all_models[[paste(y, "IV-LP", sep = "_")]],
                             "fit_chg_logMA_86_60_s0_elow")
        b_h   <- get_ma_coef(all_models[[paste(y, "IV-H",  sep = "_")]],
                             "fit_chg_logMA_86_60_s0_elow")
        b_b   <- get_ma_coef(all_models[[paste(y, "IV-B",  sep = "_")]],
                             "fit_chg_logMA_86_60_s0_elow")
        message(sprintf("%-30s  %-9s %-9s %-9s %-9s",
                        y,
                        format_coef_se(b_ols), format_coef_se(b_lp),
                        format_coef_se(b_h),   format_coef_se(b_b)))
    }

    # --- Build LaTeX table ------------------------------------------------
    # modelsummary supports a list of named models and will stack them.
    # Coef map keeps only the main regressor; controls and constant
    # are omitted for space. A single row for the 1st-stage F is added.

    coef_map <- c(
        "chg_logMA_86_60_s0_elow"     = "$\\Delta \\ln \\mathrm{MA}^{\\mathrm{full}}$",
        "fit_chg_logMA_86_60_s0_elow" = "$\\Delta \\ln \\mathrm{MA}^{\\mathrm{full}}$"
    )

    gof_custom <- list(
        list("raw" = "nobs", "clean" = "Observations", "fmt" = 0)
    )

    # One table per outcome, concatenated into a single .tex file with
    # a \bigskip between panels.
    tex_chunks <- character()
    for (out in outcomes) {
        y <- out$var
        models_this <- list(
            "(1) OLS"      = all_models[[paste(y, "OLS",   sep = "_")]],
            "(2) IV-LP"    = all_models[[paste(y, "IV-LP", sep = "_")]],
            "(3) IV-Hypo"  = all_models[[paste(y, "IV-H",  sep = "_")]],
            "(4) IV-Both"  = all_models[[paste(y, "IV-B",  sep = "_")]]
        )

        # Add first-stage F as a row
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
            stars    = c('*' = .1, '**' = .05, '***' = .01),
            escape   = FALSE,
            add_rows = add_rows,
            title    = sprintf("Outcome: %s", out$label)
        )
        tex_chunks <- c(tex_chunks, as.character(tbl), "", "\\bigskip", "")
    }

    out_tex <- file.path(dir_tables, "table_9_population_iv.tex")
    writeLines(c(
        "% Table 9: Main IV regressions of population outcomes on ΔlogMA.",
        "% Generated by code/analysis/table_9_population.R.",
        "%",
        "% Each panel is one outcome. Columns (1)–(4) are:",
        "%   (1) OLS",
        "%   (2) IV-LP (Larkin-Plan instrument)",
        "%   (3) IV-Hypo (LCP-MST hypothetical-road instrument)",
        "%   (4) IV-Both",
        "%",
        "% All specs include baseline log MA, baseline log pop, and the",
        "% six standardized geographic controls. Robust (HC1) standard",
        "% errors. The first-stage F reported is the Wald F for the",
        "% excluded instrument(s) in that column.",
        "%",
        "% Urban-share caveat: IPUMS 1991 uses a different urban/rural",
        "% classification than the 1960 digitized census. See Section 3.",
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
            co <- summary(m)$coeftable
            if (!(coef_name %in% rownames(co))) next
            csv_rows[[length(csv_rows) + 1L]] <- data.frame(
                outcome  = y,
                spec     = spec,
                estimate = co[coef_name, 1],
                std_err  = co[coef_name, 2],
                t_value  = co[coef_name, 3],
                p_value  = co[coef_name, 4],
                n_obs    = nobs(m),
                first_stage_F = ifelse(
                    spec == "OLS",
                    NA_real_,
                    fitstat_F(m)
                )
            )
        }
    }
    csv_df <- do.call(rbind, csv_rows)
    out_csv <- file.path(dir_tables, "table_9_population_iv.csv")
    write.csv(csv_df, out_csv, row.names = FALSE)
    message("Saved: ", out_csv)
}

# ---------------------------------------------------------------------------
# Helpers (table-local: console-print format differs from shared safe_coef)
# ---------------------------------------------------------------------------
get_ma_coef <- function(m, coef_name) {
    co <- summary(m)$coeftable
    if (!(coef_name %in% rownames(co))) return(c(NA_real_, NA_real_))
    c(co[coef_name, 1], co[coef_name, 2])
}

format_coef_se <- function(vec) {
    if (any(is.na(vec))) return("  NA    ")
    sprintf("%+.3f (%.3f)", vec[1], vec[2])
}

main()

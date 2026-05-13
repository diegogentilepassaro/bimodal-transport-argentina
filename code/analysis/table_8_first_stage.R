# ===========================================================================
# table_8_first_stage.R
#
# PURPOSE: Paper Table 8 — first-stage regressions for the main IV.
#          Shows instrument strength (coefficient + F-stat).
#
# DEP VAR: chg_logMA_86_60_s0_elow (main treatment; sector 0, θ_low)
#
# COLUMNS:
#   (1) LP only              — instrument: chg_logMA_stu_s0_elow
#   (2) Hypo only            — instrument: chg_logMA_lcp_mst_s0_elow
#   (3) Both                 — both instruments
#
# CONTROLS (X_i, per Section 4):
#   - logMA_actual_1960_s0_elow  (baseline log MA)
#   - log_pop_1960               (baseline log pop)
#   - elev_mean_std, rugged_mea_std, wheat_std,
#     preCal_std, postCal_std, dist_to_BA_std (geographic)
#
# SE: heteroskedasticity-robust (HC1).
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/table_8_first_stage.tex        (LaTeX, booktabs)
#   results/tables/table_8_first_stage.csv        (convenience copy)
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
    library(modelsummary)
})

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)

    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)

    d <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet")
    )

    # Controls — read from config.R (geo_controls_main)
    geo_controls_expr <- paste(geo_controls_main, collapse = " + ")

    # --- Three specifications --------------------------------------------
    # Hypo instrument is Table 8's role: document strength of each of the
    # alternative hypo instruments (main spec = lcp_mst; robustness table
    # reports euc_mst, lcp, euc). Kept hardcoded to lcp_mst here to
    # mirror the main-spec choice while signaling to the reader that the
    # variants lcp / euc_mst / euc would sit in the robustness table.
    f1 <- as.formula(sprintf(
        "chg_logMA_86_60_s0_elow ~ chg_logMA_stu_s0_elow + %s",
        geo_controls_expr
    ))
    f2 <- as.formula(sprintf(
        "chg_logMA_86_60_s0_elow ~ chg_logMA_lcp_mst_s0_elow + %s",
        geo_controls_expr
    ))
    f3 <- as.formula(sprintf(
        "chg_logMA_86_60_s0_elow ~ chg_logMA_stu_s0_elow + chg_logMA_lcp_mst_s0_elow + %s",
        geo_controls_expr
    ))

    m1 <- feols(f1, data = d, vcov = "hetero")
    m2 <- feols(f2, data = d, vcov = "hetero")
    m3 <- feols(f3, data = d, vcov = "hetero")

    # --- Diagnostics for the footer of the table ---------------------------
    F1 <- first_stage_F(m1, "chg_logMA_stu_s0_elow")
    F2 <- first_stage_F(m2, "chg_logMA_lcp_mst_s0_elow")
    F3 <- first_stage_F_joint(m3, c("chg_logMA_stu_s0_elow",
                                     "chg_logMA_lcp_mst_s0_elow"))

    message(sprintf("\n[t8] F-stats: LP=%.2f, Hypo=%.2f, Both=%.2f\n",
                    F1, F2, F3))

    # --- Write LaTeX ------------------------------------------------------
    models <- list(
        "(1) LP only"   = m1,
        "(2) Hypo only" = m2,
        "(3) Both"      = m3
    )

    # Pretty variable names in the table
    coef_map <- c(
        "chg_logMA_stu_s0_elow"      = "$\\Delta \\ln \\mathrm{MA}^{\\mathrm{LP}}$",
        "chg_logMA_lcp_mst_s0_elow"  = "$\\Delta \\ln \\mathrm{MA}^{\\mathrm{hypo}}$",
        "logMA_actual_1960_s0_elow"  = "Log MA, 1960",
        "log_pop_1960"               = "Log population, 1960",
        "elev_mean_std"              = "Elevation (std)",
        "rugged_mea_std"             = "Ruggedness (std)",
        "wheat_std"                  = "Wheat suitability (std)",
        "preCal_std"                 = "Caloric pot.\\ pre-1500 (std)",
        "postCal_std"                = "Caloric pot.\\ post-1500 (std)",
        "dist_to_BA_std"             = "Distance to B.A. (std)",
        "(Intercept)"                = "Constant"
    )

    # Footer rows (N, adj R², F-stats)
    gof_rows <- data.frame(
        raw    = c("F.instr", "F.instr.lbl"),
        clean  = c("First-stage $F$ (instruments)", ""),
        stringsAsFactors = FALSE
    )
    footer <- tibble::tibble(
        "_" = c("First-stage $F$ (instruments)"),
        "(1) LP only"   = sprintf("%.2f", F1),
        "(2) Hypo only" = sprintf("%.2f", F2),
        "(3) Both"      = sprintf("%.2f", F3)
    )
    names(footer)[1] <- ""

    # modelsummary expects `goodness-of-fit` configuration
    gof_custom <- list(
        list("raw" = "nobs",   "clean" = "Observations",  "fmt" = 0),
        list("raw" = "adj.r.squared", "clean" = "Adj.\\ R$^2$", "fmt" = 3)
    )

    # Set kableExtra-style (booktabs, tabularx-compatible) output.
    options(modelsummary_factory_latex = "kableExtra")
    # Disable siunitx \num{} wrapping so the table compiles without
    # requiring \usepackage{siunitx} in the preamble.
    options(modelsummary_format_numeric_latex = "plain")

    # Build modelsummary LaTeX (booktabs via kableExtra for wider
    # preamble compatibility).
    tbl <- modelsummary(
        models,
        output    = "latex",
        coef_map  = coef_map,
        gof_map   = gof_custom,
        stars     = c('*' = .1, '**' = .05, '***' = .01),
        escape    = FALSE,
        add_rows  = footer,
        title     = "First stage: instrument strength",
        notes     = paste(
            "Dependent variable: $\\Delta \\ln \\mathrm{MA}^{\\mathrm{full}}$",
            "(sector $s=0$, $\\theta=4.55$).",
            "Robust (HC1) standard errors in parentheses.",
            "All columns include baseline log MA, baseline log population,",
            "and the six standardized geographic controls",
            "(elevation, ruggedness, wheat suitability, pre- and post-1500",
            "caloric potential, distance to Buenos Aires).",
            "$^{*}p<0.10,\\;^{**}p<0.05,\\;^{***}p<0.01$."
        )
    )

    out_tex <- file.path(dir_tables, "table_8_first_stage.tex")
    writeLines(as.character(tbl), out_tex)
    message("Saved: ", out_tex)

    # CSV with the coefficient matrix for convenience
    out_csv <- file.path(dir_tables, "table_8_first_stage.csv")
    csv_df <- extract_coef_matrix(list(m1, m2, m3),
                                  c("LP only", "Hypo only", "Both"))
    write.csv(csv_df, out_csv, row.names = FALSE)
    message("Saved: ", out_csv)
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
first_stage_F <- function(model, coef_name) {
    # Single-instrument F: (t-stat)^2 under robust SE
    b  <- coef(model)[coef_name]
    se <- summary(model)$se[coef_name]
    as.numeric((b / se)^2)
}

first_stage_F_joint <- function(model, coef_names) {
    # Joint F via fixest::wald()
    w <- fixest::wald(model, coef_names, print = FALSE)
    as.numeric(w$stat)
}

extract_coef_matrix <- function(models, labels) {
    out <- NULL
    for (i in seq_along(models)) {
        co <- summary(models[[i]])$coeftable
        rownames_i <- rownames(co)
        df <- data.frame(
            spec     = labels[i],
            variable = rownames_i,
            estimate = co[, 1],
            std_err  = co[, 2],
            t_value  = co[, 3],
            p_value  = co[, 4],
            stringsAsFactors = FALSE
        )
        out <- rbind(out, df)
    }
    out
}

main()

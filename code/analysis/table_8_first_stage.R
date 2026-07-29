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
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)

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
        "%s ~ %s + %s",
        main_treatment, main_lp_instrument, geo_controls_expr
    ))
    f2 <- as.formula(sprintf(
        "%s ~ chg_logMA_lcp_mst_s0_elow + %s",
        main_treatment, geo_controls_expr
    ))
    f3 <- as.formula(sprintf(
        "%s ~ %s + chg_logMA_lcp_mst_s0_elow + %s",
        main_treatment, main_lp_instrument, geo_controls_expr
    ))

    m1 <- feols(f1, data = d, vcov = "hetero")
    m2 <- feols(f2, data = d, vcov = "hetero")
    m3 <- feols(f3, data = d, vcov = "hetero")

    # --- Diagnostics for the footer of the table ---------------------------
    F1 <- first_stage_F(m1, main_lp_instrument)
    F2 <- first_stage_F(m2, "chg_logMA_lcp_mst_s0_elow")
    F3 <- first_stage_F_joint(m3, c(main_lp_instrument,
                                     "chg_logMA_lcp_mst_s0_elow"))
    # Montiel Olea-Pflueger effective F alongside the classical one. This
    # is the first-stage table, so it is where the distinction matters
    # most: the classical F assumes homoskedasticity while every
    # specification here uses HC1, and for the two-instrument column the
    # classical F has no defined critical value. eff_F_from_fit() is the
    # implementation validated in diagnostic_modern_iv.R.
    E1 <- eff_F_from_fit(d, main_treatment, main_lp_instrument,
                         geo_controls_main)
    E2 <- eff_F_from_fit(d, main_treatment, "chg_logMA_lcp_mst_s0_elow",
                         geo_controls_main)
    E3 <- eff_F_from_fit(d, main_treatment,
                         c(main_lp_instrument,
                           "chg_logMA_lcp_mst_s0_elow"),
                         geo_controls_main)

    message(sprintf("\n[t8] F-stats: LP=%.2f, Hypo=%.2f, Both=%.2f\n",
                    F1, F2, F3))
    message(sprintf("[t8] Effective F (MOP): LP=%.2f, Hypo=%.2f, Both=%.2f\n",
                    E1, E2, E3))

    # --- Write LaTeX ------------------------------------------------------
    models <- list(
        "(1) LP only"   = m1,
        "(2) Hypo only" = m2,
        "(3) Both"      = m3
    )

    # Pretty variable names in the table
    coef_map <- c(
        setNames("$\\Delta \\ln \\mathrm{MA}^{\\mathrm{LP}}$",
                 main_lp_instrument),
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
        "_" = c("First-stage $F$ (instruments)",
                "Effective $F$ (MOP)"),
        "(1) LP only"   = c(sprintf("%.2f", F1), sprintf("%.2f", E1)),
        "(2) Hypo only" = c(sprintf("%.2f", F2), sprintf("%.2f", E2)),
        "(3) Both"      = c(sprintf("%.2f", F3), sprintf("%.2f", E3))
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
            "The first-stage $F$ in this table is already",
            "heteroskedasticity-robust: a squared robust $t$ in columns (1)",
            "and (2), a robust Wald statistic in column (3).",
            "``Effective $F$ (MOP)'' is the Montiel Olea and Pflueger (2013)",
            "effective $F$, computed with the included controls partialled",
            "out of the treatment and the instruments. With one instrument",
            "the two coincide by construction, which columns (1) and (2)",
            "confirm. They differ only in column (3), where the effective",
            "$F$ is the statistic with a defined critical value under two",
            "instruments.",
            "$^{*}p<0.10,\\;^{**}p<0.05,\\;^{***}p<0.01$."
        )
    )

    out_tex <- file.path(dir_tables, "table_8_first_stage.tex")
    tbl_txt <- inject_first_label(as.character(tbl), "tab:first_stage")
    writeLines(tbl_txt, out_tex)
    message("Saved: ", out_tex)

    # CSV with the coefficient matrix for convenience. The .tex is
    # gitignored, so this is the only copy visible in the repo; the two F
    # statistics are appended as pseudo-variable rows so they travel with
    # it rather than living only in a LaTeX build.
    out_csv <- file.path(dir_tables, "table_8_first_stage.csv")
    csv_df <- extract_coef_matrix(list(m1, m2, m3),
                                  c("LP only", "Hypo only", "Both"))
    f_rows <- data.frame(
        spec      = rep(c("LP only", "Hypo only", "Both"), each = 2L),
        variable  = rep(c("_first_stage_F", "_effective_F_MOP"), times = 3L),
        estimate  = c(F1, E1, F2, E2, F3, E3),
        std_err   = NA_real_, t_value = NA_real_, p_value = NA_real_,
        stringsAsFactors = FALSE
    )
    csv_df <- rbind(csv_df, f_rows[, names(csv_df)])
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

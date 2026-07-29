# ===========================================================================
# table_10_sectoral.R
#
# PURPOSE: Paper Table 10 — IV regressions of sectoral activity on the
#          change in log market access. This is the "sectoral effects"
#          counterpart to Table 9 (population).
#
# DEP VARS (five outcomes, labeled in pairs: manufacturing then agricultural):
#   Manufacturing (industrial census 1954 vs 1985):
#     chg_log_nestab_85_54     # establishments
#     chg_log_valprod_85_54    # value of production
#     chg_log_massal_85_54     # wage mass (total labor payments)
#   Agriculture (agricultural census 1960 vs 1988):
#     chg_log_nexp_88_60       # farms
#     chg_log_areatot_ha_88_60 # total farmed area
#
# NOTE ON OUTCOME CHOICE:
#   The paper skeleton's original plan was "agricultural / manufacturing /
#   services *employment*" as three outcomes. That mapping doesn't survive
#   contact with the data:
#     - `nemp_1954` and `nemp_1985` (industrial employment from the
#       industrial census) have zero overlapping districts by geolev2 —
#       unusable.
#     - IPUMS `indgen` sectoral shares exist in the district-year panel
#       (indgen_10 = agriculture, indgen_30 = manufacturing, etc.) but
#       were not propagated into the wide estimation sample. Adding them
#       would require a pipeline extension and a coauthor conversation
#       about which shares to use (Arg census occupation codes differ
#       from IPUMS general industry codes).
#   The available industrial- and agricultural-census outcomes above
#   capture the same economic question — did manufacturing and
#   agriculture respond differently to market access changes — via the
#   activity-level measures that are present in the data. Flagged in the
#   commit message so Cote can push back if he'd rather we pause and
#   build IPUMS sectoral shares first.
#
# COLUMNS (same structure as Table 9):
#   (1) OLS
#   (2) IV-LP
#   (3) IV-Hypo
#   (4) IV-Both
#
# CONTROLS: baseline log MA + log pop + six standardized geo controls
#   (same as Tables 6-9).
#
# SE: heteroskedasticity-robust (HC1).
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/table_10_sectoral_iv.{tex,csv}
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
        list(var = "chg_log_nestab_85_54",
             label = "Mfg.\\ establishments",
             panel = "A",
             panel_title = "A. Manufacturing (industrial census 1954-1985)"),
        list(var = "chg_log_valprod_85_54",
             label = "Mfg.\\ production value",
             panel = "A",
             panel_title = "A. Manufacturing (industrial census 1954-1985)"),
        list(var = "chg_log_massal_85_54",
             label = "Mfg.\\ wage mass",
             panel = "A",
             panel_title = "A. Manufacturing (industrial census 1954-1985)"),
        list(var = "chg_log_nexp_88_60",
             label = "Ag.\\ farms",
             panel = "B",
             panel_title = "B. Agriculture (agricultural census 1960-1988)"),
        list(var = "chg_log_areatot_ha_88_60",
             label = "Ag.\\ farmed area",
             panel = "B",
             panel_title = "B. Agriculture (agricultural census 1960-1988)")
    )

    # Build and fit 5 × 4 = 20 models
    all_models <- list()
    f_stats    <- list()
    for (out in outcomes) {
        y <- out$var
        fits <- fit_iv_quad(
            y = y, data = d,
            endog = main_treatment,
            lp_instr = main_lp_instrument,
            hypo_instr = main_hypo_instrument,
            ctrls_vec = geo_controls_main
        )
        for (spec in names(fits)) {
            all_models[[paste(y, spec, sep = "_")]] <- fits[[spec]]
        }

        # Classical and Montiel Olea-Pflueger effective first-stage F.
        # The effective F is the statistic to quote under HC1 and with
        # more than one instrument; see the note in _iv_helpers.R. It
        # must be computed on the SAME complete-case rows as the fit,
        # because fit_iv_quad NA-drops per model and the sectoral
        # outcomes have real missingness.
        cc_vars <- c(y, main_treatment, main_lp_instrument,
                     main_hypo_instrument, geo_controls_main)
        dd <- as.data.frame(d)
        d_cc <- dd[complete.cases(dd[, cc_vars]), ]
        stopifnot(nrow(d_cc) == nobs(fits[["IV-B"]]))
        f_stats[[y]] <- list(
            lp   = fitstat_F(fits[["IV-LP"]]),
            hypo = fitstat_F(fits[["IV-H"]]),
            both = fitstat_F(fits[["IV-B"]]),
            eff_lp   = eff_F_from_fit(d_cc, main_treatment,
                                      main_lp_instrument,
                                      geo_controls_main),
            eff_hypo = eff_F_from_fit(d_cc, main_treatment,
                                      main_hypo_instrument,
                                      geo_controls_main),
            eff_both = eff_F_from_fit(d_cc, main_treatment,
                                      c(main_lp_instrument,
                                        main_hypo_instrument),
                                      geo_controls_main)
        )
    }

    # Print coefficient matrix for quick inspection
    message("\n[t10] Coefficient on ΔlogMA across specifications:")
    message(sprintf("%-32s  %-13s %-13s %-13s %-13s  %-5s",
                    "Outcome", "OLS", "IV-LP", "IV-Hypo", "IV-Both", "N"))
    for (out in outcomes) {
        y <- out$var
        b_ols <- safe_coef(all_models[[paste(y, "OLS",   sep = "_")]],
                           main_treatment)
        b_lp  <- safe_coef(all_models[[paste(y, "IV-LP", sep = "_")]],
                           paste0("fit_", main_treatment))
        b_h   <- safe_coef(all_models[[paste(y, "IV-H",  sep = "_")]],
                           paste0("fit_", main_treatment))
        b_b   <- safe_coef(all_models[[paste(y, "IV-B",  sep = "_")]],
                           paste0("fit_", main_treatment))
        n_ols <- nobs(all_models[[paste(y, "OLS", sep = "_")]])
        message(sprintf("%-32s  %-13s %-13s %-13s %-13s  %-5d",
                        y,
                        format_co(b_ols), format_co(b_lp),
                        format_co(b_h),   format_co(b_b),
                        n_ols))
    }

    # --- Build LaTeX ------------------------------------------------------
    coef_map <- setNames(
        rep("$\\Delta \\ln \\mathrm{MA}^{\\mathrm{full}}$", 2L),
        c(main_treatment, paste0("fit_", main_treatment))
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
            ` `           = c("First-stage $F$",
                              "Effective $F$ (MOP)"),
            `(1) OLS`     = c("---", "---"),
            `(2) IV-LP`   = c(sprintf("%.1f", fs$lp),
                              sprintf("%.1f", fs$eff_lp)),
            `(3) IV-Hypo` = c(sprintf("%.1f", fs$hypo),
                              sprintf("%.1f", fs$eff_hypo)),
            `(4) IV-Both` = c(sprintf("%.1f", fs$both),
                              sprintf("%.1f", fs$eff_both))
        )
        tbl <- modelsummary(
            models_this,
            output   = "latex",
            coef_map = coef_map,
            gof_map  = gof_custom,
            stars    = c("*" = .1, "**" = .05, "***" = .01),
            escape   = FALSE,
            add_rows = add_rows,
            title    = sprintf("%s. Outcome: %s", out$panel_title, out$label)
        )
        tbl_txt <- as.character(tbl)
        if (is_first_panel) {
            tbl_txt <- inject_first_label(tbl_txt, "tab:sectoral_iv")
            is_first_panel <- FALSE
        }
        tex_chunks <- c(tex_chunks, tbl_txt, "", "\\bigskip", "")
    }

    out_tex <- file.path(dir_tables, "table_10_sectoral_iv.tex")
    writeLines(c(
        "% Table 10: Sectoral activity IV regressions.",
        "% Generated by code/analysis/table_10_sectoral.R.",
        "%",
        "% Panel A: Manufacturing outcomes from the 1954 and 1985 industrial",
        "%   censuses (establishments, value of production, wage mass).",
        "% Panel B: Agricultural outcomes from the 1960 and 1988 agricultural",
        "%   censuses (number of farms, total farmed area).",
        "%",
        "% Each panel is one outcome. Columns (1)-(4) are OLS, IV-LP,",
        "% IV-Hypo, IV-Both. All specs include baseline log MA,",
        "% baseline log pop, and the six standardized geographic controls.",
        "% Robust (HC1) standard errors. First-stage F is the Wald F for",
        "% the excluded instrument(s) in that column.",
        "%",
        "% Effective F (MOP) is the Montiel Olea-Pflueger (2013) effective",
        "% F, controls partialled out. It is the one to read: the",
        "% first-stage F row above it is the CLASSICAL F, which assumes",
        "% homoskedasticity while these specifications use HC1, and with two",
        "% instruments the classical F has no defined critical value.",
        "% Anderson-Rubin sets and MOP critical values are in",
        "% results/tables/diagnostic_modern_iv.txt.",
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
            coef_name <- if (spec == "OLS") main_treatment
                         else paste0("fit_", main_treatment)
            co <- safe_coef(m, coef_name)
            csv_rows[[length(csv_rows) + 1L]] <- data.frame(
                panel    = out$panel,
                outcome  = y,
                spec     = spec,
                estimate = co$est,
                std_err  = co$se,
                t_value  = co$t,
                p_value  = co$p,
                n_obs    = nobs(m),
                first_stage_F = if (spec == "OLS") NA_real_ else
                                fitstat_F(m),
                # The .tex is gitignored, so the CSV is the only
                # coauthor-visible copy in the repo; the effective F has
                # to be here or it is invisible outside a LaTeX build.
                effective_F = switch(
                    spec,
                    "OLS"   = NA_real_,
                    "IV-LP" = f_stats[[y]]$eff_lp,
                    "IV-H"  = f_stats[[y]]$eff_hypo,
                    "IV-B"  = f_stats[[y]]$eff_both
                ),
                stringsAsFactors = FALSE
            )
        }
    }
    csv_df <- do.call(rbind, csv_rows)
    out_csv <- file.path(dir_tables, "table_10_sectoral_iv.csv")
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

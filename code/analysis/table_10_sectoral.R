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
    options(modelsummary_factory_latex = "kableExtra")
    options(modelsummary_format_numeric_latex = "plain")

    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)

    d <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet")
    )

    HYPO <- main_hypo_instrument
    ctrls <- paste(geo_controls_main, collapse = " + ")

    outcomes <- list(
        list(var = "chg_log_nestab_85_54",
             label = "Mfg.\\ establishments",
             panel = "A. Manufacturing (industrial census 1954-1985)"),
        list(var = "chg_log_valprod_85_54",
             label = "Mfg.\\ production value",
             panel = "A. Manufacturing (industrial census 1954-1985)"),
        list(var = "chg_log_massal_85_54",
             label = "Mfg.\\ wage mass",
             panel = "A. Manufacturing (industrial census 1954-1985)"),
        list(var = "chg_log_nexp_88_60",
             label = "Ag.\\ farms",
             panel = "B. Agriculture (agricultural census 1960-1988)"),
        list(var = "chg_log_areatot_ha_88_60",
             label = "Ag.\\ farmed area",
             panel = "B. Agriculture (agricultural census 1960-1988)")
    )

    # Build and fit 5 × 4 = 20 models
    all_models <- list()
    f_stats    <- list()
    for (out in outcomes) {
        y <- out$var
        f_ols <- as.formula(sprintf("%s ~ chg_logMA_86_60_s0_elow + %s",
                                    y, ctrls))
        m_ols <- feols(f_ols, data = d, vcov = "hetero")

        f_iv_lp <- as.formula(sprintf(
            "%s ~ %s | chg_logMA_86_60_s0_elow ~ chg_logMA_stu_s0_elow",
            y, ctrls))
        m_iv_lp <- feols(f_iv_lp, data = d, vcov = "hetero")

        f_iv_h <- as.formula(sprintf(
            "%s ~ %s | chg_logMA_86_60_s0_elow ~ %s",
            y, ctrls, HYPO))
        m_iv_h <- feols(f_iv_h, data = d, vcov = "hetero")

        f_iv_b <- as.formula(sprintf(
            "%s ~ %s | chg_logMA_86_60_s0_elow ~ chg_logMA_stu_s0_elow + %s",
            y, ctrls, HYPO))
        m_iv_b <- feols(f_iv_b, data = d, vcov = "hetero")

        all_models[[paste(y, "OLS",   sep = "_")]] <- m_ols
        all_models[[paste(y, "IV-LP", sep = "_")]] <- m_iv_lp
        all_models[[paste(y, "IV-H",  sep = "_")]] <- m_iv_h
        all_models[[paste(y, "IV-B",  sep = "_")]] <- m_iv_b

        f_stats[[y]] <- list(
            lp   = fitstat_F(m_iv_lp),
            hypo = fitstat_F(m_iv_h),
            both = fitstat_F(m_iv_b)
        )
    }

    # Print coefficient matrix for quick inspection
    message("\n[t10] Coefficient on ΔlogMA across specifications:")
    message(sprintf("%-32s  %-13s %-13s %-13s %-13s  %-5s",
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
        message(sprintf("%-32s  %-13s %-13s %-13s %-13s  %-5d",
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
            title    = sprintf("%s. Outcome: %s", out$panel, out$label)
        )
        tex_chunks <- c(tex_chunks, as.character(tbl), "", "\\bigskip", "")
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
                panel    = substr(out$panel, 1, 1),  # "A" or "B"
                outcome  = y,
                spec     = spec,
                estimate = co$est,
                std_err  = co$se,
                t_value  = co$t,
                p_value  = co$p,
                n_obs    = nobs(m),
                first_stage_F = if (spec == "OLS") NA_real_ else
                                fitstat_F(m),
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
# Helpers
# ---------------------------------------------------------------------------
fitstat_F <- function(iv_model) {
    fs <- fitstat(iv_model, type = "ivf")
    if (is.list(fs) && !is.null(fs[[1]]$stat)) {
        return(as.numeric(fs[[1]]$stat))
    }
    fs2 <- fitstat(iv_model, type = "ivf", simplify = TRUE)
    if (is.list(fs2) && !is.null(fs2$stat)) return(as.numeric(fs2$stat))
    NA_real_
}

safe_coef <- function(model, cname) {
    co <- summary(model)$coeftable
    if (!(cname %in% rownames(co))) {
        return(list(est = NA_real_, se = NA_real_,
                    t = NA_real_, p = NA_real_))
    }
    list(est = co[cname, 1], se = co[cname, 2],
         t = co[cname, 3], p = co[cname, 4])
}

format_co <- function(co) {
    if (is.na(co$est)) return("     NA      ")
    stars <- ifelse(co$p < 0.01, "***",
            ifelse(co$p < 0.05, "**",
            ifelse(co$p < 0.10, "*", "")))
    sprintf("%+6.3f%-3s(%.3f)", co$est, stars, co$se)
}

main()

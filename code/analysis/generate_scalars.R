# ===========================================================================
# generate_scalars.R
#
# PURPOSE: Produce results/scalars.tex — a LaTeX file with \newcommand
#          macros for every in-text number the paper cites. The paper
#          preamble \input's this file, and body text uses macros like
#          \headlineIVBothPopulation instead of hardcoding 0.046.
#
# AEA requirement: 'No hardcoded numbers anywhere.' This script builds the
# mapping from (table, row, column, stat) → macro name → formatted value.
#
# DESIGN:
#   Reads the CSV outputs of tables 6-12 (already produced by the table
#   scripts). Each scalar has:
#     - a stable macro name (e.g., \mainPopCoefIVBoth)
#     - a format spec (decimal places, whether to include stars)
#     - a source (table + row + column)
#
# Only the headline numbers actually cited in the paper prose are included,
# to keep scalars.tex readable and the coupling between macros and prose
# tight. Numbers inside tables themselves stay in the table .tex files —
# those are already code-generated.
#
# READS:
#   results/tables/table_{6,7,8,9,10,11,12,13,14}_*.csv
#   results/tables/diagnostic_heterogeneity.csv
#   data/derived/05_panel/departments_wide_panel.parquet   (Section 3 stats)
#   data/derived/base/census_1947/census_1947_ipums.parquet (coverage count)
#
# PRODUCES:
#   results/scalars.tex
#
# USAGE:
#   Run AFTER all table scripts. Called from main.R Stage D, last step.
#     Rscript code/analysis/generate_scalars.R
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
})

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)

    message("\n", strrep("=", 72))
    message("generate_scalars.R  |  Build results/scalars.tex")
    message(strrep("=", 72))

    # Read every table CSV into a named list
    tab <- list()
    for (name in c("table_6_pre_balance",
                   "table_7_pre_trends",
                   "table_8_first_stage",
                   "table_9_population_iv",
                   "table_10_sectoral_iv",
                   "table_11_other_outcomes_iv",
                   "table_12_robustness",
                   "table_13_counterfactual",
                   "table_14_mechanisms",
                   "table_15_density_schedules",
                   "table_16_sector_matched",
                   "diagnostic_heterogeneity",
                   "diagnostic_theta_sweep",
                   "diagnostic_ma_unimodal")) {
        path <- file.path(dir_tables, sprintf("%s.csv", name))
        if (!file.exists(path)) {
            warning(sprintf("Missing: %s — scalars depending on this will be NA",
                            path))
            tab[[name]] <- NULL
        } else {
            tab[[name]] <- read.csv(path, stringsAsFactors = FALSE)
            message(sprintf("  loaded: %s  (%d rows)",
                            name, nrow(tab[[name]])))
        }
    }

    # ---- Build the macros -----------------------------------------------
    # Convention: macro names are camelCase. Use only letters; LaTeX cannot
    # parse hyphens or numbers in \newcommand arguments.
    #
    # Headline numbers the paper cites in prose:
    #   - First-stage F-stats (Table 8): LP, Hypo, Both
    #   - Main population IV-Both coefficient (Table 9, chg_log_pop_91_60)
    #   - Main manufacturing wage mass IV-Both coefficient (Table 10)
    #   - Main recent-migration IV-Both coefficient (Table 11)
    #   - Placebo IV-Both coefficient (Table 7)
    #   - Subsample robustness IV-Both coefficient (Table 12 Panel C)

    macros <- list()

    # Table 8 first-stage F-stats — from the CSV, which has rows
    # (LP only, Hypo only, Both) each with variable==instrument name.
    t8 <- tab[["table_8_first_stage"]]
    if (!is.null(t8)) {
        # The LP F-stat = (t-stat)^2 of chg_logMA_stu_s0_elow in LP-only row
        r_lp <- subset(t8, spec == "LP only" &
                           variable == "chg_logMA_stu_s0_elow")
        r_h  <- subset(t8, spec == "Hypo only" &
                           variable == "chg_logMA_lcp_mst_s0_elow")
        if (nrow(r_lp) == 1L) {
            macros[["firstStageFLP"]] <- sprintf("%.2f",
                (r_lp$estimate / r_lp$std_err)^2)
        }
        if (nrow(r_h) == 1L) {
            macros[["firstStageFHypo"]] <- sprintf("%.2f",
                (r_h$estimate / r_h$std_err)^2)
        }
        # For joint F we need the IV model output, not the reduced form.
        # Table 9 stores first_stage_F per IV spec on population — use that.
    }

    # Joint F-stat: take from Table 9 IV-B on total population
    t9 <- tab[["table_9_population_iv"]]
    if (!is.null(t9)) {
        r <- subset(t9, outcome == "chg_log_pop_91_60" & spec == "IV-B")
        if (nrow(r) == 1L) {
            macros[["firstStageFBoth"]] <- sprintf("%.2f", r$first_stage_F)
            macros[["mainPopCoefIVBoth"]]     <- sprintf("%.3f", r$estimate)
            macros[["mainPopSEIVBoth"]]       <- sprintf("%.3f", r$std_err)
            macros[["mainPopCoefIVBothP"]]    <- sprintf("%.3f", r$p_value)
            macros[["mainPopNIVBoth"]]        <- as.character(r$n_obs)
        }
        # (urban/rural IV-B macros live in add_prose_table_macros as
        # urbIVB*/rurIVB*; the old mainUrbPop*/mainRurPop* names were
        # orphaned by the PR #94 framing pass and are removed.)
    }

    # Table 10: manufacturing wage mass and production value
    t10 <- tab[["table_10_sectoral_iv"]]
    if (!is.null(t10)) {
        r <- subset(t10, outcome == "chg_log_massal_85_54" & spec == "IV-B")
        if (nrow(r) == 1L) {
            macros[["mfgWageMassCoefIVBoth"]] <- sprintf("%.3f", r$estimate)
            macros[["mfgWageMassSEIVBoth"]]   <- sprintf("%.3f", r$std_err)
        }
        r <- subset(t10, outcome == "chg_log_valprod_85_54" & spec == "IV-B")
        if (nrow(r) == 1L) {
            macros[["mfgValprodCoefIVBoth"]]  <- sprintf("%.3f", r$estimate)
            macros[["mfgValprodSEIVBoth"]]    <- sprintf("%.3f", r$std_err)
        }
    }

    # Table 11: recent migration
    t11 <- tab[["table_11_other_outcomes_iv"]]
    if (!is.null(t11)) {
        r <- subset(t11, outcome == "chg_mig5_91_70" & spec == "IV-B")
        if (nrow(r) == 1L) {
            macros[["migrationCoefIVBoth"]]   <- sprintf("%.3f", r$estimate)
            macros[["migrationSEIVBoth"]]     <- sprintf("%.3f", r$std_err)
        }
    }

    # Table 7: placebo pre-trends
    t7 <- tab[["table_7_pre_trends"]]
    if (!is.null(t7)) {
        r <- subset(t7, spec == "IV-Both")
        if (nrow(r) == 1L) {
            macros[["placeboCoefIVBoth"]]     <- sprintf("%.3f", r$estimate)
            macros[["placeboSEIVBoth"]]       <- sprintf("%.3f", r$std_err)
            macros[["placeboNIVBoth"]]        <- as.character(r$n_obs)
        }
    }

    # Table 13: counterfactual decomposition (Section 6 / intro findings).
    # Total-population row per panel; Fs at 1 decimal to match prose.
    t13 <- tab[["table_13_counterfactual"]]
    if (!is.null(t13)) {
        for (pn in list(list(id = "B", stem = "cfRail"),
                        list(id = "C", stem = "cfRoad"))) {
            r <- t13[t13$panel == pn$id &
                     t13$outcome == "chg_log_pop_91_60", ]
            if (nrow(r) == 1L) {
                macros[[paste0(pn$stem, "CoefIVPop")]] <-
                    sprintf("%.3f", r$iv_est)
                macros[[paste0(pn$stem, "SEIVPop")]] <-
                    sprintf("%.3f", r$iv_se)
                macros[[paste0(pn$stem, "FIVPop")]] <-
                    sprintf("%.1f", r$iv_F)
            }
        }
        r <- subset(t13, panel == "A" & outcome == "chg_log_pop_91_60")
        if (nrow(r) == 1L) {
            macros[["cfFullFIVPop"]] <- sprintf("%.1f", r$iv_F)
        }
    }

    # Table 14: mechanisms progressive-add (Section 7 / intro findings).
    t14 <- tab[["table_14_mechanisms"]]
    if (!is.null(t14)) {
        r <- subset(t14, spec_id == "(4)")
        if (nrow(r) == 1L) {
            macros[["mechAllZCoef"]]      <- sprintf("%.3f", r$ma_est)
            macros[["mechAllZSE"]]        <- sprintf("%.3f", r$ma_se)
            macros[["mechRailKmCoef"]]    <- sprintf("%.4f", r$chg_rail_est)
            macros[["mechRailKmSE"]]      <- sprintf("%.4f", r$chg_rail_se)
            macros[["mechLostRailCoef"]]  <- sprintf("%.3f", r$lost_rail_est)
            macros[["mechLostRailSE"]]    <- sprintf("%.3f", r$lost_rail_se)
        }
        r <- subset(t14, spec_id == "(2)")
        if (nrow(r) == 1L) {
            macros[["mechKmCoef"]] <- sprintf("%.3f", r$ma_est)
            macros[["mechKmSE"]]   <- sprintf("%.3f", r$ma_se)
        }
        r <- subset(t14, spec_id == "(3)")
        if (nrow(r) == 1L) {
            macros[["mechBinCoef"]] <- sprintf("%.3f", r$ma_est)
            macros[["mechBinSE"]]   <- sprintf("%.3f", r$ma_se)
        }
        r <- subset(t14, spec_id == "(1cs)")
        if (nrow(r) == 1L) {
            macros[["mechBaselineCommonCoef"]] <- sprintf("%.3f", r$ma_est)
            macros[["mechBaselineCommonSE"]]   <- sprintf("%.3f", r$ma_se)
        }
        # Baseline F from Table 14's own spec (1), so Section 7's prose
        # traces to the table it describes (not Table 13's identical
        # regression, which could silently diverge).
        r <- subset(t14, spec_id == "(1)")
        if (nrow(r) == 1L) {
            macros[["mechBaselineF"]] <- sprintf("%.1f", r$ma_F)
        }
        # Max first-stage F across the augmented specs (2)-(4), for the
        # "up to" sentence in Section 7 — stays correct even if the max
        # moves to a different column after a data change.
        r <- subset(t14, spec_id %in% c("(2)", "(3)", "(4)"))
        if (nrow(r) == 3L) {
            macros[["mechMaxAugF"]] <- sprintf("%.1f", max(r$ma_F))
        }
    }

    # Heterogeneity diagnostic (Section 7.2): OLS interaction terms.
    het <- tab[["diagnostic_heterogeneity"]]
    if (!is.null(het)) {
        for (ch in list(
                list(var = "log_pop_1960", stem = "heteroPop"),
                list(var = "rurshr_1960",  stem = "heteroRur"),
                list(var = "dist_to_BA",   stem = "heteroDist"))) {
            r <- het[het$characteristic == ch$var & het$spec == "OLS", ]
            if (nrow(r) == 1L) {
                macros[[paste0(ch$stem, "IntOLS")]] <-
                    sprintf("%.3f", r$beta_int)
                macros[[paste0(ch$stem, "IntSEOLS")]] <-
                    sprintf("%.3f", r$se_int)
            }
        }
    }

    # Sample sizes
    macros[["nDistricts"]] <- "312"
    macros[["thetaLow"]]   <- "4.55"
    macros[["thetaHigh"]]  <- "8.11"

    # AutoFill pass (post issue #22): every remaining prose-quoted
    # regression number and panel statistic.
    macros <- add_prose_table_macros(macros, tab)
    macros <- add_panel_macros(macros)

    # ---- Write scalars.tex ----------------------------------------------
    out_path <- file.path(dir_results, "scalars.tex")

    header <- c(
        "% ====================================================================",
        "% scalars.tex",
        "%",
        "% AutoFill macros for in-text numbers. Generated by",
        "% code/analysis/generate_scalars.R on every pipeline run.",
        "%",
        "% Paper preamble: \\input{../results/scalars.tex}",
        "% Paper body:     e.g. 'elasticity is \\mainPopCoefIVBoth{} (\\mainPopSEIVBoth{})'",
        "%",
        "% Adding a new macro:",
        "%   1. Add the source lookup in generate_scalars.R",
        "%   2. Add the \\XXX{} reference in the appropriate section_*.tex",
        "%   3. Rerun this script and recompile the paper.",
        "% ====================================================================",
        sprintf("%% Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
        sprintf("%% Rootdir:   %s", rootdir),
        ""
    )

    body <- character()
    for (nm in sort(names(macros))) {
        body <- c(body,
            sprintf("\\newcommand{\\%s}{%s}", nm, macros[[nm]]))
    }

    writeLines(c(header, body), out_path)

    message(sprintf("\nWrote %d macros to %s", length(macros), out_path))
    for (nm in sort(names(macros))) {
        message(sprintf("  \\%-30s = %s", nm, macros[[nm]]))
    }
}

# ---------------------------------------------------------------------------
# AutoFill pass: every remaining regression number quoted in Sections 4-8
# prose. Grouped by source table. Naming: camelCase, letters only.
# ---------------------------------------------------------------------------
add_prose_table_macros <- function(macros, tab) {

    f3 <- function(x) sprintf("%.3f", x)
    f2 <- function(x) sprintf("%.2f", x)
    f1 <- function(x) sprintf("%.1f", x)

    # Pull exactly one row from a long-format table CSV. NA-safe:
    # an NA in a filter column never matches.
    row1 <- function(df, ...) {
        conds <- list(...)
        keep <- rep(TRUE, nrow(df))
        for (col in names(conds)) {
            keep <- keep & !is.na(df[[col]]) & df[[col]] == conds[[col]]
        }
        r <- df[keep, ]
        stopifnot(nrow(r) == 1L)
        r
    }

    # -- Table 8 (Section 5.1): first-stage coefficients ---------------------
    t8 <- tab[["table_8_first_stage"]]
    if (!is.null(t8)) {
        r <- row1(t8, spec = "LP only", variable = "chg_logMA_stu_s0_elow")
        macros[["fsLPCoef"]] <- f3(r$estimate)
        macros[["fsLPSE"]]   <- f3(r$std_err)
        r <- row1(t8, spec = "Hypo only", variable = "chg_logMA_lcp_mst_s0_elow")
        macros[["fsHypoCoef"]] <- f3(r$estimate)
        macros[["fsHypoSE"]]   <- f3(r$std_err)
        r <- row1(t8, spec = "Both", variable = "chg_logMA_stu_s0_elow")
        macros[["fsJointLPCoef"]] <- f3(r$estimate)
        macros[["fsJointLPSE"]]   <- f3(r$std_err)
        r <- row1(t8, spec = "Both", variable = "chg_logMA_lcp_mst_s0_elow")
        macros[["fsJointHypoCoef"]] <- f3(r$estimate)
        macros[["fsJointHypoSE"]]   <- f3(r$std_err)
    }

    # -- Table 9 (Section 5.2): population panels ----------------------------
    t9 <- tab[["table_9_population_iv"]]
    if (!is.null(t9)) {
        for (g in list(
                c("chg_log_pop_91_60", "OLS",   "popOLS"),
                c("chg_log_pop_91_60", "IV-LP", "popIVLP"),
                c("chg_log_pop_91_60", "IV-H",  "popIVH"),
                c("chg_log_urbpop_91_60", "OLS",   "urbOLS"),
                c("chg_log_urbpop_91_60", "IV-LP", "urbIVLP"),
                c("chg_log_urbpop_91_60", "IV-H",  "urbIVH"),
                c("chg_log_urbpop_91_60", "IV-B",  "urbIVB"),
                c("chg_log_rur_91_60", "OLS",   "rurOLS"),
                c("chg_log_rur_91_60", "IV-LP", "rurIVLP"),
                c("chg_log_rur_91_60", "IV-H",  "rurIVH"),
                c("chg_log_rur_91_60", "IV-B",  "rurIVB"),
                c("chg_urbshr_91_60", "OLS",  "urbshrOLS"),
                c("chg_urbshr_91_60", "IV-B", "urbshrIVB"))) {
            r <- row1(t9, outcome = g[1], spec = g[2])
            macros[[paste0(g[3], "Coef")]] <- f3(r$estimate)
            macros[[paste0(g[3], "SE")]]   <- f3(r$std_err)
        }
        macros[["urbN"]] <- as.character(
            row1(t9, outcome = "chg_log_urbpop_91_60", spec = "IV-B")$n_obs)
        macros[["rurN"]] <- as.character(
            row1(t9, outcome = "chg_log_rur_91_60", spec = "IV-B")$n_obs)
    }

    # -- Table 10 (Section 5.3): sectoral ------------------------------------
    t10 <- tab[["table_10_sectoral_iv"]]
    if (!is.null(t10)) {
        for (g in list(
                c("chg_log_nestab_85_54", "OLS",  "estabOLS"),
                c("chg_log_nestab_85_54", "IV-B", "estabIVB"),
                c("chg_log_valprod_85_54", "OLS",   "valprodOLS"),
                c("chg_log_valprod_85_54", "IV-LP", "valprodIVLP"),
                c("chg_log_massal_85_54", "OLS",   "massalOLS"),
                c("chg_log_massal_85_54", "IV-LP", "massalIVLP"),
                c("chg_log_nexp_88_60", "OLS",  "farmsOLS"),
                c("chg_log_nexp_88_60", "IV-B", "farmsIVB"),
                c("chg_log_areatot_ha_88_60", "OLS",  "areaOLS"),
                c("chg_log_areatot_ha_88_60", "IV-B", "areaIVB"))) {
            r <- row1(t10, outcome = g[1], spec = g[2])
            macros[[paste0(g[3], "Coef")]] <- f3(r$estimate)
            macros[[paste0(g[3], "SE")]]   <- f3(r$std_err)
        }
        macros[["valprodN"]] <- as.character(
            row1(t10, outcome = "chg_log_valprod_85_54", spec = "IV-B")$n_obs)
        macros[["massalN"]] <- as.character(
            row1(t10, outcome = "chg_log_massal_85_54", spec = "IV-B")$n_obs)
    }

    # -- Table 11 (Section 5.4): other outcomes ------------------------------
    t11 <- tab[["table_11_other_outcomes_iv"]]
    if (!is.null(t11)) {
        cll <- t11[t11$outcome == "chg_college_91_70", ]
        macros[["collegeMin"]] <- f3(min(cll$estimate))
        macros[["collegeMax"]] <- f3(max(cll$estimate))
        r <- row1(t11, outcome = "chg_secondary_91_70", spec = "OLS")
        macros[["secondaryOLSCoef"]] <- f3(r$estimate)
        r <- row1(t11, outcome = "chg_secondary_91_70", spec = "IV-B")
        macros[["secondaryIVBCoef"]] <- f3(r$estimate)
        r <- row1(t11, outcome = "chg_secondary_91_70", spec = "IV-LP")
        macros[["secondaryIVLPCoef"]] <- f3(r$estimate)
        macros[["secondaryIVLPP"]]    <- f3(r$p_value)
        r <- row1(t11, outcome = "chg_mig5_91_70", spec = "OLS")
        macros[["migrationOLSCoef"]] <- f3(r$estimate)
        macros[["migrationOLSSE"]]   <- f3(r$std_err)
        r <- row1(t11, outcome = "chg_mig5_91_70", spec = "IV-B")
        # Prose says "a X decrease": fail loudly if the sign ever flips.
        stopifnot(r$estimate < 0)
        macros[["migrationCoefAbs"]] <- f3(abs(r$estimate))
        r <- row1(t11, outcome = "chg_empstat_emp_91_70", spec = "OLS")
        macros[["employmentOLSCoef"]] <- f3(r$estimate)
        macros[["employmentOLSSE"]]   <- f3(r$std_err)
        r <- row1(t11, outcome = "chg_empstat_emp_91_70", spec = "IV-B")
        macros[["employmentIVBCoef"]] <- f3(r$estimate)
        macros[["employmentIVBSE"]]   <- f3(r$std_err)
    }

    # -- Table 7 (Section 4.6): placebo columns ------------------------------
    t7 <- tab[["table_7_pre_trends"]]
    if (!is.null(t7)) {
        for (m in list(c("OLS", "placeboOLS"),
                       c("IV-LP", "placeboIVLP"),
                       c("IV-Hypo", "placeboIVH"))) {
            r <- row1(t7, spec = m[1])
            macros[[paste0(m[2], "Coef")]] <- f3(r$estimate)
            macros[[paste0(m[2], "SE")]]   <- f3(r$std_err)
        }
        macros[["placeboFLP"]]   <- f2(row1(t7, spec = "IV-LP")$first_stage_F)
        macros[["placeboFHypo"]] <- f2(row1(t7, spec = "IV-Hypo")$first_stage_F)
        macros[["placeboFBoth"]] <- f2(row1(t7, spec = "IV-Both")$first_stage_F)
    }

    # -- Table 12 (Section 5.5): robustness ----------------------------------
    t12 <- tab[["table_12_robustness"]]
    if (!is.null(t12)) {
        r <- t12[t12$panel == "A", ]
        stopifnot(nrow(r) == 1L)
        macros[["thetaHighIVBCoef"]] <- f3(r$iv_b_est)
        macros[["thetaHighIVBSE"]]   <- f3(r$iv_b_se)
        b <- t12[t12$panel == "B", ]
        macros[["hypoAltIVBMin"]] <- f3(min(b$iv_b_est))
        macros[["hypoAltIVBMax"]] <- f3(max(b$iv_b_est))
        r <- t12[t12$panel == "C" & grepl("Placebo", t12$label), ]
        stopifnot(nrow(r) == 1L)
        macros[["subOLSCoef"]] <- f3(r$ols_est)
        macros[["subOLSSE"]]   <- f3(r$ols_se)
        macros[["subIVBCoef"]] <- f3(r$iv_b_est)
        macros[["subIVBSE"]]   <- f3(r$iv_b_se)
        macros[["subIVBP"]]    <- f3(r$iv_b_p)
    }

    # -- Table 13 (Section 6.2): counterfactual urban/rural ------------------
    t13 <- tab[["table_13_counterfactual"]]
    if (!is.null(t13)) {
        for (g in list(
                c("B", "chg_log_urbpop_91_60", "cfRailUrb"),
                c("B", "chg_log_rur_91_60",    "cfRailRur"),
                c("C", "chg_log_urbpop_91_60", "cfRoadUrb"),
                c("C", "chg_log_rur_91_60",    "cfRoadRur"))) {
            r <- t13[t13$panel == g[1] & t13$outcome == g[2], ]
            stopifnot(nrow(r) == 1L)
            macros[[paste0(g[3], "Coef")]] <- f3(r$iv_est)
            macros[[paste0(g[3], "SE")]]   <- f3(r$iv_se)
        }
        r <- row1(t13, panel = "B", outcome = "chg_log_pop_91_60")
        macros[["cfRailOLSPopCoef"]] <- f3(r$ols_est)
        macros[["cfRailOLSPopSE"]]   <- f3(r$ols_se)
        b <- t13[t13$panel == "B", ]
        macros[["cfRailFMin"]] <- f1(min(b$iv_F))
        macros[["cfRailFMax"]] <- f1(max(b$iv_F))
        # Section 6.3: only-road identification weakens from total to
        # urban population. (Total-pop F is the existing \cfRoadFIVPop.)
        r <- row1(t13, panel = "C", outcome = "chg_log_urbpop_91_60")
        macros[["cfRoadFUrb"]] <- f1(r$iv_F)
    }

    # -- Table 14 (Section 7): sample counts ---------------------------------
    t14 <- tab[["table_14_mechanisms"]]
    if (!is.null(t14)) {
        macros[["mechNBase"]] <- as.character(
            row1(t14, spec_id = "(1)")$n_obs)
        macros[["mechNAug"]] <- as.character(
            row1(t14, spec_id = "(2)")$n_obs)
    }

    # -- Table 6 (Section 4.5): pre-period balance ----------------------------
    # Prose quotes coefficients and p-values for the two instruments'
    # balance regressions, plus a computed count of hypo-instrument
    # imbalances (the "uncorrelated with all nine" sentence).
    t6 <- tab[["table_6_pre_balance"]]
    if (!is.null(t6)) {
        # p-value with its comparison operator, so prose writes "$p\X{}$"
        # uniformly: "<0.001", "=0.003", "=0.74".
        fp <- function(x) {
            if (x < 0.001) {
                "<0.001"
            } else if (x < 0.01) {
                sprintf("=%.3f", x)
            } else {
                sprintf("=%.2f", x)
            }
        }
        bal <- function(outc, stem) {
            r <- row1(t6, outcome = outc)
            macros[[paste0("balLP", stem, "Coef")]] <<- f3(r$lp_only_coef)
            macros[[paste0("balLP", stem, "P")]]    <<- fp(r$lp_only_p)
        }
        bal("log_pop_1960",   "Pop")
        bal("urbshr_1960",    "Urbshr")
        bal("wheat_std",      "Wheat")
        bal("preCal_std",     "PreCal")
        bal("postCal_std",    "PostCal")
        bal("dist_to_BA_std", "DistBA")
        bal("log_area_km2",   "Area")
        # Hypo instrument: count of characteristics imbalanced at 5%.
        macros[["balHypoNImbalanced"]] <-
            as.character(sum(t6$hypo_only_p < 0.05))
    }

    # -- Density-schedule table (Section 5.2) ---------------------------------
    ds <- tab[["table_15_density_schedules"]]
    if (!is.null(ds)) {
        for (g in list(c("s1", "densHigh"), c("s2", "densLow"))) {
            r <- row1(ds, schedule = g[1])
            macros[[paste0(g[2], "IVBCoef")]] <- f3(r$iv_b_est)
            macros[[paste0(g[2], "IVBSE")]]   <- f3(r$iv_b_se)
            macros[[paste0(g[2], "IVBP")]]    <- f3(r$iv_b_p)
            # f2, not f1: this F is quoted in one sentence alongside
            # \firstStageFBoth (2 dp), so precisions must match.
            macros[[paste0(g[2], "F")]]       <- sprintf("%.2f", r$iv_b_F)
            macros[[paste0(g[2], "CorrBase")]] <- sprintf("%.2f",
                                                          r$corr_treat_s0)
        }
    }

    # -- Sector-matched MA table (Section 5.3) ---------------------------------
    sm <- tab[["table_16_sector_matched"]]
    if (!is.null(sm)) {
        for (g in list(
                c("chg_log_valprod_85_54",    "smValprod"),
                c("chg_log_massal_85_54",     "smMassal"),
                c("chg_log_nexp_88_60",       "smNexp"),
                c("chg_log_areatot_ha_88_60", "smArea"))) {
            r <- row1(sm, outcome = g[1])
            macros[[paste0(g[2], "IVBCoef")]] <- f3(r$iv_b_est)
            macros[[paste0(g[2], "IVBSE")]]   <- f3(r$iv_b_se)
            macros[[paste0(g[2], "IVBP")]]    <- f3(r$iv_b_p)
        }
        r <- row1(sm, outcome = "chg_log_valprod_85_54")
        macros[["smMfgF"]] <- f1(r$iv_b_F)
        r <- row1(sm, outcome = "chg_log_massal_85_54")
        macros[["smMassalF"]] <- f1(r$iv_b_F)
        r <- row1(sm, outcome = "chg_log_nexp_88_60")
        macros[["smAgrF"]] <- f1(r$iv_b_F)
    }

    # -- Transshipment bound (Section 5.5 paragraph) ---------------------------
    un <- tab[["diagnostic_ma_unimodal"]]
    if (!is.null(un)) {
        g <- function(stat) row1(un, stat = stat)$value
        macros[["uniGainShareBase"]]  <- f1(g("gain_share_baseline"))
        macros[["uniGainShareUni"]]   <- f1(g("gain_share_unimodal"))
        macros[["uniMedRatioBase"]]   <- f2(g("median_ratio_baseline"))
        macros[["uniMedRatioUni"]]    <- f2(g("median_ratio_unimodal"))
        macros[["uniOLSBase"]]        <- f3(g("ols_beta_baseline"))
        macros[["uniOLSBaseSE"]]      <- f3(g("ols_se_baseline"))
        macros[["uniOLSUni"]]         <- f3(g("ols_beta_unimodal"))
        macros[["uniOLSUniSE"]]       <- f3(g("ols_se_unimodal"))
        macros[["uniN"]]              <- as.character(g("n_unimodal"))
    }

    # -- Theta sweep (Section 5.5 table; Section 8.2 interpretation) ----------
    sw <- tab[["diagnostic_theta_sweep"]]
    if (!is.null(sw)) {
        r <- row1(sw, theta = 1)
        macros[["sweepBetaThetaOne"]] <- sprintf("%.2f", r$iv_beta)
        r <- row1(sw, theta = 12)
        macros[["sweepBetaThetaTwelve"]] <- sprintf("%.2f", r$iv_beta)
        macros[["sweepFMin"]] <- f1(min(sw$first_stage_F, na.rm = TRUE))
        macros[["sweepFMax"]] <- f1(max(sw$first_stage_F, na.rm = TRUE))
    }

    macros
}

# ---------------------------------------------------------------------------
# Panel-derived statistics quoted in Section 3 (the numbers that moved in
# issue #22). Read from the wide panel and the 1947 census output so the
# prose can never drift from the pipeline again.
#
# Sign convention: minima and the only-rail mean are emitted as ABSOLUTE
# values because the prose carries the minus sign explicitly (e.g.
# "mean $-\cfRailMean{}$"); this keeps LaTeX math-mode signs clean.
# ---------------------------------------------------------------------------
add_panel_macros <- function(macros) {

    panel_path <- file.path(dir_derived_panel,
                            "departments_wide_panel.parquet")
    if (!file.exists(panel_path)) {
        warning("Panel parquet missing — Section 3 macros skipped")
        return(macros)
    }
    p <- as.data.frame(arrow::read_parquet(panel_path))

    big <- function(x) formatC(round(x), format = "d", big.mark = "{,}")

    # Population (all 312 districts; Section 3 reports full-geography stats)
    macros[["popMeanBase"]]  <- big(mean(p$pop_1960, na.rm = TRUE))
    macros[["popTotalBase"]] <- sprintf("%.1f",
        sum(p$pop_1960, na.rm = TRUE) / 1e6)
    macros[["popMeanEnd"]]   <- big(mean(p$pop_1991, na.rm = TRUE))
    macros[["popTotalEnd"]]  <- sprintf("%.1f",
        sum(p$pop_1991, na.rm = TRUE) / 1e6)
    v <- p$chg_log_pop_91_60
    macros[["chgLogPopMean"]] <- sprintf("%+.2f", mean(v, na.rm = TRUE))
    macros[["chgLogPopSD"]]   <- sprintf("%.2f", sd(v, na.rm = TRUE))

    # Treatment variable distribution
    m <- p$chg_logMA_86_60_s0_elow
    macros[["maMean"]]     <- sprintf("%+.2f", mean(m, na.rm = TRUE))
    macros[["maSD"]]       <- sprintf("%.2f", sd(m, na.rm = TRUE))
    # Prose writes the minus sign explicitly: fail loudly on sign flip.
    stopifnot(min(m, na.rm = TRUE) < 0)
    macros[["maMin"]]      <- sprintf("%.2f", abs(min(m, na.rm = TRUE)))
    macros[["maMax"]]      <- sprintf("%+.2f", max(m, na.rm = TRUE))
    macros[["maSharePos"]] <- sprintf("%.0f", 100 * mean(m > 0, na.rm = TRUE))

    # Counterfactual component means
    r <- mean(p$chg_logMA_only_rail_s0_elow, na.rm = TRUE)
    d <- mean(p$chg_logMA_only_road_s0_elow, na.rm = TRUE)
    # Prose writes the minus sign explicitly: fail loudly on sign flip.
    stopifnot(r < 0)
    macros[["cfRailMean"]]      <- sprintf("%.2f", abs(r))
    macros[["cfRoadMean"]]      <- sprintf("%+.2f", d)
    macros[["cfSumComponents"]] <- sprintf("%+.2f", r + d)

    # 1947 census coverage (file rows)
    c47_path <- file.path(dir_derived_census1947,
                          "census_1947_ipums.parquet")
    if (file.exists(c47_path)) {
        c47 <- arrow::read_parquet(c47_path)
        macros[["placeboCoverage"]] <- as.character(nrow(c47))
    }

    macros
}

main()

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
#   results/tables/table_{6,7,8,9,10,11,12}_*.csv
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
                   "diagnostic_heterogeneity")) {
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
        r <- subset(t9, outcome == "chg_log_urbpop_91_60" & spec == "IV-B")
        if (nrow(r) == 1L) {
            macros[["mainUrbPopCoefIVBoth"]]  <- sprintf("%.3f", r$estimate)
            macros[["mainUrbPopSEIVBoth"]]    <- sprintf("%.3f", r$std_err)
        }
        r <- subset(t9, outcome == "chg_log_rur_91_60" & spec == "IV-B")
        if (nrow(r) == 1L) {
            macros[["mainRurPopCoefIVBoth"]]  <- sprintf("%.3f", r$estimate)
            macros[["mainRurPopSEIVBoth"]]    <- sprintf("%.3f", r$std_err)
        }
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
            macros[["mechAllZF"]]         <- sprintf("%.1f", r$ma_F)
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

main()

# ===========================================================================
# table_a1_descriptives.R
#
# PURPOSE: Appendix Table A1 — descriptive statistics for the estimation
#          sample. Serves tasks C35/C36 (industrial + agricultural census
#          appendix detail) and fills a gap: the paper had no summary-
#          statistics exhibit at all (the Gibbons et al. 2024 model:
#          descriptives in Appendix Table A1).
#
# DESIGN DECISION (2026-07-18, decide-and-document; see ledger):
#   - C34 (dedicated theta = 8.11 appendix table) is MOOTED: Table 12
#     Panel A reports the full four-estimator grid at theta = 8.11, and
#     the theta-sweep table reports the whole grid. No new exhibit.
#   - C35/C36 are served by one descriptives table covering the sectoral
#     census outcomes alongside every other regression variable, rather
#     than two sector-specific level tables: what a referee needs is the
#     scale and coverage of each variable entering the regressions.
#
# ROWS: outcomes (population, urban, rural, urban share; five sectoral
#       outcomes; four IPUMS outcomes; placebo), treatment, the two
#       instruments, and the controls of geo_controls_main.
# COLUMNS: mean, sd, min, max, N (non-missing, estimation sample).
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/table_a1_descriptives.{tex,csv}
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
})

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)

    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)

    d <- as.data.frame(arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet")
    ))

    # IPUMS changes are constructed at estimation time (identically to
    # table_11_other_outcomes.R), not stored in the sample.
    d$chg_college_91_70     <- d$college_1991   - d$college_1970
    d$chg_secondary_91_70   <- d$secondary_1991 - d$secondary_1970
    d$chg_mig5_91_70        <- d$mig5_1991      - d$mig5_1970
    d$chg_empstat_emp_91_70 <- d$empstat_1_1991 - d$empstat_1_1970

    vars <- list(
        # group header, variable, label
        c("Outcomes: population (1960--1991)",
          "chg_log_pop_91_60",        "$\\Delta \\ln$ total population"),
        c("", "chg_log_urbpop_91_60", "$\\Delta \\ln$ urban population"),
        c("", "chg_log_rur_91_60",    "$\\Delta \\ln$ rural population"),
        c("", "chg_urbshr_91_60",     "$\\Delta$ urban share"),
        c("Outcomes: sectoral censuses",
          "chg_log_nestab_85_54",     "$\\Delta \\ln$ mfg.\\ establishments"),
        c("", "chg_log_valprod_85_54","$\\Delta \\ln$ mfg.\\ production value"),
        c("", "chg_log_massal_85_54", "$\\Delta \\ln$ mfg.\\ wage mass"),
        c("", "chg_log_nexp_88_60",   "$\\Delta \\ln$ farms"),
        c("", "chg_log_areatot_ha_88_60", "$\\Delta \\ln$ farmed area"),
        c("Outcomes: IPUMS (1970--1991)",
          "chg_college_91_70",        "$\\Delta$ college share"),
        c("", "chg_secondary_91_70",  "$\\Delta$ secondary share"),
        c("", "chg_mig5_91_70",       "$\\Delta$ recent-migrant share"),
        c("", "chg_empstat_emp_91_70","$\\Delta$ employment rate"),
        c("Placebo",
          "chg_log_placebo_pop_60_47","$\\Delta \\ln$ population 1947--1960"),
        c("Treatment and instruments",
          "chg_logMA_86_60_s0_elow",  "$\\Delta \\ln \\mathrm{MA}^{\\mathrm{full}}$"),
        c("", "chg_logMA_stu_s0_elow","$\\Delta \\ln \\mathrm{MA}$, Larkin instrument"),
        c("", "chg_logMA_lcp_mst_s0_elow",
          "$\\Delta \\ln \\mathrm{MA}$, hypothetical-road instrument"),
        c("Controls",
          "logMA_actual_1960_s0_elow","Baseline $\\ln \\mathrm{MA}$ (1960)"),
        c("", "log_pop_1960",         "Baseline $\\ln$ population (1960)"),
        c("", "elev_mean_std",        "Elevation (std.)"),
        c("", "rugged_mea_std",       "Ruggedness (std.)"),
        c("", "wheat_std",            "Wheat suitability (std.)"),
        c("", "preCal_std",           "Pre-1500 caloric potential (std.)"),
        c("", "postCal_std",          "Post-1500 caloric potential (std.)"),
        c("", "dist_to_BA_std",       "Distance to Buenos Aires (std.)")
    )

    # Every listed variable must exist; fail loudly rather than skip.
    missing_vars <- vapply(vars, function(v) v[2], "")[
        !vapply(vars, function(v) v[2], "") %in% names(d)]
    if (length(missing_vars) > 0L) {
        stop("Variables not in estimation sample: ",
             paste(missing_vars, collapse = ", "))
    }

    # Guard against drift from the main specification (cr-review PR #104):
    # the instrument row must be the spec's hypothetical-road instrument,
    # and the Controls block must list exactly geo_controls_main.
    stopifnot("chg_logMA_lcp_mst_s0_elow" == main_hypo_instrument)
    listed_controls <- vapply(vars, function(v) v[2], "")[
        cumsum(vapply(vars, function(v) v[1], "") == "Controls") > 0]
    stopifnot(setequal(listed_controls, geo_controls_main))

    rows <- lapply(vars, function(v) {
        x <- d[[v[2]]]
        data.frame(group = v[1], variable = v[2], label = v[3],
                   mean = mean(x, na.rm = TRUE),
                   sd = sd(x, na.rm = TRUE),
                   min = min(x, na.rm = TRUE),
                   max = max(x, na.rm = TRUE),
                   n = sum(!is.na(x)),
                   stringsAsFactors = FALSE)
    })
    df <- do.call(rbind, rows)

    message(sprintf("[tA1] %d variables summarised over %d districts",
                    nrow(df), nrow(d)))

    # ----------------------------------------------------------------------
    # LaTeX
    # ----------------------------------------------------------------------
    f <- function(x) sprintf("%.3f", x)
    tex_lines <- c(
        "% Appendix Table A1: descriptive statistics (estimation sample).",
        "% Generated by code/analysis/table_a1_descriptives.R.",
        "",
        "\\begin{table}[htbp]",
        "\\centering",
        "\\caption{Descriptive Statistics, Estimation Sample}",
        "\\label{tab:descriptives}",
        "\\small",
        "\\begin{tabular}{lrrrrr}",
        "\\toprule",
        " & Mean & SD & Min & Max & $N$ \\\\",
        "\\midrule")
    for (i in seq_len(nrow(df))) {
        r <- df[i, ]
        if (nzchar(r$group)) {
            tex_lines <- c(tex_lines,
                sprintf("\\multicolumn{6}{l}{\\textit{%s}} \\\\", r$group))
        }
        tex_lines <- c(tex_lines,
            sprintf("\\quad %s & %s & %s & %s & %s & %d \\\\",
                    r$label, f(r$mean), f(r$sd), f(r$min), f(r$max), r$n))
    }
    tex_lines <- c(tex_lines,
        "\\bottomrule",
        "\\end{tabular}",
        "",
        "\\footnotesize",
        paste0("\\emph{Notes}: Estimation sample (",
               sprintf("%d", nrow(d)),
               " districts; Capital Federal excluded as an observation, ",
               "Section~\\ref{sec:data}). $N$ counts non-missing ",
               "observations per variable; coverage differences reflect ",
               "the sectoral censuses, the 1947 placebo subsample ",
               "described in Section~\\ref{sec:data}, and log changes in ",
               "urban and rural population being undefined for districts ",
               "with zero urban or rural population in either census ",
               "year. The Larkin-instrument change is nonpositive by ",
               "construction (removing studied rail segments cannot ",
               "raise market access). Standardised ",
               "controls have mean zero and unit variance by ",
               "construction over the 312 mainland districts."),
        "\\end{table}"
    )

    out_tex <- file.path(dir_tables, "table_a1_descriptives.tex")
    writeLines(tex_lines, out_tex)
    message("Saved: ", out_tex)

    out_csv <- file.path(dir_tables, "table_a1_descriptives.csv")
    write.csv(df, out_csv, row.names = FALSE)
    message("Saved: ", out_csv)
}

main()

# ===========================================================================
# table_7_pre_trends.R
#
# PURPOSE: Paper Table 7 — pre-trends placebo. Tests whether the
#          1960–1986 market-access change predicts pre-reform
#          population growth (1947–1960). If the instruments (or the
#          treatment) correlate with pre-reform trends, the
#          identification strategy is in trouble.
#
# DEP VAR: chg_log_placebo_pop_60_47 (log change in district
#          population between 1947 and 1960).
#
# REGRESSOR: chg_logMA_86_60_s0_elow (the main post-reform treatment).
#
# COLUMNS (same structure as Table 9):
#   (1) OLS       — direct regression of placebo outcome on treatment
#   (2) IV-LP     — instrument the treatment with LP
#   (3) IV-Hypo   — instrument with LCP-MST
#   (4) IV-Both   — instrument with both
#
# Interpretation:
#   - If the coefficient is near zero and insignificant, post-reform
#     infrastructure changes did NOT predict pre-reform population
#     growth. This supports a causal reading of the post-reform
#     relationship in Table 9.
#   - If the coefficient is significant, there's a pre-existing trend
#     correlated with the eventual infrastructure changes, which
#     threatens the causal interpretation.
#
# CONTROLS: placebo_controls (config.R) — the six standardized geographic
#           controls + baseline log MA (1960) + baseline log pop (1947).
#           This DIFFERS from geo_controls_main (Tables 6, 8, 9) in one
#           term: the population baseline is 1947, not 1960. The placebo
#           outcome is log pop 1960 - log pop 1947, so log pop 1960 is the
#           TERMINAL level of the window under test and conditioning on it
#           conditions on a component of the outcome. Adopted 2026-07-27
#           (agenda item B); rationale and the rejected alternative
#           (dropping the MA baseline too) are in config.R.
#
# SAMPLE: the subset for which the 1947 census provides
#         comparable population data — see Section 3.2).
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/table_7_pre_trends.{tex,csv}
#   results/tables/table_b2_placebo_ladder.{tex,csv}  (write_ladder)
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

    y <- "chg_log_placebo_pop_60_47"
    fits <- fit_iv_quad(
        y = y, data = d,
        endog = main_treatment,
        lp_instr = main_lp_instrument,
        hypo_instr = main_hypo_instrument,
        ctrls_vec = placebo_controls
    )
    m_ols   <- fits[["OLS"]]
    m_iv_lp <- fits[["IV-LP"]]
    m_iv_h  <- fits[["IV-H"]]
    m_iv_b  <- fits[["IV-B"]]

    # First-stage F-stats per IV spec
    fs_lp <- fitstat_F(m_iv_lp)
    fs_h  <- fitstat_F(m_iv_h)
    fs_b  <- fitstat_F(m_iv_b)

    message("\n[t7] Pre-trends placebo on Δlog(pop_60_47):")
    message(sprintf("%-12s  %-20s  N = %d", "OLS",
                    format_co(safe_coef(m_ols, main_treatment)),
                    nobs(m_ols)))
    message(sprintf("%-12s  %-20s  F = %.1f", "IV-LP",
                    format_co(safe_coef(m_iv_lp,
                        paste0("fit_", main_treatment))),
                    fs_lp))
    message(sprintf("%-12s  %-20s  F = %.1f", "IV-Hypo",
                    format_co(safe_coef(m_iv_h,
                        paste0("fit_", main_treatment))),
                    fs_h))
    message(sprintf("%-12s  %-20s  F = %.1f", "IV-Both",
                    format_co(safe_coef(m_iv_b,
                        paste0("fit_", main_treatment))),
                    fs_b))

    # --- Build LaTeX ------------------------------------------------------
    models <- list(
        "(1) OLS"      = m_ols,
        "(2) IV-LP"    = m_iv_lp,
        "(3) IV-Hypo"  = m_iv_h,
        "(4) IV-Both"  = m_iv_b
    )

    coef_map <- setNames(
        rep("$\\Delta \\ln \\mathrm{MA}^{\\mathrm{full}}$", 2L),
        c(main_treatment, paste0("fit_", main_treatment))
    )

    gof_custom <- list(
        list("raw" = "nobs", "clean" = "Observations", "fmt" = 0)
    )

    add_rows <- tibble::tibble(
        ` `           = "First-stage $F$",
        `(1) OLS`     = "---",
        `(2) IV-LP`   = sprintf("%.1f", fs_lp),
        `(3) IV-Hypo` = sprintf("%.1f", fs_h),
        `(4) IV-Both` = sprintf("%.1f", fs_b)
    )

    tbl <- modelsummary(
        models,
        output   = "latex",
        coef_map = coef_map,
        gof_map  = gof_custom,
        stars    = c("*" = .1, "**" = .05, "***" = .01),
        escape   = FALSE,
        add_rows = add_rows,
        title    = "Pre-trends placebo: does $\\Delta \\ln \\mathrm{MA}^{\\mathrm{full}}$ predict 1947--1960 population growth?"
    )

    # Notes are appended as raw LaTeX before \end{table}, because
    # modelsummary's note-escaping mangles backslashes. Use plain
    # \footnotesize text instead of threeparttable to keep the
    # dependency surface minimal (matches Table 9 which has no notes).
    # Minipage for the same reason as the ladder below: inside
    # \begin{table}\centering a bare paragraph is centred line by line,
    # which strands the "Notes:" label at the right margin once the note
    # runs past a couple of lines (cr-review PR #145).
    notes_tex <- paste(c(
        "\\vspace{0.5em}",
        "\\begin{minipage}{0.92\\textwidth}",
        "{\\footnotesize \\textit{Notes:}",
        "Dependent variable: $\\Delta \\ln(\\mathrm{pop}_{1960}/\\mathrm{pop}_{1947})$.",
        "Robust (HC1) SE in parentheses.",
        "All columns include baseline log MA (1960), baseline log pop",
        "(\\emph{1947}), and the six standardized geographic controls.",
        "The 1947 population baseline replaces the 1960 one used in",
        "Tables~\\ref{tab:pre_balance}, \\ref{tab:first_stage} and",
        "\\ref{tab:population_iv}: the outcome here is",
        "$\\ln \\mathrm{pop}_{1960} - \\ln \\mathrm{pop}_{1947}$, so the 1960",
        "level is the terminal value of the window under test.",
        "Table~\\ref{tab:placebo_ladder} reports the estimate under",
        "alternative baseline sets.",
        "A near-zero and insignificant coefficient is evidence that",
        "post-reform $\\Delta \\ln \\mathrm{MA}$ is not picking up pre-reform trends.",
        "$^{*}p<0.10,\\;^{**}p<0.05,\\;^{***}p<0.01$.}",
        "\\end{minipage}"
    ), collapse = "\n")

    # Inject the notes just before \end{table}. Use gsub with fixed = TRUE
    # on the replacement to avoid backreference interpretation of backslashes.
    tbl_txt <- as.character(tbl)
    tbl_txt <- inject_first_label(tbl_txt, "tab:pre_trends")
    end_marker <- "\\end{table}"
    tbl_txt <- sub(end_marker, paste0(notes_tex, "\n", end_marker),
                   tbl_txt, fixed = TRUE)

    out_tex <- file.path(dir_tables, "table_7_pre_trends.tex")
    writeLines(c(
        "% Table 7: Pre-trends placebo.",
        "% Generated by code/analysis/table_7_pre_trends.R.",
        "%",
        "% Sample is the districts where the 1947 census provides",
        "% comparable population data (see Section 3.2).",
        "",
        tbl_txt
    ), out_tex)
    message("Saved: ", out_tex)

    # CSV — iterate over models directly so the loop doesn't depend on
    # string-matching column labels to keys.
    csv_rows <- list()
    for (nm in names(models)) {
        m <- models[[nm]]
        is_ols <- grepl("OLS", nm, fixed = TRUE)
        coef_name <- if (is_ols) main_treatment
                     else paste0("fit_", main_treatment)
        spec_label <- sub("^\\(\\d+\\)\\s*", "", nm)  # strip "(1) "
        co <- safe_coef(m, coef_name)
        csv_rows[[length(csv_rows) + 1L]] <- data.frame(
            spec          = spec_label,
            estimate      = co$est,
            std_err       = co$se,
            t_value       = co$t,
            p_value       = co$p,
            n_obs         = nobs(m),
            first_stage_F = if (is_ols) NA_real_ else fitstat_F(m),
            stringsAsFactors = FALSE
        )
    }
    csv_df <- do.call(rbind, csv_rows)
    out_csv <- file.path(dir_tables, "table_7_pre_trends.csv")
    write.csv(csv_df, out_csv, row.names = FALSE)
    message("Saved: ", out_csv)

    write_ladder(d)
}

# ---------------------------------------------------------------------------
# write_ladder(d): appendix table showing the placebo estimate under four
# baseline control sets.
#
# WHY THIS IS IN THE PAPER (agenda item B, 2026-07-27): the placebo's
# verdict depends on which baselines are conditioned on, and the swing
# across these four sets is large enough to change the reading (see the
# generated table_b2_placebo_ladder.csv for the current numbers).
# Reporting only the adopted set would leave a referee to discover that;
# the ladder puts it on the page. Computed here rather than read
# from diagnostic_placebo_1947.csv so that no paper exhibit depends on a
# diagnostic output.
#
# The four sets are the same ones diagnostic_placebo_1947baseline.R
# reports (that diagnostic remains the fuller treatment: all four
# estimators). The link is enforced from the OTHER side since PR #145:
# the diagnostic anchors its own variants to this table's CSV, so a
# divergence fails there. What IS asserted here is that row (2)
# reproduces the Table 7 fit computed above in this same script, which
# is the claim the caption makes.
# ---------------------------------------------------------------------------
write_ladder <- function(d) {
    sets <- list(
        list(tag = "(1) 1960 baselines",
             ctrls = geo_controls_main),
        list(tag = "(2) 1947 pop baseline",
             ctrls = placebo_controls),
        list(tag = "(3) 1947 pop, no MA baseline",
             ctrls = setdiff(placebo_controls,
                             "logMA_actual_1960_s0_elow")),
        list(tag = "(4) no baselines",
             ctrls = setdiff(placebo_controls,
                             c("logMA_actual_1960_s0_elow",
                               "log_pop_1947")))
    )
    y <- "chg_log_placebo_pop_60_47"
    # One sample across rows, so the ladder isolates the control set and
    # not the sample: complete cases on the union of everything used.
    all_v <- unique(c(y, main_treatment, main_lp_instrument,
                      main_hypo_instrument,
                      unlist(lapply(sets, `[[`, "ctrls"))))
    dd <- as.data.frame(d)[complete.cases(as.data.frame(d)[, all_v]), ]

    rows <- list()
    for (s in sets) {
        fits <- fit_iv_quad(y = y, data = dd, endog = main_treatment,
                            lp_instr = main_lp_instrument,
                            hypo_instr = main_hypo_instrument,
                            ctrls_vec = s$ctrls)
        co_o <- safe_coef(fits[["OLS"]], main_treatment)
        co_b <- safe_coef(fits[["IV-B"]], paste0("fit_", main_treatment))
        rows[[length(rows) + 1L]] <- data.frame(
            control_set = s$tag,
            ols_est = co_o$est, ols_se = co_o$se, ols_p = co_o$p,
            ivb_est = co_b$est, ivb_se = co_b$se, ivb_p = co_b$p,
            ivb_F = fitstat_F(fits[["IV-B"]]), n_obs = nobs(fits[["IV-B"]]),
            stringsAsFactors = FALSE)
    }
    L <- do.call(rbind, rows)
    stopifnot(nrow(L) == 4L, length(unique(L$n_obs)) == 1L,
              !any(is.na(L$ols_est)), !any(is.na(L$ivb_est)),
              !any(is.na(L$ols_p)),   !any(is.na(L$ivb_p)))
    # Row (2) IS the specification Table 7 reports. Assert it rather
    # than stating it in the caption and hoping (cr-review PR #145).
    t7_csv <- read.csv(file.path(dir_tables, "table_7_pre_trends.csv"),
                       stringsAsFactors = FALSE)
    r2 <- L[L$control_set == "(2) 1947 pop baseline", ]
    for (pair in list(c("OLS", "ols"), c("IV-Both", "ivb"))) {
        ref <- t7_csv[t7_csv$spec == pair[1], ]
        stopifnot(nrow(ref) == 1L,
                  abs(r2[[paste0(pair[2], "_est")]] - ref$estimate) < 1e-10,
                  abs(r2[[paste0(pair[2], "_se")]]  - ref$std_err) < 1e-10)
    }

    st <- function(p) ifelse(p < 0.01, "^{***}",
                      ifelse(p < 0.05, "^{**}",
                      ifelse(p < 0.10, "^{*}", "")))
    body <- character()
    for (i in seq_len(nrow(L))) {
        body <- c(body, sprintf(
            "%s & $%+.3f%s$ & (%.3f) & $%+.3f%s$ & (%.3f) & %.1f \\\\",
            L$control_set[i], L$ols_est[i], st(L$ols_p[i]), L$ols_se[i],
            L$ivb_est[i], st(L$ivb_p[i]), L$ivb_se[i], L$ivb_F[i]))
    }
    tex <- c(
        "% Appendix ladder for Table 7: placebo estimate by baseline set.",
        "% Generated by code/analysis/table_7_pre_trends.R (write_ladder).",
        "\\begin{table}[htbp]",
        "\\centering",
        paste0("\\caption{Pre-trends placebo under alternative ",
               "baseline control sets}"),
        "\\label{tab:placebo_ladder}",
        "\\begin{tabular}{lccccc}",
        "\\toprule",
        " & \\multicolumn{2}{c}{OLS} & \\multicolumn{2}{c}{IV-Both} & \\\\",
        "\\cmidrule(lr){2-3} \\cmidrule(lr){4-5}",
        "Baseline controls & $\\beta$ & (SE) & $\\beta$ & (SE) & First-stage $F$ \\\\",
        "\\midrule",
        body,
        "\\bottomrule",
        "\\end{tabular}",
        # The notes go in a minipage: inside \begin{table}\centering a bare
        # paragraph gets centred line by line, which renders ragged and
        # pushes the "Notes:" label out to the right margin.
        "\\vspace{0.5em}",
        "\\begin{minipage}{0.92\\textwidth}",
        paste0("{\\footnotesize \\textit{Notes:} Dependent variable: ",
               "$\\Delta \\ln(\\mathrm{pop}_{1960}/\\mathrm{pop}_{1947})$. ",
               "All rows include the six standardized geographic controls ",
               "and use one common sample of ", L$n_obs[1], " districts, ",
               "so differences across rows come from the baseline controls ",
               "alone. Row (2) is the specification reported in ",
               "Table~\\ref{tab:pre_trends}. Row (1) substitutes log pop ",
               "1960 for log pop 1947, conditioning on the terminal ",
               "rather than the initial level of the outcome window. ",
               "Row (3) drops the baseline market-access control and row ",
               "(4) drops the population baseline as well; the estimate ",
               "collapses toward zero, which is why the choice is stated ",
               "rather than assumed. The first-stage $F$ column refers to ",
               "the IV-Both specification. ",
               "Robust (HC1) SE. ",
               "$^{*}p<0.10,\\;^{**}p<0.05,\\;^{***}p<0.01$.}"),
        "\\end{minipage}",
        "\\end{table}"
    )
    out <- file.path(dir_tables, "table_b2_placebo_ladder.tex")
    writeLines(tex, out)
    write.csv(L, file.path(dir_tables, "table_b2_placebo_ladder.csv"),
              row.names = FALSE)
    message("Saved: ", out, " and .csv")
    invisible(L)
}

# ---------------------------------------------------------------------------
# Helpers (table-local: console-print formatter)
# ---------------------------------------------------------------------------
format_co <- function(co) {
    if (is.na(co$est)) return("NA")
    stars <- ifelse(co$p < 0.01, "***",
            ifelse(co$p < 0.05, "**",
            ifelse(co$p < 0.10, "*", "")))
    sprintf("%+6.3f%-3s (%.3f)", co$est, stars, co$se)
}

main()

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
            endog = main_treatment,
            lp_instr = main_lp_instrument,
            hypo_instr = main_hypo_instrument,
            ctrls_vec = geo_controls_main
        )
        for (spec in names(fits)) {
            all_models[[paste(y, spec, sep = "_")]] <- fits[[spec]]
        }

        # Pull out IV first-stage F for reporting, classical and
        # effective. The Montiel Olea-Pflueger effective F is the
        # statistic a referee now asks for: it is robust to
        # heteroskedasticity (the classical F is not, and every spec here
        # uses HC1) and, unlike the classical F, its critical values are
        # defined for more than one instrument. eff_F_from_fit() is the
        # implementation validated in diagnostic_modern_iv.R.
        #
        # Sample: fit_iv_quad NA-drops per model, so the effective F must
        # be computed on the same complete-case rows as the fit it sits
        # beside, not on the full frame.
        cc_vars <- c(y, main_treatment, main_lp_instrument,
                     main_hypo_instrument, geo_controls_main)
        d_cc <- as.data.frame(d)[complete.cases(as.data.frame(d)[, cc_vars]), ]
        stopifnot(nrow(d_cc) == nobs(fits[["IV-B"]]))
        # Anderson-Rubin 95% sets. Reported because the effective F above
        # is judged against a critical value that depends on an estimated
        # bias bound, and for the two-instrument column that bound does
        # real work; AR inference needs no strength threshold at all, so it
        # is the statement that survives if the bound is contested.
        ar_of <- function(spec, instrs) {
            cc <- safe_coef(fits[[spec]], paste0("fit_", main_treatment))
            ar_from_fit(d_cc, y, main_treatment, instrs,
                        geo_controls_main,
                        beta_hat = cc$est, se_hat = cc$se)
        }
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
                                      geo_controls_main),
            ar_lp   = ar_of("IV-LP", main_lp_instrument),
            ar_hypo = ar_of("IV-H",  main_hypo_instrument),
            ar_both = ar_of("IV-B",  c(main_lp_instrument,
                                       main_hypo_instrument))
        )
    }

    # --- Print beta on ΔlogMA across all 16 specifications to stdout ----
    message("\n[t9] Coefficient on ΔlogMA across specifications:")
    message(sprintf("%-30s  %-9s %-9s %-9s %-9s",
                    "Outcome", "OLS", "IV-LP", "IV-Hypo", "IV-Both"))
    for (out in outcomes) {
        y <- out$var
        b_ols <- get_ma_coef(all_models[[paste(y, "OLS",   sep = "_")]],
                             main_treatment)
        b_lp  <- get_ma_coef(all_models[[paste(y, "IV-LP", sep = "_")]],
                             paste0("fit_", main_treatment))
        b_h   <- get_ma_coef(all_models[[paste(y, "IV-H",  sep = "_")]],
                             paste0("fit_", main_treatment))
        b_b   <- get_ma_coef(all_models[[paste(y, "IV-B",  sep = "_")]],
                             paste0("fit_", main_treatment))
        message(sprintf("%-30s  %-9s %-9s %-9s %-9s",
                        y,
                        format_coef_se(b_ols), format_coef_se(b_lp),
                        format_coef_se(b_h),   format_coef_se(b_b)))
    }

    # --- Build LaTeX table ------------------------------------------------
    # modelsummary supports a list of named models and will stack them.
    # Coef map keeps only the main regressor; controls and constant
    # are omitted for space. A single row for the 1st-stage F is added.

    coef_map <- setNames(
        rep("$\\Delta \\ln \\mathrm{MA}^{\\mathrm{full}}$", 2L),
        c(main_treatment, paste0("fit_", main_treatment))
    )

    gof_custom <- list(
        list("raw" = "nobs", "clean" = "Observations", "fmt" = 0)
    )

    # One table per outcome, concatenated into a single .tex file with
    # a \bigskip between panels. The first panel carries the canonical
    # \label{tab:population_iv} so paper-side \ref{} resolves cleanly.
    # Reader-visible notes, following table_11_other_outcomes.R: the full
    # note on the first panel, a pointer on the rest, so ninety words do
    # not repeat four times. Added in PR #157 — before it, the
    # classical-vs-effective F distinction lived only in this file's LaTeX
    # `%` header comments, which are invisible in the PDF, so "Effective
    # $F$ (MOP)" appeared as an unexpanded acronym on the paper's headline
    # table (cr-review PR #156).
    table_note <- paste(
        "Each panel is one outcome; the regressor is the 1960--1986 change",
        "in log market access. All columns include baseline log market",
        "access (1960), baseline log population (1960), and the six",
        "standardized geographic controls. Robust (HC1) standard errors.",
        "Observations differ across panels because the urban and rural",
        "outcomes are defined only for districts with positive urban or",
        "rural population in both census years; see",
        "Section~\\ref{sec:results}.",
        f_rows_note(classical_row_is_robust = FALSE),
        ar_row_note()
    )
    table_note_short <- paste(
        "Controls, standard errors, and the definitions of the two $F$ rows",
        "are as in Table~\\ref{tab:population_iv}. Observations differ by",
        "outcome, as noted there."
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

        # Add first-stage F as a row
        fs <- f_stats[[y]]
        add_rows <- tibble::tibble(
            ` `           = c("First-stage $F$",
                              "Effective $F$ (MOP)",
                              "AR 95\\% set"),
            `(1) OLS`     = c("---", "---", "---"),
            `(2) IV-LP`   = c(sprintf("%.1f", fs$lp),
                              sprintf("%.1f", fs$eff_lp),
                              ar_cell(fs$ar_lp)),
            `(3) IV-Hypo` = c(sprintf("%.1f", fs$hypo),
                              sprintf("%.1f", fs$eff_hypo),
                              ar_cell(fs$ar_hypo)),
            `(4) IV-Both` = c(sprintf("%.1f", fs$both),
                              sprintf("%.1f", fs$eff_both),
                              ar_cell(fs$ar_both))
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
        tbl_txt <- as.character(tbl)
        tbl_txt <- add_table_note(
            tbl_txt,
            if (is_first_panel) table_note else table_note_short
        )
        if (is_first_panel) {
            tbl_txt <- inject_first_label(tbl_txt, "tab:population_iv")
            is_first_panel <- FALSE
        }
        tex_chunks <- c(tex_chunks, tbl_txt, "", "\\bigskip", "")
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
        "% errors.",
        "%",
        "% The two F rows are explained in the table's own reader-visible",
        "% Notes block (see table_note in this script) -- not repeated here,",
        "% because duplicating it invites the two copies to drift.",
        "% FOR WHOEVER EDITS THIS FILE, the parts a reader does not need:",
        "%   - Table 8's first-stage F is ROBUST while this table's is",
        "%     CLASSICAL, so the two tables' F rows are not the same",
        "%     statistic under the same label. Deliberate, recorded in",
        "%     agenda item C; do not 'harmonize' without a decision.",
        "%   - AR sets are IN the table since PR #158; the fuller record",
        "%     (AR p at zero, robust F) is diagnostic_modern_iv.txt.",
        "%   - MOP critical values: results/tables/diagnostic_mop_critical.txt",
        "%     (a DIFFERENT file; diagnostic_modern_iv.R deliberately does",
        "%     not compute them).",
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
            coef_name <- if (spec == "OLS") main_treatment
                         else paste0("fit_", main_treatment)
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
                ),
                # The .tex is gitignored, so the CSV is the only
                # coauthor-visible copy in the repo; the effective F has
                # to be here or it is invisible outside a LaTeX build.
                effective_F = switch(
                    spec,
                    "OLS"   = NA_real_,
                    "IV-LP" = f_stats[[y]]$eff_lp,
                    "IV-H"  = f_stats[[y]]$eff_hypo,
                    "IV-B"  = f_stats[[y]]$eff_both
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

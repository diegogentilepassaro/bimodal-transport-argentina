# ===========================================================================
# table_12_robustness.R
#
# PURPOSE: Paper Table 12 — robustness of the headline Table 9 result
#          (total population, chg_log_pop_91_60). Three panels:
#
#   Panel A: Alternative trade elasticity theta_high (main spec uses
#            theta_low; both from config.R). All MA variables switch
#            from _elow to _ehigh.
#
#   Panel B: Alternative hypothetical-road instruments. Main spec uses
#            chg_logMA_lcp_mst_s0_elow. Alternatives: euc_mst, lcp, euc.
#            Each row is a separate Just-IV-Hypo regression (one
#            instrument at a time); rows are labeled by the instrument
#            used. LP and Both columns reuse the main spec's LP
#            instrument paired with the alternative hypo.
#
#   Panel C: Sample robustness. Refit the main Table 9 specification on
#            the placebo subsample where the 1947 placebo outcome
#            is defined (see Section 4.5). Tests whether the OLS-IV gap
#            or the coefficient level changes on that subsample.
#
#   Panel D: Controls ladder. The control blocks enter one at a time,
#            ending at the main specification. Each instrumented cell
#            also carries that rung's robust first-stage F, because the
#            baseline log-MA control is part of the identification and
#            not a nuisance covariate, so the rungs change the first
#            stage too. See the block comment at Panel D.
#
# CONTROLS and SE: Panels A-C use geo_controls_main; Panel D varies the
# control set by construction. HC1 throughout.
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/table_12_robustness.{tex,csv}
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
    library(modelsummary)
})

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_table_helpers.R"), echo = FALSE)
    options(modelsummary_factory_latex = "kableExtra")
    options(modelsummary_format_numeric_latex = "plain")

    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)

    d <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet")
    )

    # Baseline log-MA control is theta-specific. For Panel A we swap the
    # _elow control to _ehigh so the control variable matches the theta
    # used in the treatment and instruments.
    ctrls_elow <- geo_controls_main  # uses logMA_actual_1960_s0_elow
    ctrls_ehigh <- c(setdiff(geo_controls_main, "logMA_actual_1960_s0_elow"),
                     "logMA_actual_1960_s0_ehigh")

    rows <- list()

    # ----------------------------------------------------------------------
    # Panel A: alternative theta (theta_high). Replicates Table 9 col 4
    # (main outcome) with _ehigh MA vars.
    # ----------------------------------------------------------------------
    fits_A <- fit_iv_quad(
        y = "chg_log_pop_91_60", data = d,
        endog = "chg_logMA_86_60_s0_ehigh",
        lp_instr = "chg_logMA_stu_s0_ehigh",
        hypo_instr = "chg_logMA_lcp_mst_s0_ehigh",
        ctrls_vec = ctrls_ehigh
    )
    rows[[length(rows) + 1L]] <- build_row(
        panel = "A", label = sprintf("Alt. theta = %s", format(theta[["high"]])),
        fits = fits_A, endog = "chg_logMA_86_60_s0_ehigh"
    )

    # ----------------------------------------------------------------------
    # Panel B: alternative hypothetical-road instruments. Each row is a
    # full IV-Both specification using the row's named hypo instrument.
    # ----------------------------------------------------------------------
    hypo_alts <- list(
        list(name = "chg_logMA_lcp_mst_s0_elow", label = "LCP-MST (main)"),
        list(name = "chg_logMA_euc_mst_s0_elow", label = "Euclidean-MST"),
        list(name = "chg_logMA_lcp_s0_elow",     label = "LCP (bilateral)"),
        list(name = "chg_logMA_euc_s0_elow",     label = "Euclidean (bilateral)")
    )
    for (h in hypo_alts) {
        fits_B <- fit_iv_quad(
            y = "chg_log_pop_91_60", data = d,
            endog = main_treatment,
            lp_instr = main_lp_instrument,
            hypo_instr = h$name,
            ctrls_vec = ctrls_elow
        )
        rows[[length(rows) + 1L]] <- build_row(
            panel = "B", label = sprintf("Hypo = %s", h$label),
            fits = fits_B, endog = main_treatment
        )
    }

    # ----------------------------------------------------------------------
    # Panel C: subsample stability. Refit the main spec on the subset
    # where chg_log_placebo_pop_60_47 is defined (Table 7 sample; the
    # count is computed, not hardcoded — 237 since issue #22).
    # ----------------------------------------------------------------------
    d_sub <- d[!is.na(d$chg_log_placebo_pop_60_47), ]
    n_sub <- nrow(d_sub)
    fits_C <- fit_iv_quad(
        y = "chg_log_pop_91_60", data = d_sub,
        endog = main_treatment,
        lp_instr = main_lp_instrument,
        hypo_instr = main_hypo_instrument,
        ctrls_vec = ctrls_elow
    )
    rows[[length(rows) + 1L]] <- build_row(
        panel = "C", label = sprintf("Placebo subsample (N=%d)", nrow(d_sub)),
        fits = fits_C, endog = main_treatment
    )

    # For comparison, also report the main-spec IV-Both on the full sample
    fits_main <- fit_iv_quad(
        y = "chg_log_pop_91_60", data = d,
        endog = main_treatment,
        lp_instr = main_lp_instrument,
        hypo_instr = main_hypo_instrument,
        ctrls_vec = ctrls_elow
    )
    rows[[length(rows) + 1L]] <- build_row(
        panel = "C", label = "Full sample (for reference)",
        fits = fits_main, endog = main_treatment
    )

    # ------------------------------------------------------------------
    # Panel D: controls ladder. Built in build_panel_d() below, both to
    # keep main() inside the 200-line limit and because the panel needs
    # its own sample and its own assertions. Read that function before
    # reading the panel: the rungs change the first stage, not only the
    # control set.
    # ------------------------------------------------------------------
    D_out <- build_panel_d(d, dir_tables = dir_tables)
    rows_D <- D_out$rows
    n_D <- D_out$n_obs
    rows <- c(rows, rows_D)

    # ----------------------------------------------------------------------
    # Assemble data frame and print summary
    # ----------------------------------------------------------------------
    df <- do.call(rbind, rows)

    message("\n[t12] Robustness: coefficient on ΔlogMA across variations")
    message(sprintf("%-1s  %-35s  %-13s %-13s %-13s %-13s  %-5s",
                    "P", "Variation", "OLS", "IV-LP", "IV-Hypo",
                    "IV-Both", "N"))
    for (i in seq_len(nrow(df))) {
        r <- df[i, ]
        message(sprintf("%-1s  %-35s  %s %s %s %s  %-5d",
                        r$panel, r$label,
                        fmt(r$ols_est,    r$ols_se,    r$ols_p),
                        fmt(r$iv_lp_est,  r$iv_lp_se,  r$iv_lp_p),
                        fmt(r$iv_h_est,   r$iv_h_se,   r$iv_h_p),
                        fmt(r$iv_b_est,   r$iv_b_se,   r$iv_b_p),
                        r$n_obs))
    }

    # ------------------------------------------------------------------
    # LaTeX output. Extracted to write_tex() below: with Panel D added,
    # main() ran past the 200-line limit in the project standards.
    # ------------------------------------------------------------------
    write_tex(df, n_sub = n_sub, n_D = n_D, theta = theta,
              dir_tables = dir_tables)

    out_csv <- file.path(dir_tables, "table_12_robustness.csv")
    write.csv(df, out_csv, row.names = FALSE)
    message("Saved: ", out_csv)
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# --------------------------------------------------------------------------
# build_panel_d(): the controls-ladder rows of Panel D, with the panel's
# own complete-case sample and its own assertions. Returns the row list
# and the shared N. Split out of main() for the 200-line limit
# (cr-review PR #163).
# --------------------------------------------------------------------------
build_panel_d <- function(d, dir_tables) {
    # ----------------------------------------------------------------------
    # Panel D: controls ladder (coauthor request, 2026-09). Adds the
    # control blocks one at a time so the sensitivity of the headline
    # estimate to the control set is on the page.
    #
    # READ THIS BEFORE READING THE PANEL. The rungs are NOT the same
    # estimator with fewer covariates; controls_ladder() in _iv_helpers.R
    # documents why. The short version is that the baseline log-MA term is
    # part of the identification, so removing it changes the first stage.
    # In this panel that is the largest single movement: the
    # hypothetical-road instrument's robust F is about 26 at rung (2) and
    # about 4 at rung (3), while the Larkin Plan instrument's holds in the
    # low twenties throughout. Figures are approximate on purpose -- the
    # exact values move with theta, which changed from 4.55 to 4.14 this
    # cycle, and the CSV is the place to read them.
    #
    # WHAT THAT DOES AND DOES NOT ESTABLISH. It locates the weakness of
    # the hypothetical-road instrument in its relationship with baseline
    # market access. Two readings are available and this code does not
    # choose between them: that the weakness is an artifact of
    # conditioning, or that the instrument genuinely has little variation
    # orthogonal to baseline MA, which would be a property of the
    # instrument. Section 5.1's characterization and the open question
    # about an LP-only main specification both turn on which is right, so
    # it is a coauthor decision (cr-review PR #163). Every rung prints its
    # F so the reader can see the movement and judge.
    #
    # The rungs come from controls_ladder() in _iv_helpers.R so that this
    # panel and appendix Table B3 cannot drift apart.
    #
    # ONE SAMPLE ACROSS RUNGS, per table_7_pre_trends.R: complete cases on
    # the union of every rung's variables, so the panel isolates the
    # control set rather than the sample. The controls contribute no
    # missingness today and the rungs would share a sample anyway, but one
    # NA in a future control would silently turn this into a
    # sample-composition ladder. The row count is asserted constant below
    # and reported in the table note.
    # ----------------------------------------------------------------------
    ladder <- controls_ladder(geo_controls_main)
    y_D <- "chg_log_pop_91_60"
    all_v_D <- unique(c(y_D, main_treatment, main_lp_instrument,
                        main_hypo_instrument,
                        setdiff(unlist(lapply(ladder, `[[`, "ctrls")), "1")))
    d_D <- as.data.frame(d)[complete.cases(as.data.frame(d)[, all_v_D]), ]
    rows_D <- list()
    for (rung in ladder) {
        fits_D <- fit_iv_quad(
            y = y_D, data = d_D,
            endog = main_treatment,
            lp_instr = main_lp_instrument,
            hypo_instr = main_hypo_instrument,
            ctrls_vec = rung$ctrls
        )
        rows_D[[length(rows_D) + 1L]] <- build_row(
            panel = "D", label = rung$label,
            fits = fits_D, endog = main_treatment
        )
    }
    D <- do.call(rbind, rows_D)
    n_D <- unique(D$n_obs)
    # Every cell must compute. safe_coef() returns NA when the coefficient
    # name is absent, all.equal(NA, NA) is TRUE, and tex_cell() renders NA
    # as a blank -- so without this the top-rung check below could pass on
    # a panel of empty cells (cr-review PR #163). Assert presence first.
    stopifnot(
        "Panel D must have one row per rung" = nrow(D) == length(ladder),
        "Panel D rungs must share one sample" = length(n_D) == 1L,
        "Panel D estimates must all compute" =
            !any(is.na(c(D$ols_est, D$iv_lp_est, D$iv_h_est, D$iv_b_est))),
        "Panel D first-stage F must all compute" =
            !any(is.na(c(D$iv_lp_F, D$iv_h_F, D$iv_b_F)))
    )
    # The top rung IS the specification Table 9 reports, so assert it
    # against Table 9's OWN CSV rather than against a refit here. A refit
    # with the same control vector on the same data is the same call to a
    # deterministic function: it cannot disagree, and asserting it while
    # the table note tells the reader the published table was reproduced
    # is a tautology dressed as an external check (cr-review PR #163).
    # Explicit 1e-10, and the SE too: all.equal's default 1.5e-8 relative
    # tolerance is not "machine precision", and a vcov change could move
    # the SE while leaving the point estimate untouched.
    t9_csv <- read.csv(file.path(dir_tables, "table_9_population_iv.csv"),
                       stringsAsFactors = FALSE)
    top <- D[D$label == ladder[[length(ladder)]]$label, ]
    for (pair in list(c("OLS", "ols"), c("IV-LP", "iv_lp"),
                      c("IV-H", "iv_h"), c("IV-B", "iv_b"))) {
        ref <- t9_csv[t9_csv$outcome == y_D & t9_csv$spec == pair[1], ]
        stopifnot(
            "Table 9 must carry this outcome and spec" = nrow(ref) == 1L,
            "Panel D top rung must reproduce Table 9's estimate" =
                abs(top[[paste0(pair[2], "_est")]] - ref$estimate) < 1e-10,
            "Panel D top rung must reproduce Table 9's SE" =
                abs(top[[paste0(pair[2], "_se")]] - ref$std_err) < 1e-10
        )
    }
    list(rows = rows_D, n_obs = n_D)
}

# --------------------------------------------------------------------------
# write_tex(): emit the Table 12 LaTeX. Split out of main() when Panel D
# pushed it over the 200-line function limit (cr-review PR #163). Takes
# what it needs explicitly rather than reaching for config.R globals,
# which are local to main() in this project's pattern.
# --------------------------------------------------------------------------
write_tex <- function(df, n_sub, n_D, theta, dir_tables) {
    # ----------------------------------------------------------------------
    # LaTeX output: one table with panel headers
    # ----------------------------------------------------------------------
    tex_lines <- c(
        "% Table 12: Robustness of the main population result (chg_log_pop_91_60).",
        "% Generated by code/analysis/table_12_robustness.R.",
        "%",
        sprintf("%% Panel A: alternative trade elasticity theta = %s (main = %s).",
                format(theta[["high"]]), format(theta[["low"]])),
        "% Panel B: alternative hypothetical-road instruments.",
        "% Panel C: subsample stability (placebo subset; N computed).",
        "% Panel D: controls ladder; each IV cell carries its rung's",
        "%          robust first-stage F, because the rungs change the",
        "%          instrument as well as the control set.",
        "%",
        "% Columns (1)-(4) are OLS / IV-LP / IV-Hypo / IV-Both. Panels A-C",
        "% include baseline log MA, baseline log pop, and the six",
        "% standardized geographic controls; Panel D varies that set by",
        "% construction. Robust (HC1) standard errors throughout.",
        "",
        "\\begin{table}[htbp]",
        "\\centering",
        "\\caption{Robustness of the main population elasticity (outcome:",
        "$\\Delta \\ln \\mathrm{Pop}_{1960 \\to 1991}$)}",
        "\\label{tab:robustness}",
        "\\small",
        "\\begin{tabular}{llcccc}",
        "\\toprule",
        " & Variation & (1) OLS & (2) IV-LP & (3) IV-Hypo & (4) IV-Both \\\\",
        "\\midrule",
        "\\multicolumn{6}{l}{\\textit{Panel A: alternative trade elasticity}} \\\\"
    )

    for (i in seq_len(nrow(df))) {
        r <- df[i, ]
        if (r$panel == "B" && i > 1 && df$panel[i - 1] == "A") {
            tex_lines <- c(tex_lines,
                "\\midrule",
                "\\multicolumn{6}{l}{\\textit{Panel B: alternative hypothetical-road instruments}} \\\\"
            )
        }
        if (r$panel == "C" && i > 1 && df$panel[i - 1] == "B") {
            tex_lines <- c(tex_lines,
                "\\midrule",
                "\\multicolumn{6}{l}{\\textit{Panel C: sample robustness}} \\\\"
            )
        }
        if (r$panel == "D" && i > 1 && df$panel[i - 1] == "C") {
            tex_lines <- c(tex_lines,
                "\\midrule",
                paste0("\\multicolumn{6}{l}{\\textit{Panel D: controls ",
                       "ladder (first-stage $F$ in brackets)}} \\\\")
            )
        }
        # Panel D carries a third line per IV cell holding the first-stage
        # F, because the rungs change the instrument as well as the control
        # set. See the block comment on Panel D above. if/else rather than
        # a `next`, so that any per-row logic added at the end of this loop
        # later applies to Panel D too (cr-review PR #163).
        if (r$panel == "D") {
            tex_lines <- c(tex_lines,
                sprintf("%s & %s & %s & %s & %s & %s \\\\",
                        r$panel,
                        r$label,
                        tex_cell(r$ols_est, r$ols_se, r$ols_p),
                        tex_cell_F(r$iv_lp_est, r$iv_lp_se, r$iv_lp_p, r$iv_lp_F),
                        tex_cell_F(r$iv_h_est,  r$iv_h_se,  r$iv_h_p,  r$iv_h_F),
                        tex_cell_F(r$iv_b_est,  r$iv_b_se,  r$iv_b_p,  r$iv_b_F))
            )
        } else {
            tex_lines <- c(tex_lines,
                sprintf("%s & %s & %s & %s & %s & %s \\\\",
                        r$panel,
                        r$label,
                        tex_cell(r$ols_est,   r$ols_se,   r$ols_p),
                        tex_cell(r$iv_lp_est, r$iv_lp_se, r$iv_lp_p),
                        tex_cell(r$iv_h_est,  r$iv_h_se,  r$iv_h_p),
                        tex_cell(r$iv_b_est,  r$iv_b_se,  r$iv_b_p))
            )
        }
    }

    tex_lines <- c(tex_lines,
        "\\bottomrule",
        "\\end{tabular}",
        "",
        "\\footnotesize",
        paste0("\\emph{Notes}: All regressions use ",
               "$\\Delta \\ln(\\mathrm{Pop}_{1991}/\\mathrm{Pop}_{1960})$ ",
               "as the outcome. Panel~A swaps the MA elasticity from ",
               sprintf("$\\theta = %s$ to $\\theta = %s$ (all MA variables, ",
                       format(theta[["low"]]), format(theta[["high"]])),
               "treatment, instruments, and baseline control, switch to ",
               "\\texttt{\\_ehigh} variants). Panel~B holds $\\theta$ at ",
               sprintf("%s and replaces the LCP-MST hypothetical-road ",
                       format(theta[["low"]])),
               "instrument with one of three alternatives. Panel~C ",
               sprintf("refits the main specification on the %d-district ",
                       n_sub),
               "subset for which the 1947 placebo outcome is defined. ",
               "Panel~D adds the control blocks one at a time, ending at ",
               "the specification used in Table~\\ref{tab:population_iv}, ",
               sprintf("on one common sample of %d districts so that ", n_D),
               "differences across rungs come from the control set and ",
               "not from sample composition; the top rung reproduces ",
               "Table~\\ref{tab:population_iv} exactly, which the ",
               "generating script asserts against that table's own ",
               "output. The bracketed figure in each instrumented cell is ",
               "that rung's heteroskedasticity-robust first-stage $F$. ",
               "The rungs are not the same estimator with fewer ",
               "covariates: baseline log market access is the standard ",
               "convergence control, separating the effect of changing ",
               "market access from the level at which a district started, ",
               "so removing it changes the first stage and not only the ",
               "second. Movement between rungs~(2) and~(3) therefore ",
               "mixes sensitivity to the control set with a change in ",
               "instrument strength, which is why the $F$ is shown on ",
               "every rung. ",
               "Robust (HC1) SE. Significance: ",
               "$^{*}p<0.10,\\;^{**}p<0.05,\\;^{***}p<0.01$."),
        "\\end{table}"
    )

    out_tex <- file.path(dir_tables, "table_12_robustness.tex")
    writeLines(tex_lines, out_tex)
    message("\nSaved: ", out_tex)
}

# tex_cell_F() now lives in _table_helpers.R, shared with appendix Table
# B3 (cr-review PR #163). Panel D takes the stacked default.

# Build one row of the results data frame from a fit_iv_quad() output
build_row <- function(panel, label, fits, endog) {
    co_ols <- safe_coef(fits[["OLS"]],   endog)
    co_lp  <- safe_coef(fits[["IV-LP"]], paste0("fit_", endog))
    co_h   <- safe_coef(fits[["IV-H"]],  paste0("fit_", endog))
    co_b   <- safe_coef(fits[["IV-B"]],  paste0("fit_", endog))

    data.frame(
        panel        = panel,
        label        = label,
        ols_est      = co_ols$est, ols_se   = co_ols$se,
        ols_p        = co_ols$p,
        iv_lp_est    = co_lp$est,  iv_lp_se = co_lp$se,
        iv_lp_p      = co_lp$p,
        iv_h_est     = co_h$est,   iv_h_se  = co_h$se,
        iv_h_p       = co_h$p,
        iv_b_est     = co_b$est,   iv_b_se  = co_b$se,
        iv_b_p       = co_b$p,
        iv_lp_F      = fitstat_F_robust(fits[["IV-LP"]]),
        iv_h_F       = fitstat_F_robust(fits[["IV-H"]]),
        iv_b_F       = fitstat_F_robust(fits[["IV-B"]]),
        n_obs        = nobs(fits[["OLS"]]),
        stringsAsFactors = FALSE
    )
}

main()

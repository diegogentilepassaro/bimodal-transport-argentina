# ===========================================================================
# table_b3_sectoral_ladder.R
#
# PURPOSE: Appendix Table B3 — the controls ladder of Table 12 Panel D,
#          run on the five sectoral outcomes of Table 10 instead of on
#          total population. Added on the coauthor's request (2026-09).
#
# The rungs come from controls_ladder() in _iv_helpers.R, so this table
# and Table 12 Panel D cannot drift apart. Read that function's comment
# before reading this table: the rungs change the first stage as well as
# the control set, which is why every cell carries its rung's robust F.
#
# WHY A SEPARATE SCRIPT rather than an extra output of
# table_10_sectoral.R, which is the pattern table_7_pre_trends.R uses for
# Table B2: table_10 builds its panels through modelsummary with per-cell
# AR sets, effective F and overidentification rows. A hand-built ladder
# grafted onto that pipeline would be harder to follow than a small
# standalone script, and the shared rung definition already prevents the
# duplication that mattered.
#
# COLUMNS: OLS / IV-LP / IV-Hypo / IV-Both, as in Tables 10 and 12.
# SE: heteroskedasticity-robust (HC1) throughout.
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/table_b3_sectoral_ladder.{tex,csv}
# ===========================================================================
suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
})
main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_table_helpers.R"), echo = FALSE)
    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)

    d <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet")
    )

    # Same five outcomes, same two panels, same labels as Table 10. Kept
    # in this order so the appendix table reads against the main one row
    # for row.
    outcomes <- list(
        list(var = "chg_log_nestab_85_54",
             label = "$\\Delta \\ln$ establishments",
             panel = "A"),
        list(var = "chg_log_valprod_85_54",
             label = "$\\Delta \\ln$ value of production",
             panel = "A"),
        list(var = "chg_log_massal_85_54",
             label = "$\\Delta \\ln$ wage mass",
             panel = "A"),
        list(var = "chg_log_nexp_88_60",
             label = "$\\Delta \\ln$ farms",
             panel = "B"),
        list(var = "chg_log_areatot_ha_88_60",
             label = "$\\Delta \\ln$ area farmed",
             panel = "B")
    )
    panel_titles <- c(
        A = "Panel A: manufacturing (industrial census 1954--1985)",
        B = "Panel B: agriculture (agricultural census 1960--1988)"
    )

    ladder <- controls_ladder(geo_controls_main)

    rows <- list()
    for (out in outcomes) {
        fits_top <- NULL
        for (rung in ladder) {
            fits <- fit_iv_quad(
                y = out$var, data = d,
                endog = main_treatment,
                lp_instr = main_lp_instrument,
                hypo_instr = main_hypo_instrument,
                ctrls_vec = rung$ctrls
            )
            co_ols <- safe_coef(fits[["OLS"]],   main_treatment)
            co_lp  <- safe_coef(fits[["IV-LP"]], paste0("fit_", main_treatment))
            co_h   <- safe_coef(fits[["IV-H"]],  paste0("fit_", main_treatment))
            co_b   <- safe_coef(fits[["IV-B"]],  paste0("fit_", main_treatment))
            rows[[length(rows) + 1L]] <- data.frame(
                panel     = out$panel,
                outcome   = out$var,
                label     = out$label,
                rung      = rung$label,
                ols_est   = co_ols$est, ols_se = co_ols$se, ols_p = co_ols$p,
                iv_lp_est = co_lp$est,  iv_lp_se = co_lp$se, iv_lp_p = co_lp$p,
                iv_h_est  = co_h$est,   iv_h_se = co_h$se,   iv_h_p = co_h$p,
                iv_b_est  = co_b$est,   iv_b_se = co_b$se,   iv_b_p = co_b$p,
                iv_lp_F   = fitstat_F_robust(fits[["IV-LP"]]),
                iv_h_F    = fitstat_F_robust(fits[["IV-H"]]),
                iv_b_F    = fitstat_F_robust(fits[["IV-B"]]),
                n_obs     = nobs(fits[["OLS"]]),
                stringsAsFactors = FALSE
            )
            fits_top <- fits
        }
        # The top rung is the main specification, so it must reproduce
        # Table 10 for this outcome. Asserting it here is what makes the
        # lower rungs interpretable: if rung (4) drifted, the ladder would
        # be measuring something other than the paper's spec.
        ref <- fit_iv_quad(
            y = out$var, data = d,
            endog = main_treatment,
            lp_instr = main_lp_instrument,
            hypo_instr = main_hypo_instrument,
            ctrls_vec = geo_controls_main
        )
        for (key in c("OLS", "IV-LP", "IV-H", "IV-B")) {
            nm <- if (key == "OLS") main_treatment else
                paste0("fit_", main_treatment)
            stopifnot(
                "B3 top rung must reproduce the Table 10 specification" =
                    isTRUE(all.equal(safe_coef(fits_top[[key]], nm)$est,
                                     safe_coef(ref[[key]], nm)$est))
            )
        }
    }
    df <- do.call(rbind, rows)

    # ----- Console summary -----
    message("\n[B3] Sectoral controls ladder — coefficient on ",
            "\u0394log MA (F in brackets)")
    message(sprintf("%-1s %-26s %-30s %-13s %-13s %-13s %-5s",
                    "P", "Outcome", "Rung", "OLS", "IV-LP", "IV-Both", "N"))
    for (i in seq_len(nrow(df))) {
        r <- df[i, ]
        message(sprintf("%-1s %-26s %-30s %s %s %s %-5d",
                        r$panel, sub("\\$\\\\Delta \\\\ln\\$ ", "", r$label),
                        r$rung,
                        fmt(r$ols_est,   r$ols_se,   r$ols_p),
                        fmt(r$iv_lp_est, r$iv_lp_se, r$iv_lp_p),
                        fmt(r$iv_b_est,  r$iv_b_se,  r$iv_b_p),
                        r$n_obs))
    }

    # ----- LaTeX -----
    tex_lines <- c(
        "% Table B3: controls ladder on the sectoral outcomes.",
        "% Generated by code/analysis/table_b3_sectoral_ladder.R.",
        "%",
        "% One block per outcome; within a block the control set grows to",
        "% the main specification. Bracketed figures are that rung's",
        "% robust first-stage F: the rungs change the first stage as well",
        "% as the control set. See controls_ladder() in _iv_helpers.R.",
        "",
        "\\begin{table}[htbp]",
        "\\centering",
        "\\caption{Controls ladder, sectoral outcomes}",
        "\\label{tab:sectoral_ladder}",
        "\\scriptsize",
        "\\begin{tabular}{llcccc}",
        "\\toprule",
        paste("Outcome & Controls & (1) OLS & (2) IV-LP & (3) IV-Hypo &",
              "(4) IV-Both \\\\"),
        "\\midrule"
    )
    last_panel <- ""
    last_outcome <- ""
    for (i in seq_len(nrow(df))) {
        r <- df[i, ]
        if (r$panel != last_panel) {
            if (last_panel != "") tex_lines <- c(tex_lines, "\\midrule")
            tex_lines <- c(tex_lines, sprintf(
                "\\multicolumn{6}{l}{\\textit{%s}} \\\\",
                panel_titles[[r$panel]]))
            last_panel <- r$panel
            last_outcome <- ""
        }
        # The outcome label prints once per block; the rungs beneath it are
        # the same object under a growing control set, and repeating the
        # label on every line would suggest four different outcomes.
        if (r$outcome != last_outcome) {
            if (last_outcome != "") tex_lines <- c(tex_lines, "\\addlinespace")
            lab <- r$label
            last_outcome <- r$outcome
        } else {
            lab <- ""
        }
        tex_lines <- c(tex_lines, sprintf(
            "%s & %s & %s & %s & %s & %s \\\\",
            lab, r$rung,
            tex_cell(r$ols_est, r$ols_se, r$ols_p),
            tex_cell_F_b3(r$iv_lp_est, r$iv_lp_se, r$iv_lp_p, r$iv_lp_F),
            tex_cell_F_b3(r$iv_h_est,  r$iv_h_se,  r$iv_h_p,  r$iv_h_F),
            tex_cell_F_b3(r$iv_b_est,  r$iv_b_se,  r$iv_b_p,  r$iv_b_F)))
    }
    tex_lines <- c(tex_lines,
        "\\bottomrule",
        "\\end{tabular}",
        "",
        "\\footnotesize",
        paste0("\\emph{Notes}: The controls ladder of ",
               "Table~\\ref{tab:robustness} Panel~D, applied to the five ",
               "sectoral outcomes of Table~\\ref{tab:sectoral_iv}. Within ",
               "each block the control set grows from none to the ",
               "specification those tables use: the six standardized ",
               "geographic controls, then baseline log market access ",
               "(1960), then baseline log population (1960). The top rung ",
               "of every block reproduces Table~\\ref{tab:sectoral_iv} ",
               "exactly, which the generating script asserts. Bracketed ",
               "figures are that rung's heteroskedasticity-robust ",
               "first-stage $F$. As in Table~\\ref{tab:robustness} ",
               "Panel~D, whose note explains why, the rungs are not the ",
               "same estimator with fewer covariates and movement across ",
               "the market-access rung mixes sensitivity to the control ",
               "set with a change in instrument strength. ",
               "Robust (HC1) SE. Significance: ",
               "$^{*}p<0.10,\\;^{**}p<0.05,\\;^{***}p<0.01$."),
        "\\end{table}"
    )

    out_tex <- file.path(dir_tables, "table_b3_sectoral_ladder.tex")
    writeLines(tex_lines, out_tex)
    message("\nSaved: ", out_tex)
    out_csv <- file.path(dir_tables, "table_b3_sectoral_ladder.csv")
    write.csv(df, out_csv, row.names = FALSE)
    message("Saved: ", out_csv)
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# As table_12_robustness.R's tex_cell_F(), but two lines rather than three:
# the SE and the bracketed F share a line.
#
# WHY: with twenty data rows this table is four times the height of Table 12
# Panel D, and at three lines per cell the float ran past the bottom of the
# page. LaTeX dropped the tail of the notes silently -- no Overfull \vbox,
# no error, just a missing caveat sentence and a missing significance
# legend in the compiled PDF. Two lines per cell brings it back inside the
# page. Checked by reading the note's last sentence out of the PDF text
# layer, not by looking at the .tex.
tex_cell_F_b3 <- function(est, se, p, F_stat) {
    if (is.na(est)) return(" ")
    sprintf(
        paste0("\\begin{tabular}{@{}c@{}} %.3f%s \\\\ ",
               "(%.3f)~{\\tiny [%.1f]} \\end{tabular}"),
        est, star_str(p, tex = TRUE), se, F_stat
    )
}

main()

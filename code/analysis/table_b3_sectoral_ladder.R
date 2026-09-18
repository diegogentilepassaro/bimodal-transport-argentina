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

    # The same five outcomes as Table 10, in the same order and the same
    # two panels, so the appendix table reads against the main one row for
    # row. The DISPLAY LABELS differ: Table 10 disambiguates by sector
    # ("Mfg.\ establishments") because its panels sit in separate floats,
    # whereas here the panel heading already carries the sector and the
    # outcome column has room for the operator. The outcome set is asserted
    # against Table 10's CSV below, which is the part that would actually
    # break the table if it drifted; the labels are cosmetic and no
    # assertion can or should tie them together (cr-review PR #163).
    #
    # label_txt is the console-summary label. Carried explicitly rather
    # than stripped out of `label` with a regex, which printed raw LaTeX
    # the moment a label changed shape.
    outcomes <- list(
        list(var = "chg_log_nestab_85_54",
             label = "$\\Delta \\ln$ establishments",
             label_txt = "establishments", panel = "A"),
        list(var = "chg_log_valprod_85_54",
             label = "$\\Delta \\ln$ value of production",
             label_txt = "value of production", panel = "A"),
        list(var = "chg_log_massal_85_54",
             label = "$\\Delta \\ln$ wage mass",
             label_txt = "wage mass", panel = "A"),
        list(var = "chg_log_nexp_88_60",
             label = "$\\Delta \\ln$ farms",
             label_txt = "farms", panel = "B"),
        list(var = "chg_log_areatot_ha_88_60",
             label = "$\\Delta \\ln$ area farmed",
             label_txt = "area farmed", panel = "B")
    )
    panel_titles <- c(
        A = "Panel A: manufacturing (industrial census 1954--1985)",
        B = "Panel B: agriculture (agricultural census 1960--1988)"
    )

    ladder <- controls_ladder(geo_controls_main)

    # Unlike Table 12 Panel D this table cannot use one sample across the
    # whole exhibit: the five outcomes come from three different censuses
    # and genuinely cover different districts. The sample is held constant
    # WITHIN each outcome's block, over the union of every rung's
    # variables, so a block's rungs differ by control set alone; N is
    # asserted constant per block and printed in each block's rung labels.
    ctrl_union <- setdiff(unlist(lapply(ladder, `[[`, "ctrls")), "1")

    # tex_cell_F() from _table_helpers.R, un-stacked: twenty data rows at
    # three lines per cell pushed the float past the bottom of the page and
    # LaTeX dropped the tail of the notes silently.
    f_cell <- function(est, se, p, F_stat) {
        tex_cell_F(est, se, p, F_stat, stacked = FALSE, size = "tiny")
    }

    t10_csv <- read.csv(file.path(dir_tables, "table_10_sectoral_iv.csv"),
                        stringsAsFactors = FALSE)
    # Guard the one thing the hand-copied outcome list can get wrong in a
    # way that matters: which outcomes are covered. The display labels here
    # deliberately differ from Table 10's (see the comment on `outcomes`),
    # so they cannot be compared, but the outcome set must match or this
    # table is a ladder of a different exhibit (cr-review PR #163).
    stopifnot(
        "B3 must cover exactly Table 10's outcomes" =
            setequal(vapply(outcomes, `[[`, character(1), "var"),
                     unique(t10_csv$outcome))
    )

    rows <- list()
    for (out in outcomes) {
        # Complete cases over this outcome and every rung's controls, so
        # the block's rungs differ by control set and not by sample.
        v_out <- unique(c(out$var, main_treatment, main_lp_instrument,
                          main_hypo_instrument, ctrl_union))
        d_out <- as.data.frame(d)[complete.cases(as.data.frame(d)[, v_out]), ]
        block <- list()
        for (rung in ladder) {
            fits <- fit_iv_quad(
                y = out$var, data = d_out,
                endog = main_treatment,
                lp_instr = main_lp_instrument,
                hypo_instr = main_hypo_instrument,
                ctrls_vec = rung$ctrls
            )
            co_ols <- safe_coef(fits[["OLS"]],   main_treatment)
            co_lp  <- safe_coef(fits[["IV-LP"]], paste0("fit_", main_treatment))
            co_h   <- safe_coef(fits[["IV-H"]],  paste0("fit_", main_treatment))
            co_b   <- safe_coef(fits[["IV-B"]],  paste0("fit_", main_treatment))
            block[[length(block) + 1L]] <- data.frame(
                panel     = out$panel,
                outcome   = out$var,
                label     = out$label,
                label_txt = out$label_txt,
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
        }
        B <- do.call(rbind, block)
        # Every cell must compute before anything is compared. safe_coef()
        # returns NA for an absent coefficient name, all.equal(NA, NA) is
        # TRUE, and tex_cell() renders NA as a blank, so without this the
        # check below could pass on a block of empty cells.
        stopifnot(
            "each block must have one row per rung" =
                nrow(B) == length(ladder),
            "a block's rungs must share one sample" =
                length(unique(B$n_obs)) == 1L,
            "every estimate in the block must compute" =
                !any(is.na(c(B$ols_est, B$iv_lp_est, B$iv_h_est, B$iv_b_est))),
            "every first-stage F in the block must compute" =
                !any(is.na(c(B$iv_lp_F, B$iv_h_F, B$iv_b_F)))
        )
        # The top rung IS the specification Table 10 reports, so assert it
        # against Table 10's OWN output. Refitting with the same control
        # vector on the same data and comparing would be the same call to a
        # deterministic function twice: it cannot disagree, so it would
        # verify nothing while the note told the reader otherwise
        # (cr-review PR #163). Explicit 1e-10 on estimate AND SE:
        # all.equal's default 1.5e-8 relative tolerance is not machine
        # precision, and a vcov change could move the SE alone.
        top <- B[B$rung == ladder[[length(ladder)]]$label, ]
        for (pair in list(c("OLS", "ols"), c("IV-LP", "iv_lp"),
                          c("IV-H", "iv_h"), c("IV-B", "iv_b"))) {
            ref <- t10_csv[t10_csv$outcome == out$var &
                               t10_csv$spec == pair[1], ]
            stopifnot(
                "Table 10 must carry this outcome and spec" = nrow(ref) == 1L,
                "B3 top rung must reproduce Table 10's estimate" =
                    abs(top[[paste0(pair[2], "_est")]] - ref$estimate) < 1e-10,
                "B3 top rung must reproduce Table 10's SE" =
                    abs(top[[paste0(pair[2], "_se")]] - ref$std_err) < 1e-10
            )
        }
        rows <- c(rows, block)
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
                        r$panel, r$label_txt,
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
            # N under the label in a nested tabular, not beside it: side by
            # side the column takes the width of both and the tabular ran
            # 11.9pt over the text block.
            lab <- sprintf(
                paste0("\\begin{tabular}{@{}l@{}} %s \\\\ ",
                       "{\\tiny ($N=%d$)} \\end{tabular}"),
                r$label, r$n_obs)
            last_outcome <- r$outcome
        } else {
            lab <- ""
        }
        tex_lines <- c(tex_lines, sprintf(
            "%s & %s & %s & %s & %s & %s \\\\",
            lab, r$rung,
            tex_cell(r$ols_est, r$ols_se, r$ols_p),
            f_cell(r$iv_lp_est, r$iv_lp_se, r$iv_lp_p, r$iv_lp_F),
            f_cell(r$iv_h_est,  r$iv_h_se,  r$iv_h_p,  r$iv_h_F),
            f_cell(r$iv_b_est,  r$iv_b_se,  r$iv_b_p,  r$iv_b_F)))
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
               "exactly, which the generating script asserts against that ",
               "table's own output. Within a block the sample is held ",
               "fixed, so the rungs differ by control set alone; it ",
               "differs across blocks because the sectoral censuses do ",
               "not cover the same districts, and each block's $N$ is ",
               "given beside its outcome. Bracketed ",
               "figures are that rung's heteroskedasticity-robust ",
               "first-stage $F$. As in Table~\\ref{tab:robustness} ",
               "Panel~D, whose note explains why, the rungs are not the ",
               "same estimator with fewer covariates: baseline log market ",
               "access is the standard convergence control, separating ",
               "the effect of changing market access from the level at ",
               "which a district started, so movement across that rung ",
               "mixes sensitivity to the control set with a change in ",
               "instrument strength. ",
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

main()

# ===========================================================================
# table_14_mechanisms.R
#
# PURPOSE: Paper Table 14 — local-infrastructure mechanism check. The
#          headline IV-Both elasticity (Table 9) treats market access
#          as a single regional channel: the change in MA captures the
#          aggregate effect of all infrastructure changes on the
#          district. This table asks: how much of the population
#          response runs through global MA changes vs. local
#          infrastructure presence?
#
# DESIGN: progressive-add specification. Regress Δlog pop on
#         Δlog MA and successively add district-level local
#         infrastructure changes (Z_i) as controls. Track β on
#         Δlog MA across columns.
#
#   (1) Baseline: Δlog MA only + standard controls (= Table 9 IV-Both,
#       full sample).
#   (2) Add chg_tot_rails_86_60 and chg_pav_and_grav_86_54 (Δkm of
#       rail and road in the district itself).
#   (3) Add lost_all_rails_86 (district had rails in 1960, none in
#       1986) and gained_first_road_86 (district had no paved/gravel
#       roads in 1954, has some in 1986). These are the binary
#       'extreme' margins.
#   (4) All Z_i jointly.
#
#   Z_i variables not built here (need additional raw data; flagged
#   in build_estimation_sample.R header):
#     - gained/lost national highway
#     - gained/lost railway station
#     - lost railway depot
#
# OUTCOME: total population log change (Δlog pop, 1960 → 1991). Same
#          headline outcome as Table 9 Panel A.
#
# IV: Δlog MA is instrumented as in Table 9 (IV-Both: LP and Hypo).
#     Z_i are NOT instrumented — they are controls representing
#     direct exposure to the local infrastructure shock. This is
#     the standard 'mechanism' specification (e.g., Donaldson and
#     Hornbeck 2016, Section 4.4).
#
# READING:
#   - If β on Δlog MA is similar in (1) and (4), the population
#     response runs through MA-style regional connectivity, not
#     through local infrastructure presence per se.
#   - If β shrinks substantially when Z_i is added, local
#     infrastructure effects explain part of the headline.
#   - If the Z_i coefficients are themselves significant, they
#     enter the regression with their own sign and magnitude on top
#     of the MA channel.
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/table_14_mechanisms.{tex,csv}
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

    y      <- "chg_log_pop_91_60"
    endog  <- "chg_logMA_86_60_s0_elow"
    lp     <- "chg_logMA_stu_s0_elow"
    hypo   <- main_hypo_instrument
    ctrls  <- paste(geo_controls_main, collapse = " + ")

    # Specifications: each adds a different Z_i bundle on top of standard controls
    specs <- list(
        list(id = "(1)", label = "Baseline (Table 9 spec)",
             extra = ""),
        list(id = "(2)", label = "+ Δrail km, Δroad km",
             extra = " + chg_tot_rails_86_60 + chg_pav_and_grav_86_54"),
        list(id = "(3)", label = "+ Lost-all-rails, Gained-first-road",
             extra = " + lost_all_rails_86 + gained_first_road_86"),
        list(id = "(4)", label = "All local Z_i jointly",
             extra = paste0(" + chg_tot_rails_86_60 + chg_pav_and_grav_86_54",
                            " + lost_all_rails_86 + gained_first_road_86"))
    )

    rows <- list()
    fits <- list()

    for (s in specs) {
        f_iv <- as.formula(sprintf(
            "%s ~ %s%s | %s ~ %s + %s",
            y, ctrls, s$extra, endog, lp, hypo))
        m_iv <- feols(f_iv, data = d, vcov = "hetero")

        fits[[s$id]] <- m_iv
        co <- safe_coef(m_iv, paste0("fit_", endog))
        fs <- fitstat_F(m_iv)

        # Pull Z_i coefficients (NA when not in this spec)
        co_chg_rail  <- safe_coef(m_iv, "chg_tot_rails_86_60")
        co_chg_road  <- safe_coef(m_iv, "chg_pav_and_grav_86_54")
        co_lost_rail <- safe_coef(m_iv, "lost_all_rails_86")
        co_gain_road <- safe_coef(m_iv, "gained_first_road_86")

        rows[[length(rows) + 1L]] <- data.frame(
            spec_id    = s$id,
            spec_label = s$label,
            ma_est     = co$est, ma_se = co$se, ma_p = co$p,
            ma_F       = fs,
            chg_rail_est  = co_chg_rail$est,  chg_rail_se  = co_chg_rail$se,
            chg_road_est  = co_chg_road$est,  chg_road_se  = co_chg_road$se,
            lost_rail_est = co_lost_rail$est, lost_rail_se = co_lost_rail$se,
            gain_road_est = co_gain_road$est, gain_road_se = co_gain_road$se,
            n_obs      = nobs(m_iv),
            stringsAsFactors = FALSE
        )
    }
    # Common-sample baseline: spec (1) re-estimated on the 306 districts
    # with non-missing kilometer measures, so the (1)-vs-(4) comparison
    # can be separated from the 3-district sample change. CSV-only row
    # (the tex table keeps its four columns); cited in Section 7.1.
    d_cs <- d[!is.na(d$chg_tot_rails_86_60) &
              !is.na(d$chg_pav_and_grav_86_54), ]
    f_cs <- as.formula(sprintf("%s ~ %s | %s ~ %s + %s",
                               y, ctrls, endog, lp, hypo))
    m_cs <- feols(f_cs, data = d_cs, vcov = "hetero")
    co_cs <- safe_coef(m_cs, paste0("fit_", endog))
    rows[[length(rows) + 1L]] <- data.frame(
        spec_id    = "(1cs)",
        spec_label = "Baseline, common km-measure sample (CSV only)",
        ma_est     = co_cs$est, ma_se = co_cs$se, ma_p = co_cs$p,
        ma_F       = fitstat_F(m_cs),
        chg_rail_est  = NA_real_, chg_rail_se  = NA_real_,
        chg_road_est  = NA_real_, chg_road_se  = NA_real_,
        lost_rail_est = NA_real_, lost_rail_se = NA_real_,
        gain_road_est = NA_real_, gain_road_se = NA_real_,
        n_obs      = nobs(m_cs),
        stringsAsFactors = FALSE
    )

    df <- do.call(rbind, rows)

    # ----- Console summary -----
    message("\n[t14] Mechanism: β on Δlog MA across progressive-add specs")
    for (i in seq_len(nrow(df))) {
        r <- df[i, ]
        message(sprintf("%s  %-40s  β=%s  F=%5.1f  N=%d",
                        r$spec_id, r$spec_label,
                        fmt(r$ma_est, r$ma_se, r$ma_p),
                        r$ma_F, r$n_obs))
    }

    # ----- LaTeX output -----
    tex_lines <- c(
        "% Table 14: Mechanisms — local infrastructure controls.",
        "% Generated by code/analysis/table_14_mechanisms.R.",
        "%",
        "% Outcome: chg_log_pop_91_60 (total population log change 1960-1991)",
        "% Treatment: Δlog MA^full, instrumented by both LP and hypo (IV-Both)",
        "% Z_i: progressively added local infrastructure controls (NOT instrumented)",
        "%",
        "% All specifications include baseline log MA (1960), baseline log pop",
        "% (1960), and the six standardized geographic controls.",
        "% Robust (HC1) SE.",
        "",
        "\\begin{table}[htbp]",
        "\\centering",
        "\\caption{Local-infrastructure mechanism: progressive-add of $Z_i$ to the headline IV-Both regression}",
        "\\label{tab:mechanisms}",
        "\\small",
        "\\begin{tabular}{lcccc}",
        "\\toprule",
        " & (1) & (2) & (3) & (4) \\\\",
        " & Baseline & + $\\Delta$km rail/road & + binary margins & all $Z_i$ \\\\",
        "\\midrule",
        sprintf("$\\Delta \\ln \\mathrm{MA}^{\\mathrm{full}}$ & %s & %s & %s & %s \\\\",
                tex_cell(df$ma_est[1], df$ma_se[1], df$ma_p[1]),
                tex_cell(df$ma_est[2], df$ma_se[2], df$ma_p[2]),
                tex_cell(df$ma_est[3], df$ma_se[3], df$ma_p[3]),
                tex_cell(df$ma_est[4], df$ma_se[4], df$ma_p[4])),
        "\\midrule",
        "\\multicolumn{5}{l}{\\textit{Local infrastructure $Z_i$:}} \\\\",
        sprintf("$\\Delta$ rail km (1960-1986) & %s & %s & %s & %s \\\\",
                tex_cell_or_blank(df$chg_rail_est[1], df$chg_rail_se[1]),
                tex_cell_or_blank(df$chg_rail_est[2], df$chg_rail_se[2]),
                tex_cell_or_blank(df$chg_rail_est[3], df$chg_rail_se[3]),
                tex_cell_or_blank(df$chg_rail_est[4], df$chg_rail_se[4])),
        sprintf("$\\Delta$ road km (1954-1986) & %s & %s & %s & %s \\\\",
                tex_cell_or_blank(df$chg_road_est[1], df$chg_road_se[1]),
                tex_cell_or_blank(df$chg_road_est[2], df$chg_road_se[2]),
                tex_cell_or_blank(df$chg_road_est[3], df$chg_road_se[3]),
                tex_cell_or_blank(df$chg_road_est[4], df$chg_road_se[4])),
        sprintf("Lost all rails by 1986 & %s & %s & %s & %s \\\\",
                tex_cell_or_blank(df$lost_rail_est[1], df$lost_rail_se[1]),
                tex_cell_or_blank(df$lost_rail_est[2], df$lost_rail_se[2]),
                tex_cell_or_blank(df$lost_rail_est[3], df$lost_rail_se[3]),
                tex_cell_or_blank(df$lost_rail_est[4], df$lost_rail_se[4])),
        sprintf("Gained first paved or gravel road & %s & %s & %s & %s \\\\",
                tex_cell_or_blank(df$gain_road_est[1], df$gain_road_se[1]),
                tex_cell_or_blank(df$gain_road_est[2], df$gain_road_se[2]),
                tex_cell_or_blank(df$gain_road_est[3], df$gain_road_se[3]),
                tex_cell_or_blank(df$gain_road_est[4], df$gain_road_se[4])),
        "\\midrule",
        sprintf("First-stage $F$ & %.1f & %.1f & %.1f & %.1f \\\\",
                df$ma_F[1], df$ma_F[2], df$ma_F[3], df$ma_F[4]),
        sprintf("Observations & %d & %d & %d & %d \\\\",
                df$n_obs[1], df$n_obs[2], df$n_obs[3], df$n_obs[4]),
        "\\bottomrule",
        "\\end{tabular}",
        "",
        "\\footnotesize",
        paste0("\\emph{Notes}: Each column is one IV regression of ",
               "$\\Delta \\ln(\\mathrm{Pop}_{1991}/\\mathrm{Pop}_{1960})$ on ",
               "$\\Delta \\ln \\mathrm{MA}^{\\mathrm{full}}_i$ (instrumented by ",
               "both the Larkin Plan and the LCP-MST hypothetical-road instruments) ",
               "plus the local infrastructure controls listed. Local controls ",
               "are not instrumented. Standard controls (baseline log MA, baseline ",
               "log pop, and the six standardized geographic controls) are ",
               "partialled out and not displayed. Robust (HC1) standard errors. ",
               "$^{*}p<0.10,\\;^{**}p<0.05,\\;^{***}p<0.01$."),
        "\\end{table}"
    )

    out_tex <- file.path(dir_tables, "table_14_mechanisms.tex")
    writeLines(tex_lines, out_tex)
    message("\nSaved: ", out_tex)

    out_csv <- file.path(dir_tables, "table_14_mechanisms.csv")
    write.csv(df, out_csv, row.names = FALSE)
    message("Saved: ", out_csv)
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
fmt <- function(est, se, p) {
    if (is.na(est)) return("    NA       ")
    stars <- ifelse(p < 0.01, "***",
            ifelse(p < 0.05, "**",
            ifelse(p < 0.10, "*", "")))
    sprintf("%+6.3f%-3s(%.3f)", est, stars, se)
}

tex_cell <- function(est, se, p) {
    if (is.na(est)) return(" ")
    stars <- ifelse(p < 0.01, "$^{***}$",
            ifelse(p < 0.05, "$^{**}$",
            ifelse(p < 0.10, "$^{*}$", "")))
    sprintf(
        "\\begin{tabular}{@{}c@{}} %.3f%s \\\\ (%.3f) \\end{tabular}",
        est, stars, se
    )
}

tex_cell_or_blank <- function(est, se) {
    if (is.na(est)) return(" ")
    # No stars row in this view (we display SE only; user looks at p in the CSV)
    sprintf(
        "\\begin{tabular}{@{}c@{}} %.4f \\\\ (%.4f) \\end{tabular}",
        est, se
    )
}

main()

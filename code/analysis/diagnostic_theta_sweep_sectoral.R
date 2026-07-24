# ===========================================================================
# diagnostic_theta_sweep_sectoral.R
#
# PURPOSE: Is the QUALITATIVE pattern (manufacturing responds, agriculture
#          does not) robust across theta? The population theta sweep
#          (diagnostic_theta_sweep.R) showed the population BETA LEVEL is
#          highly theta-sensitive. This script extends the same re-weighting
#          to the sectoral outcomes, so we can read whether the sign /
#          significance pattern across sectors holds across the whole theta
#          grid — robustness useful under EITHER theta resolution (structural
#          iceberg or centrality). Feeds memo Decision A.
#
#   MA_i = sum_{j!=i} Pop_j / tau_ij^theta. Different theta only RE-WEIGHTS
#   the existing tau matrices — no Dijkstra rerun (minutes, not hours).
#
# SPEC: IV-Both per outcome, controls = geo_controls_main with the baseline
#   logMA control recomputed at THIS theta (l60). HC1 SE. First-stage F
#   reported as ivf (IID, comparable to outcome Tables 9/10/11) AND robust
#   (ivwald joint Wald).
#
# OUTCOMES (six):
#   chg_log_pop_91_60          population (anchor: Table 9)
#   chg_log_valprod_85_54      mfg production value (Table 10 A)
#   chg_log_massal_85_54       mfg wage mass (Table 10 A)
#   chg_log_nestab_85_54       mfg establishments (Table 10 A)
#   chg_log_nexp_88_60         agriculture farms (Table 10 B)
#   chg_log_areatot_ha_88_60   agriculture farmed area (Table 10 B)
#
# THETA GRID: 1, 2, 3, 4.55 (main), 6, 8.11 (alt), 10, 12 (matches the
#   population sweep).
#
# READS:
#   data/derived/03_taus/tau_{actual_1960,actual_1986}_s0.parquet
#   data/derived/03_taus/tau_instrument_{stu,lcp_mst}_s0.parquet
#   data/derived/base/census_1960/census_1960_ipums.parquet
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/diagnostic_theta_sweep_sectoral.{txt,csv}
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
})

THETA_GRID <- c(1, 2, 3, 4.55, 6, 8.11, 10, 12)

OUTCOMES <- list(
    list(var = "chg_log_pop_91_60",        lab = "population"),
    list(var = "chg_log_valprod_85_54",    lab = "mfg production value"),
    list(var = "chg_log_massal_85_54",     lab = "mfg wage mass"),
    list(var = "chg_log_nestab_85_54",     lab = "mfg establishments"),
    list(var = "chg_log_nexp_88_60",       lab = "ag farms"),
    list(var = "chg_log_areatot_ha_88_60", lab = "ag farmed area")
)

GEO6 <- c("elev_mean_std", "rugged_mea_std", "wheat_std",
          "preCal_std", "postCal_std", "dist_to_BA_std")

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_diagnostic_helpers.R"),
           echo = FALSE)

    report_path <- file.path(dir_tables,
                             "diagnostic_theta_sweep_sectoral.txt")
    con <- file(report_path, open = "wt")
    rep <- function(...) { line <- sprintf(...); cat(line, "\n")
                           cat(line, "\n", file = con) }

    rep("%s", strrep("=", 70))
    rep("THETA SWEEP — SECTORAL PATTERN (IV-Both beta by outcome x theta)")
    rep("Is 'manufacturing responds, agriculture does not' robust to theta?")
    rep("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
    rep("Stars: * p<.10  ** p<.05  *** p<.01 ; HC1 SE")
    rep("%s", strrep("=", 70))

    pop <- load_1960_pop()
    tau <- list(
        a60 = read_tau("actual_1960_s0"), a86 = read_tau("actual_1986_s0"),
        stu = read_tau("instrument_stu_s0"),
        lcp = read_tau("instrument_lcp_mst_s0"))

    base <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))
    base <- ensure_geolev2_char(base)
    out_vars <- vapply(OUTCOMES, function(o) o$var, character(1))
    keep <- c("geolev2", out_vars, "log_pop_1960", GEO6)
    base <- base[, keep]

    rows <- list()
    for (th in THETA_GRID) {
        ma <- build_ma_changes(tau, pop, th)  # geolev2, chg, chgstu, chglcp, l60
        d0 <- merge(base, ma, by = "geolev2")
        for (o in OUTCOMES) {
            r <- fit_iv_both(d0, o$var, th)
            r$outcome <- o$lab
            rows[[length(rows) + 1L]] <- r
        }
    }
    df <- do.call(rbind, rows)

    print_matrix(df, rep)
    write.csv(df[, c("outcome", "theta", "beta", "se", "p", "stars",
                     "F_ivf", "F_robust", "n_obs")],
              file.path(dir_tables, "diagnostic_theta_sweep_sectoral.csv"),
              row.names = FALSE)

    rep("\n%s", strrep("=", 70))
    rep("READING: scan each row across theta. If the mfg rows stay")
    rep("positive+significant and the ag rows stay ~0/ns across the grid,")
    rep("the sectoral pattern is robust to theta even though the population")
    rep("LEVEL is not (it rises toward Gibbons ~0.3 only near theta=1).")
    rep("%s", strrep("=", 70))
    close(con)
    message("\nSaved: ", report_path)
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
read_tau <- function(case) {
    arrow::read_parquet(
        file.path(dir_derived_taus, sprintf("tau_%s.parquet", case)))
}



# Build the treatment + two instrument changes + baseline logMA at one theta
build_ma_changes <- function(tau, pop, th) {
    ma60  <- compute_ma(tau$a60, pop, th)
    ma86  <- compute_ma(tau$a86, pop, th)
    mastu <- compute_ma(tau$stu, pop, th)
    malcp <- compute_ma(tau$lcp, pop, th)
    ma <- Reduce(function(a, b) merge(a, b, by = "geolev2"), list(
        data.frame(geolev2 = ma60$geolev2,  l60 = ma60$logMA),
        data.frame(geolev2 = ma86$geolev2,  l86 = ma86$logMA),
        data.frame(geolev2 = mastu$geolev2, lstu = mastu$logMA),
        data.frame(geolev2 = malcp$geolev2, llcp = malcp$logMA)))
    ma <- ensure_geolev2_char(ma)
    ma$chg    <- ma$l86  - ma$l60
    ma$chgstu <- ma$lstu - ma$l60
    ma$chglcp <- ma$llcp - ma$l60
    ma[, c("geolev2", "chg", "chgstu", "chglcp", "l60")]
}

# Fit IV-Both for one outcome at one theta. Mirrors the population template:
# six standardized geo controls + log_pop_1960 + baseline logMA at THIS theta
# (l60). Two instruments (chgstu = Larkin, chglcp = LCP-MST). HC1 SE.
# Returns one row. F_ivf = IID first-stage F (comparable to outcome Tables
# 9/10/11, which report ivf via fitstat_F); F_robust = heteroskedasticity-
# robust joint first-stage Wald (model vcov; ~ Table 8's robust Wald).
fit_iv_both <- function(d0, yvar, th) {
    d0 <- d0[is.finite(d0$chg) & is.finite(d0$chgstu) &
             is.finite(d0$chglcp) & is.finite(d0$l60) &
             !is.na(d0[[yvar]]), ]
    ctrls <- paste(c(GEO6, "log_pop_1960", "l60"), collapse = " + ")
    fml <- as.formula(
        sprintf("%s ~ %s | chg ~ chgstu + chglcp", yvar, ctrls))
    m <- suppressMessages(fixest::feols(fml, data = d0, vcov = "hetero"))

    b  <- unname(coef(m)["fit_chg"])
    se <- unname(m$se["fit_chg"])
    p  <- unname(m$coeftable["fit_chg", 4])
    stars <- if (is.na(p)) "" else if (p < .01) "***" else
             if (p < .05) "**" else if (p < .10) "*" else ""

    f_ivf <- tryCatch({
        f <- fitstat(m, type = "ivf")
        if (is.list(f) && !is.null(f[[1]]$stat)) as.numeric(f[[1]]$stat)
        else NA_real_
    }, error = function(e) NA_real_)
    f_rob <- tryCatch({
        f <- fitstat(m, type = "ivwald")
        if (is.list(f) && !is.null(f[[1]]$stat)) as.numeric(f[[1]]$stat)
        else NA_real_
    }, error = function(e) NA_real_)

    data.frame(theta = th, beta = b, se = se, p = p, stars = stars,
               F_ivf = f_ivf, F_robust = f_rob, n_obs = m$nobs,
               stringsAsFactors = FALSE)
}

# Readable matrices: rows = outcomes, cols = theta. First the IV-Both beta
# (with stars), then the IID first-stage F so weak-instrument regions are
# visible at a glance.
print_matrix <- function(df, rep) {
    thetas <- sort(unique(df$theta))
    outs   <- unique(df$outcome)

    hdr <- sprintf("%-22s", "outcome \\ theta")
    for (th in thetas) hdr <- paste0(hdr, sprintf(" %9s", sprintf("%.2f", th)))

    rep("\n%s", "IV-Both beta (stars: * .10  ** .05  *** .01):")
    rep("%s", hdr)
    rep("%s", strrep("-", nchar(hdr)))
    for (o in outs) {
        line <- sprintf("%-22s", o)
        for (th in thetas) {
            r <- df[df$outcome == o & abs(df$theta - th) < 1e-9, ]
            line <- paste0(line, sprintf(" %9s",
                           sprintf("%+.3f%s", r$beta, r$stars)))
        }
        rep("%s", line)
    }

    rep("\n%s", "First-stage F (ivf, IID — comparable to outcome Tables 9/10/11):")
    rep("%s", hdr)
    rep("%s", strrep("-", nchar(hdr)))
    for (o in outs) {
        line <- sprintf("%-22s", o)
        for (th in thetas) {
            r <- df[df$outcome == o & abs(df$theta - th) < 1e-9, ]
            line <- paste0(line, sprintf(" %9.1f", r$F_ivf))
        }
        rep("%s", line)
    }

    # ---- Paper exhibit (.tex), added per Cote reading note #44 -----------
    # (2026-07-24): the sweep existed only in this archive CSV; the
    # paper now shows the five sectoral outcomes (population's sweep
    # is tab:theta_sweep). Rows = theta grid, columns = outcomes,
    # cells = IV-Both coefficient with stars, HC1 SE beneath.
    sec_outs <- setdiff(outs, "population")
    col_lab <- c("mfg production value" = "Value of prod.",
                 "mfg wage mass"        = "Wage mass",
                 "mfg establishments"   = "Establishments",
                 "ag farms"             = "Farms",
                 "ag farmed area"       = "Farmed area")
    tex_cell <- function(r) {
        sprintf("\\begin{tabular}{@{}c@{}} %.3f%s \\\\ (%.3f) \\end{tabular}",
                r$beta, ifelse(nchar(r$stars) > 0,
                               sprintf("$^{%s}$", r$stars), ""),
                r$se)
    }
    tex <- c(
        "% Sectoral theta sweep table.",
        "% Generated by code/analysis/diagnostic_theta_sweep_sectoral.R.",
        "\\begin{table}[htbp]",
        "\\centering",
        "\\caption{Sectoral Elasticities Across the Trade-Elasticity",
        "Sweep (combined-IV estimates)}",
        "\\label{tab:theta_sweep_sectoral}",
        "\\footnotesize",
        "\\begin{tabular}{lccccc}",
        "\\toprule",
        " & \\multicolumn{3}{c}{Manufacturing} & \\multicolumn{2}{c}{Agriculture} \\\\",
        "\\cmidrule(lr){2-4} \\cmidrule(lr){5-6}",
        paste0("$\\theta$ & ",
               paste(col_lab[sec_outs], collapse = " & "), " \\\\"),
        "\\midrule")
    for (th in thetas) {
        tag <- if (abs(th - 4.55) < 1e-9) " (main)"
               else if (abs(th - 8.11) < 1e-9) " (alt.)" else ""
        cells <- vapply(sec_outs, function(o) {
            tex_cell(df[df$outcome == o & abs(df$theta - th) < 1e-9, ])
        }, character(1))
        tex <- c(tex, sprintf("%.2f%s & %s \\\\", th, tag,
                              paste(cells, collapse = " & ")))
    }
    n_rng <- range(df$n_obs[df$outcome %in% sec_outs])
    f_rng <- range(df$F_ivf[df$outcome %in% sec_outs])
    tex <- c(tex,
        "\\bottomrule",
        "\\end{tabular}",
        "",
        "\\footnotesize",
        paste0("\\emph{Notes}: Each row recomputes market access from the ",
               "existing transport-cost matrices with the row's $\\theta$ ",
               "(treatment, both instruments, and the baseline log-MA ",
               "control all switch), then re-estimates the combined-IV ",
               "specification of Table~\\ref{tab:sectoral_iv} for each ",
               "sectoral outcome. ",
               sprintf("$N$ = %d--%d by outcome. ", n_rng[1], n_rng[2]),
               sprintf("First-stage $F$ between %.1f and %.1f across ",
                       f_rng[1], f_rng[2]),
               "the grid. Robust (HC1) SE in parentheses. ",
               "Significance: $^{*}p<0.10,\\;^{**}p<0.05,\\;^{***}p<0.01$. ",
               "The population sweep is in Table~\\ref{tab:theta_sweep}."),
        "\\end{table}")
    writeLines(tex, file.path(dir_tables,
                              "diagnostic_theta_sweep_sectoral.tex"))
    message("Saved: ",
            file.path(dir_tables, "diagnostic_theta_sweep_sectoral.tex"))
}

main()

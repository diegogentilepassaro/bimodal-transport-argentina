# ===========================================================================
# diagnostic_theta_sweep.R
#
# PURPOSE: Bloque-1 cheap lever. Sweep the distance-decay / trade
#          elasticity theta and report how the main population elasticity
#          moves. Cote greenlit this as the cheap thing to rule out before
#          deeper work.
#
#   MA_i = sum_{j!=i} Pop_j / tau_ij^theta. Different theta only RE-WEIGHTS
#   the existing tau matrices — no Dijkstra rerun. So this is minutes, not
#   hours.
#
#   NOTE on cost magnitudes: a global rescale of all transport costs by a
#   constant k is invariant for the elasticity (tau -> k*tau shifts log MA
#   by a constant -theta*log(k) that cancels in the 1960->1986 change). So
#   only theta and RELATIVE mode costs move the elasticity. Relative-cost
#   variants need raster+tau rebuilds (separate, more expensive job); this
#   script covers only the theta dimension.
#
# BENCHMARK: Gibbons et al. (2024) ~= 0.3 (rail removal, 20th c.,
#   population) is the clean comparator for this paper — NOT Donaldson &
#   Hornbeck (expansion, 19th c., land value). Per Cote's correction.
#
# THETA GRID: 1, 2, 3, 4.55 (main), 6, 8.11 (alt), 10, 12.
#
# READS:
#   data/derived/03_taus/tau_actual_{1960,1986}_s0.parquet
#   data/derived/03_taus/tau_instrument_{stu,lcp_mst}_s0.parquet
#   data/derived/base/census_1960/census_1960_ipums.parquet
#   data/derived/06_analysis/estimation_sample.parquet  (outcome + controls)
#
# PRODUCES:
#   results/tables/diagnostic_theta_sweep.{txt,csv}
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
})

THETA_GRID <- c(1, 2, 3, 4.55, 6, 8.11, 10, 12)

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_diagnostic_helpers.R"),
           echo = FALSE)

    report_path <- file.path(dir_tables, "diagnostic_theta_sweep.txt")
    con <- file(report_path, open = "wt")
    rep <- function(...) { line <- sprintf(...); cat(line, "\n")
                           cat(line, "\n", file = con) }

    rep("%s", strrep("=", 70))
    rep("THETA SWEEP (Bloque 1 cheap lever)")
    rep("Main population elasticity vs theta; benchmark Gibbons 2024 ~0.3")
    rep("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
    rep("%s", strrep("=", 70))

    pop <- load_1960_pop()
    tau <- list(
        a60 = arrow::read_parquet(
            file.path(dir_derived_taus, "tau_actual_1960_s0.parquet")),
        a86 = arrow::read_parquet(
            file.path(dir_derived_taus, "tau_actual_1986_s0.parquet")),
        stu = arrow::read_parquet(
            file.path(dir_derived_taus, "tau_instrument_stu_s0.parquet")),
        lcp = arrow::read_parquet(
            file.path(dir_derived_taus, "tau_instrument_lcp_mst_s0.parquet"))
    )

    base <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))
    base <- ensure_geolev2_char(base)
    ctrls <- "elev_mean_std + rugged_mea_std + wheat_std + preCal_std + postCal_std + dist_to_BA_std + log_pop_1960"

    rep("\n%-7s  %-18s  %-18s  %-7s", "theta", "OLS beta (SE)",
        "IV-Both beta (SE)", "first-stage F")
    rep("%s", strrep("-", 60))

    rows <- list()
    for (th in THETA_GRID) {
        # MA per case at this theta
        ma60 <- compute_ma(tau$a60, pop, th)
        ma86 <- compute_ma(tau$a86, pop, th)
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

        m <- merge(base[, c("geolev2", "chg_log_pop_91_60", "log_pop_1960",
                            "elev_mean_std", "rugged_mea_std", "wheat_std",
                            "preCal_std", "postCal_std", "dist_to_BA_std")],
                   ma[, c("geolev2", "chg", "chgstu", "chglcp", "l60")],
                   by = "geolev2")
        m <- m[is.finite(m$chg) & is.finite(m$chgstu) &
               is.finite(m$chglcp) & is.finite(m$l60), ]

        # Controls match Table 9 / geo_controls_main: six standardized geo
        # vars + log_pop_1960 + baseline logMA at THIS theta (l60).
        ctrls_th <- paste(ctrls, "+ l60")
        m_ols <- suppressMessages(fixest::feols(
            as.formula(sprintf("chg_log_pop_91_60 ~ chg + %s", ctrls_th)),
            data = m, vcov = "hetero"))
        m_iv <- suppressMessages(fixest::feols(
            as.formula(sprintf(
                "chg_log_pop_91_60 ~ %s | chg ~ chgstu + chglcp", ctrls_th)),
            data = m, vcov = "hetero"))
        fs <- tryCatch({
            f <- fitstat(m_iv, type = "ivf")
            if (is.list(f) && !is.null(f[[1]]$stat)) as.numeric(f[[1]]$stat)
            else NA_real_
        }, error = function(e) NA_real_)

        b_ols <- coef(m_ols)["chg"]; s_ols <- m_ols$se["chg"]
        b_iv  <- coef(m_iv)["fit_chg"]; s_iv <- m_iv$se["fit_chg"]
        tag <- if (abs(th - 4.55) < 1e-9) " <- main"
               else if (abs(th - 8.11) < 1e-9) " <- alt" else ""
        rep("%-7.2f  %+6.3f (%.3f)     %+6.3f (%.3f)     %6.1f%s",
            th, b_ols, s_ols, b_iv, s_iv, fs, tag)

        rows[[length(rows)+1L]] <- data.frame(
            theta = th, ols_beta = b_ols, ols_se = s_ols,
            iv_beta = b_iv, iv_se = s_iv, first_stage_F = fs, n_obs = m_iv$nobs)
    }

    df <- do.call(rbind, rows)
    write.csv(df, file.path(dir_tables, "diagnostic_theta_sweep.csv"),
              row.names = FALSE)

    # LaTeX fragment for the paper's robustness subsection (Section 5.5):
    # the full sweep, presented next to the baseline results per the
    # 2026-07-17 decision (previously the sweep was quoted only in the
    # Discussion section).
    tex_cell <- function(est, se) {
        if (is.na(est)) return(" ")
        sprintf("%.3f (%.3f)", est, se)
    }
    tex <- c(
        "% Theta sweep table. Generated by diagnostic_theta_sweep.R.",
        "\\begin{table}[htbp]",
        "\\centering",
        "\\caption{Population Elasticity Across the Trade-Elasticity Sweep",
        "(outcome: $\\Delta \\ln \\mathrm{Pop}_{1960 \\to 1991}$)}",
        "\\label{tab:theta_sweep}",
        "\\small",
        "\\begin{tabular}{lccc}",
        "\\toprule",
        "$\\theta$ & OLS & IV-Both & First-stage $F$ \\\\",
        "\\midrule")
    for (i in seq_len(nrow(df))) {
        r <- df[i, ]
        tag <- if (abs(r$theta - 4.55) < 1e-9) " (main)"
               else if (abs(r$theta - 8.11) < 1e-9) " (alt.)" else ""
        tex <- c(tex, sprintf("%.2f%s & %s & %s & %.1f \\\\",
                              r$theta, tag,
                              tex_cell(r$ols_beta, r$ols_se),
                              tex_cell(r$iv_beta, r$iv_se),
                              r$first_stage_F))
    }
    tex <- c(tex,
        "\\bottomrule",
        "\\end{tabular}",
        "",
        "\\footnotesize",
        paste0("\\emph{Notes}: Each row recomputes market access from the ",
               "existing transport-cost matrices with the row's $\\theta$ ",
               "(treatment, both instruments, and the baseline log-MA ",
               "control all switch), then re-estimates the OLS and ",
               "combined-IV specifications of Table~",
               "\\ref{tab:population_iv}. ",
               sprintf("$N = %d$. ", df$n_obs[1]),
               "Robust (HC1) SE in parentheses."),
        "\\end{table}")
    # message(), not rep(): rep() writes into the committed report file,
    # and a machine-specific absolute path must not land in an artifact.
    stopifnot(length(unique(df$n_obs)) == 1L)
    writeLines(tex, file.path(dir_tables, "diagnostic_theta_sweep.tex"))
    message("Saved: ", file.path(dir_tables, "diagnostic_theta_sweep.tex"))

    rep("\n%s", strrep("=", 70))
    rep("READING: if the IV beta stays ~0.02-0.06 across the whole theta")
    rep("grid, theta is NOT the reason the elasticity is far below the")
    rep("Gibbons 2024 ~0.3 benchmark. If beta rises toward 0.3 at some")
    rep("plausible theta, the main-spec theta choice is implicated.")
    rep("Cost MAGNITUDES: a global cost rescale is invariant for the")
    rep("elasticity; only RELATIVE mode costs matter, and those need a")
    rep("raster+tau rebuild (separate job, not in this sweep).")
    rep("%s", strrep("=", 70))

    close(con)
    message("\nSaved: ", report_path)
}



main()

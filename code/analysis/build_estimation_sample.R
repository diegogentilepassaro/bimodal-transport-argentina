# ===========================================================================
# build_estimation_sample.R
#
# PURPOSE: Produce a regression-ready estimation sample from the full
#          wide panel. Derives the four Table-9 outcomes
#          (total pop, urban pop, rural pop, urban share) and names
#          the treatment / instrument / control columns consistently
#          for downstream table scripts.
#
# READS:
#   data/derived/05_panel/departments_wide_panel.parquet
#
# PRODUCES:
#   data/derived/06_analysis/estimation_sample.parquet
#       312 rows × (one row per geolev2).
#       New columns beyond the panel:
#         rur_1991              = pop_1991 - urbpop_1991
#         chg_log_rur_91_60     = log(rur_1991) - log(rur_1960)
#         urbshr_1960           = urbpop_1960 / pop_1960
#         urbshr_1991           = urbpop_1991 / pop_1991
#         chg_urbshr_91_60      = urbshr_1991 - urbshr_1960 (share, not log)
#         log_pop_1960          = log(pop_1960)   (baseline control)
#   data/derived/06_analysis/data_file_manifest.log
#
# DESIGN:
#   - Sample: keep all 312 districts. NA is auto-dropped by fixest in
#     each regression. This means:
#       - Total-pop regressions have N=309 (pop_1960 is NA for CF and
#         the two TdF districts in the 1960 census).
#       - Urban-pop regressions have N<312 because urbpop_1960 is only
#         defined where pop_1960 is.
#       - Urban-share regressions have the same N as urban-pop.
#   - Rural pop regressions use `log(rur)` where rur > 0. 37 districts
#     have rur_1960 = 0 (fully-urban in 1960) and are auto-dropped.
#   - Log transformations applied where the outcome is a strictly
#     positive quantity. Urban share is already a [0,1] variable and
#     is differenced in levels, not logs.
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
})

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    message("\n", strrep("=", 72))
    message("build_estimation_sample.R  |  Table 9 outcomes + controls")
    message(strrep("=", 72))

    if (!dir.exists(dir_derived_analysis)) {
        dir.create(dir_derived_analysis, recursive = TRUE)
    }

    d <- arrow::read_parquet(
        file.path(dir_derived_panel, "departments_wide_panel.parquet")
    )
    d <- ensure_geolev2_char(d)
    message(sprintf("[est] Loaded panel: %d rows, %d cols",
                    nrow(d), ncol(d)))

    # ---- 1. Derive rural pop 1991 and log-change ----------------------------
    d$rur_1991 <- d$pop_1991 - d$urbpop_1991
    # Where rur_1991 or rur_1960 is <=0 or NA, the log-change is NA.
    valid_rur <- !is.na(d$rur_1960) & !is.na(d$rur_1991) &
                  d$rur_1960 > 0     &  d$rur_1991 > 0
    d$chg_log_rur_91_60 <- NA_real_
    d$chg_log_rur_91_60[valid_rur] <- log(d$rur_1991[valid_rur]) -
                                      log(d$rur_1960[valid_rur])

    # ---- 2. Derive urban share and change in share -------------------------
    d$urbshr_1960 <- d$urbpop_1960 / d$pop_1960
    d$urbshr_1991 <- d$urbpop_1991 / d$pop_1991
    d$chg_urbshr_91_60 <- d$urbshr_1991 - d$urbshr_1960

    # ---- 3. Baseline controls ----------------------------------------------
    # log population in 1960 (baseline). NA where pop_1960 is missing.
    valid_lp <- !is.na(d$pop_1960) & d$pop_1960 > 0
    d$log_pop_1960 <- NA_real_
    d$log_pop_1960[valid_lp] <- log(d$pop_1960[valid_lp])

    # log MA in 1960 is already a column (logMA_actual_1960_s0_elow);
    # no transformation needed.

    # ---- 4. Validation / logging ------------------------------------------
    for (v in c("chg_log_pop_91_60",
                "chg_log_urbpop_91_60",
                "chg_log_rur_91_60",
                "chg_urbshr_91_60",
                "chg_logMA_86_60_s0_elow",
                "chg_logMA_stu_s0_elow",
                "chg_logMA_lcp_mst_s0_elow",
                "log_pop_1960",
                "logMA_actual_1960_s0_elow")) {
        n <- sum(!is.na(d[[v]]))
        mn <- mean(d[[v]], na.rm = TRUE)
        sdv <- sd(d[[v]], na.rm = TRUE)
        message(sprintf("  %-35s  N=%d  mean=%+.3f  sd=%.3f",
                        v, n, mn, sdv))
    }

    # ---- 5. Save ------------------------------------------------------------
    out_path <- file.path(dir_derived_analysis,
                          "estimation_sample.parquet")
    arrow::write_parquet(d, out_path)
    message(sprintf("\n[est] Saved: %s (%d rows, %d cols)",
                    out_path, nrow(d), ncol(d)))

    # ---- 6. Manifest --------------------------------------------------------
    log_path <- file.path(dir_derived_analysis, "data_file_manifest.log")
    sink(log_path); on.exit(sink(), add = TRUE)
    cat("Data file manifest — build_estimation_sample.R\n")
    cat(sprintf("Generated: %s\n", Sys.time()))
    cat(sprintf("rootdir:   %s\n\n", rootdir))
    cat(strrep("=", 60), "\n")
    cat("FILE: estimation_sample.parquet\n")
    cat(strrep("=", 60), "\n")
    cat(sprintf("Rows: %d\nColumns: %d\nKey: geolev2\n\n",
                nrow(d), ncol(d)))
    cat("New outcome / control columns added by this script:\n")
    cat("  rur_1991           = pop_1991 - urbpop_1991\n")
    cat("  chg_log_rur_91_60  = log(rur_1991) - log(rur_1960)\n")
    cat("  urbshr_1960        = urbpop_1960 / pop_1960\n")
    cat("  urbshr_1991        = urbpop_1991 / pop_1991\n")
    cat("  chg_urbshr_91_60   = urbshr_1991 - urbshr_1960 (level, not log)\n")
    cat("  log_pop_1960       = log(pop_1960)\n\n")
    cat("Non-NA counts (regression sample sizes):\n")
    for (v in c("chg_log_pop_91_60",
                "chg_log_urbpop_91_60",
                "chg_log_rur_91_60",
                "chg_urbshr_91_60",
                "chg_logMA_86_60_s0_elow",
                "chg_logMA_stu_s0_elow",
                "chg_logMA_lcp_mst_s0_elow",
                "log_pop_1960",
                "logMA_actual_1960_s0_elow",
                "elev_mean_std", "rugged_mea_std", "wheat_std",
                "preCal_std", "postCal_std", "dist_to_BA_std")) {
        if (!(v %in% names(d))) next
        cat(sprintf("  %-35s  N=%d\n", v, sum(!is.na(d[[v]]))))
    }
    message(sprintf("[est] Manifest: %s", log_path))
}

main()

# ===========================================================================
# diagnostic_ma_nofluvial.R
#
# PURPOSE: Bloque-1 test (a) REPORT. After run_nofluvial_variant.sh has
#          produced the _nofluvial MA files, this script compares the
#          no-fluvial market access against the baseline: does disabling
#          the navigation channel reduce the 91% MA-gain share and/or move
#          the population elasticity toward the Donaldson-Hornbeck 0.5-0.7
#          benchmark?
#
# READS:
#   data/derived/04_market_access/ma_actual_1960_s0_nofluvial_elow.parquet
#   data/derived/04_market_access/ma_actual_1986_s0_nofluvial_elow.parquet
#   data/derived/04_market_access/ma_instrument_stu_s0_nofluvial_elow.parquet
#   data/derived/04_market_access/ma_instrument_lcp_mst_s0_nofluvial_elow.parquet
#   data/derived/06_analysis/estimation_sample.parquet  (baseline + controls)
#
# PRODUCES:
#   results/tables/diagnostic_ma_nofluvial.txt
#
# Run AFTER run_nofluvial_variant.sh.
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
})

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    report_path <- file.path(dir_tables, "diagnostic_ma_nofluvial.txt")
    con <- file(report_path, open = "wt")
    rep <- function(...) { line <- sprintf(...); cat(line, "\n")
                           cat(line, "\n", file = con) }

    rep("%s", strrep("=", 70))
    rep("MA NO-FLUVIAL DIAGNOSTIC (Bloque 1 test a / C28)")
    rep("Navigation channel disabled vs baseline")
    rep("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
    rep("%s", strrep("=", 70))

    # Helper: load a no-fluvial logMA file -> named data frame
    load_logma <- function(case) {
        p <- file.path(dir_derived_ma,
                       sprintf("ma_%s_nofluvial_elow.parquet", case))
        if (!file.exists(p)) {
            stop("Missing no-fluvial MA file: ", p,
                 "\nRun code/analysis/run_nofluvial_variant.sh first.")
        }
        d <- arrow::read_parquet(p)
        d <- ensure_geolev2_char(d)
        data.frame(geolev2 = d$geolev2, logMA = d$logMA)
    }

    ma1960 <- load_logma("actual_1960_s0")
    ma1986 <- load_logma("actual_1986_s0")
    mastu  <- load_logma("instrument_stu_s0")
    malcp  <- load_logma("instrument_lcp_mst_s0")

    names(ma1960)[2] <- "logMA_1960"
    names(ma1986)[2] <- "logMA_1986"
    names(mastu)[2]  <- "logMA_stu"
    names(malcp)[2]  <- "logMA_lcp"

    ma <- Reduce(function(a, b) merge(a, b, by = "geolev2"),
                 list(ma1960, ma1986, mastu, malcp))
    ma$chg_v     <- ma$logMA_1986 - ma$logMA_1960
    ma$chg_stu_v <- ma$logMA_stu  - ma$logMA_1960
    ma$chg_lcp_v <- ma$logMA_lcp  - ma$logMA_1960

    base <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))
    base <- ensure_geolev2_char(base)

    # ---- [A] gain share comparison ----------------------------------------
    rep("\n%s", strrep("-", 70))
    rep("[A] MA GAIN: baseline (with fluvial) vs no-fluvial")
    rep("%s", strrep("-", 70))
    cb <- base$chg_logMA_86_60_s0_elow
    cv <- ma$chg_v
    rep("  baseline:    share gain %.1f%%  mean %.3f  median %.3f",
        100*mean(cb > 0, na.rm=TRUE), mean(cb, na.rm=TRUE),
        median(cb, na.rm=TRUE))
    rep("  no-fluvial:  share gain %.1f%%  mean %.3f  median %.3f",
        100*mean(cv > 0, na.rm=TRUE), mean(cv, na.rm=TRUE),
        median(cv, na.rm=TRUE))

    # ---- [B] population elasticity, no-fluvial ----------------------------
    rep("\n%s", strrep("-", 70))
    rep("[B] POPULATION ELASTICITY, no-fluvial (sector 0, theta low)")
    rep("%s", strrep("-", 70))
    m <- merge(
        base[, c("geolev2", "chg_log_pop_91_60", "log_pop_1960",
                 "elev_mean_std", "rugged_mea_std", "wheat_std",
                 "preCal_std", "postCal_std", "dist_to_BA_std")],
        ma[, c("geolev2", "chg_v", "chg_stu_v", "chg_lcp_v")],
        by = "geolev2")
    ctrls <- "elev_mean_std + rugged_mea_std + wheat_std + preCal_std + postCal_std + dist_to_BA_std + log_pop_1960"

    m_ols <- suppressMessages(fixest::feols(
        as.formula(sprintf("chg_log_pop_91_60 ~ chg_v + %s", ctrls)),
        data = m, vcov = "hetero"))
    m_iv <- suppressMessages(fixest::feols(
        as.formula(sprintf(
            "chg_log_pop_91_60 ~ %s | chg_v ~ chg_stu_v + chg_lcp_v", ctrls)),
        data = m, vcov = "hetero"))

    rep("  OLS  beta = %+.3f (%.3f)  N=%d",
        coef(m_ols)["chg_v"], m_ols$se["chg_v"], m_ols$nobs)
    rep("  IV   beta = %+.3f (%.3f)  N=%d",
        coef(m_iv)["fit_chg_v"], m_iv$se["fit_chg_v"], m_iv$nobs)
    rep("  (baseline-with-fluvial IV-Both was +0.046 (0.033); D-H ~0.5-0.7)")

    rep("\n%s", strrep("=", 70))
    rep("READING: if no-fluvial gain share drops well below 91%% and the")
    rep("IV beta moves up toward 0.5-0.7, the fluvial channel (C28) was")
    rep("inflating MA. If both are basically unchanged, fluvial is NOT the")
    rep("driver and the issue is the overall road-cost magnitude / HMI.")
    rep("%s", strrep("=", 70))

    close(con)
    message("\nSaved report: ", report_path)
}

main()

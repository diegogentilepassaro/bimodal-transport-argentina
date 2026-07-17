# ===========================================================================
# diagnostic_ma_unimodal.R
#
# PURPOSE: Transshipment bound (paper Section 5.5). After
#          code/pipeline/07_unimodal_taus.R (main.R step D.13f) has
#          produced the six single-mode taus (road/rail/water ×
#          1960/1986), this builds the INFINITE-TRANSSHIPMENT (unimodal)
#          tau as the element-wise min across modes, recomputes MA, and
#          compares the gain share and OLS elasticity to baseline on a
#          common sample. Originally the Bloque-1 test (c) screen.
#
#   Baseline = zero transshipment (free mode-switching). Unimodal =
#   infinite transshipment (one mode per trip). The truth is between.
#   If the 91% gain survives the unimodal bound, transshipment is not
#   the driver. If it collapses, the zero-transshipment assumption is
#   load-bearing and the full mode-expanded-graph build is warranted.
#
# READS:
#   data/derived/03_taus/tau_actual_{1960,1986}_s0_{road,rail,water}only.parquet
#   data/derived/base/census_1960/census_1960_ipums.parquet
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   data/derived/06_analysis/unimodal_variant/tau_unimodal_{1960,1986}.parquet
#   results/tables/diagnostic_ma_unimodal.txt
#   results/tables/diagnostic_ma_unimodal.csv   (AutoFill source, Section 5.5)
#
# NOTE: This is a SCREEN. It reports the gain share (decisive, cheap) and
#   the OLS elasticity. A full IV elasticity would also need unimodal
#   instruments (stu, lcp_mst) — built only if this screen implicates
#   transshipment.
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
})

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_diagnostic_helpers.R"),
           echo = FALSE)

    out_dir <- file.path(dir_derived_analysis, "unimodal_variant")
    if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

    report_path <- file.path(dir_tables, "diagnostic_ma_unimodal.txt")
    con <- file(report_path, open = "wt")
    rep <- function(...) { line <- sprintf(...); cat(line, "\n")
                           cat(line, "\n", file = con) }

    rep("%s", strrep("=", 70))
    rep("MA UNIMODAL / TRANSSHIPMENT-BOUND DIAGNOSTIC (Bloque 1 test c)")
    rep("Infinite transshipment (one mode per trip) vs baseline (free switch)")
    rep("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
    rep("%s", strrep("=", 70))

    # ---- Build unimodal tau (min across modes) per period -----------------
    build_unimodal <- function(year) {
        modes <- c("road", "rail", "water")
        tau_mode <- lapply(modes, function(md) {
            p <- file.path(dir_derived_taus,
                sprintf("tau_actual_%d_s0_%sonly.parquet", year, md))
            if (!file.exists(p)) {
                stop("Missing single-mode tau: ", p,
                     "\nRun code/pipeline/07_unimodal_taus.R ",
                     "(main.R step D.13f) first.")
            }
            d <- arrow::read_parquet(p)
            d$key <- paste(d$origin_geolev2, d$destination_geolev2, sep = "_")
            d
        })
        # Align on key (all three share the same 48516 lower-triangle pairs)
        base <- tau_mode[[1]][, c("origin_geolev2", "destination_geolev2",
                                  "key", "tau")]
        names(base)[names(base) == "tau"] <- "tau_road"
        base$tau_rail  <- tau_mode[[2]]$tau[match(base$key, tau_mode[[2]]$key)]
        base$tau_water <- tau_mode[[3]]$tau[match(base$key, tau_mode[[3]]$key)]
        # Infinite-transshipment tau = min across modes (NA/Inf safe)
        m <- cbind(base$tau_road, base$tau_rail, base$tau_water)
        base$tau <- apply(m, 1, function(x) {
            x <- x[is.finite(x)]
            if (length(x) == 0) Inf else min(x)
        })
        out <- base[, c("origin_geolev2", "destination_geolev2", "tau")]
        arrow::write_parquet(out,
            file.path(out_dir, sprintf("tau_unimodal_%d.parquet", year)))
        out
    }

    rep("\nBuilding unimodal tau (element-wise min across road/rail/water)...")
    tau60 <- build_unimodal(1960)
    tau86 <- build_unimodal(1986)

    # ---- Compute MA for each period ---------------------------------------
    pop <- load_1960_pop()
    ma60 <- compute_ma(tau60, pop, theta[["low"]])
    ma86 <- compute_ma(tau86, pop, theta[["low"]])
    ma <- merge(
        data.frame(geolev2 = ma60$geolev2, logMA_1960 = ma60$logMA),
        data.frame(geolev2 = ma86$geolev2, logMA_1986 = ma86$logMA),
        by = "geolev2")
    ma <- ensure_geolev2_char(ma)
    ma$chg_uni <- ma$logMA_1986 - ma$logMA_1960

    base <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))
    base <- ensure_geolev2_char(base)

    # ---- [A] gain share ----------------------------------------------------
    rep("\n%s", strrep("-", 70))
    rep("[A] MA GAIN: baseline (zero transshipment) vs unimodal (infinite)")
    rep("%s", strrep("-", 70))
    # Common district set for both shares (cr-review PR #100): previously
    # baseline used the estimation sample and unimodal used every district
    # with finite unimodal MA (311 vs 312).
    ab <- merge(base[, c("geolev2", "chg_logMA_86_60_s0_elow")],
                ma[, c("geolev2", "chg_uni")], by = "geolev2")
    ab <- ab[!is.na(ab$chg_logMA_86_60_s0_elow) & is.finite(ab$chg_uni), ]
    cb <- ab$chg_logMA_86_60_s0_elow
    cu <- ab$chg_uni
    rep("  common district set: N=%d", nrow(ab))
    rep("  baseline:  share gain %.1f%%  mean %.3f  median %.3f",
        100*mean(cb > 0), mean(cb), median(cb))
    rep("  unimodal:  share gain %.1f%%  mean %.3f  median %.3f",
        100*mean(cu > 0), mean(cu), median(cu))

    # ---- [B] pairwise tau-change comparison -------------------------------
    rep("\n%s", strrep("-", 70))
    rep("[B] PAIRWISE tau CHANGE 1960->1986: how much do pairs get cheaper?")
    rep("%s", strrep("-", 70))
    pair_ratio <- function(t60, t86) {
        k60 <- paste(t60$origin_geolev2, t60$destination_geolev2, sep="_")
        k86 <- paste(t86$origin_geolev2, t86$destination_geolev2, sep="_")
        r <- t86$tau[match(k60, k86)] / t60$tau
        r[is.finite(r) & r > 0]
    }
    r_uni <- pair_ratio(tau60, tau86)
    # Baseline computed live from the committed multimodal taus (the old
    # report quoted stale pre-issue-#22 constants here).
    tau60_base <- arrow::read_parquet(
        file.path(dir_derived_taus, "tau_actual_1960_s0.parquet"))
    tau86_base <- arrow::read_parquet(
        file.path(dir_derived_taus, "tau_actual_1986_s0.parquet"))
    r_base <- pair_ratio(tau60_base, tau86_base)
    rep("  unimodal: %.1f%% of pairs cheaper; median tau ratio %.3f",
        100*mean(r_uni < 1), median(r_uni))
    rep("  baseline: %.1f%% of pairs cheaper; median tau ratio %.3f",
        100*mean(r_base < 1), median(r_base))

    # ---- [C] OLS elasticity under unimodal MA -----------------------------
    rep("\n%s", strrep("-", 70))
    rep("[C] POPULATION OLS elasticity under unimodal MA (sector 0)")
    rep("%s", strrep("-", 70))
    # COMMON SAMPLE for both regressions (cr-review PR #100): the earlier
    # version filtered the two regressions differently and their equal N
    # was an unasserted coincidence. Both now run on districts where the
    # outcome and BOTH MA measures are defined, and equality is asserted.
    m <- merge(
        base[, c("geolev2", "chg_log_pop_91_60", "log_pop_1960",
                 "chg_logMA_86_60_s0_elow",
                 "elev_mean_std", "rugged_mea_std", "wheat_std",
                 "preCal_std", "postCal_std", "dist_to_BA_std")],
        ma[, c("geolev2", "chg_uni")], by = "geolev2")
    stopifnot(nrow(m) == nrow(base))  # post-merge check: ma covers base
    m <- m[is.finite(m$chg_uni) &
           !is.na(m$chg_logMA_86_60_s0_elow) &
           !is.na(m$chg_log_pop_91_60), ]
    ctrls <- "elev_mean_std + rugged_mea_std + wheat_std + preCal_std + postCal_std + dist_to_BA_std + log_pop_1960"
    m_ols <- suppressMessages(fixest::feols(
        as.formula(sprintf("chg_log_pop_91_60 ~ chg_uni + %s", ctrls)),
        data = m, vcov = "hetero"))
    rep("  OLS  beta = %+.3f (%.3f)  N=%d",
        coef(m_ols)["chg_uni"], m_ols$se["chg_uni"], m_ols$nobs)
    # Baseline OLS on the SAME rows and controls. This simple spec omits
    # the baseline log-MA control by design (there is no unimodal
    # analogue), so it differs from Table 9 column (1); the two rows here
    # are compared like-for-like.
    m_ols_base <- suppressMessages(fixest::feols(
        as.formula(sprintf("chg_log_pop_91_60 ~ chg_logMA_86_60_s0_elow + %s",
                           ctrls)),
        data = m, vcov = "hetero"))
    stopifnot(m_ols$nobs == m_ols_base$nobs)
    rep("  baseline OLS (same spec + sample, multimodal MA) = %+.3f (%.3f)  N=%d",
        coef(m_ols_base)["chg_logMA_86_60_s0_elow"],
        m_ols_base$se["chg_logMA_86_60_s0_elow"], m_ols_base$nobs)
    rep("  NOTE: full IV needs unimodal instruments too — built only if")
    rep("  this screen implicates transshipment.")

    # ---- CSV for AutoFill (paper Section 5.5 transshipment paragraph) -----
    csv_df <- data.frame(
        stat = c("gain_share_baseline", "gain_share_unimodal",
                 "pair_cheaper_baseline", "pair_cheaper_unimodal",
                 "median_ratio_baseline", "median_ratio_unimodal",
                 "ols_beta_baseline", "ols_beta_unimodal",
                 "ols_se_baseline", "ols_se_unimodal",
                 "n_unimodal"),
        value = c(100*mean(cb > 0), 100*mean(cu > 0),
                  100*mean(r_base < 1), 100*mean(r_uni < 1),
                  median(r_base), median(r_uni),
                  unname(coef(m_ols_base)["chg_logMA_86_60_s0_elow"]),
                  unname(coef(m_ols)["chg_uni"]),
                  unname(m_ols_base$se["chg_logMA_86_60_s0_elow"]),
                  unname(m_ols$se["chg_uni"]),
                  m_ols$nobs))
    out_csv <- file.path(dir_tables, "diagnostic_ma_unimodal.csv")
    write.csv(csv_df, out_csv, row.names = FALSE)
    message("Saved: ", out_csv)

    rep("\n%s", strrep("=", 70))
    rep("READING: if unimodal gain share stays ~91%% and median tau ratio")
    rep("stays ~0.8, transshipment is NOT the driver — the broad cost")
    rep("collapse happens even without free mode-switching. If the gain")
    rep("share drops sharply and pairs stop getting uniformly cheaper,")
    rep("the zero-transshipment assumption is load-bearing and the full")
    rep("mode-expanded-graph model (one node per cell x mode, switch")
    rep("edges with a penalty) is warranted as the real fix.")
    rep("%s", strrep("=", 70))

    close(con)
    message("\nSaved report: ", report_path)
}



main()

# ===========================================================================
# diagnostic_ma_unimodal.R
#
# PURPOSE: Bloque-1 test (c) SCREEN report. After run_unimodal_variant.sh
#          has produced the six single-mode taus (road/rail/water ×
#          1960/1986), this builds the INFINITE-TRANSSHIPMENT (unimodal)
#          tau as the element-wise min across modes, recomputes MA, and
#          compares the gain share and OLS elasticity to baseline.
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
                     "\nRun code/analysis/run_unimodal_variant.sh first.")
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
    cb <- base$chg_logMA_86_60_s0_elow
    cu <- ma$chg_uni[is.finite(ma$chg_uni)]
    rep("  baseline:  share gain %.1f%%  mean %.3f  median %.3f  (N=%d)",
        100*mean(cb > 0, na.rm=TRUE), mean(cb, na.rm=TRUE),
        median(cb, na.rm=TRUE), sum(!is.na(cb)))
    rep("  unimodal:  share gain %.1f%%  mean %.3f  median %.3f  (N=%d)",
        100*mean(cu > 0), mean(cu), median(cu), length(cu))

    # ---- [B] pairwise tau-change comparison -------------------------------
    rep("\n%s", strrep("-", 70))
    rep("[B] PAIRWISE tau CHANGE 1960->1986: how much do pairs get cheaper?")
    rep("%s", strrep("-", 70))
    k60 <- paste(tau60$origin_geolev2, tau60$destination_geolev2, sep="_")
    k86 <- paste(tau86$origin_geolev2, tau86$destination_geolev2, sep="_")
    mm <- match(k60, k86)
    r <- tau86$tau[mm] / tau60$tau
    r <- r[is.finite(r) & r > 0]
    rep("  unimodal: %.1f%% of pairs cheaper; median tau ratio %.3f",
        100*mean(r < 1), median(r))
    rep("  (baseline was 91.6%% cheaper, median ratio 0.795 — see diagnostic 1)")

    # ---- [C] OLS elasticity under unimodal MA -----------------------------
    rep("\n%s", strrep("-", 70))
    rep("[C] POPULATION OLS elasticity under unimodal MA (sector 0)")
    rep("%s", strrep("-", 70))
    m <- merge(
        base[, c("geolev2", "chg_log_pop_91_60", "log_pop_1960",
                 "elev_mean_std", "rugged_mea_std", "wheat_std",
                 "preCal_std", "postCal_std", "dist_to_BA_std")],
        ma[, c("geolev2", "chg_uni")], by = "geolev2")
    m <- m[is.finite(m$chg_uni), ]
    ctrls <- "elev_mean_std + rugged_mea_std + wheat_std + preCal_std + postCal_std + dist_to_BA_std + log_pop_1960"
    m_ols <- suppressMessages(fixest::feols(
        as.formula(sprintf("chg_log_pop_91_60 ~ chg_uni + %s", ctrls)),
        data = m, vcov = "hetero"))
    rep("  OLS  beta = %+.3f (%.3f)  N=%d",
        coef(m_ols)["chg_uni"], m_ols$se["chg_uni"], m_ols$nobs)
    rep("  (baseline OLS was +0.022; baseline IV-Both +0.046; Gibbons 2024 ~0.3)")
    rep("  NOTE: full IV needs unimodal instruments too — built only if")
    rep("  this screen implicates transshipment.")

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

load_1960_pop <- function() {
    d <- arrow::read_parquet(
        file.path(dir_derived_census1960, "census_1960_ipums.parquet"))
    d <- ensure_geolev2_char(d)
    data.frame(geolev2 = d$geolev2, pop = as.numeric(d$pop))
}

compute_ma <- function(tau_df, pop_df, theta_val) {
    tau_df <- ensure_geolev2_char(tau_df, "origin_geolev2")
    tau_df <- ensure_geolev2_char(tau_df, "destination_geolev2")
    sym <- rbind(
        tau_df[, c("origin_geolev2", "destination_geolev2", "tau")],
        data.frame(origin_geolev2      = tau_df$destination_geolev2,
                   destination_geolev2 = tau_df$origin_geolev2,
                   tau                 = tau_df$tau))
    sym <- merge(sym,
                 data.frame(destination_geolev2 = pop_df$geolev2,
                            pop_dest = pop_df$pop),
                 by = "destination_geolev2", all.x = TRUE)
    sym$pop_dest[is.na(sym$pop_dest)] <- 0
    sym$weight <- ifelse(is.finite(sym$tau) & sym$tau > 0,
                         1 / (sym$tau^theta_val), 0)
    sym$contrib <- sym$weight * sym$pop_dest
    ma_df <- aggregate(contrib ~ origin_geolev2, data = sym, FUN = sum)
    names(ma_df) <- c("geolev2", "MA")
    ma_df$logMA <- log(ma_df$MA)
    ma_df
}

main()

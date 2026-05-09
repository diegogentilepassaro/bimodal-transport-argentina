# ===========================================================================
# 04_market_access.R
#
# PURPOSE: Compute district-level market access (MA) from pairwise tau
#          matrices and 1960 population, for all sectors × both elasticities.
#
# READS:
#   data/derived/03_taus/tau_<case>.parquet
#   data/derived/base/census_1960/census_1960_ipums.parquet  (pop weights)
#
# PRODUCES:
#   data/derived/04_market_access/ma_<case>_<elas>.parquet
#       where <elas> ∈ {elow, ehigh} identifies θ = 4.55 vs 8.11.
#       Columns: geolev2 (chr, key), MA (num), logMA (num).
#
# FORMULA:
#   MA_i = sum_{j != i} Pop_j / (tau_ij)^theta
#
# DESIGN DECISIONS:
#   - Population anchor: 1960 IPUMS-constructed population. Used for ALL
#     cases so that variation in MA comes from variation in transport
#     costs (tau), not population. Matches Donaldson & Hornbeck (2016).
#   - Districts not in the 1960 census file (e.g. CF, TdF) use pop = 0
#     in the summation. They still appear as rows in the output but their
#     contribution to other districts' MA is zero, and their own MA is
#     computed from the 310 districts that do have population.
#   - theta: both θ_low = 4.55 and θ_high = 8.11 are computed in one pass,
#     yielding two output files per case.
#   - Inf tau (disconnected pair) contributes exactly 0 to the sum since
#     1/Inf^θ = 0.
#   - Self-distance (i == j) is excluded from the sum by construction.
#
# USAGE:
#   Rscript code/pipeline/04_market_access.R <case_label> [<case_label> ...]
#   If no args, processes every tau file in dir_derived_taus/.
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
})

# Elasticity label ↔ theta-name lookup
elasticity_labels <- c(elow = "low", ehigh = "high")

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    if (!dir.exists(dir_derived_ma)) {
        dir.create(dir_derived_ma, recursive = TRUE)
    }

    args <- commandArgs(trailingOnly = TRUE)
    if (length(args) > 0) {
        cases <- args
    } else {
        pq_files <- list.files(dir_derived_taus,
                               pattern = "^tau_.+\\.parquet$",
                               full.names = FALSE)
        cases <- sub("^tau_", "", sub("\\.parquet$", "", pq_files))
    }

    message("\n", strrep("=", 72))
    message("04_market_access.R  |  MA_i = sum_{j≠i} Pop_j / τ_ij^θ")
    message(strrep("=", 72))
    message(sprintf("Cases: %s\n", paste(cases, collapse = ", ")))
    message(sprintf("Elasticities: θ_low = %.3f, θ_high = %.3f",
                    theta[["low"]], theta[["high"]]))

    pop <- load_1960_population()

    for (case in cases) {
        for (elas_lbl in names(elasticity_labels)) {
            theta_name <- elasticity_labels[[elas_lbl]]
            compute_ma_one <- compute_ma_one_case
            compute_ma_one(case, pop, theta[[theta_name]], elas_lbl)
        }
    }

    message(strrep("=", 72))
    message("04_market_access.R  |  Complete.")
    message(strrep("=", 72), "\n")
}

# ---------------------------------------------------------------------------
# Load 1960 census population as the universal MA weight
# ---------------------------------------------------------------------------
load_1960_population <- function() {
    path <- file.path(dir_derived_census1960, "census_1960_ipums.parquet")
    stopifnot(file.exists(path))
    d <- arrow::read_parquet(path)
    d <- ensure_geolev2_char(d)
    stopifnot(c("geolev2", "pop") %in% names(d))
    d <- data.frame(geolev2 = d$geolev2, pop = as.numeric(d$pop))

    message(sprintf("[ma] Loaded 1960 pop: %d districts, total=%s",
                    nrow(d), format(round(sum(d$pop)), big.mark = ",")))
    d
}

# ---------------------------------------------------------------------------
# Compute MA for one case × one elasticity
# ---------------------------------------------------------------------------
compute_ma_one_case <- function(case, pop_df, theta_val, elas_lbl) {
    message(sprintf("\n[ma] ==== CASE: %s (%s, θ=%.3f) ====",
                    case, elas_lbl, theta_val))

    tau_path <- file.path(dir_derived_taus,
                          sprintf("tau_%s.parquet", case))
    tau_df <- arrow::read_parquet(tau_path)
    tau_df <- ensure_geolev2_char(tau_df, "origin_geolev2")
    tau_df <- ensure_geolev2_char(tau_df, "destination_geolev2")

    all_geo <- sort(unique(c(tau_df$origin_geolev2,
                             tau_df$destination_geolev2)))
    message(sprintf("[ma]   τ file has %d districts (%d pairs)",
                    length(all_geo), nrow(tau_df)))

    # Symmetrise: the τ file only stores the lower triangle; we need both
    # directions for each origin's MA sum.
    sym <- rbind(
        tau_df,
        data.frame(
            origin_geolev2      = tau_df$destination_geolev2,
            destination_geolev2 = tau_df$origin_geolev2,
            tau                 = tau_df$tau
        )
    )

    # Merge destination population
    sym <- merge(
        sym,
        data.frame(destination_geolev2 = pop_df$geolev2,
                   pop_dest            = pop_df$pop),
        by = "destination_geolev2", all.x = TRUE
    )
    sym$pop_dest[is.na(sym$pop_dest)] <- 0

    # 1/τ^θ; Inf τ → 0
    sym$weight <- ifelse(is.finite(sym$tau) & sym$tau > 0,
                         1 / (sym$tau^theta_val), 0)
    sym$contrib <- sym$weight * sym$pop_dest

    # Sum per origin
    ma_df <- aggregate(contrib ~ origin_geolev2, data = sym, FUN = sum)
    names(ma_df) <- c("geolev2", "MA")
    ma_df$logMA <- log(ma_df$MA)

    # Sanity: log is finite for districts with any non-zero contribution
    n_inf_log <- sum(!is.finite(ma_df$logMA))
    message(sprintf(
        "[ma]   MA computed for %d districts; %d have logMA = -Inf (MA = 0)",
        nrow(ma_df), n_inf_log
    ))
    if (n_inf_log > 0) {
        zeros <- ma_df$geolev2[!is.finite(ma_df$logMA)]
        message(sprintf("[ma]   logMA -Inf for: %s",
                        paste(head(zeros, 10), collapse = ", ")))
    }

    finite_ma <- ma_df$MA[ma_df$MA > 0]
    message(sprintf(
        "[ma]   MA summary (MA>0): min=%.3e median=%.3e max=%.3e",
        min(finite_ma), median(finite_ma), max(finite_ma)
    ))
    finite_log <- ma_df$logMA[is.finite(ma_df$logMA)]
    message(sprintf(
        "[ma]   logMA summary (finite): min=%.2f  mean=%.2f  max=%.2f",
        min(finite_log), mean(finite_log), max(finite_log)
    ))

    out_path <- file.path(dir_derived_ma,
                          sprintf("ma_%s_%s.parquet", case, elas_lbl))
    arrow::write_parquet(ma_df, out_path)
    message(sprintf("[ma]   Saved: %s", out_path))
}

main()

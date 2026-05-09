# ===========================================================================
# 06_merge_ma_into_panel.R
#
# PURPOSE: Merge Phase 1 market access columns into the wide estimation
#          panel. Adds logMA levels and logMA change variables (treatment
#          + two instruments).
#
# READS:
#   data/derived/05_panel/departments_wide_panel.parquet    (existing)
#   data/derived/04_market_access/ma_<case>.parquet         (4 cases)
#
# PRODUCES:
#   data/derived/05_panel/departments_wide_panel.parquet    (overwritten)
#       New columns:
#         logMA_actual_1960_s0_elow
#         logMA_actual_1986_s0_elow
#         logMA_instrument_stu_s0_elow
#         logMA_instrument_lcp_mst_s0_elow
#         chg_logMA_86_60_s0_elow            = actual_1986 - actual_1960
#         chg_logMA_stu_s0_elow              = instrument_stu - actual_1960
#         chg_logMA_lcp_mst_s0_elow          = instrument_lcp_mst - actual_1960
#
# NAMING CONVENTION:
#   logMA_<timing>_s<sector>_e<elasticity>
#     timing ∈ {actual_1960, actual_1986, instrument_stu, instrument_lcp_mst}
#     sector ∈ {0}  (Phase 1 only)
#     elasticity ∈ {low}  (θ = 4.55, Phase 1 only)
#
# NOTES:
#   - Districts where logMA is -Inf (e.g. TdF on a land-only cost surface)
#     produce NA in the corresponding logMA column, and NA in any change
#     variable that uses that column. NA is auto-dropped by regression
#     functions, which is the intended downstream behavior.
#   - Phase 2 will add sector 1, sector 2, and θ = 8.11 columns. This
#     script is structured so extending it means adding rows to
#     `ma_cases_spec` below.
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
})

# Case spec: case_label → column suffix used in the panel
ma_cases_spec <- list(
    list(case = "actual_1960_s0",        suffix = "actual_1960_s0_elow"),
    list(case = "actual_1986_s0",        suffix = "actual_1986_s0_elow"),
    list(case = "instrument_stu_s0",     suffix = "instrument_stu_s0_elow"),
    list(case = "instrument_lcp_mst_s0", suffix = "instrument_lcp_mst_s0_elow")
)

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    message("\n", strrep("=", 72))
    message("06_merge_ma_into_panel.R  |  merge MA into wide panel")
    message(strrep("=", 72))

    panel_path <- file.path(dir_derived_panel,
                            "departments_wide_panel.parquet")
    stopifnot(file.exists(panel_path))
    panel <- arrow::read_parquet(panel_path)
    panel <- ensure_geolev2_char(panel)
    message(sprintf("[panel] Starting panel: %d × %d", nrow(panel), ncol(panel)))

    # Drop any pre-existing logMA_* or chg_logMA_* columns so the merge is
    # idempotent if the script re-runs.
    drop <- grep("^(logMA_|chg_logMA_)", names(panel), value = TRUE)
    if (length(drop)) {
        message(sprintf("[panel] Removing %d stale MA columns: %s",
                        length(drop), paste(drop, collapse = ", ")))
        panel <- panel[, setdiff(names(panel), drop)]
    }

    # Merge each MA case
    for (spec in ma_cases_spec) {
        panel <- merge_one_case(panel, spec$case, spec$suffix)
    }

    # Add change variables
    panel <- add_change_vars(panel)

    # Save + manifest
    save_and_manifest(panel)

    message(strrep("=", 72))
    message("06_merge_ma_into_panel.R  |  Complete.")
    message(strrep("=", 72), "\n")
}

# ---------------------------------------------------------------------------
# Merge a single MA case into the panel
# ---------------------------------------------------------------------------
merge_one_case <- function(panel, case, suffix) {
    path <- file.path(dir_derived_ma, sprintf("ma_%s.parquet", case))
    stopifnot(file.exists(path))
    ma <- arrow::read_parquet(path)
    ma <- ensure_geolev2_char(ma)

    # Recode logMA = -Inf to NA (disconnected districts)
    n_neginf <- sum(!is.finite(ma$logMA) & ma$logMA < 0)
    if (n_neginf > 0) {
        message(sprintf(
            "[panel] %-25s  %d districts with logMA=-Inf recoded to NA",
            case, n_neginf
        ))
        ma$logMA[!is.finite(ma$logMA) & ma$logMA < 0] <- NA
    }
    # logMA = +Inf (shouldn't happen but handle it): NA as well
    ma$logMA[is.infinite(ma$logMA) & ma$logMA > 0] <- NA

    # Rename logMA column and drop the raw MA column (memory)
    keep <- data.frame(
        geolev2 = ma$geolev2,
        logMA   = ma$logMA
    )
    names(keep)[names(keep) == "logMA"] <- paste0("logMA_", suffix)

    n_before <- nrow(panel)
    panel <- merge(panel, keep, by = "geolev2", all.x = TRUE)
    stopifnot(nrow(panel) == n_before)
    message(sprintf("[panel] Merged %s as logMA_%s", case, suffix))
    panel
}

# ---------------------------------------------------------------------------
# Build change variables
# ---------------------------------------------------------------------------
add_change_vars <- function(panel) {
    base <- "logMA_actual_1960_s0_elow"
    pairs <- list(
        list(post = "logMA_actual_1986_s0_elow",
             out  = "chg_logMA_86_60_s0_elow"),
        list(post = "logMA_instrument_stu_s0_elow",
             out  = "chg_logMA_stu_s0_elow"),
        list(post = "logMA_instrument_lcp_mst_s0_elow",
             out  = "chg_logMA_lcp_mst_s0_elow")
    )
    for (p in pairs) {
        stopifnot(base %in% names(panel), p$post %in% names(panel))
        panel[[p$out]] <- panel[[p$post]] - panel[[base]]
        n_valid <- sum(!is.na(panel[[p$out]]))
        mn <- mean(panel[[p$out]], na.rm = TRUE)
        sdv <- sd(panel[[p$out]], na.rm = TRUE)
        message(sprintf("[panel] %-28s  N=%d  mean=%+.3f  sd=%.3f",
                        p$out, n_valid, mn, sdv))
    }
    panel
}

# ---------------------------------------------------------------------------
# Save and update manifest
# ---------------------------------------------------------------------------
save_and_manifest <- function(panel) {
    panel <- panel[order(panel$geolev2), ]
    stopifnot(!any(duplicated(panel$geolev2)))

    out_path <- file.path(dir_derived_panel,
                          "departments_wide_panel.parquet")
    arrow::write_parquet(panel, out_path)
    message(sprintf("[panel] Saved: %s (%d × %d)",
                    out_path, nrow(panel), ncol(panel)))

    # Update manifest
    log_path <- file.path(dir_derived_panel, "data_file_manifest.log")
    sink(log_path)
    on.exit(sink(), add = TRUE)
    cat("Data file manifest — 05_build_panel.R + 06_merge_ma_into_panel.R\n")
    cat(sprintf("Regenerated: %s\n", Sys.time()))
    cat(sprintf("rootdir: %s\n\n", rootdir))
    cat(strrep("=", 60), "\n")
    cat("FILE: departments_wide_panel.parquet\n")
    cat(strrep("=", 60), "\n")
    cat(sprintf("Rows: %d\nColumns: %d\nKey: geolev2\n\n",
                nrow(panel), ncol(panel)))

    cat("Summary of every variable:\n")
    for (v in names(panel)) {
        if (v == "geolev2") next
        vals <- panel[[v]]
        if (!is.numeric(vals)) {
            cat(sprintf("  %-32s (class=%s, n_unique=%d, NA=%d)\n",
                        v, class(vals)[1],
                        length(unique(vals)), sum(is.na(vals))))
            next
        }
        n_na <- sum(is.na(vals))
        n_ok <- sum(!is.na(vals))
        if (n_ok == 0) {
            cat(sprintf("  %-32s  ALL NA\n", v))
            next
        }
        cat(sprintf(
            "  %-32s N=%d  mean=%.3g  sd=%.3g  min=%.3g  max=%.3g  NA=%d\n",
            v, n_ok,
            mean(vals, na.rm = TRUE), sd(vals, na.rm = TRUE),
            min(vals, na.rm = TRUE), max(vals, na.rm = TRUE),
            n_na
        ))
    }

    message(sprintf("[panel] Manifest: %s", log_path))
}

main()

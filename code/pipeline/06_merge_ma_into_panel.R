# ===========================================================================
# 06_merge_ma_into_panel.R
#
# PURPOSE: Merge market access columns into the wide estimation panel.
#          Adds logMA levels and Δ logMA change variables for every
#          (case × sector × elasticity) combination.
#
# READS:
#   data/derived/05_panel/departments_wide_panel.parquet    (existing)
#   data/derived/04_market_access/ma_<case>_<elas>.parquet  (24 files)
#
# PRODUCES:
#   data/derived/05_panel/departments_wide_panel.parquet    (overwritten)
#
#   New columns (Phase 1 + Phase 2a + Phase 2b + Phase 2c):
#     logMA_<case>_<s>_<e>           — 42 level columns
#     chg_logMA_<timing>_<s>_<e>     — 36 change columns
#   where:
#     case ∈ {actual_1960, actual_1986, instrument_stu,
#             instrument_lcp_mst, instrument_euc_mst,
#             instrument_lcp, instrument_euc}
#     s    ∈ {s0, s1, s2}
#     e    ∈ {elow, ehigh}
#     timing ∈ {86_60, stu, lcp_mst, euc_mst, lcp, euc}
#
# NAMING CONVENTION:
#   logMA_<case>_<s>_<e>
#     s0/s1/s2 = overall / agricultural / manufacturing
#     elow/ehigh = θ = 4.55 / 8.11
#   chg_logMA_<timing>_<s>_<e>
#     86_60   = actual_1986 − actual_1960 (main treatment variable)
#     stu     = instrument_stu − actual_1960 (Larkin-plan instrument)
#     lcp_mst = instrument_lcp_mst − actual_1960 (hypothetical-road instrument)
#
# NOTES:
#   - Districts where logMA is −Inf (e.g. TdF on a land-only cost surface)
#     produce NA in the corresponding logMA column, and NA in any change
#     variable that uses that column. NA is auto-dropped by regression
#     functions, which is the intended downstream behavior.
#   - The idempotent logic drops any pre-existing logMA_* / chg_logMA_*
#     columns at the start so re-runs don't duplicate columns.
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
})

# ---------------------------------------------------------------------------
# Build ma_cases_spec programmatically:
#   4 network specs × 3 sectors × 2 elasticities = 24 cases.
# Each entry: the MA file to read is ma_<case>_<elas>.parquet; the
# column written to the panel is logMA_<case>_<elas>.
# ---------------------------------------------------------------------------
build_ma_cases_spec <- function() {
    network_specs <- c("actual_1960", "actual_1986",
                       "instrument_stu",
                       "instrument_lcp_mst", "instrument_euc_mst",
                       "instrument_lcp",     "instrument_euc")
    sectors       <- c("s0", "s1", "s2")
    elasticities  <- c("elow", "ehigh")

    out <- list()
    for (n in network_specs) {
        for (s in sectors) {
            for (e in elasticities) {
                case_lbl <- paste(n, s, sep = "_")
                suffix   <- paste(case_lbl, e, sep = "_")
                out[[length(out) + 1L]] <- list(case = case_lbl,
                                                 elas = e,
                                                 suffix = suffix)
            }
        }
    }
    out
}
ma_cases_spec <- build_ma_cases_spec()

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

    # Merge each MA case × elasticity
    for (spec in ma_cases_spec) {
        panel <- merge_one_case(panel, spec$case, spec$elas, spec$suffix)
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
# Merge a single MA case × elasticity into the panel
# ---------------------------------------------------------------------------
merge_one_case <- function(panel, case, elas, suffix) {
    path <- file.path(dir_derived_ma,
                      sprintf("ma_%s_%s.parquet", case, elas))
    stopifnot(file.exists(path))
    ma <- arrow::read_parquet(path)
    ma <- ensure_geolev2_char(ma)

    # Recode logMA = -Inf to NA (disconnected districts)
    n_neginf <- sum(!is.finite(ma$logMA) & ma$logMA < 0)
    if (n_neginf > 0) {
        message(sprintf(
            "[panel] %-35s  %d districts with logMA=-Inf recoded to NA",
            suffix, n_neginf
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
    panel
}

# ---------------------------------------------------------------------------
# Build change variables.
#
# For each (sector, elasticity) combination, build three change variables:
#   chg_logMA_86_60_<s>_<e>   = logMA_actual_1986 - logMA_actual_1960
#   chg_logMA_stu_<s>_<e>     = logMA_instrument_stu - logMA_actual_1960
#   chg_logMA_lcp_mst_<s>_<e> = logMA_instrument_lcp_mst - logMA_actual_1960
# Total: 3 timings × 3 sectors × 2 elasticities = 18 change variables.
# ---------------------------------------------------------------------------
add_change_vars <- function(panel) {
    timings <- c(
        "86_60"   = "logMA_actual_1986",
        "stu"     = "logMA_instrument_stu",
        "lcp_mst" = "logMA_instrument_lcp_mst",
        "euc_mst" = "logMA_instrument_euc_mst",
        "lcp"     = "logMA_instrument_lcp",
        "euc"     = "logMA_instrument_euc"
    )
    for (s in c("s0", "s1", "s2")) {
        for (e in c("elow", "ehigh")) {
            base <- sprintf("logMA_actual_1960_%s_%s", s, e)
            stopifnot(base %in% names(panel))
            for (tlabel in names(timings)) {
                post <- sprintf("%s_%s_%s", timings[[tlabel]], s, e)
                stopifnot(post %in% names(panel))
                out <- sprintf("chg_logMA_%s_%s_%s", tlabel, s, e)
                panel[[out]] <- panel[[post]] - panel[[base]]
            }
        }
    }

    # Log the change variables
    chg_cols <- grep("^chg_logMA_", names(panel), value = TRUE)
    message(sprintf("[panel] %d change variables built:", length(chg_cols)))
    for (v in chg_cols) {
        vals <- panel[[v]]
        n_valid <- sum(!is.na(vals))
        message(sprintf("  %-45s  N=%d  mean=%+.3f  sd=%.3f",
                        v, n_valid,
                        mean(vals, na.rm = TRUE),
                        sd(vals, na.rm = TRUE)))
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

    cat(
      "NOTE ON logMA/chg_logMA COLUMNS:\n",
      "  24 logMA columns = 4 cases × 3 sectors × 2 elasticities.\n",
      "  18 chg_logMA columns = 3 timings × 3 sectors × 2 elasticities.\n",
      "\n",
      "  Sector labels: s0 = overall (medium density); s1 = agricultural\n",
      "  (high density); s2 = manufacturing (low density).\n",
      "  Elasticity labels: elow = θ = 4.55; ehigh = θ = 8.11.\n",
      "\n",
      "  Tierra del Fuego districts (32094001, 32094002) are NA in all\n",
      "  land-only logMA columns (actual_*, instrument_stu) because they\n",
      "  are disconnected from the mainland on the Phase 1/2a land-only\n",
      "  cost surface. Phase 2b navigation will reconnect them via sea\n",
      "  routes. The instrument_lcp_mst columns have values for them\n",
      "  because the LCP-MST hypothetical network was routed on a Faber\n",
      "  cost surface that treats water as passable (25x baseline), not\n",
      "  infinite.\n",
      "\n", sep = ""
    )

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

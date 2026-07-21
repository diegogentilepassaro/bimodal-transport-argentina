# ===========================================================================
# diagnostic_recentering_draws.R
#
# PURPOSE: Stage 1 of the recentering diagnostic (Borusyak & Hull 2023):
#          the permutation engine. For each draw s = 1..S, reassign the
#          Larkin study designations across line units WITHIN strata
#          (region x branch; see diagnostic_recentering_lines.R), rebuild
#          the instrument_stu cost raster with the permuted assignment,
#          and push it through the existing, tested pipeline chain
#          (03a raster -> 03b transition -> 03c tau -> 04 MA) as child
#          processes. Store each draw's district-level logMA vector.
#
#          Draw 0 is the IDENTITY draw: studied_perm = studied_co. Its
#          logMA must match the baseline ma_instrument_stu_s0_elow
#          parquet to float precision; this validates the entire hook
#          chain (RECENTER_ASSIGN_FILE / RECENTER_TAG in 03a) before any
#          permuted draw runs.
#
# DESIGN:
#   - Permutation: within each stratum, sample() the vector of unit
#     statuses across units (preserves the count of studied units per
#     cell). Deterministic: draw s uses set.seed(recentering_seed + s),
#     units ordered by line_id.
#   - Checkpointing: each draw writes
#     data/derived/07_recentering/draws/z_rc<s>.parquet and existing
#     draws are skipped, so a crashed or interrupted run resumes.
#   - Disk hygiene: per-draw intermediates (raster ~30 MB, transition
#     ~500 MB, tau, ma parquets) are DELETED after the logMA vector is
#     stored. 100 draws would otherwise leave ~50 GB behind.
#   - Child env vars via Sys.setenv (inherited), not system2 env=
#     (portability; same as 07_unimodal_taus.R per cr-review PR #100).
#
# RUNTIME: ~4.3 min per draw. Draws 1..S run in parallel across
#          n_workers forked processes (default n_cores_heavy = 4; each
#          chain is single-threaded with ~1.1 GB peak, so 4 workers stay
#          within the memory envelope n_cores_heavy was calibrated for
#          after PR #97's crash, and leave most cores free).
#          S = 100 with 4 workers => ~2 h. The identity draw runs first,
#          alone, as a gate.
#
# READS:
#   data/derived/07_recentering/rail_segments_strata.parquet
#   data/derived/07_recentering/rail_lines_strata.parquet
#   data/derived/04_market_access/ma_instrument_stu_s0_elow.parquet
#     (baseline, for the identity check)
#
# PRODUCES:
#   data/derived/07_recentering/draws/z_rc<s>.parquet  (s = 0..S)
#       Columns: geolev2 (chr, key), logMA (num), draw (int),
#                studied_km (num, total under the draw's assignment).
#   data/derived/07_recentering/draws_manifest.log
#
# USAGE:
#   Rscript code/analysis/diagnostic_recentering_draws.R [S] [n_workers]
#   S defaults to recentering_S (config.R); n_workers defaults to
#   n_cores_heavy. Smoke test: S = 3.
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
    library(parallel)
})

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    args <- commandArgs(trailingOnly = TRUE)
    S <- if (length(args) >= 1) as.integer(args[1]) else recentering_S
    n_workers <- if (length(args) >= 2) as.integer(args[2]) else
        n_cores_heavy
    stopifnot(!is.na(S), S >= 1L, !is.na(n_workers), n_workers >= 1L)

    message("\n", strrep("=", 72))
    message(sprintf(
        "diagnostic_recentering_draws.R  |  S = %d draws + identity, %d workers",
        S, n_workers))
    message(strrep("=", 72))

    dir_draws <- file.path(dir_derived_recentering, "draws")
    if (!dir.exists(dir_draws)) dir.create(dir_draws, recursive = TRUE)

    seg <- arrow::read_parquet(
        file.path(dir_derived_recentering, "rail_segments_strata.parquet"))
    lin <- arrow::read_parquet(
        file.path(dir_derived_recentering, "rail_lines_strata.parquet"))
    stopifnot(nrow(seg) == 565L, all(seg$seg_row == seq_len(565L)))
    lin <- lin[order(lin$line_id), ]   # deterministic permutation order

    rscript <- file.path(R.home("bin"), "Rscript")
    p <- function(fname) file.path(dir_code, "pipeline", fname)

    run_child <- function(script, args) {
        status <- system2(rscript, c(shQuote(script), shQuote(args)),
                          stdout = "", stderr = "")
        if (!identical(status, 0L)) {
            stop(sprintf("[draws] child failed (%d): %s %s",
                         status, basename(script),
                         paste(args, collapse = " ")))
        }
    }

    # ---- one draw end-to-end -------------------------------------------------
    run_draw <- function(s) {
        tag  <- sprintf("rc%03d", s)
        out  <- file.path(dir_draws, sprintf("z_%s.parquet", tag))
        if (file.exists(out)) {
            message(sprintf("[draws] %s: exists, skipping", tag))
            return(invisible())
        }
        t0 <- Sys.time()

        # Permuted line-unit assignment (identity for s = 0).
        perm_status <- lin$studied_co
        if (s > 0L) {
            set.seed(recentering_seed + s)
            for (st in unique(lin$stratum)) {
                sel <- which(lin$stratum == st)
                perm_status[sel] <- sample(lin$studied_co[sel])
            }
        }
        # Per-stratum counts preserved by construction; assert anyway.
        stopifnot(identical(
            tapply(perm_status, lin$stratum, sum),
            tapply(lin$studied_co, lin$stratum, sum)))

        # Segment-level assignment file for the 03a hook.
        perm_of_line <- setNames(perm_status, lin$line_id)
        assign_df <- data.frame(
            seg_row      = seg$seg_row,
            studied_co   = seg$studied_co,
            studied_perm = as.integer(perm_of_line[as.character(seg$line_id)])
        )
        studied_km <- sum(seg$length_km[assign_df$studied_perm == 1L])
        afile <- file.path(tempdir(), sprintf("recenter_assign_%s.csv", tag))
        utils::write.csv(assign_df, afile, row.names = FALSE)

        case   <- "instrument_stu_s0"
        case_t <- sprintf("%s_%s", case, tag)

        Sys.setenv(RECENTER_ASSIGN_FILE = afile, RECENTER_TAG = tag)
        on.exit(Sys.unsetenv(c("RECENTER_ASSIGN_FILE", "RECENTER_TAG")),
                add = TRUE)

        run_child(p("03a_build_cost_raster.R"), case)
        Sys.unsetenv(c("RECENTER_ASSIGN_FILE", "RECENTER_TAG"))
        run_child(p("03b_transition_grids.R"), case_t)
        run_child(p("03c_compute_taus_parallel.R"), c("1", case_t))
        run_child(p("04_market_access.R"), case_t)

        ma_path <- file.path(dir_derived_ma,
                             sprintf("ma_%s_elow.parquet", case_t))
        ma <- arrow::read_parquet(ma_path)
        ma <- ensure_geolev2_char(ma)
        stopifnot(nrow(ma) == 312L, !any(duplicated(ma$geolev2)))

        z <- data.frame(geolev2    = ma$geolev2,
                        logMA      = ma$logMA,
                        draw       = s,
                        studied_km = studied_km)
        z <- z[order(z$geolev2), ]
        arrow::write_parquet(z, out)

        # Identity check: draw 0 must reproduce the baseline instrument MA.
        if (s == 0L) {
            base_path <- file.path(dir_derived_ma,
                                   "ma_instrument_stu_s0_elow.parquet")
            stopifnot(file.exists(base_path))
            base <- ensure_geolev2_char(arrow::read_parquet(base_path))
            m <- merge(z, base[, c("geolev2", "logMA")],
                       by = "geolev2", suffixes = c("_id", "_base"))
            stopifnot(nrow(m) == 312L)
            max_dev <- max(abs(m$logMA_id - m$logMA_base), na.rm = TRUE)
            message(sprintf(
                "[draws] identity check: max |logMA - baseline| = %.2e",
                max_dev))
            if (max_dev > 1e-8) {
                stop("[draws] IDENTITY CHECK FAILED: the hook chain does ",
                     "not reproduce the baseline instrument MA (max dev ",
                     sprintf("%.2e", max_dev), "). Do not run draws.")
            }
        }

        # Disk hygiene: remove this draw's intermediates.
        unlink(c(
            file.path(dir_derived_rasters,
                      sprintf("ucost_%s.tif", case_t)),
            file.path(dir_derived_transitions,
                      sprintf("transition_%s.rds", case_t)),
            file.path(dir_derived_taus,
                      sprintf("tau_%s.parquet", case_t)),
            file.path(dir_derived_ma,
                      sprintf("ma_%s_elow.parquet", case_t)),
            file.path(dir_derived_ma,
                      sprintf("ma_%s_ehigh.parquet", case_t)),
            afile
        ))

        message(sprintf(
            "[draws] %s done in %.1f min (studied km = %.0f)",
            tag, as.numeric(difftime(Sys.time(), t0, units = "mins")),
            studied_km))
    }

    # Identity draw first, alone: it gates the permuted draws (a failed
    # identity check must stop everything before any compute is spent).
    run_draw(0L)

    # Permuted draws in parallel. Forked workers have independent
    # environments, so the per-worker Sys.setenv calls cannot collide;
    # assignment files carry per-tag names in the shared tempdir.
    results <- mclapply(seq_len(S), function(s) {
        tryCatch({ run_draw(s); NULL },
                 error = function(e) sprintf("rc%03d: %s", s,
                                             conditionMessage(e)))
    }, mc.cores = n_workers, mc.preschedule = FALSE)
    fails <- Filter(Negate(is.null), results)
    if (length(fails) > 0L) {
        for (f in fails) message("[draws] FAIL ", f)
        stop(sprintf("[draws] %d of %d draws failed", length(fails), S))
    }

    done <- list.files(file.path(dir_derived_recentering, "draws"),
                       pattern = "^z_rc\\d+\\.parquet$")
    sink(file.path(dir_derived_recentering, "draws_manifest.log"))
    cat("Data file manifest -- diagnostic_recentering_draws.R\n")
    cat(sprintf("Generated: %s\n", Sys.time()))
    cat(sprintf("Draw files present: %d (incl. identity rc000)\n",
                length(done)))
    cat(sprintf("Seed base: %d  |  snap tol: %d m\n",
                recentering_seed, recentering_snap_tol_m))
    sink()
    message(sprintf("[draws] Complete: %d draw files present.",
                    length(done)))
}

main()

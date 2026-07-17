# ===========================================================================
# 07_unimodal_taus.R
#
# PURPOSE: Build the six single-mode tau matrices that bound the
#          transshipment assumption (Section 5.5's transshipment-bound
#          paragraph; diagnostic_ma_unimodal.R consumes them):
#
#            tau_actual_{1960,1986}_s0_{road,rail,water}only.parquet
#
#          The baseline cost surface allows free mode switching (zero
#          transshipment cost). Each single-mode surface keeps one mode
#          plus off-network land; the infinite-transshipment tau is the
#          element-wise min across the three (one mode per trip).
#
# DESIGN:  Drives the existing, tested pipeline scripts as child
#          Rscript processes rather than re-implementing their logic:
#          03a_build_cost_raster.R honours the MODE_VARIANT environment
#          variable (roadonly/railonly/wateronly), 03b builds the
#          transition, and 03c_compute_taus_parallel.R computes the
#          missing taus in one call capped at n_cores_heavy workers.
#          This replaces the manual run_unimodal_variant.sh screen
#          driver (kept for reference) so the hands-off main.R run
#          regenerates every number the paper quotes.
#
#          Idempotent: cases whose tau parquet already exists are
#          skipped entirely (same convention as 03c's no-args mode).
#
# RUNTIME: ~15-25 minutes cold (6 rasters + 6 transitions + 6 taus,
#          taus parallelised across n_cores_heavy workers).
#
# READS:   data/derived/base/** (via 03a), 01_cost_rasters, transitions
# PRODUCES:
#   data/derived/01_cost_rasters/ucost_actual_{1960,1986}_s0_<mode>only.tif
#   data/derived/02_transition_grids/transition_..._<mode>only.rds
#   data/derived/03_taus/tau_actual_{1960,1986}_s0_<mode>only.parquet
# ===========================================================================

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)

    rscript <- file.path(R.home("bin"), "Rscript")
    p <- function(fname) file.path(dir_code, "pipeline", fname)

    run_child <- function(script, args, env = character()) {
        status <- system2(rscript, c(shQuote(script), shQuote(args)),
                          env = env, stdout = "", stderr = "")
        if (!identical(status, 0L)) {
            stop(sprintf("[07] child failed (%d): %s %s [%s]",
                         status, basename(script), args,
                         paste(env, collapse = " ")))
        }
    }

    bases    <- c("actual_1960_s0", "actual_1986_s0")
    variants <- c("roadonly", "railonly", "wateronly")

    todo_taus <- character()
    for (base in bases) {
        for (v in variants) {
            case_v <- sprintf("%s_%s", base, v)
            tau_p <- file.path(dir_derived_taus,
                               sprintf("tau_%s.parquet", case_v))
            if (file.exists(tau_p)) {
                message(sprintf("[07] %s: tau exists, skipping", case_v))
                next
            }
            message(sprintf("[07] %s: raster", case_v))
            run_child(p("03a_build_cost_raster.R"), base,
                      env = sprintf("MODE_VARIANT=%s", v))
            message(sprintf("[07] %s: transition", case_v))
            run_child(p("03b_transition_grids.R"), case_v)
            todo_taus <- c(todo_taus, case_v)
        }
    }

    if (length(todo_taus) > 0L) {
        message(sprintf("[07] computing %d taus (ncores = %d): %s",
                        length(todo_taus), n_cores_heavy,
                        paste(todo_taus, collapse = ", ")))
        run_child(p("03c_compute_taus_parallel.R"),
                  c(as.character(n_cores_heavy), todo_taus))
    } else {
        message("[07] all six single-mode taus present; nothing to do")
    }

    # Final assertion: every consumer-facing artifact exists.
    expected <- as.vector(outer(bases, variants, function(b, v)
        file.path(dir_derived_taus, sprintf("tau_%s_%s.parquet", b, v))))
    missing <- expected[!file.exists(expected)]
    if (length(missing) > 0L) {
        stop("[07] missing single-mode taus after run:\n",
             paste(" -", missing, collapse = "\n"))
    }
    message("[07] all six single-mode taus verified present")
}

main()

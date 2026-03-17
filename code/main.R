# ==============================================================================
# main.R
#
# PURPOSE: Master controller for the full replication pipeline. Sources every
#          script in sequence, from environment bootstrap through final tables
#          and figures. Running this file end-to-end (via R CMD BATCH) is the
#          single required step to reproduce all results from raw data.
#
# USAGE:
#   From a terminal at the repo root:
#     R CMD BATCH --no-save --no-restore code/main.R logs/main.Rout
#   Or interactively in RStudio (for development):
#     source("code/main.R")
#
# READS:   code/config.R, code/00_setup.R, code/01–07_*.R
# PRODUCES:
#   logs/main.Rout             — full console log (via R CMD BATCH)
#   logs/makelog.log           — timestamped build log written by this script
#   results/tables/*.tex       — LaTeX table fragments
#   results/figures/*.pdf      — figures
#   results/scalars.tex        — in-text numbers via AutoFill pattern
# ==============================================================================

# ---- 0. Bootstrap: here guard then config and renv -------------------------
if (!requireNamespace("here", quietly = TRUE)) {
    stop(
        "[main.R] The 'here' package is not installed.\n",
        "         Install it once with: install.packages('here')\n",
        "         Then re-run: R CMD BATCH --no-save --no-restore code/main.R"
    )
}

source(file.path(here::here(), "code", "config.R"), echo = FALSE)
source(file.path(dir_code, "00_setup.R"),            echo = TRUE)

# ==============================================================================
# LOW-LEVEL HELPERS
# ==============================================================================

log_step <- function(status, step_id, description, logfile) {
    ts  <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    sep <- strrep("-", 72)
    msg <- sprintf("[%s]  %s  |  %s  |  %s", ts, status, step_id, description)
    cat(sep,  "\n", file = logfile, append = TRUE)
    cat(msg,  "\n", file = logfile, append = TRUE)
    message(sep)
    message(msg)
}

run_r_step <- function(step_id, script, description, logfile) {
    log_step("START", step_id, description, logfile)
    t_start <- proc.time()
    source(script, echo = TRUE)
    elapsed <- round((proc.time() - t_start)[["elapsed"]])
    log_step(sprintf("END [%ds]", elapsed), step_id, description, logfile)
    invisible(elapsed)
}

run_py_step <- function(step_id, script, description, logfile, py_logfile) {
    log_step("START", step_id, description, logfile)
    t_start <- proc.time()

    python_exe <- Sys.which("python3")
    if (nchar(python_exe) == 0L) python_exe <- Sys.which("python")
    if (nchar(python_exe) == 0L) {
        stop("[main.R] Python executable not found on PATH.")
    }

    env_vars <- c(
        sprintf("ROOTDIR=%s",   rootdir),
        sprintf("PATH=%s",      Sys.getenv("PATH")),
        sprintf("GDAL_DATA=%s", Sys.getenv("GDAL_DATA"))
    )

    exit_code <- system2(
        command = python_exe,
        args    = shQuote(script),
        stdout  = py_logfile,
        stderr  = py_logfile,
        wait    = TRUE,
        env     = env_vars
    )

    elapsed <- round((proc.time() - t_start)[["elapsed"]])
    cat(sprintf("  Python log: %s\n", py_logfile), file = logfile, append = TRUE)

    if (exit_code != 0L) {
        py_tail <- tryCatch(
            tail(readLines(py_logfile, warn = FALSE), 30L),
            error = function(e) character(0)
        )
        stop(
            sprintf("[main.R] %s failed with exit code %d.\n",
                    basename(script), exit_code),
            "Last lines of Python log:\n",
            paste(py_tail, collapse = "\n")
        )
    }

    log_step(sprintf("END [%ds]", elapsed), step_id, description, logfile)
    invisible(elapsed)
}

verify_outputs <- function(step_id, files, logfile) {
    missing <- files[!file.exists(files)]
    if (length(missing) > 0L) {
        msg <- sprintf(
            "[main.R] Step %s: %d expected output(s) missing:\n%s",
            step_id, length(missing),
            paste(" -", missing, collapse = "\n")
        )
        cat(msg, "\n", file = logfile, append = TRUE)
        stop(msg)
    }
    cat(sprintf("  [verify] All %d expected outputs present.\n", length(files)),
        file = logfile, append = TRUE)
    message(sprintf("[main.R] verify_outputs: %d/%d present for %s.",
                    length(files), length(files), step_id))
}

log_output_inventory <- function(logfile) {
    cat(sprintf("\n%s\nOUTPUT FILE INVENTORY  (%s)\n%s\n",
                strrep("-", 72),
                format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
                strrep("-", 72)),
        file = logfile, append = TRUE)

    result_files <- list.files(dir_results, recursive = TRUE, full.names = TRUE)

    if (length(result_files) == 0L) {
        cat("  (no files in results/)\n", file = logfile, append = TRUE)
        return(invisible(NULL))
    }

    for (f in sort(result_files)) {
        info <- file.info(f)
        cat(sprintf("  %-65s  %8.1f KB  %s\n",
                    sub(rootdir, "", f, fixed = TRUE),
                    info$size / 1024,
                    format(info$mtime, "%Y-%m-%d %H:%M")),
            file = logfile, append = TRUE)
    }
}

# ==============================================================================
# PIPELINE STEP FUNCTIONS
# Each step function encapsulates one numbered script: it declares expected
# outputs, calls run_r_step / run_py_step, then calls verify_outputs.
# ==============================================================================

step_01_rasters <- function(makelog) {
    expected <- unlist(lapply(network_periods_main, function(p)
        lapply(network_sectors, function(s)
            file.path(dir_derived_rasters,
                      sprintf("ucost_%s_%d.tif", p, s)))))

    run_py_step(
        step_id     = "01_build_rasters",
        script      = file.path(dir_code, "01_build_rasters.py"),
        description = "Build cost rasters from network shapefiles (Python/rasterio)",
        logfile     = makelog,
        py_logfile  = file.path(dir_logs, "01_build_rasters.log")
    )
    verify_outputs("01_build_rasters", expected, makelog)
}

step_02_transitions <- function(makelog) {
    expected <- unlist(lapply(network_periods_main, function(p)
        lapply(network_sectors, function(s)
            file.path(dir_derived_transitions,
                      sprintf("transition_%s_%d.rds", p, s)))))

    run_r_step(
        step_id     = "02_transition_grids",
        script      = file.path(dir_code, "02_transition_grids.R"),
        description = "Convert cost rasters to gdistance transition grids",
        logfile     = makelog
    )
    verify_outputs("02_transition_grids", expected, makelog)
}

step_03_taus <- function(makelog) {
    expected <- unlist(lapply(network_periods_main, function(p)
        lapply(network_sectors, function(s)
            file.path(dir_derived_taus,
                      sprintf("tau_%s_%d.parquet", p, s)))))

    run_r_step(
        step_id     = "03_compute_taus",
        script      = file.path(dir_code, "03_compute_taus.R"),
        description = "Dijkstra least-cost paths → 312×312 tau matrices (parallel)",
        logfile     = makelog
    )
    verify_outputs("03_compute_taus", expected, makelog)
}

step_04_ma <- function(makelog) {
    expected <- file.path(dir_derived_ma, "market_access.parquet")

    run_r_step(
        step_id     = "04_market_access",
        script      = file.path(dir_code, "04_market_access.R"),
        description = "Market access indices MA = Σ Pop_j / tau_ij^θ",
        logfile     = makelog
    )
    verify_outputs("04_market_access", expected, makelog)
}

step_05_panel <- function(makelog) {
    expected <- file.path(dir_derived_panel, "panel_estimation.parquet")

    run_r_step(
        step_id     = "05_build_panel",
        script      = file.path(dir_code, "05_build_panel.R"),
        description = "Merge all sources → 312-district estimation panel",
        logfile     = makelog
    )
    verify_outputs("05_build_panel", expected, makelog)
}

step_06_analysis <- function(makelog) {
    expected <- c(
        file.path(dir_results, "scalars.tex"),
        file.path(dir_tables,  "table_first_stage.tex"),
        file.path(dir_tables,  "table_main_population.tex"),
        file.path(dir_tables,  "table_main_employment.tex"),
        file.path(dir_tables,  "table_summary_stats.tex")
    )

    run_r_step(
        step_id     = "06_analysis",
        script      = file.path(dir_code, "06_analysis.R"),
        description = "OLS + IV regressions, modelsummary tables, AutoFill scalars",
        logfile     = makelog
    )
    verify_outputs("06_analysis", expected, makelog)
}

step_07_maps <- function(makelog) {
    expected <- c(
        file.path(dir_figures, "figure_1_network_changes.pdf"),
        file.path(dir_figures, "figure_2_delta_ma.pdf"),
        file.path(dir_figures, "figure_3_rail_road_scatter.pdf"),
        file.path(dir_figures, "figure_4_infra_ma_scatter.pdf")
    )

    run_r_step(
        step_id     = "07_maps",
        script      = file.path(dir_code, "07_maps.R"),
        description = "Maps and figures (tmap + ggplot2 → PDF + PNG)",
        logfile     = makelog
    )
    verify_outputs("07_maps", expected, makelog)
}

# ==============================================================================
# MAIN
# ==============================================================================

main <- function() {

    makelog <- file.path(dir_logs, "makelog.log")
    dir.create(dir_logs, recursive = TRUE, showWarnings = FALSE)

    cat(strrep("=", 72), "\n",              file = makelog, append = FALSE)
    cat("REPLICATION PACKAGE — BUILD LOG\n", file = makelog, append = TRUE)
    cat(sprintf("Started:   %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
        file = makelog, append = TRUE)
    cat(sprintf("rootdir:   %s\n", rootdir), file = makelog, append = TRUE)
    cat(sprintf("R version: %s\n", R.version.string), file = makelog, append = TRUE)
    cat(strrep("=", 72), "\n",              file = makelog, append = TRUE)

    build_start <- proc.time()

    # ---- Run pipeline steps -------------------------------------------------
    step_01_rasters(makelog)
    step_02_transitions(makelog)
    step_03_taus(makelog)
    step_04_ma(makelog)
    step_05_panel(makelog)
    step_06_analysis(makelog)
    step_07_maps(makelog)

    # ---- Timing summary -----------------------------------------------------
    total_elapsed <- round((proc.time() - build_start)[["elapsed"]])
    summary_msg   <- sprintf(
        "\nBuild complete: %s\nTotal wall-clock time: %d min %d sec\n",
        format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
        total_elapsed %/% 60L,
        total_elapsed %%  60L
    )

    cat(strrep("=", 72), "\n", file = makelog, append = TRUE)
    cat(summary_msg,            file = makelog, append = TRUE)
    cat(strrep("=", 72), "\n", file = makelog, append = TRUE)
    message(strrep("=", 72))
    message(summary_msg)
    message(strrep("=", 72))

    log_output_inventory(makelog)

    invisible(TRUE)
}

# ==============================================================================
# ENTRY POINT
# ==============================================================================
main()

# ==============================================================================
# 00_setup.R
#
# PURPOSE: Environment bootstrap for the replication package.
#   1. Restores the renv lockfile so all R packages are at exact pinned versions.
#   2. Creates all output directories that downstream scripts write into.
#   3. Prints a diagnostic snapshot (R version, platform, key package versions).
#
# READS:   renv.lock (at rootdir)
# PRODUCES: directory tree under data/derived/ and results/; logs/session_info.txt
#
# RUN VIA: sourced from main.R — do not run in isolation.
# ==============================================================================

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)

    message("\n", strrep("=", 72))
    message("00_setup.R  |  Environment bootstrap")
    message(strrep("=", 72))

    # ---- 1. renv: restore exact package versions ----------------------------
    bootstrap_renv()

    # ---- 2. Create output directory tree ------------------------------------
    create_output_dirs()

    # ---- 3. Check Python and system dependencies ----------------------------
    check_system_deps()

    # ---- 4. Print and save session info -------------------------------------
    print_session_info()

    message(strrep("=", 72))
    message("00_setup.R  |  Bootstrap complete.")
    message(strrep("=", 72), "\n")
}

# ---- Helper: bootstrap renv -------------------------------------------------
bootstrap_renv <- function() {
    message("\n[setup] Step 1 — renv: restoring package library from lockfile")

    if (!requireNamespace("renv", quietly = TRUE)) {
        message("[setup]   renv not found — installing from CRAN")
        install.packages("renv", repos = "https://cloud.r-project.org")
    }

    lockfile <- file.path(rootdir, "renv.lock")

    if (!file.exists(lockfile)) {
        message("[setup]   renv.lock not found — running renv::init() to create it")
        message("[setup]   NOTE: run renv::snapshot() after installing all packages")
        renv::init(project = rootdir, restart = FALSE)
    } else {
        message(sprintf("[setup]   Restoring from %s", lockfile))
        renv::restore(project = rootdir, prompt = FALSE)
        message("[setup]   renv::restore() complete")
    }
}

# ---- Helper: create all output directories -----------------------------------
create_output_dirs <- function() {
    message("\n[setup] Step 2 — creating output directory tree")

    dirs_to_create <- c(
        dir_derived_rasters,
        dir_derived_transitions,
        dir_derived_taus,
        dir_derived_ma,
        dir_derived_census,
        dir_derived_agr,
        dir_derived_ind,
        dir_derived_geo,
        dir_derived_networks,
        dir_derived_panel,
        dir_derived_analysis,
        dir_tables,
        dir_figures,
        dir_logs,
        file.path(dir_derived, "temp")
    )

    for (d in dirs_to_create) {
        if (!dir.exists(d)) {
            dir.create(d, recursive = TRUE, showWarnings = FALSE)
            message(sprintf("[setup]   Created: %s", d))
        } else {
            message(sprintf("[setup]   Exists:  %s", d))
        }
    }
}

# ---- Helper: check Python and GDAL ------------------------------------------
check_system_deps <- function() {
    message("\n[setup] Step 3 — checking system dependencies")

    python_cmd <- Sys.which("python3")
    if (nchar(python_cmd) == 0L) python_cmd <- Sys.which("python")

    if (nchar(python_cmd) == 0L) {
        warning(
            "[setup] Python not found on PATH. Script 01_build_rasters.py will fail.\n",
            "        Install Python 3.9+ and ensure it is on PATH before running main.R."
        )
    } else {
        py_ver <- tryCatch(
            system2(python_cmd, args = "--version", stdout = TRUE, stderr = TRUE),
            error = function(e) "unknown"
        )
        message(sprintf("[setup]   Python: %s  (%s)", py_ver[1], python_cmd))

        req_file <- file.path(rootdir, "requirements.txt")
        if (!file.exists(req_file)) {
            warning("[setup] requirements.txt not found at ", req_file)
        } else {
            message(sprintf("[setup]   requirements.txt found: %s", req_file))
        }
    }

    gdal_cmd <- Sys.which("gdal_translate")
    if (nchar(gdal_cmd) == 0L) {
        warning(
            "[setup] gdal_translate not found. Spatial raster operations may fail.\n",
            "        Install GDAL >= 3.0 (conda: `conda install -c conda-forge gdal`)."
        )
    } else {
        gdal_ver <- tryCatch(
            system2(gdal_cmd, args = "--version", stdout = TRUE, stderr = TRUE),
            error = function(e) "unknown"
        )
        message(sprintf("[setup]   GDAL: %s", gdal_ver[1]))
    }
}

# ---- Helper: print and save session info -------------------------------------
print_session_info <- function() {
    message("\n[setup] Step 4 — R session diagnostics")

    si <- sessionInfo()
    message(sprintf("[setup]   R version:  %s", R.version.string))
    message(sprintf("[setup]   Platform:   %s", si$platform))
    message(sprintf("[setup]   OS:         %s", si$running))
    message(sprintf("[setup]   rootdir:    %s", rootdir))
    message(sprintf("[setup]   renv lib:   %s", .libPaths()[1]))

    key_pkgs <- list(
        sf = "vector spatial data", terra = "raster data",
        gdistance = "least-cost path / Dijkstra", arrow = "parquet I/O",
        dplyr = "data manipulation", tidyr = "reshaping",
        fixest = "OLS / IV / panel FE", modelsummary = "LaTeX table generation",
        here = "rootdir resolution", renv = "package version management",
        parallel = "parallel computation for tau matrices"
    )

    pkg_status <- vapply(names(key_pkgs), function(pkg) {
        if (requireNamespace(pkg, quietly = TRUE)) {
            as.character(utils::packageVersion(pkg))
        } else {
            "NOT INSTALLED"
        }
    }, character(1L))

    message("\n[setup]   Key package versions:")
    for (pkg in names(pkg_status)) {
        flag <- if (pkg_status[pkg] == "NOT INSTALLED") " *** MISSING ***" else ""
        message(sprintf("[setup]     %-20s %s%s", pkg, pkg_status[pkg], flag))
    }

    if (any(pkg_status == "NOT INSTALLED")) {
        missing_pkgs <- names(pkg_status)[pkg_status == "NOT INSTALLED"]
        warning("[setup] Missing packages: ", paste(missing_pkgs, collapse = ", "))
    }

    session_log <- file.path(dir_logs, "session_info.txt")
    sink(session_log)
    on.exit(sink(), add = TRUE)
    cat(sprintf("Session info captured: %s\n\n", Sys.time()))
    cat(sprintf("rootdir: %s\n\n", rootdir))
    print(si)
    message(sprintf("\n[setup]   Full session info written to: %s", session_log))
}

# ---- Entry point -------------------------------------------------------------
main()

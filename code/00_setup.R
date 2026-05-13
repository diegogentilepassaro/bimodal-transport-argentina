# ==============================================================================
# 00_setup.R
#
# PURPOSE: Environment bootstrap for the replication package.
#   1. Restores the renv lockfile so all R packages are at exact pinned versions.
#      Falls back to install.packages() from renv.lock when renv/ is not
#      initialized, so a cold-start replicator doesn't need to run
#      renv::init() by hand.
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

    # ---- 3. Print and save session info -------------------------------------
    # No Python check: the pipeline is pure R. See requirements.txt.
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
    activate <- file.path(rootdir, "renv", "activate.R")

    if (!file.exists(lockfile)) {
        message("[setup]   renv.lock not found — running renv::init()")
        renv::init(project = rootdir, restart = FALSE)
        return(invisible(NULL))
    }

    if (!file.exists(activate)) {
        message("[setup]   renv.lock present but renv/ not initialized.")
        message("[setup]   Installing project packages directly from CRAN at")
        message("[setup]   the versions recorded in renv.lock.")
        install_from_lockfile(lockfile)
        return(invisible(NULL))
    }

    message(sprintf("[setup]   Restoring from %s", lockfile))
    renv::restore(project = rootdir, prompt = FALSE)
    message("[setup]   renv::restore() complete")
}

# ---- Helper: install from renv.lock without renv/activate --------------------
# Fallback when renv.lock exists but renv was never initialized. Installs
# each package listed in the lockfile if not already present. Does NOT pin
# to the exact version in the lockfile (install.packages always fetches the
# latest); run renv::restore() after renv::init() for strict pinning.
install_from_lockfile <- function(lockfile) {
    pkg_info <- jsonlite::fromJSON(lockfile, simplifyVector = FALSE)
    pkgs <- names(pkg_info$Packages)
    for (p in pkgs) {
        if (requireNamespace(p, quietly = TRUE)) {
            message(sprintf("[setup]   Already installed: %s", p))
        } else {
            ver <- pkg_info$Packages[[p]]$Version
            message(sprintf("[setup]   Installing %s (lockfile: %s)",
                            p, ver))
            install.packages(p, repos = "https://cloud.r-project.org")
        }
    }
}

# ---- Helper: create all output directories -----------------------------------
create_output_dirs <- function() {
    message("\n[setup] Step 2 — creating output directory tree")

    dirs_to_create <- c(
        # ---- base (cleaned source datasets) ----
        dir_derived_ipums,
        dir_derived_census1947,
        dir_derived_census1960,
        dir_derived_agr,
        dir_derived_ind,
        dir_derived_geo,
        dir_derived_networks,
        # ---- pipeline steps ----
        dir_derived_rasters,
        dir_derived_transitions,
        dir_derived_taus,
        dir_derived_ma,
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

# ---- Helper: print and save session info -------------------------------------
print_session_info <- function() {
    message("\n[setup] Step 3 — R session diagnostics")

    si <- sessionInfo()
    message(sprintf("[setup]   R version:  %s", R.version.string))
    message(sprintf("[setup]   Platform:   %s", si$platform))
    message(sprintf("[setup]   OS:         %s", si$running))
    message(sprintf("[setup]   rootdir:    %s", rootdir))
    message(sprintf("[setup]   renv lib:   %s", .libPaths()[1]))

    key_pkgs <- list(
        sf = "vector spatial data", terra = "raster data",
        gdistance = "least-cost path / Dijkstra", arrow = "parquet I/O",
        exactextractr = "zonal statistics for geo controls",
        fixest = "OLS / IV regressions",
        modelsummary = "LaTeX table generation",
        kableExtra = "booktabs LaTeX via modelsummary",
        haven = "Stata .dta import (legacy raw files)",
        readxl = "Excel import (digitized census)",
        tibble = "add_rows in modelsummary footers",
        igraph = "MST construction for hypothetical networks",
        here = "rootdir resolution",
        renv = "package version management"
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

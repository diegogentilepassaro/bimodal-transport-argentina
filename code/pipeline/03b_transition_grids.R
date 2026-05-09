# ===========================================================================
# 03b_transition_grids.R
#
# PURPOSE: Convert each per-case cost raster into a gdistance transition
#          object, ready for Dijkstra least-cost-path computation.
#
# READS:
#   data/derived/01_cost_rasters/ucost_<case>.tif
#
# PRODUCES:
#   data/derived/02_transition_grids/transition_<case>.rds
#
# DESIGN (matches old pipeline transition_grids/build.R):
#   - 8-connected neighbours.
#   - Transition weight = 1 / mean(neighbour cell costs), so cells with
#     lower cost conduct more (cheaper edges).
#   - geoCorrection() applies the √2 factor for diagonal edges, so paths
#     in the output Dijkstra respect real-world distance.
#
# NOTES:
#   - gdistance is old (last update 2020) and unmaintained but it is the
#     standard and the old pipeline used it, so we stick with it. If
#     gdistance breaks in a future R version, the successor is terra::
#     costDistance() plus igraph — but that port is out of scope for
#     Phase 1.
#   - .rds is used because gdistance's TransitionLayer is an S4 object
#     that isn't naturally serialisable to parquet.
#   - NA cost cells become 0-conductance edges (impassable), which is the
#     correct behaviour: they act as hard barriers.
#
# USAGE:
#   Rscript code/pipeline/03b_transition_grids.R <case_label> [<case_label> ...]
#   If called without arguments, processes all .tif files in
#   data/derived/01_cost_rasters/.
# ===========================================================================

suppressPackageStartupMessages({
    library(raster)     # gdistance still relies on the legacy raster package
    library(gdistance)
})

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)

    if (!dir.exists(dir_derived_transitions)) {
        dir.create(dir_derived_transitions, recursive = TRUE)
    }

    args <- commandArgs(trailingOnly = TRUE)
    if (length(args) > 0) {
        cases <- args
    } else {
        all_tifs <- list.files(dir_derived_rasters,
                               pattern = "^ucost_.+\\.tif$",
                               full.names = FALSE)
        cases <- sub("^ucost_", "", sub("\\.tif$", "", all_tifs))
    }

    message("\n", strrep("=", 72))
    message("03b_transition_grids.R  |  cost raster → gdistance transition")
    message(strrep("=", 72))
    message(sprintf("Cases to process: %s\n", paste(cases, collapse = ", ")))

    for (case in cases) build_one_transition(case)

    message(strrep("=", 72))
    message("03b_transition_grids.R  |  Complete.")
    message(strrep("=", 72), "\n")
}

# ---------------------------------------------------------------------------
# Build the transition object for one case
# ---------------------------------------------------------------------------
build_one_transition <- function(case) {
    message(sprintf("\n[trans] ==== CASE: %s ====", case))

    cost_path <- file.path(dir_derived_rasters,
                           sprintf("ucost_%s.tif", case))
    if (!file.exists(cost_path)) {
        stop("Cost raster not found: ", cost_path)
    }

    message(sprintf("[trans]   Reading %s", cost_path))
    r <- raster::raster(cost_path)

    message("[trans]   Building 8-neighbour transition (1/mean(cost))")
    t0 <- Sys.time()
    tg <- gdistance::transition(r,
                                transitionFunction = function(x) 1 / mean(x),
                                directions = 8)
    message(sprintf("[trans]   transition() took %.1f s",
                    as.numeric(difftime(Sys.time(), t0, units = "secs"))))

    message("[trans]   Applying geoCorrection (diagonal edges)")
    t0 <- Sys.time()
    tg <- gdistance::geoCorrection(tg)
    message(sprintf("[trans]   geoCorrection() took %.1f s",
                    as.numeric(difftime(Sys.time(), t0, units = "secs"))))

    out_path <- file.path(dir_derived_transitions,
                          sprintf("transition_%s.rds", case))
    saveRDS(tg, out_path)
    message(sprintf("[trans]   Saved: %s", out_path))
}

main()

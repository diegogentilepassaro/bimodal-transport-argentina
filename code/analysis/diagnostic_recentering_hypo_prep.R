# ===========================================================================
# diagnostic_recentering_hypo_prep.R
#
# PURPOSE: One-time preparation for Part C of the recentering deep dive
#          (hypo-instrument node randomization; design approved by Diego
#          2026-07-22). EPISTEMIC STATUS: the randomization these
#          artifacts enable is the researcher's, not the world's --
#          Part C characterizes the instrument, it does not certify
#          design-based validity. Compute pairwise LCP costs AND geometries for
#          the FULL 68-city candidate pool of ciudades_seleccion2, so
#          that each permutation draw can rebuild an LCP-MST instantly
#          from the precomputed matrix and union of pair geometries.
#
#          Also builds the node classification per the approved
#          Option 2 design:
#            anchor   capitals (any size) + non-capital cities with
#                     pop1960 >= 25,000 -- ALWAYS in the network.
#            band     non-capital cities with 10,000 <= pop1960 < 25,000
#                     -- the marginal pool whose membership is permuted.
#            out      non-capital cities with pop1960 < 10,000 --
#                     always out.
#          Observed membership (the paper's 15,000-or-capital rule) is
#          recorded so the identity draw reproduces the actual network.
#          Band cities are assigned INDEC regions (via the district
#          shapefile PARENT code) for stratified permutation.
#
# VERIFY (fail-loud):
#   - The MST over the observed node subset, computed from the new
#     68-city cost matrix, must have the same total cost as the MST of
#     the committed lcp_mst.gpkg (same transition settings).
#
# READS:
#   data/derived/01_cost_rasters/construction_costs.tif
#   data/raw/networks_hypo/ciudades_seleccion2.shp
#   data/raw/geo/geo2_ar1970_2010.shp
#   data/derived/02_hypothetical_networks/lcp_mst.gpkg   (verification)
#
# PRODUCES (data/derived/07_recentering/hypo/):
#   city_pool.parquet        68 rows: city_id, localidad, pop1960,
#                            capital, class (anchor/band/out),
#                            observed_in, region
#   pair_costs.parquet       2278 rows: i, j, cost
#   pair_geoms.gpkg          2278 LCP linestrings (i, j attributes)
#   prep_manifest.log
#
# RUNTIME: transition ~2-4 min + 2278 LCP geometries on n_cores_heavy
#          workers (~1-2 h). Run AFTER the sector draw runs finish to
#          respect the memory envelope.
#
# USAGE:  Rscript code/analysis/diagnostic_recentering_hypo_prep.R [ncores]
# ===========================================================================

suppressPackageStartupMessages({
    library(sf)
    library(terra)
    library(raster)
    library(gdistance)
    library(igraph)
    library(arrow)
    library(parallel)
})

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    args <- commandArgs(trailingOnly = TRUE)
    ncores <- if (length(args) >= 1) as.integer(args[1]) else n_cores_heavy

    message("\n", strrep("=", 72))
    message("diagnostic_recentering_hypo_prep.R  |  68-city LCP pool")
    message(strrep("=", 72))

    dir_hypo <- file.path(dir_derived_recentering, "hypo")
    if (!dir.exists(dir_hypo)) dir.create(dir_hypo, recursive = TRUE)

    # ---- 1. City pool and classification ------------------------------------
    cities <- sf::st_read(
        file.path(dir_raw, "networks_hypo", "ciudades_seleccion2.shp"),
        quiet = TRUE)
    if (sf::st_crs(cities) != sf::st_crs("EPSG:4326")) {
        cities <- sf::st_transform(cities, "EPSG:4326")
    }
    cities$pop1960 <- suppressWarnings(as.numeric(cities$pop1960))
    cities$capital <- suppressWarnings(as.integer(cities$capital))
    cities$capital[is.na(cities$capital)] <- 0L
    stopifnot(!any(is.na(cities$pop1960)))
    n <- nrow(cities)
    stopifnot(n == 68L)
    cities$city_id <- seq_len(n)

    cities$class <- ifelse(
        cities$capital == 1L | cities$pop1960 >= 25000, "anchor",
        ifelse(cities$pop1960 >= 10000, "band", "out"))
    # The paper's rule: pop >= 15,000 OR capital.
    cities$observed_in <- cities$capital == 1L | cities$pop1960 >= 15000

    # Sanity: anchors are all observed-in; 'out' cities all observed-out.
    stopifnot(all(cities$observed_in[cities$class == "anchor"]),
              !any(cities$observed_in[cities$class == "out"]))

    # Region via the district shapefile (as in the Larkin lines script).
    dist_shp <- sf::st_read(file.path(dir_raw_geo, "geo2_ar1970_2010.shp"),
                            quiet = TRUE)
    dist_shp <- sf::st_make_valid(dist_shp)
    hit  <- sf::st_nearest_feature(cities,
                                   sf::st_transform(dist_shp, 4326))
    prov <- as.character(dist_shp$PARENT[hit])
    cities$region <- region_of_province[prov]
    stopifnot(!any(is.na(cities$region)))

    tab <- table(cities$class, cities$observed_in)
    message("[prep] class x observed_in:"); print(tab)
    band <- cities[cities$class == "band", ]
    message(sprintf(
        "[prep] band pool: %d cities (%d observed-in), regions: %s",
        nrow(band), sum(band$observed_in),
        paste(sprintf("%s=%d", names(table(band$region)),
                      table(band$region)), collapse = ", ")))

    # ---- 2. Transition + pairwise cost matrix (68 cities) --------------------
    cost <- terra::rast(file.path(dir_derived_rasters,
                                  "construction_costs.tif"))
    cost_r <- raster::raster(cost)
    message("[prep] building transition (16-neighbour, as in 02)")
    tr <- gdistance::transition(cost_r,
                                transitionFunction = function(x) 1 / mean(x),
                                directions = 16)
    tr <- gdistance::geoCorrection(tr, type = "c")

    coords <- sf::st_coordinates(cities)
    message("[prep] pairwise cost matrix (68 x 68)")
    cost_mat <- as.matrix(gdistance::costDistance(tr, coords, coords))
    stopifnot(nrow(cost_mat) == n, !any(is.nan(cost_mat)))
    # Off-diagonal must be finite and strictly positive: Inf (an
    # unreachable pair) or exact 0 (two cities snapped to one cell)
    # would silently corrupt the MST rebuild (igraph drops 0-edges).
    od <- cost_mat[row(cost_mat) != col(cost_mat)]
    stopifnot(all(is.finite(od)), all(od > 0))

    # ---- 3. Verification: observed-subset MST cost vs committed lcp_mst -----
    obs_idx <- which(cities$observed_in)
    mst_edges <- function(idx) {
        sub <- cost_mat[idx, idx]
        g <- igraph::graph_from_adjacency_matrix(sub, mode = "undirected",
                                                 weighted = TRUE)
        igraph::V(g)$name <- as.character(idx)
        m <- igraph::mst(g)
        el <- igraph::as_edgelist(m)
        data.frame(i = pmin(as.integer(el[, 1]), as.integer(el[, 2])),
                   j = pmax(as.integer(el[, 1]), as.integer(el[, 2])),
                   cost = igraph::E(m)$weight)
    }
    obs_mst <- mst_edges(obs_idx)
    committed <- sf::st_read(
        file.path(dir_derived, "02_hypothetical_networks", "lcp_mst.gpkg"),
        quiet = TRUE)
    dev <- abs(sum(obs_mst$cost) - sum(committed$cost)) /
        sum(committed$cost)
    message(sprintf(
        "[prep] observed-subset MST total cost vs committed: rel dev = %.2e",
        dev))
    if (dev > 1e-6) {
        stop("[prep] MST VERIFICATION FAILED (rel dev ", sprintf("%.2e", dev),
             "): the 68-city cost matrix does not reproduce the committed ",
             "LCP-MST. Check transition settings before running draws.")
    }

    # ---- 4. LCP geometries for all 2278 pairs --------------------------------
    pairs <- utils::combn(n, 2)
    n_pairs <- ncol(pairs)
    message(sprintf("[prep] %d LCP geometries on %d cores", n_pairs, ncores))
    one_lcp <- function(k) {
        i <- pairs[1, k]; j <- pairs[2, k]
        sp <- tryCatch(
            gdistance::shortestPath(tr, origin = coords[i, ],
                                    goal = coords[j, ],
                                    output = "SpatialLines"),
            error = function(e) NULL)
        if (is.null(sp) || length(sp@lines) == 0L) {
            return(sf::st_linestring(rbind(coords[i, ], coords[j, ])))
        }
        geom <- sf::st_geometry(sf::st_as_sf(sp))[[1]]
        if (length(geom) == 0L) {
            return(sf::st_linestring(rbind(coords[i, ], coords[j, ])))
        }
        geom
    }
    t0 <- Sys.time()
    geoms <- parallel::mclapply(seq_len(n_pairs), one_lcp,
                                mc.cores = ncores, mc.preschedule = TRUE)
    message(sprintf("[prep] LCPs done in %.1f min",
                    as.numeric(difftime(Sys.time(), t0, units = "mins"))))

    pair_sf <- sf::st_sf(
        i = pairs[1, ], j = pairs[2, ],
        cost = cost_mat[cbind(pairs[1, ], pairs[2, ])],
        geometry = sf::st_sfc(geoms, crs = 4326))

    # ---- 5. Save --------------------------------------------------------------
    arrow::write_parquet(
        sf::st_drop_geometry(cities)[, c("city_id", "localidad", "pop1960",
                                         "capital", "class", "observed_in",
                                         "region")],
        file.path(dir_hypo, "city_pool.parquet"))
    arrow::write_parquet(
        sf::st_drop_geometry(pair_sf),
        file.path(dir_hypo, "pair_costs.parquet"))
    sf::st_write(pair_sf, file.path(dir_hypo, "pair_geoms.gpkg"),
                 delete_dsn = TRUE, quiet = TRUE)

    sink(file.path(dir_hypo, "prep_manifest.log"))
    cat("Data file manifest -- diagnostic_recentering_hypo_prep.R\n")
    cat(sprintf("Generated: %s\n", Sys.time()))
    cat(sprintf("Cities: %d (anchor %d / band %d / out %d); observed-in %d\n",
                n, sum(cities$class == "anchor"),
                sum(cities$class == "band"), sum(cities$class == "out"),
                sum(cities$observed_in)))
    cat(sprintf("Pairs: %d  |  MST verification rel dev: %.2e\n",
                n_pairs, dev))
    sink()
    message("[prep] Saved city pool, pair costs, pair geometries.")
}

main()

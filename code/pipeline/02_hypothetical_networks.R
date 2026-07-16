# ===========================================================================
# 02_hypothetical_networks.R
#
# PURPOSE: Compute the four hypothetical networks used as instruments in
#          the roads IV specification:
#            - Full LCP (bilateral least-cost paths)
#            - LCP-MST (minimum spanning tree over the LCP cost matrix)
#            - Full EUC (bilateral Euclidean lines)
#            - EUC-MST (minimum spanning tree over the Euclidean distances)
#
# READS:
#   data/derived/01_cost_rasters/construction_costs.tif
#   data/raw/networks_hypo/<city_layer>.shp              — node set
#
# PRODUCES:
#   data/derived/02_hypothetical_networks/lcp_network.gpkg
#   data/derived/02_hypothetical_networks/lcp_mst.gpkg
#   data/derived/02_hypothetical_networks/euc_network.gpkg
#   data/derived/02_hypothetical_networks/euc_mst.gpkg
#   data/derived/02_hypothetical_networks/data_file_manifest.log
#
# REFERENCE:
#   Plan/cost_raster_and_lcp_decisions.md
#   Plan/hypothetical_networks_plan.md
#
# NOTES:
#   - LCP computation uses gdistance 1.6.5 with 16-neighbor connectivity,
#     conductance = 1/mean(cost), and geoCorrection(type="c") for
#     distance-weighted costs.
#   - MST computed with igraph::mst using cost/distance as edge weight.
#   - City point set is configurable via `city_layer`. Filter rule:
#     `pop1960 >= pop_threshold OR capital == 1`.
#   - LCP geometry extraction is parallelized via parallel::mclapply,
#     capped at 8 cores.
# ===========================================================================

main <- function(city_layer = "ciudades_seleccion2",
                 pop_threshold = 15000,
                 include_capitals = TRUE) {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)

    message("\n", strrep("=", 72))
    message(sprintf("02_hypothetical_networks.R  |  city_layer = %s",
                    city_layer))
    message(sprintf("  pop_threshold = %d  |  include_capitals = %s",
                    pop_threshold, include_capitals))
    message(strrep("=", 72))

    # --- 1. Load inputs ----------------------------------------------------
    cost <- terra::rast(file.path(dir_derived_rasters,
                                  "construction_costs.tif"))
    cost_r <- raster::raster(cost)  # gdistance still uses raster::raster
    cities_all <- sf::st_read(
        file.path(dir_raw, "networks_hypo",
                  sprintf("%s.shp", city_layer)),
        quiet = TRUE
    )
    if (sf::st_crs(cities_all) != sf::st_crs("EPSG:4326")) {
        cities_all <- sf::st_transform(cities_all, "EPSG:4326")
    }
    # Filter: population threshold OR provincial capital
    cities_all$pop1960 <- suppressWarnings(as.numeric(cities_all$pop1960))
    cities_all$capital <- suppressWarnings(as.integer(cities_all$capital))
    keep <- (!is.na(cities_all$pop1960) & cities_all$pop1960 >= pop_threshold)
    if (include_capitals) {
        keep <- keep | (!is.na(cities_all$capital) & cities_all$capital == 1L)
    }
    cities <- cities_all[keep, ]
    message(sprintf("[net]   cities in shapefile: %d", nrow(cities_all)))
    message(sprintf("[net]   cities kept (filter): %d", nrow(cities)))

    coords <- sf::st_coordinates(cities)
    rownames(coords) <- seq_len(nrow(coords))
    message(sprintf("[net]   cost raster: %d x %d cells",
                    terra::nrow(cost), terra::ncol(cost)))

    # --- 2. Build transition matrix (conductance) --------------------------
    message("\n[net] Step 2 — Building transition matrix")
    t0 <- proc.time()
    tr <- gdistance::transition(
        cost_r,
        transitionFunction = function(x) 1 / mean(x),
        directions = 16
    )
    tr <- gdistance::geoCorrection(tr, type = "c")
    message(sprintf("[net]   transition matrix built in %.0fs",
                    (proc.time() - t0)[["elapsed"]]))

    # --- 3. Full N x N cost matrix -----------------------------------------
    message("\n[net] Step 3 — Pairwise cost matrix")
    t1 <- proc.time()
    cost_mat <- gdistance::costDistance(tr, fromCoords = coords,
                                            toCoords   = coords)
    cost_mat <- as.matrix(cost_mat)
    message(sprintf("[net]   %dx%d cost matrix in %.0fs",
                    nrow(cost_mat), ncol(cost_mat),
                    (proc.time() - t1)[["elapsed"]]))
    if (any(is.infinite(cost_mat[upper.tri(cost_mat)]))) {
        warning("Some LCP costs are infinite — check barrier cells near cities")
    }

    # --- 4. Full LCP network (all pairs) -----------------------------------
    message("\n[net] Step 4 — Computing LCP geometries for all pairs")
    t2 <- proc.time()
    lcp <- build_lcp_network(tr, coords, cost_mat, cities)
    message(sprintf("[net]   %d LCPs in %.0fs",
                    nrow(lcp), (proc.time() - t2)[["elapsed"]]))

    # --- 5. LCP-MST --------------------------------------------------------
    message("\n[net] Step 5 — LCP minimum spanning tree")
    lcp_mst <- build_mst(lcp, cost_col = "cost")

    # --- 6. Full EUC network -----------------------------------------------
    message("\n[net] Step 6 — Bilateral Euclidean network")
    euc <- build_euc_network(cities)

    # --- 7. EUC-MST --------------------------------------------------------
    message("\n[net] Step 7 — EUC minimum spanning tree")
    euc_mst <- build_mst(euc, cost_col = "distance_m")

    # --- 8. Save -----------------------------------------------------------
    out_dir <- file.path(dir_derived, "02_hypothetical_networks")
    if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

    save_layer <- function(x, name) {
        p <- file.path(out_dir, sprintf("%s.gpkg", name))
        sf::st_write(x, p, delete_dsn = TRUE, quiet = TRUE)
        message(sprintf("[net]   saved %s (%d features)", name, nrow(x)))
    }
    save_layer(lcp,     "lcp_network")
    save_layer(lcp_mst, "lcp_mst")
    save_layer(euc,     "euc_network")
    save_layer(euc_mst, "euc_mst")

    write_manifest(lcp, lcp_mst, euc, euc_mst, out_dir, city_layer,
                   nrow(cities))

    message(strrep("=", 72))
    message("02_hypothetical_networks.R  |  Complete.")
    message(strrep("=", 72), "\n")
}

# ---------------------------------------------------------------------------
# Helper: build full bilateral LCP network by running shortestPath for
# each unique pair (i,j) with i < j. cost_mat supplies the cost attribute.
# Parallelized via parallel::mclapply (fork-based on macOS/Linux).
# ---------------------------------------------------------------------------
build_lcp_network <- function(tr, coords, cost_mat, cities) {
    n <- nrow(coords)
    pairs <- utils::combn(n, 2)
    n_pairs <- ncol(pairs)
    # Memory-heavy fork: each worker rebuilds the graph from the
    # transition matrix per shortestPath call. Capped by n_cores_heavy
    # (config.R) after 8 workers crashed a 36 GB machine (2026-07-16).
    n_cores <- n_cores_heavy

    message(sprintf("[net]     using %d cores for %d LCPs",
                    n_cores, n_pairs))

    one_lcp <- function(k) {
        i <- pairs[1, k]; j <- pairs[2, k]
        sp <- tryCatch(
            gdistance::shortestPath(tr,
                                    origin = coords[i, ],
                                    goal   = coords[j, ],
                                    output = "SpatialLines"),
            error = function(e) NULL
        )
        if (is.null(sp) || length(sp@lines) == 0L) {
            # Fall back to a 2-point line if shortestPath returns nothing
            return(sf::st_linestring(rbind(coords[i, ], coords[j, ])))
        }
        geom <- sf::st_geometry(sf::st_as_sf(sp))[[1]]
        if (length(geom) == 0L) {
            return(sf::st_linestring(rbind(coords[i, ], coords[j, ])))
        }
        geom
    }

    geom_list <- parallel::mclapply(seq_len(n_pairs), one_lcp,
                                    mc.cores = n_cores)

    # Guard against worker errors (non-sfg entries → fallback line)
    for (k in seq_len(n_pairs)) {
        if (!inherits(geom_list[[k]], "sfg")) {
            i <- pairs[1, k]; j <- pairs[2, k]
            geom_list[[k]] <- sf::st_linestring(
                rbind(coords[i, ], coords[j, ])
            )
        }
    }

    geom <- sf::st_sfc(geom_list, crs = sf::st_crs(cities))
    sf::st_sf(
        from = pairs[1, ],
        to   = pairs[2, ],
        cost = cost_mat[cbind(pairs[1, ], pairs[2, ])],
        geometry = geom
    )
}

# ---------------------------------------------------------------------------
# Helper: minimum spanning tree over a bilateral edge set.
# ---------------------------------------------------------------------------
build_mst <- function(edges, cost_col) {
    nodes <- sort(unique(c(edges$from, edges$to)))
    g <- igraph::graph_from_data_frame(
        d = as.data.frame(sf::st_drop_geometry(
            edges[, c("from", "to", cost_col)])),
        directed = FALSE,
        vertices = data.frame(name = as.character(nodes))
    )
    igraph::E(g)$weight <- edges[[cost_col]]
    mst_g <- igraph::mst(g)

    # Map MST edges back to rows in the input `edges` sf object
    mst_edges <- igraph::as_edgelist(mst_g)
    keep <- logical(nrow(edges))
    for (k in seq_len(nrow(mst_edges))) {
        u <- as.integer(mst_edges[k, 1])
        v <- as.integer(mst_edges[k, 2])
        match_idx <- which(
            (edges$from == u & edges$to == v) |
            (edges$from == v & edges$to == u)
        )
        if (length(match_idx) >= 1L) keep[match_idx[1]] <- TRUE
    }
    out <- edges[keep, ]
    message(sprintf("[net]   MST: %d edges (expected %d)",
                    nrow(out), length(nodes) - 1L))
    out
}

# ---------------------------------------------------------------------------
# Helper: bilateral Euclidean network between city points.
# ---------------------------------------------------------------------------
build_euc_network <- function(cities) {
    n <- nrow(cities)
    pairs <- utils::combn(n, 2)
    coords <- sf::st_coordinates(cities)
    lines <- vector("list", ncol(pairs))
    for (k in seq_len(ncol(pairs))) {
        i <- pairs[1, k]; j <- pairs[2, k]
        lines[[k]] <- sf::st_linestring(rbind(coords[i, ], coords[j, ]))
    }
    geom <- sf::st_sfc(lines, crs = sf::st_crs(cities))
    out <- sf::st_sf(
        from = pairs[1, ],
        to   = pairs[2, ],
        geometry = geom
    )
    out$distance_m <- as.numeric(sf::st_length(out))  # geodesic on WGS84
    out
}

# ---------------------------------------------------------------------------
# Helper: write a short manifest log.
# ---------------------------------------------------------------------------
write_manifest <- function(lcp, lcp_mst, euc, euc_mst, out_dir, city_layer,
                           n_cities) {
    log_path <- file.path(out_dir, "data_file_manifest.log")
    sink(log_path)
    on.exit(sink(), add = TRUE)
    cat("Data file manifest — 02_hypothetical_networks.R\n")
    cat(sprintf("Generated: %s\n", Sys.time()))
    cat(sprintf("rootdir: %s\n", rootdir))
    cat(sprintf("city_layer: %s\n", city_layer))
    cat(sprintf("n_cities: %d\n\n", n_cities))
    cat(strrep("=", 60), "\n")
    f <- function(x, name, cost_col) {
        cat(sprintf("%s: %d edges, total %s = %.1f\n",
                    name, nrow(x), cost_col, sum(x[[cost_col]])))
    }
    f(lcp,     "lcp_network", "cost")
    f(lcp_mst, "lcp_mst",     "cost")
    f(euc,     "euc_network", "distance_m")
    f(euc_mst, "euc_mst",     "distance_m")
    cat("\nSanity checks:\n")
    cat(sprintf("  LCP-MST edges = N-1 ?  %s\n",
                nrow(lcp_mst) == (n_cities - 1L)))
    cat(sprintf("  EUC-MST edges = N-1 ?  %s\n",
                nrow(euc_mst) == (n_cities - 1L)))
    cat(sprintf("  Sum(LCP)   >= Sum(LCP-MST) ?  %s\n",
                sum(lcp$cost) >= sum(lcp_mst$cost)))
    cat(sprintf("  Sum(EUC_m) >= Sum(EUC-MST_m) ?  %s\n",
                sum(euc$distance_m) >= sum(euc_mst$distance_m)))
    message(sprintf("[net]   manifest: %s", log_path))
}

main()

# ===========================================================================
# diagnostic_roadtiming_prep.R
#
# PURPOSE: One-time preparation for the road-timing design instrument
#          (issue #114; design approved by Diego 2026-07-23).
#          Real-world shock: among settlements NOT connected to the
#          1954 road network, which got connected early (by 1970) vs
#          late (by 1986) vs never -- observable from the three road
#          vintages of comparacion_54_70_86.shp.
#
#   1. Classify the 517-settlement universe by connectivity (within
#      roadtiming_conn_tol_m of a vintage road): connected54 (out of
#      scope), early, late, never.
#      NOTE (cr-review PR #115): classification uses the FULL 1954
#      network incl. type2 = 4 (present 1954+1970, gone by 1986; 6% of
#      1954 km) -- the right notion of real-world 1954 connectivity.
#      The 03a instrument_roadtiming case rasterizes only c(1, 5, 7),
#      matching the actual_1960 convention that the z differencing
#      requires. The asymmetry is deliberate.
#   2. Strata for the unconnected set: region x distance-to-1954-
#      network tercile, thin cells merged deterministically
#      (>= recentering_min_cell early AND non-early per cell).
#      LIMITATION (documented): the settlement CSV has no population
#      field, so size stratification is unavailable.
#   3. Predicted link per unconnected settlement: LCP on the Faber
#      construction-cost surface from the settlement to the nearest
#      point of the 1954 network. APPROXIMATION (documented): the goal
#      is the geometrically nearest network point, not the globally
#      cheapest entry point; for short connector links the difference
#      is second-order.
#
# READS:
#   data/raw/networks_hypo/city_universe/asentamientos_humanos_coord.csv
#   data/raw/networks/comparacion_54_70_86.shp
#   data/raw/geo/geo2_ar1970_2010.shp
#   data/derived/01_cost_rasters/construction_costs.tif
#
# PRODUCES (data/derived/07_recentering/roadtiming/):
#   settlements.parquet   (sid, class, early, region, dist54_km,
#                          stratum, geolev2 of containing district)
#   links.gpkg            (sid, cost, geometry; one link per
#                          unconnected settlement)
#   prep_manifest.log, prep_report.txt
#
# RUNTIME: transition ~4 min + ~216 LCPs at n_cores_heavy (~15-25 min).
# USAGE:  Rscript code/analysis/diagnostic_roadtiming_prep.R [ncores]
# ===========================================================================

suppressPackageStartupMessages({
    library(sf)
    library(terra)
    library(raster)
    library(gdistance)
    library(arrow)
    library(parallel)
})

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    args <- commandArgs(trailingOnly = TRUE)
    ncores <- if (length(args) >= 1) as.integer(args[1]) else n_cores_heavy

    message("\n", strrep("=", 72))
    message("diagnostic_roadtiming_prep.R  |  issue #114 prep")
    message(strrep("=", 72))

    dir_rt <- file.path(dir_derived_recentering, "roadtiming")
    if (!dir.exists(dir_rt)) dir.create(dir_rt, recursive = TRUE)

    # ---- 1. Settlements + connectivity classification -----------------------
    u <- read.csv(file.path(dir_raw, "networks_hypo", "city_universe",
                            "asentamientos_humanos_coord.csv"))
    stopifnot(all(c("X", "Y") %in% names(u)), nrow(u) == 517L)
    pts <- sf::st_as_sf(u, coords = c("X", "Y"), crs = 4326)
    pts_p <- sf::st_transform(pts, crs = crs_raster)
    pts_p$sid <- seq_len(nrow(pts_p))

    roads <- sf::st_make_valid(sf::st_read(
        file.path(dir_raw_networks, "comparacion_54_70_86.shp"),
        quiet = TRUE))
    roads_p <- sf::st_transform(roads, crs = crs_raster)
    net54 <- sf::st_union(roads_p[roads_p$type2 %in% c(1, 4, 5, 7), ])
    net70 <- sf::st_union(roads_p[roads_p$type2 %in% c(1, 2, 4, 5, 6), ])
    net86 <- sf::st_union(roads_p[roads_p$type2 %in% roads_type2_1986, ])

    tol <- roadtiming_conn_tol_m
    on54 <- sf::st_is_within_distance(pts_p, net54, dist = tol,
                                      sparse = FALSE)[, 1]
    on70 <- sf::st_is_within_distance(pts_p, net70, dist = tol,
                                      sparse = FALSE)[, 1]
    on86 <- sf::st_is_within_distance(pts_p, net86, dist = tol,
                                      sparse = FALSE)[, 1]
    pts_p$class <- ifelse(on54, "connected54",
                    ifelse(on70, "early",
                    ifelse(on86, "late", "never")))
    pts_p$early <- pts_p$class == "early"
    message(sprintf("[rt] classes: %s",
        paste(sprintf("%s=%d", names(table(pts_p$class)),
                      table(pts_p$class)), collapse = ", ")))

    unc <- pts_p[pts_p$class != "connected54", ]
    n_unc <- nrow(unc)

    # ---- 2. Region, district, distance strata --------------------------------
    dist_shp <- sf::st_make_valid(sf::st_read(
        file.path(dir_raw_geo, "geo2_ar1970_2010.shp"), quiet = TRUE))
    dist_p <- sf::st_transform(dist_shp, crs = crs_raster)
    hit <- sf::st_nearest_feature(unc, dist_p)
    unc$geolev2 <- sub("^0+", "",
                       as.character(dist_p$GEOLEVEL2[hit]))
    prov <- as.character(dist_p$PARENT[hit])
    unc$region <- region_of_province[prov]
    stopifnot(!any(is.na(unc$region)))

    unc$dist54_km <- as.numeric(sf::st_distance(unc, net54)) / 1000
    terc <- quantile(unc$dist54_km, probs = c(1/3, 2/3))
    unc$dterc <- cut(unc$dist54_km, c(-Inf, terc, Inf),
                     labels = c("d1", "d2", "d3"))
    unc$stratum <- paste(unc$region, unc$dterc, sep = ":")

    # Deterministic thin-cell merging (>= min early AND non-early).
    merges <- character(0)
    ok <- function(df, s) {
        sel <- df$stratum == s
        sum(df$early[sel]) >= recentering_min_cell &&
        sum(!df$early[sel]) >= recentering_min_cell
    }
    for (r in unique(unc$region)) {          # pass 1: drop terciles
        for (dd in c("d1", "d2", "d3")) {
            s <- paste(r, dd, sep = ":")
            if (s %in% unc$stratum && !ok(unc, s)) {
                unc$stratum[unc$region == r] <- paste(r, "all", sep = ":")
                merges <- c(merges, sprintf(
                    "region %s: distance terciles dropped (thin %s)", r, s))
                break
            }
        }
    }
    # Pass 2: converging collapse -- all still-thin cells (and any
    # existing pooled cell) merge into one national POOLED:all until
    # every cell passes. Converges because the global margin
    # (60 early / 156 non-early) satisfies the minimum.
    repeat {
        bad <- Filter(function(s) !ok(unc, s), unique(unc$stratum))
        if (length(bad) == 0L) break
        if (identical(bad, "POOLED:all")) {
            # No-op fixed point: POOLED:all is thin and alone. Absorb
            # the smallest good cell (deterministic tie-break by name)
            # until it passes. Terminates: worst case one national cell.
            good <- setdiff(unique(unc$stratum), "POOLED:all")
            stopifnot(length(good) > 0L)
            sizes <- vapply(good, function(s) sum(unc$stratum == s),
                            integer(1))
            victim <- good[order(sizes, good)][1]
            unc$stratum[unc$stratum == victim] <- "POOLED:all"
            merges <- c(merges, sprintf(
                "POOLED:all thin; absorbed %s", victim))
        } else {
            unc$stratum[unc$stratum %in% c(bad, "POOLED:all")] <-
                "POOLED:all"
            merges <- c(merges, sprintf("collapsed into POOLED:all: %s",
                                        paste(bad, collapse = ", ")))
        }
    }
    for (s in unique(unc$stratum)) stopifnot(ok(unc, s))
    message(sprintf("[rt] strata: %d cells", length(unique(unc$stratum))))

    # ---- 3. Predicted LCP links to the 1954 network ---------------------------
    cost <- terra::rast(file.path(dir_derived_rasters,
                                  "construction_costs.tif"))
    cost_r <- raster::raster(cost)
    message("[rt] building transition (16-neighbour)")
    tr <- gdistance::transition(cost_r,
                                transitionFunction = function(x) 1 / mean(x),
                                directions = 16)
    tr <- gdistance::geoCorrection(tr, type = "c")

    # Origins and goals in EPSG:4326 (the cost raster's CRS, as in 02).
    unc_ll <- sf::st_transform(unc, 4326)
    net54_ll <- sf::st_transform(net54, 4326)
    near_pts <- sf::st_cast(sf::st_nearest_points(unc_ll, net54_ll),
                            "POINT")[seq(2, 2 * n_unc, by = 2)]
    o_xy <- sf::st_coordinates(unc_ll)
    g_xy <- sf::st_coordinates(near_pts)

    message(sprintf("[rt] %d LCP links on %d cores", n_unc, ncores))
    one_link <- function(k) {
        sp <- tryCatch(
            gdistance::shortestPath(tr, origin = o_xy[k, , drop = FALSE],
                                    goal = g_xy[k, , drop = FALSE],
                                    output = "SpatialLines"),
            error = function(e) NULL)
        geom <- if (!is.null(sp) && length(sp@lines) > 0L) {
            g <- sf::st_geometry(sf::st_as_sf(sp))[[1]]
            if (length(g) > 0L) g else NULL
        } else NULL
        if (is.null(geom)) {
            geom <- sf::st_linestring(rbind(o_xy[k, ], g_xy[k, ]))
        }
        geom
    }
    t0 <- Sys.time()
    geoms <- parallel::mclapply(seq_len(n_unc), one_link,
                                mc.cores = ncores, mc.preschedule = TRUE)
    message(sprintf("[rt] links done in %.1f min",
                    as.numeric(difftime(Sys.time(), t0, units = "mins"))))
    links <- sf::st_sf(sid = unc$sid,
                       geometry = sf::st_sfc(geoms, crs = 4326))
    links$len_km <- as.numeric(sf::st_length(links)) / 1000

    # ---- 4. Save ---------------------------------------------------------------
    st <- sf::st_drop_geometry(unc)[, c("sid", "class", "early",
                                        "region", "geolev2",
                                        "dist54_km", "stratum")]
    stopifnot(!any(duplicated(st$sid)))
    arrow::write_parquet(st, file.path(dir_rt, "settlements.parquet"))
    sf::st_write(links, file.path(dir_rt, "links.gpkg"),
                 delete_dsn = TRUE, quiet = TRUE)

    rpt <- file.path(dir_rt, "prep_report.txt")
    sink(rpt)
    cat("diagnostic_roadtiming_prep.R report\n")
    cat(sprintf("Generated: %s  |  tol %d m\n\n", Sys.time(), tol))
    cat(sprintf("Universe: 517 | connected54: %d | unconnected: %d\n",
                sum(pts_p$class == "connected54"), n_unc))
    cat(sprintf("early: %d | late: %d | never: %d\n\n",
                sum(unc$class == "early"), sum(unc$class == "late"),
                sum(unc$class == "never")))
    cat("Strata (early / non-early):\n")
    for (s in sort(unique(st$stratum))) {
        sel <- st$stratum == s
        cat(sprintf("  %-16s %3d / %3d\n", s,
                    sum(st$early[sel]), sum(!st$early[sel])))
    }
    cat("\nMerges:\n")
    if (length(merges) == 0L) cat("  (none)\n") else
        for (m in merges) cat("  -", m, "\n")
    cat(sprintf("\nLink lengths km: min %.1f  median %.1f  max %.1f\n",
                min(links$len_km), median(links$len_km),
                max(links$len_km)))
    sink()

    sink(file.path(dir_rt, "prep_manifest.log"))
    cat("Data file manifest -- diagnostic_roadtiming_prep.R\n")
    cat(sprintf("Generated: %s\n", Sys.time()))
    cat(sprintf("settlements.parquet: %d rows, key sid\n", nrow(st)))
    cat(sprintf("links.gpkg: %d rows, key sid\n", nrow(links)))
    sink()
    message(readLines(rpt) |> paste(collapse = "\n"))
}

main()

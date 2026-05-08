# ===========================================================================
# 01_cost_raster.R
#
# PURPOSE: Build the construction cost raster used as input for least-cost
#          path computation in the hypothetical networks pipeline.
#
# READS:
#   data/raw/networks_hypo/pais.shp                           — country polygon
#   data/raw/networks_hypo/banados.shp                        — wetlands
#   data/raw/networks_hypo/cuerpos_de_agua.shp                — water bodies
#   data/raw/networks_hypo/cursos_de_agua.shp                 — rivers
#   data/raw/networks_hypo/areas_de_aguas_continentales_BH140.shp
#   data/raw/networks_hypo/pendiente_2.tif                    — slope
#   data/raw/geo/areas_de_asentamientos_y_edificios_020105.shp — settlements
#
# PRODUCES:
#   data/derived/01_cost_rasters/construction_costs.tif
#
# REFERENCE:
#   Old data/Train/base/construction_costs/code/compute_construction_cost.py
#   Plan/hypothetical_networks_pipeline.md
#   Plan/hypothetical_networks_plan.md
#
# NOTES:
#   - All inputs and output in EPSG:4326.
#   - Base grid: cost_raster_ncols × cost_raster_nrows cells (config.R).
#   - Final raster cropped to mainland Argentina (raster_xmin/xmax/ymin/ymax).
#   - Cost values (matching old pipeline):
#       * Outside Argentina: cost_out_of_country (barrier)
#       * Obstacle cells (water, wetlands, settlements): + cost_obstacle
#       * All cells: + slope value (degrees)
#   - City cells (ciudades_seleccion2.shp) are burned in as cost=0 at the
#     end to ensure coastal cities (Puerto Madryn, Río Grande) are not
#     barriers. See Step 5 for details.
# ===========================================================================

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)

    message("\n", strrep("=", 72))
    message("01_cost_raster.R  |  Building construction cost raster")
    message(strrep("=", 72))

    rast_ext <- terra::ext(cost_raster_rast_xmin, cost_raster_rast_xmax,
                           cost_raster_rast_ymin, cost_raster_rast_ymax)
    final_ext <- terra::ext(raster_xmin, raster_xmax,
                            raster_ymin, raster_ymax)

    # --- 1. Base country barrier raster ------------------------------------
    message("\n[cost] Step 1 — Country barrier raster")
    pais <- sf::st_read(file.path(dir_raw, "networks_hypo", "pais.shp"),
                        quiet = TRUE)
    pais <- sf::st_make_valid(pais)

    base <- terra::rast(rast_ext,
                        ncol = cost_raster_ncols,
                        nrow = cost_raster_nrows,
                        crs = "EPSG:4326")
    terra::values(base) <- cost_out_of_country
    pais_rast <- terra::rasterize(terra::vect(pais), base, field = 1,
                                  background = cost_out_of_country)
    pais_rast[pais_rast == 1] <- 0  # inside Argentina: no country-barrier cost
    message(sprintf("[cost]   base raster: %d x %d cells",
                    cost_raster_nrows, cost_raster_ncols))

    # --- 2. Obstacle raster (rasterize each layer separately, combine) ----
    #
    # Rationale: st_buffer on cursos_de_agua (~1M river segments) then
    # rbind + rasterize is slow/memory-heavy. Rasterizing each layer
    # independently and taking the max in raster space is equivalent and
    # much faster.
    message("\n[cost] Step 2 — Obstacle raster")
    obstacles_rast <- build_obstacles_raster(base, cost_obstacle)
    n_obstacle_cells <- sum(terra::values(obstacles_rast) == cost_obstacle,
                            na.rm = TRUE)
    message(sprintf("[cost]   obstacle cells: %d", n_obstacle_cells))

    # --- 3. Slope raster aligned to base grid ------------------------------
    message("\n[cost] Step 3 — Slope raster")
    slope_raw <- terra::rast(file.path(dir_raw, "networks_hypo",
                                       "pendiente_2.tif"))
    slope_resampled <- terra::resample(slope_raw, base, method = "bilinear")
    # Replace NA slopes (outside slope coverage) with 0
    slope_resampled[is.na(slope_resampled)] <- 0
    message(sprintf("[cost]   slope range after resample: %.2f – %.2f",
                    terra::global(slope_resampled, "min", na.rm = TRUE)[1, 1],
                    terra::global(slope_resampled, "max", na.rm = TRUE)[1, 1]))

    # --- 4. Sum the three layers -------------------------------------------
    message("\n[cost] Step 4 — Summing layers")
    cost <- pais_rast + obstacles_rast + slope_resampled
    names(cost) <- "construction_costs"

    # --- 5. Burn in city points as land (fix coastal-barrier bug) ---------
    #
    # 2 of 68 cities (Puerto Madryn, Río Grande) are coastal and land on
    # barrier cells after rasterization at this resolution. Burn cities as
    # 0-cost cells so LCP endpoints are always on land. For interior
    # cities this is a no-op (they already have near-0 base cost).
    message("\n[cost] Step 5 — Burning city points as land")
    cost <- burn_cities_as_land(cost)

    # --- 6. Crop to final analysis extent ----------------------------------
    message("\n[cost] Step 6 — Cropping to analysis extent")
    cost <- terra::crop(cost, final_ext)
    message(sprintf("[cost]   final raster: %d x %d cells, extent %s",
                    terra::nrow(cost), terra::ncol(cost),
                    paste(round(as.vector(terra::ext(cost)), 2),
                          collapse = ", ")))
    r <- terra::global(cost, c("min", "max", "mean"), na.rm = TRUE)
    message(sprintf("[cost]   cost range: %.1f – %.1f  (mean %.1f)",
                    r$min, r$max, r$mean))

    # --- 7. Save -----------------------------------------------------------
    out_dir <- dir_derived_rasters
    if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
    out_path <- file.path(out_dir, "construction_costs.tif")
    terra::writeRaster(cost, out_path, overwrite = TRUE)
    message(sprintf("\n[cost] Saved: %s", out_path))

    message(strrep("=", 72))
    message("01_cost_raster.R  |  Complete.")
    message(strrep("=", 72), "\n")
}

# ---------------------------------------------------------------------------
# Helper: build obstacles raster by rasterizing each layer independently
# and taking the union in raster space.
# ---------------------------------------------------------------------------
build_obstacles_raster <- function(base, cost_obstacle) {
    hypo <- file.path(dir_raw, "networks_hypo")
    geo  <- file.path(dir_raw_geo)

    layers <- list(
        banados = file.path(hypo, "banados.shp"),
        cuerpos = file.path(hypo, "cuerpos_de_agua.shp"),
        cursos  = file.path(hypo, "cursos_de_agua.shp"),
        aguas   = file.path(hypo, "areas_de_aguas_continentales_BH140.shp"),
        settle  = file.path(geo,  "areas_de_asentamientos_y_edificios_020105.shp")
    )

    # Start from a zero raster; accumulate the max obstacle value.
    acc <- terra::rast(base)
    terra::values(acc) <- 0

    for (name in names(layers)) {
        message(sprintf("[cost]     rasterizing %s", name))
        x <- sf::st_read(layers[[name]], quiet = TRUE)
        x <- sf::st_make_valid(x)
        if (sf::st_crs(x) != sf::st_crs("EPSG:4326")) {
            x <- sf::st_transform(x, "EPSG:4326")
        }
        # For line geometries (rivers), rasterize::touches=TRUE is enough —
        # any cell the line crosses gets the obstacle value. No need to buffer.
        is_line <- inherits(sf::st_geometry(x)[[1]],
                            c("LINESTRING", "MULTILINESTRING"))
        if (is_line) {
            r <- terra::rasterize(terra::vect(x), base,
                                  field = cost_obstacle,
                                  background = 0, touches = TRUE)
        } else {
            r <- terra::rasterize(terra::vect(x), base,
                                  field = cost_obstacle, background = 0)
        }
        acc <- max(acc, r, na.rm = TRUE)
        # Free memory
        rm(x, r); gc(verbose = FALSE)
    }
    acc
}

# ---------------------------------------------------------------------------
# Helper: burn city points into the cost raster as 0-cost cells.
#
# Some cities sit right on the coast (e.g. Puerto Madryn, Río Grande) and
# after rasterization at ~0.02° x 0.033° per cell they fall on ocean cells
# that have cost = 1000 (barrier). This makes Dijkstra from those nodes
# either fail or produce garbage. The fix: at the end of cost-raster
# construction, set the cell containing each city to cost = 0 (plus slope,
# which we preserve by using the current cell value if it's < threshold
# but forcing it to 0 for barrier cells only).
#
# We overwrite barrier cells unconditionally for cities. Interior cities
# already have cost near 0 and we overwrite them with 0 too — the loss
# of a few units of slope/obstacle cost at a point cell is negligible.
# ---------------------------------------------------------------------------
burn_cities_as_land <- function(cost) {
    cities_path <- file.path(dir_raw, "networks_hypo",
                             "ciudades_seleccion2.shp")
    cities <- sf::st_read(cities_path, quiet = TRUE)
    if (sf::st_crs(cities) != sf::st_crs("EPSG:4326")) {
        cities <- sf::st_transform(cities, "EPSG:4326")
    }

    # Values before burning (diagnostic)
    before <- terra::extract(cost, terra::vect(cities))[[2]]
    n_on_barrier <- sum(before >= 1000, na.rm = TRUE)
    message(sprintf("[cost]   cities before burn: %d on barrier (>=1000)",
                    n_on_barrier))

    # Burn: set the cell containing each city to 0.
    cell_idx <- terra::cellFromXY(cost, sf::st_coordinates(cities))
    cost[cell_idx] <- 0

    after <- terra::extract(cost, terra::vect(cities))[[2]]
    n_on_barrier_after <- sum(after >= 1000, na.rm = TRUE)
    message(sprintf("[cost]   cities after burn:  %d on barrier (>=1000)",
                    n_on_barrier_after))
    if (n_on_barrier_after > 0) {
        warning(sprintf("[cost] %d cities still on barrier after burn",
                        n_on_barrier_after))
    }

    cost
}

main()

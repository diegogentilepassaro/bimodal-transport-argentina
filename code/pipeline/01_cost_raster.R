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
#   - Base grid: 1000x1000 cells over the rasterization extent.
#   - Final raster cropped to mainland Argentina:
#       xmin=-73.56, xmax=-53.70, ymin=-55.10, ymax=-21.78.
#   - Cost values (matching old pipeline):
#       * Outside Argentina: 1000 (barrier)
#       * Obstacle cells (water, wetlands, settlements): + 25
#       * All cells: + slope value (degrees)
# ===========================================================================

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)

    message("\n", strrep("=", 72))
    message("01_cost_raster.R  |  Building construction cost raster")
    message(strrep("=", 72))

    # Parameters
    cost_out_of_country <- 1000
    cost_obstacle       <- 25
    # river_buffer_deg kept for reference; unused because rasterize(touches=TRUE)
    # marks every cell a river line crosses as an obstacle directly.
    river_buffer_deg    <- 0

    # Rasterization extent — covers mainland + a bit of margin for the grid.
    # Final crop brings it down to the analysis extent.
    rast_ext <- terra::ext(-80, -47.9, -55.5, -21.4)
    rast_ncol <- 1000L
    rast_nrow <- 1000L

    final_ext <- terra::ext(raster_xmin, raster_xmax,
                            raster_ymin, raster_ymax)

    # --- 1. Base country barrier raster ------------------------------------
    message("\n[cost] Step 1 — Country barrier raster")
    pais <- sf::st_read(file.path(dir_raw, "networks_hypo", "pais.shp"),
                        quiet = TRUE)
    pais <- sf::st_make_valid(pais)

    base <- terra::rast(rast_ext,
                        ncol = rast_ncol, nrow = rast_nrow,
                        crs = "EPSG:4326")
    terra::values(base) <- cost_out_of_country
    pais_rast <- terra::rasterize(terra::vect(pais), base, field = 1,
                                  background = cost_out_of_country)
    pais_rast[pais_rast == 1] <- 0  # inside Argentina: no country-barrier cost
    message(sprintf("[cost]   base raster: %d x %d cells", rast_nrow, rast_ncol))

    # --- 2. Obstacle raster (rasterize each layer separately, combine) ----
    #
    # Rationale: st_buffer on cursos_de_agua (~1M river segments) then
    # rbind + rasterize is slow/memory-heavy. Rasterizing each layer
    # independently and taking the max in raster space is equivalent and
    # much faster.
    message("\n[cost] Step 2 — Obstacle raster")
    obstacles_rast <- build_obstacles_raster(base, cost_obstacle,
                                             river_buffer_deg)
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

    # --- 5. Crop to final analysis extent ----------------------------------
    message("\n[cost] Step 5 — Cropping to analysis extent")
    cost <- terra::crop(cost, final_ext)
    message(sprintf("[cost]   final raster: %d x %d cells, extent %s",
                    terra::nrow(cost), terra::ncol(cost),
                    paste(round(as.vector(terra::ext(cost)), 2),
                          collapse = ", ")))
    r <- terra::global(cost, c("min", "max", "mean"), na.rm = TRUE)
    message(sprintf("[cost]   cost range: %.1f – %.1f  (mean %.1f)",
                    r$min, r$max, r$mean))

    # --- 6. Save -----------------------------------------------------------
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
build_obstacles_raster <- function(base, cost_obstacle, river_buffer_deg) {
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

main()

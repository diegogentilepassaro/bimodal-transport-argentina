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
#   data/raw/networks_hypo/areas_de_aguas_continentales_BH140.shp — continental water
#   data/raw/networks_hypo/pendiente_2.tif                    — slope (degrees)
#   data/raw/geo/areas_de_asentamientos_y_edificios_020105.shp — settlements
#   data/raw/networks_hypo/ciudades_seleccion2.shp            — city point set
#
# PRODUCES:
#   data/derived/01_cost_rasters/construction_costs.tif
#
# REFERENCE:
#   Faber, B. (2014). "Trade Integration, Market Size, and Industrialization:
#     Evidence from China's National Trunk Highway System."
#     Review of Economic Studies 81(3): 1046-1070.
#   Plan/cost_raster_and_lcp_decisions.md
#
# FORMULA (Faber 2014):
#   c_i = cost_land_baseline
#       + slope_deg_i
#       + cost_developed * is_developed_i
#       + cost_wetland   * is_wetland_i
#       + cost_water     * is_water_i
#   Cells outside Argentina get cost_out_of_country (hard barrier).
#
# PARAMETERS (see config.R):
#   cost_land_baseline  = 1          cost_out_of_country = 1,000,000
#   cost_developed      = 25         cost_wetland        = 25
#   cost_water          = 25
#
# LAYER → CATEGORY MAPPING:
#   Developed ← settlements shapefile
#   Wetland   ← banados
#   Water     ← cuerpos + cursos + aguas_continentales (union)
#
# NOTES:
#   - All inputs and output in EPSG:4326.
#   - Base grid: cost_raster_ncols × cost_raster_nrows (config.R).
#   - Final raster cropped to mainland Argentina
#     (raster_xmin / xmax / ymin / ymax from config.R).
#   - City cells (active node set) are burned to
#     `cost_land_baseline + slope_local + cost_developed` so that coastal
#     cities are reachable and costs between adjacent city pairs are
#     non-zero.
# ===========================================================================

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)

    message("\n", strrep("=", 72))
    message("01_cost_raster.R  |  Building construction cost raster (Faber 2014)")
    message(strrep("=", 72))

    rast_ext  <- terra::ext(cost_raster_rast_xmin, cost_raster_rast_xmax,
                            cost_raster_rast_ymin, cost_raster_rast_ymax)
    final_ext <- terra::ext(raster_xmin, raster_xmax,
                            raster_ymin, raster_ymax)

    # --- 1. Base country mask ---------------------------------------------
    message("\n[cost] Step 1 — Country mask")
    pais <- sf::st_read(file.path(dir_raw, "networks_hypo", "pais.shp"),
                        quiet = TRUE)
    pais <- sf::st_make_valid(pais)

    base <- terra::rast(rast_ext,
                        ncol = cost_raster_ncols,
                        nrow = cost_raster_nrows,
                        crs = "EPSG:4326")
    terra::values(base) <- 1  # placeholder; gets overwritten below

    # 1 inside Argentina, 0 outside
    inside <- terra::rasterize(terra::vect(pais), base, field = 1,
                               background = 0)
    message(sprintf("[cost]   base: %d x %d cells; %d inside Argentina",
                    cost_raster_nrows, cost_raster_ncols,
                    sum(terra::values(inside) == 1, na.rm = TRUE)))

    # --- 2. Category rasters (developed / wetland / water) ---------------
    message("\n[cost] Step 2 — Category indicator rasters")
    developed <- rasterize_layer(
        base, file.path(dir_raw_geo,
                        "areas_de_asentamientos_y_edificios_020105.shp"),
        "developed"
    )
    wetland <- rasterize_layer(
        base, file.path(dir_raw, "networks_hypo", "banados.shp"),
        "wetland"
    )
    water <- rasterize_water_union(base)

    # --- 3. Slope raster aligned to base grid -----------------------------
    message("\n[cost] Step 3 — Slope raster")
    slope_raw <- terra::rast(file.path(dir_raw, "networks_hypo",
                                       "pendiente_2.tif"))
    slope <- terra::resample(slope_raw, base, method = "bilinear")
    slope[is.na(slope)] <- 0
    message(sprintf("[cost]   slope range: %.2f – %.2f deg",
                    terra::global(slope, "min", na.rm = TRUE)[1, 1],
                    terra::global(slope, "max", na.rm = TRUE)[1, 1]))

    # --- 4. Apply Faber formula -------------------------------------------
    message("\n[cost] Step 4 — Computing cost (Faber 2014 formula)")
    inside_cost <- cost_land_baseline +
                   slope +
                   cost_developed * developed +
                   cost_wetland   * wetland +
                   cost_water     * water

    # Step 4a: graduated cost for out-of-country cells. Cells near Argentine
    # land (within cost_coastal_buffer_km) are treated as water crossings
    # at cost_land_baseline + cost_water = 26 so that LCPs can cross rivers
    # (Paraná) and narrow straits (Magellan). Cells farther offshore get
    # cost_out_of_country as a hard barrier.
    message("\n[cost] Step 4a — Coastal buffer for near-shore out-of-country cells")
    dist_km <- terra::distance(inside, target = 0, unit = "km")
    # dist_km == 0 where inside == 1 (inside Argentina) per terra::distance
    near_shore <- inside == 0 & dist_km <= cost_coastal_buffer_km
    cost_near_shore <- cost_land_baseline + cost_water
    n_near <- sum(terra::values(near_shore) == 1, na.rm = TRUE)
    message(sprintf("[cost]   near-shore cells (<= %d km from land): %d",
                    cost_coastal_buffer_km, n_near))

    # Compose the three zones
    cost <- terra::ifel(inside == 1, inside_cost,
             terra::ifel(near_shore == 1, cost_near_shore,
                         cost_out_of_country))
    names(cost) <- "construction_costs"

    # --- 5. Burn city points ---------------------------------------------
    message("\n[cost] Step 5 — Burning cities as reachable urban cells")
    cost <- burn_cities(cost, slope)

    # --- 6. Crop to final analysis extent ---------------------------------
    message("\n[cost] Step 6 — Cropping to analysis extent")
    cost <- terra::crop(cost, final_ext)
    r <- terra::global(cost, c("min", "max", "mean"), na.rm = TRUE)
    message(sprintf("[cost]   final raster: %d x %d cells",
                    terra::nrow(cost), terra::ncol(cost)))
    message(sprintf("[cost]   cost range: %.1f – %.1f (mean %.1f)",
                    r$min, r$max, r$mean))

    # --- 7. Save ---------------------------------------------------------
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
# Helper: rasterize a single polygon/line layer as a 0/1 indicator.
# Uses touches=TRUE for line layers so every cell a line crosses is marked.
# ---------------------------------------------------------------------------
rasterize_layer <- function(base, shp_path, label) {
    x <- sf::st_read(shp_path, quiet = TRUE)
    x <- sf::st_make_valid(x)
    if (sf::st_crs(x) != sf::st_crs("EPSG:4326")) {
        x <- sf::st_transform(x, "EPSG:4326")
    }
    is_line <- inherits(sf::st_geometry(x)[[1]],
                        c("LINESTRING", "MULTILINESTRING"))
    r <- terra::rasterize(terra::vect(x), base,
                          field = 1, background = 0,
                          touches = is_line)
    message(sprintf("[cost]     %-10s cells: %d", label,
                    sum(terra::values(r) == 1, na.rm = TRUE)))
    r
}

# ---------------------------------------------------------------------------
# Helper: build water indicator by union of three water layers.
# ---------------------------------------------------------------------------
rasterize_water_union <- function(base) {
    message("[cost]     water (union of cuerpos + cursos + aguas):")
    r_cuerpos <- rasterize_layer(
        base, file.path(dir_raw, "networks_hypo", "cuerpos_de_agua.shp"),
        "  cuerpos"
    )
    r_cursos  <- rasterize_layer(
        base, file.path(dir_raw, "networks_hypo", "cursos_de_agua.shp"),
        "  cursos"
    )
    r_aguas   <- rasterize_layer(
        base, file.path(dir_raw, "networks_hypo",
                        "areas_de_aguas_continentales_BH140.shp"),
        "  aguas"
    )
    water <- max(r_cuerpos, r_cursos, r_aguas, na.rm = TRUE)
    message(sprintf("[cost]     water      cells: %d",
                    sum(terra::values(water) == 1, na.rm = TRUE)))
    water
}

# ---------------------------------------------------------------------------
# Helper: burn city points into the cost raster.
#
# Coastal cities (e.g. Puerto Madryn, Río Grande) can fall on out-of-country
# raster cells due to finite raster resolution. For those, and for all other
# cities in the node set, we force the cell value to the urban-equivalent
# cost: cost_land_baseline + slope_local + cost_developed. This keeps cities
# reachable for LCPs (not on the hard barrier) and ensures adjacent cities
# still have positive edge costs (because surrounding cells retain their
# full formula value).
# ---------------------------------------------------------------------------
burn_cities <- function(cost, slope) {
    cities_path <- file.path(dir_raw, "networks_hypo",
                             "ciudades_seleccion2.shp")
    cities <- sf::st_read(cities_path, quiet = TRUE)
    if (sf::st_crs(cities) != sf::st_crs("EPSG:4326")) {
        cities <- sf::st_transform(cities, "EPSG:4326")
    }

    xy <- sf::st_coordinates(cities)
    cell_idx <- terra::cellFromXY(cost, xy)

    before <- terra::extract(cost, terra::vect(cities))[[2]]
    n_on_barrier <- sum(before >= cost_out_of_country, na.rm = TRUE)
    message(sprintf("[cost]   before burn: %d cities on barrier",
                    n_on_barrier))

    # City cell cost = baseline + local slope + developed surcharge
    slope_at_cities <- terra::extract(slope, terra::vect(cities))[[2]]
    slope_at_cities[is.na(slope_at_cities)] <- 0
    city_costs <- cost_land_baseline + slope_at_cities + cost_developed
    cost[cell_idx] <- city_costs

    after <- terra::extract(cost, terra::vect(cities))[[2]]
    message(sprintf("[cost]   after burn:  %d cities on barrier",
                    sum(after >= cost_out_of_country, na.rm = TRUE)))
    message(sprintf("[cost]   city cell cost range: %.1f – %.1f",
                    min(city_costs), max(city_costs)))

    cost
}

main()

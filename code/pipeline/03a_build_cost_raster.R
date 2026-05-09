# ===========================================================================
# 03a_build_cost_raster.R
#
# PURPOSE: Build a per-ton-km cost raster for a single case (network
#          combination × sector). The raster is the input to Dijkstra
#          least-cost-path computation.
#
# READS (per case):
#   data/raw/networks/lp_1979.shp                                  (rails)
#   data/raw/networks/comparacion_54_70_86.shp                     (roads)
#   data/derived/02_hypothetical_networks/lcp_mst.gpkg             (hypo)
#   data/raw/geo/HMI.tif                                           (Özak 2018)
#   data/raw/networks_hypo/pais.shp                                (country)
#
# PRODUCES:
#   data/derived/01_cost_rasters/ucost_<case>.tif
#
# DESIGN DECISIONS (Phase 1; see Plan/tau_rebuild_plan.md Sección 6):
#   - Phase 1: land-only cost surface. No navigation layer. Added in Phase 2.
#   - Sector 0 (overall, medium cargo density per Baumgartner & Palazzo 1969).
#     One script run per case; sector is part of the case label.
#   - Rasterise in ESRI:54034 (World Cylindrical Equal Area) at the old
#     pipeline's 2399 × 3090 grid over mainland Argentina.
#   - Linear networks buffered by 1 km before rasterization (matches old
#     pipeline). Guarantees at least one full pixel per segment.
#   - Cost formula (see cost_of_pixel() below):
#       if rail & road:           cost_mininfra[sector]
#       elif rail only:           cost_rail[sector]
#       elif road only:           cost_road[sector]
#       elif in Argentina + HMI:  cost_land × HMI
#       else (outside Argentina): NA (hard barrier for Dijkstra)
#
# CASES (Phase 1, see Plan/tau_rebuild_plan.md Sección 6):
#   actual_1960_s0        — 1960 rails + 1954 roads + HMI
#   actual_1986_s0        — 1986 rails + 1986 roads + HMI
#   instrument_stu_s0     — non-studied rails only + 1954 roads + HMI
#   instrument_lcp_mst_s0 — 1960 rails + LCP-MST hypothetical roads + HMI
#
# HYPOTHETICAL-NETWORK CAVEAT:
#   The LCP-MST hypothetical road network was routed on a Faber (2014)
#   construction-cost raster that treats water as 25× baseline, not as
#   an infinite barrier. As a result, the MST includes a water-crossing
#   segment to Tierra del Fuego. In the actual and instrument_stu cases
#   (which use HMI for off-network cost), TdF is unreachable on the land
#   surface (τ = Inf), but in instrument_lcp_mst it is reachable via
#   the hypothetical water-crossing road. This is a known limitation of
#   the hypothetical instrument, not a bug. Phase 2 navigation will add
#   an explicit water layer that applies consistently to all cases.
#
# USAGE:
#   Rscript code/pipeline/03a_build_cost_raster.R <case_label>
#   Example: Rscript code/pipeline/03a_build_cost_raster.R actual_1960_s0
#
#   If called without arguments, builds all four Phase 1 cases in sequence.
# ===========================================================================

suppressPackageStartupMessages({
    library(sf)
    library(terra)
})

# ---------------------------------------------------------------------------
# Case registry — declarative spec of what each case is built from.
# Adding a new case means adding a new entry here plus (if needed) a new
# ingredient loader.
# ---------------------------------------------------------------------------
case_registry <- function() {
    list(
        actual_1960_s0 = list(
            rail_sel = function(r) r$status1979 %in% c(1, 2, 3),
            road_sel = function(r) r$type2      %in% c(1, 5, 7),
            use_hypo = FALSE,
            sector   = "overall"
        ),
        actual_1986_s0 = list(
            rail_sel = function(r) r$status1979 == 1,
            road_sel = function(r) r$type2      %in% c(1, 2, 3, 5),
            use_hypo = FALSE,
            sector   = "overall"
        ),
        instrument_stu_s0 = list(
            rail_sel = function(r) r$status1979 %in% c(1, 2, 3) &
                                    r$studied_co == 0,
            road_sel = function(r) r$type2      %in% c(1, 5, 7),
            use_hypo = FALSE,
            sector   = "overall"
        ),
        instrument_lcp_mst_s0 = list(
            rail_sel = function(r) r$status1979 %in% c(1, 2, 3),
            road_sel = NULL,          # no observed roads; hypo network replaces
            use_hypo = TRUE,
            sector   = "overall"
        )
    )
}

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)

    args <- commandArgs(trailingOnly = TRUE)
    cases <- if (length(args) > 0) args else names(case_registry())

    message("\n", strrep("=", 72))
    message("03a_build_cost_raster.R  |  Phase 1, land-only, sector 0")
    message(strrep("=", 72))
    message(sprintf("Cases to build: %s\n", paste(cases, collapse = ", ")))

    if (!dir.exists(dir_derived_rasters)) {
        dir.create(dir_derived_rasters, recursive = TRUE)
    }

    reg <- case_registry()
    for (case in cases) {
        if (!(case %in% names(reg))) {
            stop(sprintf("Unknown case label: %s", case))
        }
        build_one_case(case, reg[[case]])
    }

    message(strrep("=", 72))
    message("03a_build_cost_raster.R  |  Complete.")
    message(strrep("=", 72), "\n")
}

# ---------------------------------------------------------------------------
# Build one case end-to-end
# ---------------------------------------------------------------------------
build_one_case <- function(case, spec) {
    message(sprintf("\n[cost] ==== CASE: %s ====", case))

    # --- 1. Base raster grid (ESRI:54034) ---
    base <- build_base_raster()

    # --- 2. Country mask (for NA outside Argentina) ---
    arg_mask <- rasterize_country(base)

    # --- 3. Rasterise rail and road ingredients ---
    rail_rast <- rasterize_rails(base, spec$rail_sel)
    road_rast <- rasterize_roads_or_hypo(base, spec)

    # --- 4. Rasterise HMI (reproject + crop to Argentina extent) ---
    hmi_rast  <- rasterize_hmi(base)

    # --- 5. Combine into cost surface ---
    cost <- combine_cost(rail_rast, road_rast, hmi_rast, arg_mask,
                         sector = spec$sector)

    # --- 6. Validate and save ---
    validate_cost(cost, rail_rast, road_rast, spec$sector)
    save_cost_raster(cost, case)
}

# ---------------------------------------------------------------------------
# Base raster: empty ESRI:54034 grid matching old-pipeline dimensions
# ---------------------------------------------------------------------------
build_base_raster <- function() {
    # Old pipeline extent in ESRI:54034:
    # (-8189281.6170, -5209240.7239) x (-5971264.2267, -2352352.9641)
    ext54034 <- terra::ext(
        -8189281.61690,  # xmin
        -5209240.72390,  # xmax
        -5971264.22670,  # ymin
        -2352352.96410   # ymax
    )
    r <- terra::rast(ext54034,
                     ncol = raster_ncols,
                     nrow = raster_nrows,
                     crs  = crs_raster)
    terra::values(r) <- 0L  # all cells default to 0 (will be overwritten)
    r
}

# ---------------------------------------------------------------------------
# Country mask: 1 inside Argentina, NA outside
# ---------------------------------------------------------------------------
rasterize_country <- function(base) {
    message("[cost]   Rasterising country polygon (pais.shp)")
    pais <- sf::st_read(
        file.path(dir_raw, "networks_hypo", "pais.shp"),
        quiet = TRUE
    )
    pais <- sf::st_make_valid(pais)
    pais_p <- sf::st_transform(pais, crs = crs_raster)
    r <- terra::rasterize(terra::vect(pais_p), base, field = 1)
    # Outside the polygon is NA; set cells inside to 1
    r
}

# ---------------------------------------------------------------------------
# Rasterise rails: select segments via `sel()`, buffer 1 km, rasterise
# ---------------------------------------------------------------------------
rasterize_rails <- function(base, sel) {
    message("[cost]   Rasterising rails (selected subset)")
    rails <- sf::st_read(
        file.path(dir_raw_networks, "lp_1979.shp"), quiet = TRUE
    )
    rails <- sf::st_make_valid(rails)
    rails_sub <- rails[sel(rails), ]
    message(sprintf("[cost]     Selected %d / %d segments",
                    nrow(rails_sub), nrow(rails)))

    rails_proj <- sf::st_transform(rails_sub, crs = crs_raster)
    rails_buf  <- sf::st_buffer(rails_proj, dist = 1000)  # 1 km buffer
    rails_buf  <- sf::st_union(rails_buf)                 # single multipolygon

    r <- terra::rasterize(terra::vect(rails_buf), base, field = 1L,
                          background = 0L)
    n_cells <- sum(terra::values(r) == 1L, na.rm = TRUE)
    message(sprintf("[cost]     Rail pixels (value=1): %d", n_cells))
    r
}

# ---------------------------------------------------------------------------
# Rasterise roads OR hypothetical road network, depending on case
# ---------------------------------------------------------------------------
rasterize_roads_or_hypo <- function(base, spec) {
    if (isTRUE(spec$use_hypo)) {
        return(rasterize_hypo(base))
    }
    if (is.null(spec$road_sel)) {
        # Empty road raster (no roads at all)
        r <- base
        terra::values(r) <- 0L
        return(r)
    }
    rasterize_roads(base, spec$road_sel)
}

rasterize_roads <- function(base, sel) {
    message("[cost]   Rasterising roads (selected subset)")
    roads <- sf::st_read(
        file.path(dir_raw_networks, "comparacion_54_70_86.shp"), quiet = TRUE
    )
    roads <- sf::st_make_valid(roads)
    roads_sub <- roads[sel(roads), ]
    message(sprintf("[cost]     Selected %d / %d segments",
                    nrow(roads_sub), nrow(roads)))

    roads_proj <- sf::st_transform(roads_sub, crs = crs_raster)
    roads_buf  <- sf::st_buffer(roads_proj, dist = 1000)
    roads_buf  <- sf::st_union(roads_buf)

    r <- terra::rasterize(terra::vect(roads_buf), base, field = 1L,
                          background = 0L)
    n_cells <- sum(terra::values(r) == 1L, na.rm = TRUE)
    message(sprintf("[cost]     Road pixels (value=1): %d", n_cells))
    r
}

rasterize_hypo <- function(base) {
    message("[cost]   Rasterising hypothetical LCP-MST road network")
    hypo_path <- file.path(
        dir_derived, "02_hypothetical_networks", "lcp_mst.gpkg"
    )
    if (!file.exists(hypo_path)) {
        stop("Hypo network not found at: ", hypo_path)
    }
    hypo <- sf::st_read(hypo_path, quiet = TRUE)
    hypo <- sf::st_make_valid(hypo)

    hypo_proj <- sf::st_transform(hypo, crs = crs_raster)
    hypo_buf  <- sf::st_buffer(hypo_proj, dist = 1000)
    hypo_buf  <- sf::st_union(hypo_buf)

    r <- terra::rasterize(terra::vect(hypo_buf), base, field = 1L,
                          background = 0L)
    n_cells <- sum(terra::values(r) == 1L, na.rm = TRUE)
    message(sprintf("[cost]     Hypo road pixels (value=1): %d", n_cells))
    r
}

# ---------------------------------------------------------------------------
# Rasterise HMI onto the base grid
# ---------------------------------------------------------------------------
rasterize_hmi <- function(base) {
    message("[cost]   Projecting HMI to base raster")
    hmi_path <- file.path(dir_raw_geo, "HMI.tif")
    if (!file.exists(hmi_path)) stop("HMI raster not found: ", hmi_path)

    hmi <- terra::rast(hmi_path)
    # Zenodo file ships without a written CRS; it's documented as
    # cylindrical equal-area at ESRI:54034.
    terra::crs(hmi) <- crs_raster

    # Crop global raster to Argentina bbox + margin for speed
    hmi_cropped <- terra::crop(hmi, terra::ext(base))
    # Resample to the base raster's grid (bilinear for a continuous surface)
    hmi_on_base <- terra::resample(hmi_cropped, base, method = "bilinear")

    n_ok <- sum(!is.na(terra::values(hmi_on_base)))
    message(sprintf("[cost]     HMI pixels with value (not NA): %d", n_ok))
    hmi_on_base
}

# ---------------------------------------------------------------------------
# Combine rail + road + HMI layers into the cost surface
# ---------------------------------------------------------------------------
combine_cost <- function(rail_rast, road_rast, hmi_rast, arg_mask, sector) {
    message(sprintf("[cost]   Combining layers (sector=%s)", sector))

    rail <- terra::values(rail_rast)
    road <- terra::values(road_rast)
    hmi  <- terra::values(hmi_rast)
    mask <- terra::values(arg_mask)

    # NA-safe helpers
    is_rail <- !is.na(rail) & rail == 1L
    is_road <- !is.na(road) & road == 1L
    in_arg  <- !is.na(mask) & mask == 1

    cost <- rep(NA_real_, length(rail))

    # Overlap: rail and road → mininfra
    sel <- is_rail & is_road
    cost[sel] <- cost_mininfra[[sector]]

    # Rail only
    sel <- is_rail & !is_road
    cost[sel] <- cost_rail[[sector]]

    # Road only
    sel <- !is_rail & is_road
    cost[sel] <- cost_road[[sector]]

    # Off-network land inside Argentina: cost_land × HMI
    # (HMI may be NA for individual cells — those stay NA → hard barrier)
    sel <- !is_rail & !is_road & in_arg & !is.na(hmi)
    cost[sel] <- cost_land * hmi[sel]

    # Outside Argentina: NA (hard barrier for Dijkstra)
    # (already NA_real_ from initialisation)

    r <- terra::rast(rail_rast)   # copy grid + crs
    terra::values(r) <- cost
    names(r) <- "cost"
    r
}

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
validate_cost <- function(cost, rail_rast, road_rast, sector) {
    v <- terra::values(cost)
    r <- terra::values(rail_rast)
    d <- terra::values(road_rast)

    rail_only    <- !is.na(r) & r == 1L & (is.na(d) | d == 0L)
    road_only    <- !is.na(d) & d == 1L & (is.na(r) | r == 0L)
    both         <- !is.na(r) & r == 1L & !is.na(d) & d == 1L

    chk_rail <- all.equal(unique(v[rail_only]),
                          cost_rail[[sector]], tolerance = 1e-9)
    chk_road <- all.equal(unique(v[road_only]),
                          cost_road[[sector]], tolerance = 1e-9)
    chk_both <- length(both) == 0 ||
                all.equal(unique(v[both]),
                          cost_mininfra[[sector]], tolerance = 1e-9)

    stopifnot(isTRUE(chk_rail), isTRUE(chk_road), isTRUE(chk_both))
    message(sprintf(
        "[cost]   Validation OK: rail_only=%.4f road_only=%.4f both=%.4f",
        cost_rail[[sector]], cost_road[[sector]], cost_mininfra[[sector]]
    ))

    v_ok <- v[!is.na(v)]
    message(sprintf(
        "[cost]   Cost summary: min=%.3f  mean=%.2f  max=%.2f  N=%d",
        min(v_ok), mean(v_ok), max(v_ok), length(v_ok)
    ))
}

# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------
save_cost_raster <- function(cost, case) {
    out_path <- file.path(dir_derived_rasters,
                          sprintf("ucost_%s.tif", case))
    terra::writeRaster(cost, out_path, overwrite = TRUE,
                       datatype = "FLT4S")
    message(sprintf("[cost]   Saved: %s", out_path))
}

main()

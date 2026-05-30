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
#   data/raw/networks_hypo/cursos_de_agua.shp                      (rivers)
#   data/raw/networks_hypo/cuerpos_de_agua.shp                     (water bodies)
#   data/raw/geo/HMI.tif                                           (Özak 2018)
#   data/raw/networks_hypo/pais.shp                                (country)
#
# PRODUCES:
#   data/derived/01_cost_rasters/ucost_<case>.tif
#
# DESIGN DECISIONS (Phase 1 + 2a + 2b; see Plan/tau_rebuild_plan.md):
#   - Three cost sectors from Baumgartner & Palazzo 1969:
#       s0 (overall)       — medium cargo density, main specification
#       s1 (agricultural)  — high  cargo density, rail cheaper than road
#       s2 (manufacturing) — low   cargo density, road cheaper than rail
#   - NAVIGATION layer added in Phase 2b: navigable water gets
#     cost_nav[sector] and takes precedence over rail/road. Defined as
#     cursos_de_agua (navegabili == "SI") ∪ Strait of Magellan.
#   - Rasterise in ESRI:54034 (World Cylindrical Equal Area) at the old
#     pipeline's 2399 × 3090 grid over mainland Argentina.
#   - Linear networks buffered by 1 km before rasterization (matches old
#     pipeline). Guarantees at least one full pixel per segment.
#   - Cost formula (see combine_cost() below):
#       if navigable:             cost_nav[sector]
#       elif rail & road:         cost_mininfra[sector]
#       elif rail only:           cost_rail[sector]
#       elif road only:           cost_road[sector]
#       elif in Argentina + HMI:  cost_land × HMI    (constant across sectors)
#       else (outside Argentina): NA (hard barrier for Dijkstra)
#
# CASES (Phase 1 + 2a + 2b + 2c + 3):
#   Nine network specs × three sectors = twenty-seven cases.
#
#   Network specs:
#     actual_1960         — 1960 rails + 1954 roads + HMI + nav
#     actual_1986         — 1986 rails + 1986 roads + HMI + nav
#     instrument_stu      — non-studied rails only + 1954 roads + HMI + nav
#     instrument_lcp_mst  — 1960 rails + LCP-MST hypothetical roads + HMI + nav
#     instrument_euc_mst  — 1960 rails + Euclidean-MST hypothetical roads
#     instrument_lcp      — 1960 rails + bilateral-LCP hypothetical roads
#     instrument_euc      — 1960 rails + bilateral-Euclidean hypothetical roads
#     cf_only_rail        — 1986 rails + 1954 roads  (isolates rail shock)
#     cf_only_road        — 1960 rails + 1986 roads  (isolates road shock)
#
#   Each of the above is generated at sector s0, s1, s2 (case label
#   `<network>_<sector>`, e.g. `cf_only_rail_s1`).
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
#
# The registry is generated programmatically across three sectors
# (overall / agricultural / manufacturing, labelled s0 / s1 / s2) by
# combining a "network spec" (which rails/roads to include) with each
# sector. This means adding a new network case requires touching only
# `base_specs` below; the sector fan-out is automatic.
# ---------------------------------------------------------------------------
case_registry <- function() {
    base_specs <- list(
        actual_1960 = list(
            rail_sel = function(r) r$status1979 %in% c(1, 2, 3),
            road_sel = function(r) r$type2      %in% c(1, 5, 7),
            use_hypo = FALSE,
            hypo_file = NULL
        ),
        actual_1986 = list(
            rail_sel = function(r) r$status1979 == 1,
            road_sel = function(r) r$type2      %in% c(1, 2, 3, 5),
            use_hypo = FALSE,
            hypo_file = NULL
        ),
        instrument_stu = list(
            rail_sel = function(r) r$status1979 %in% c(1, 2, 3) &
                                    r$studied_co == 0,
            road_sel = function(r) r$type2      %in% c(1, 5, 7),
            use_hypo = FALSE,
            hypo_file = NULL
        ),
        instrument_lcp_mst = list(
            rail_sel = function(r) r$status1979 %in% c(1, 2, 3),
            road_sel = NULL,       # hypothetical network takes road slot
            use_hypo = TRUE,
            hypo_file = "lcp_mst.gpkg"
        ),
        instrument_euc_mst = list(
            rail_sel = function(r) r$status1979 %in% c(1, 2, 3),
            road_sel = NULL,
            use_hypo = TRUE,
            hypo_file = "euc_mst.gpkg"
        ),
        instrument_lcp = list(
            rail_sel = function(r) r$status1979 %in% c(1, 2, 3),
            road_sel = NULL,
            use_hypo = TRUE,
            hypo_file = "lcp_network.gpkg"
        ),
        instrument_euc = list(
            rail_sel = function(r) r$status1979 %in% c(1, 2, 3),
            road_sel = NULL,
            use_hypo = TRUE,
            hypo_file = "euc_network.gpkg"
        ),
        # ---- Counterfactual cases (Phase 3) --------------------------------
        # cf_only_rail: rails_1986 + roads_1954. Freezes roads at baseline
        # so Δlog MA vs actual_1960 isolates the rail shock.
        cf_only_rail = list(
            rail_sel = function(r) r$status1979 == 1,
            road_sel = function(r) r$type2      %in% c(1, 5, 7),
            use_hypo = FALSE,
            hypo_file = NULL
        ),
        # cf_only_road: rails_1960 + roads_1986. Freezes rails at baseline
        # so Δlog MA vs actual_1960 isolates the road shock.
        cf_only_road = list(
            rail_sel = function(r) r$status1979 %in% c(1, 2, 3),
            road_sel = function(r) r$type2      %in% c(1, 2, 3, 5),
            use_hypo = FALSE,
            hypo_file = NULL
        )
    )

    # Sector index → sector name mapping (must match names in cost_road etc.)
    sector_map <- list("s0" = "overall",
                       "s1" = "agricultural",
                       "s2" = "manufacturing")

    reg <- list()
    for (base in names(base_specs)) {
        for (scode in names(sector_map)) {
            case_label <- paste(base, scode, sep = "_")
            reg[[case_label]] <- c(base_specs[[base]],
                                   list(sector = sector_map[[scode]]))
        }
    }
    reg
}

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)

    args <- commandArgs(trailingOnly = TRUE)
    cases <- if (length(args) > 0) args else names(case_registry())

    message("\n", strrep("=", 72))
    message("03a_build_cost_raster.R  |  Phase 2b, with navigation, sectors 0/1/2")
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

    # --- 4. Rasterise navigation layer (water = cheap) ---
    nav_rast <- rasterize_navigation(base)

    # --- 5. Rasterise HMI (reproject + crop to Argentina extent) ---
    hmi_rast  <- rasterize_hmi(base)

    # --- 6. Combine into cost surface ---
    cost <- combine_cost(rail_rast, road_rast, nav_rast,
                         hmi_rast, arg_mask,
                         sector = spec$sector)

    # --- 7. Validate and save ---
    validate_cost(cost, rail_rast, road_rast, nav_rast, spec$sector)
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
        return(rasterize_hypo(base, spec$hypo_file))
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

rasterize_hypo <- function(base, hypo_file) {
    message(sprintf("[cost]   Rasterising hypothetical network (%s)",
                    hypo_file))
    hypo_path <- file.path(
        dir_derived, "02_hypothetical_networks", hypo_file
    )
    if (!file.exists(hypo_path)) {
        stop("Hypo network not found at: ", hypo_path)
    }
    hypo <- sf::st_read(hypo_path, quiet = TRUE)
    hypo <- sf::st_make_valid(hypo)

    hypo_proj <- sf::st_transform(hypo, crs = crs_raster)
    hypo_buf  <- sf::st_buffer(hypo_proj, dist = nav_linear_buffer_m)
    hypo_buf  <- sf::st_union(hypo_buf)

    r <- terra::rasterize(terra::vect(hypo_buf), base, field = 1L,
                          background = 0L)
    n_cells <- sum(terra::values(r) == 1L, na.rm = TRUE)
    message(sprintf("[cost]     Hypo road pixels (value=1): %d", n_cells))
    r
}

# ---------------------------------------------------------------------------
# Rasterise navigation layer
#
# Source (see Plan/tau_rebuild_plan.md Sección 7):
#   1. Navigable river segments from cursos_de_agua.shp (navegabili == "SI")
#   2. Strait of Magellan from cuerpos_de_agua.shp (nombre == "DE MAGALLANES")
#      The Magellan entries have navegabili = NA in the IGN data but are
#      navigable by ferry; included manually so Tierra del Fuego connects
#      to the mainland. This is the only hand-curated inclusion; all other
#      water bodies rely on the IGN navigability flag.
#
# Output raster: 1 = navigable (gets cost_nav[sector] in combine_cost),
#                0 = not navigable.
# ---------------------------------------------------------------------------
rasterize_navigation <- function(base) {
    message("[cost]   Rasterising navigation layer")
    rivers_path <- file.path(dir_raw, "networks_hypo",
                             "cursos_de_agua.shp")
    bodies_path <- file.path(dir_raw, "networks_hypo",
                             "cuerpos_de_agua.shp")

    rivers <- sf::st_read(rivers_path, quiet = TRUE)
    rivers <- sf::st_make_valid(rivers)
    navigable_rivers <- rivers[!is.na(rivers$navegabili) &
                                rivers$navegabili == "SI", ]
    message(sprintf(
        "[cost]     Navigable river segments (cursos_de_agua): %d",
        nrow(navigable_rivers)
    ))

    bodies <- sf::st_read(bodies_path, quiet = TRUE)
    bodies <- sf::st_make_valid(bodies)
    magellan <- bodies[!is.na(bodies$nombre) &
                        bodies$nombre == "DE MAGALLANES", ]
    message(sprintf(
        "[cost]     Strait of Magellan polygons (cuerpos_de_agua): %d",
        nrow(magellan)
    ))

    # Reproject + buffer rivers by nav_linear_buffer_m (config.R, default 1 km)
    rivers_proj <- sf::st_transform(navigable_rivers, crs = crs_raster)
    rivers_buf  <- sf::st_buffer(rivers_proj, dist = nav_linear_buffer_m)
    rivers_buf  <- sf::st_union(rivers_buf)

    # Reproject Magellan polygons and buffer by nav_magellan_buffer_m.
    #
    # The IGN Strait of Magellan polygons trace the water itself, which
    # in the ~1.2 km raster leaves a gap between the water and the
    # coastlines on both sides (TdF to the south, mainland to the north).
    # Dijkstra on 8-connected cells can't bridge that gap, so TdF stays
    # disconnected with zero buffer.
    #
    # The configured buffer extends the polygon across the shoreline on
    # both sides, so at least one raster cell along each coast becomes
    # navigable. config.R enforces a minimum of ≥ 2 cells per shore.
    # The alternative (port-to-port LCPs) is the old pipeline's approach;
    # noted as a Phase 2c option if this buffer produces routing
    # artifacts elsewhere.
    magellan_proj <- sf::st_transform(magellan, crs = crs_raster)
    magellan_buf  <- sf::st_buffer(magellan_proj,
                                    dist = nav_magellan_buffer_m)
    magellan_u    <- sf::st_union(magellan_buf)

    nav_union <- sf::st_union(rivers_buf, magellan_u)

    r <- terra::rasterize(terra::vect(nav_union), base, field = 1L,
                          background = 0L)
    n_cells <- sum(terra::values(r) == 1L, na.rm = TRUE)
    message(sprintf("[cost]     Navigation pixels (value=1): %d", n_cells))
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
# Combine rail + road + navigation + HMI layers into the cost surface
#
# Order of precedence (matches old pipeline's unitcostrasters.py):
#   navigation > rail+road overlap > rail only > road only > off-network
#
# Navigation goes first because water is the cheapest mode (0.621) and
# a pixel with both a river and a rail line (e.g. Rosario port) should
# take the water cost, not the rail cost.
#
# NOTE on country mask: the country polygon (pais.shp) covers Argentine
# LAND territory and excludes oceans, rivers that form borders, and the
# Strait of Magellan. Navigation cells are therefore allowed OUTSIDE
# the country mask — a water crossing of the Paraná border with Uruguay
# or the Strait of Magellan isn't "inside Argentina" in a cadastral
# sense, but should be traversable. Only non-navigation, non-network
# pixels are masked to NA when outside pais.shp.
#
# Concrete example: the Paraná river bed between Argentina and Uruguay
# gets cost_nav[sector] even though half of its pixels are outside
# pais.shp. A shipment from Rosario to Concepción del Uruguay routes
# through those pixels at nav cost. Without this exception the river
# would be a hard barrier, forcing the shipment overland.
# ---------------------------------------------------------------------------
combine_cost <- function(rail_rast, road_rast, nav_rast, hmi_rast,
                         arg_mask, sector) {
    message(sprintf("[cost]   Combining layers (sector=%s)", sector))

    # Bloque-1 test (a): allow disabling the fluvial/navigation channel via
    # an environment variable so the no-fluvial counterfactual can be built
    # without altering the default behaviour. DISABLE_NAVIGATION=1 treats
    # navigable water as non-navigable (it then falls through to the
    # off-network land/HMI rule or stays NA outside Argentina).
    disable_nav <- identical(Sys.getenv("DISABLE_NAVIGATION"), "1")
    if (disable_nav) {
        message("[cost]   DISABLE_NAVIGATION=1 -> fluvial channel OFF")
    }

    rail <- terra::values(rail_rast)
    road <- terra::values(road_rast)
    nav  <- terra::values(nav_rast)
    hmi  <- terra::values(hmi_rast)
    mask <- terra::values(arg_mask)

    # NA-safe helpers
    is_rail <- !is.na(rail) & rail == 1L
    is_road <- !is.na(road) & road == 1L
    is_nav  <- !is.na(nav)  & nav  == 1L
    if (disable_nav) is_nav <- rep(FALSE, length(is_nav))
    in_arg  <- !is.na(mask) & mask == 1

    cost <- rep(NA_real_, length(rail))

    # 1. Navigation takes precedence (cheapest mode). Allowed outside the
    #    country mask (rivers-as-borders and the Strait of Magellan).
    sel <- is_nav
    cost[sel] <- cost_nav[[sector]]

    # 2. Rail + road overlap (mode overlap → use min of the two)
    sel <- !is_nav & is_rail & is_road
    cost[sel] <- cost_mininfra[[sector]]

    # 3. Rail only
    sel <- !is_nav & is_rail & !is_road
    cost[sel] <- cost_rail[[sector]]

    # 4. Road only
    sel <- !is_nav & !is_rail & is_road
    cost[sel] <- cost_road[[sector]]

    # 5. Off-network land inside Argentina: cost_land × HMI
    # (HMI may be NA for individual cells — those stay NA → hard barrier)
    sel <- !is_nav & !is_rail & !is_road & in_arg & !is.na(hmi)
    cost[sel] <- cost_land * hmi[sel]

    # 6. Outside Argentina and not navigable: NA (hard barrier)
    # (already NA_real_ from initialisation)

    r <- terra::rast(rail_rast)   # copy grid + crs
    terra::values(r) <- cost
    names(r) <- "cost"
    r
}

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
validate_cost <- function(cost, rail_rast, road_rast, nav_rast, sector) {
    v <- terra::values(cost)
    r <- terra::values(rail_rast)
    d <- terra::values(road_rast)
    n <- terra::values(nav_rast)

    is_rail <- !is.na(r) & r == 1L
    is_road <- !is.na(d) & d == 1L
    is_nav  <- !is.na(n) & n == 1L
    # Mirror the no-fluvial toggle from combine_cost: when navigation is
    # disabled, navigable cells were treated as land, so don't validate
    # them against cost_nav.
    if (identical(Sys.getenv("DISABLE_NAVIGATION"), "1")) {
        is_nav <- rep(FALSE, length(is_nav))
    }

    nav_only_no_infra <- is_nav & !is_rail & !is_road
    rail_only         <- !is_nav & is_rail & !is_road
    road_only         <- !is_nav & !is_rail & is_road
    both_infra        <- !is_nav & is_rail & is_road

    chk_nav  <- length(v[nav_only_no_infra]) == 0 ||
                all.equal(unique(v[nav_only_no_infra]),
                          cost_nav[[sector]], tolerance = 1e-9)
    chk_rail <- length(v[rail_only]) == 0 ||
                all.equal(unique(v[rail_only]),
                          cost_rail[[sector]], tolerance = 1e-9)
    chk_road <- length(v[road_only]) == 0 ||
                all.equal(unique(v[road_only]),
                          cost_road[[sector]], tolerance = 1e-9)
    chk_both <- length(v[both_infra]) == 0 ||
                all.equal(unique(v[both_infra]),
                          cost_mininfra[[sector]], tolerance = 1e-9)

    stopifnot(isTRUE(chk_nav), isTRUE(chk_rail),
              isTRUE(chk_road), isTRUE(chk_both))
    message(sprintf(
        paste0("[cost]   Validation OK: nav=%.4f  rail_only=%.4f  ",
               "road_only=%.4f  both=%.4f"),
        cost_nav[[sector]], cost_rail[[sector]],
        cost_road[[sector]], cost_mininfra[[sector]]
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
    # Bloque-1 test (a): when navigation is disabled, write to a distinct
    # filename so the no-fluvial rasters never clobber the baseline ones.
    suffix <- if (identical(Sys.getenv("DISABLE_NAVIGATION"), "1"))
        "_nofluvial" else ""
    out_path <- file.path(dir_derived_rasters,
                          sprintf("ucost_%s%s.tif", case, suffix))
    terra::writeRaster(cost, out_path, overwrite = TRUE,
                       datatype = "FLT4S")
    message(sprintf("[cost]   Saved: %s", out_path))
}

main()

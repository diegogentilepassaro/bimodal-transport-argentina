# ===========================================================================
# _diagnostic_helpers.R
#
# Shared helpers for the diagnostic_*.R scripts. Consolidates functions
# that had been copy-pasted (and had begun to drift cosmetically) across
# seven diagnostics: the district-shapefile loader block, its point-set
# derivatives, the 1960 population loader, and the market-access
# computation.
#
# USAGE: source AFTER config.R and base/utils.R — this file assumes
#   dir_raw_geo, dir_derived_census1960, crs_raster, geolev2_exclude
#   (config.R) and ensure_geolev2_char() (base/utils.R) are defined.
#
#   source(file.path(dir_code, "analysis", "_diagnostic_helpers.R"),
#          echo = FALSE)
#
# SCOPE: analysis-side diagnostics and figure scripts (including the
# appendix map figures A2/A3, which reuse load_district_shapes() and
# add_map_furniture()). The pipeline keeps its own load_centroids() in
# 03c_compute_taus{,_parallel}.R deliberately — the pipeline is
# frozen-verified and is not re-run when diagnostics change.
# ===========================================================================

# Load-order guard: fail immediately with a named error if config.R /
# base/utils.R were not sourced first, instead of failing later inside a
# helper with a confusing "object not found."
stopifnot(
    "config.R must be sourced before _diagnostic_helpers.R" =
        exists("dir_raw_geo") && exists("crs_raster") &&
        exists("geolev2_exclude") && exists("dir_derived_census1960"),
    "base/utils.R must be sourced before _diagnostic_helpers.R" =
        exists("ensure_geolev2_char")
)

# ---------------------------------------------------------------------------
# District polygons: read, clean, filter to the 312 analysis districts.
# This is THE canonical filtering block; every point loader below builds
# on it. Returns an sf object in the shapefile's native CRS (EPSG:4326).
# ---------------------------------------------------------------------------
load_district_shapes <- function() {
    shp <- sf::st_read(file.path(dir_raw_geo, "geo2_ar1970_2010.shp"),
                       quiet = TRUE)
    shp <- sf::st_make_valid(shp)
    names(shp)[names(shp) == "GEOLEVEL2"] <- "geolev2"
    shp$geolev2 <- sub("^0+", "", as.character(shp$geolev2))
    shp <- shp[!sf::st_is_empty(shp), ]
    shp <- shp[!(shp$geolev2 %in% geolev2_exclude), ]
    shp <- shp[!grepl("0000$", shp$geolev2), ]
    stopifnot(nrow(shp) == 312L, !any(duplicated(shp$geolev2)))
    shp
}

# ---------------------------------------------------------------------------
# Geographic centroids as sp::SpatialPoints in the raster CRS.
# (Mirrors the pipeline's 03c load_centroids; used by gdistance callers.)
# ---------------------------------------------------------------------------
load_centroids_sp <- function() {
    shp <- load_district_shapes()
    cents <- suppressWarnings(sf::st_centroid(shp))
    cents <- sf::st_transform(cents, crs = crs_raster)
    sf::as_Spatial(cents[, "geolev2"])
}

# ---------------------------------------------------------------------------
# Geographic centroids as terra::SpatVector in the raster CRS.
# (Used by terra-based callers, e.g. the connector-share pre-check.)
# ---------------------------------------------------------------------------
load_centroids_vect <- function() {
    shp <- load_district_shapes()
    cents <- suppressWarnings(sf::st_centroid(shp))
    cents <- sf::st_transform(cents, crs = crs_raster)
    terra::vect(cents[, "geolev2"])
}

# ---------------------------------------------------------------------------
# Interior reference points (pole-of-inaccessibility proxy) as
# sp::SpatialPoints in the raster CRS. st_point_on_surface guarantees an
# interior point; differs from the centroid for non-convex / coastal
# districts — exactly the cases where the centroid can fall near a
# digitized network edge. (Used by diagnostic_ma_refpoint.R.)
# ---------------------------------------------------------------------------
load_interior_points <- function() {
    shp <- load_district_shapes()
    pts_sf <- suppressWarnings(sf::st_point_on_surface(shp))
    pts_sf <- sf::st_transform(pts_sf, crs = crs_raster)
    sf::as_Spatial(pts_sf[, "geolev2"])
}

# ---------------------------------------------------------------------------
# District centroids as lat/lon (EPSG:4326) data.frame, for spatial-SE
# (Conley) distance computations.
# ---------------------------------------------------------------------------
load_district_latlon <- function() {
    shp <- load_district_shapes()
    cents <- suppressWarnings(sf::st_centroid(shp))
    cents <- sf::st_transform(cents, crs = "EPSG:4326")
    xy <- sf::st_coordinates(cents)
    data.frame(geolev2 = cents$geolev2,
               lon = xy[, 1], lat = xy[, 2])
}

# ---------------------------------------------------------------------------
# 1960 census population at district level (the MA weights).
# ---------------------------------------------------------------------------
load_1960_pop <- function() {
    path <- file.path(dir_derived_census1960, "census_1960_ipums.parquet")
    d <- arrow::read_parquet(path)
    d <- ensure_geolev2_char(d)
    data.frame(geolev2 = d$geolev2, pop = as.numeric(d$pop))
}

# ---------------------------------------------------------------------------
# Market access from a lower-triangle tau table:
#   MA_i = sum_j pop_j / tau_ij^theta
# tau_df: origin_geolev2 / destination_geolev2 / tau (lower triangle;
# symmetrized here). Non-finite or zero tau contributes zero weight
# (disconnected pairs drop out). Destinations with no 1960 population
# row contribute zero.
# ---------------------------------------------------------------------------
compute_ma <- function(tau_df, pop_df, theta_val) {
    tau_df <- ensure_geolev2_char(tau_df, "origin_geolev2")
    tau_df <- ensure_geolev2_char(tau_df, "destination_geolev2")
    sym <- rbind(
        tau_df[, c("origin_geolev2", "destination_geolev2", "tau")],
        data.frame(origin_geolev2      = tau_df$destination_geolev2,
                   destination_geolev2 = tau_df$origin_geolev2,
                   tau                 = tau_df$tau))
    sym <- merge(sym,
                 data.frame(destination_geolev2 = pop_df$geolev2,
                            pop_dest = pop_df$pop),
                 by = "destination_geolev2", all.x = TRUE)
    sym$pop_dest[is.na(sym$pop_dest)] <- 0
    sym$weight <- ifelse(is.finite(sym$tau) & sym$tau > 0,
                         1 / (sym$tau^theta_val), 0)
    sym$contrib <- sym$weight * sym$pop_dest
    ma_df <- aggregate(contrib ~ origin_geolev2, data = sym, FUN = sum)
    names(ma_df) <- c("geolev2", "MA")
    ma_df$logMA <- log(ma_df$MA)
    ma_df
}

# ---------------------------------------------------------------------------
# Map furniture for base-graphics maps in geographic (lon/lat) coordinates:
# scale bar, north arrow, and CRS note, drawn in the lower-left corner.
# The km -> degrees conversion uses the spherical approximation at the
# bar's own latitude (error < 0.5%, immaterial at scale-bar precision).
# Call after all map layers; for multi-panel figures call it once, on a
# single representative panel.
# ---------------------------------------------------------------------------
add_map_furniture <- function(km = 500, crs_label = "WGS84 (EPSG:4326)") {
    usr <- graphics::par("usr")
    dx  <- usr[2] - usr[1]
    dy  <- usr[4] - usr[3]

    # Scale bar with end ticks
    x0  <- usr[1] + 0.08 * dx
    y0  <- usr[3] + 0.07 * dy
    deg <- km / (111.320 * cos(y0 * pi / 180))
    graphics::segments(x0, y0, x0 + deg, y0, lwd = 2, col = "grey20")
    graphics::segments(c(x0, x0 + deg), y0 - 0.006 * dy,
                       c(x0, x0 + deg), y0 + 0.006 * dy,
                       lwd = 2, col = "grey20")
    graphics::text(x0 + deg / 2, y0 + 0.020 * dy,
                   sprintf("%d km", km), cex = 0.7, col = "grey20")
    graphics::text(x0 + deg / 2, y0 - 0.025 * dy, crs_label,
                   cex = 0.55, col = "grey40")

    # North arrow above the bar
    xa <- x0 + deg / 2
    graphics::arrows(xa, y0 + 0.055 * dy, xa, y0 + 0.105 * dy,
                     length = 0.08, lwd = 2, col = "grey20")
    graphics::text(xa, y0 + 0.130 * dy, "N", cex = 0.8, font = 2,
                   col = "grey20")
}

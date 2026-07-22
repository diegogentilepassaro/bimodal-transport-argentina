# ===========================================================================
# _recentering_helpers.R
#
# PURPOSE: Shared data assembly for the recentering-diagnostic scripts
#          (diagnostic_recentering_{controls,grid,treatments}.R): load
#          the permutation draws, build the draw matrix / expected
#          instrument / recentered instrument, and attach the
#          predetermined covariates used by the control explorations.
#
# Exports:
#   load_recentering_data(sector = "s0")
#       Returns a list(d2, zmat, S_perm):
#         d2     estimation sample (311 rows) + mu, z_obs, z_rec,
#                region, province, rail_km_1960, rail_dens_1960,
#                lat/lon quadratic terms.
#         zmat   311 x S matrix of permuted-draw instruments
#                z^(s) = logMA^(s) - logMA_actual_1960 (same sector).
#         S_perm number of permuted draws.
#       The identity draw (rc000) is cross-checked against the panel's
#       observed instrument column and excluded from mu.
#       Draws for sector s are read from
#       data/derived/07_recentering/draws<suffix>/ where suffix is ""
#       for s0 (the original Stage 1 layout) and "_s1"/"_s2" otherwise.
# ===========================================================================

load_recentering_data <- function(sector = "s0") {
    stopifnot(sector %in% c("s0", "s1", "s2"))
    suffix <- if (sector == "s0") "" else paste0("_", sector)
    dir_draws <- file.path(dir_derived_recentering,
                           paste0("draws", suffix))
    files <- sort(list.files(dir_draws, pattern = "^z_rc\\d+\\.parquet$",
                             full.names = TRUE))
    stopifnot(length(files) >= 11L)
    draws <- do.call(rbind, lapply(files, function(f)
        as.data.frame(arrow::read_parquet(f))))
    draws <- ensure_geolev2_char(draws)
    S_perm <- length(unique(draws$draw[draws$draw > 0L]))

    d <- as.data.frame(arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet")))
    d <- ensure_geolev2_char(d)
    stopifnot(nrow(d) == 311L)

    zw <- reshape(draws[, c("geolev2", "draw", "logMA")],
                  idvar = "geolev2", timevar = "draw", direction = "wide")
    d2 <- merge(d, zw, by = "geolev2", all.x = TRUE)
    stopifnot(nrow(d2) == 311L)
    perm_cols <- sprintf("logMA.%d",
                         sort(unique(draws$draw[draws$draw > 0L])))
    base_col <- sprintf("logMA_actual_1960_%s_elow", sector)
    zmat <- as.matrix(d2[, perm_cols]) - d2[[base_col]]
    stopifnot(!anyNA(zmat))  # guard against a partial draw set

    obs_col <- sprintf("chg_logMA_stu_%s_elow", sector)
    z_id <- d2$logMA.0 - d2[[base_col]]
    max_dev <- max(abs(z_id - d2[[obs_col]]), na.rm = TRUE)
    message(sprintf(
        "[rc-helpers] sector %s: %d draws; identity vs panel dev = %.2e",
        sector, S_perm, max_dev))
    stopifnot(max_dev < 1e-6)

    d2$mu    <- rowMeans(zmat)
    d2$z_obs <- d2[[obs_col]]
    d2$z_rec <- d2$z_obs - d2$mu

    # Predetermined covariates for the control explorations.
    d2$province <- substr(d2$geolev2, 3, 5)
    stopifnot(all(d2$province %in% names(region_of_province)))
    d2$region <- region_of_province[d2$province]
    d2$rail_km_1960   <- d2$tot_rails_1960
    d2$rail_dens_1960 <- d2$tot_rails_1960 / d2$area_km2

    shp <- sf::st_read(file.path(dir_raw_geo, "geo2_ar1970_2010.shp"),
                       quiet = TRUE)
    shp <- sf::st_make_valid(shp)
    names(shp)[names(shp) == "GEOLEVEL2"] <- "geolev2"
    shp$geolev2 <- sub("^0+", "", as.character(shp$geolev2))
    cents <- suppressWarnings(
        sf::st_coordinates(sf::st_centroid(sf::st_transform(shp, 4326))))
    geo_ll <- data.frame(geolev2 = shp$geolev2,
                         lon = cents[, "X"], lat = cents[, "Y"])
    d2 <- merge(d2, geo_ll, by = "geolev2", all.x = TRUE)
    stopifnot(nrow(d2) == 311L, !any(is.na(d2$lat)))
    d2$lat2 <- d2$lat^2; d2$lon2 <- d2$lon^2; d2$latlon <- d2$lat * d2$lon

    list(d2 = d2, zmat = zmat, S_perm = S_perm)
}

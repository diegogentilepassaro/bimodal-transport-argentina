# ===========================================================================
# diagnostic_connector_share.R
#
# PURPOSE: Cheap pre-check for Cote's "connector re-cost" experiment.
#          Question: how big is the centroid -> on-network land leg as a
#          share of tau? If the land legs are a large fraction of tau, the
#          first-kilometer (capillarity) channel can inflate Delta log MA
#          and the full four-case rebuild is worth doing. If they are small,
#          the channel is minor and we can skip the build.
#
# LOGIC (mirrors Cote's toy model tau_ij = c*d_i + r_ij + c*d_j):
#   For each district centroid, in 1960 and 1986:
#     d_i      = straight-line distance (m) to nearest on-network cell
#     leg_i    = d_i * c_repr        (accumulated-cost units, same as tau)
#   A typical pair carries TWO land legs (origin + destination), so the land
#   share of a pair ~ (leg_i + leg_j) / tau_ij. We report leg_i against the
#   per-district median tau, plus the densification change (d_1986 - d_1960).
#
# WHY THIS IS FAITHFUL TO THE PIPELINE:
#   tau is accumulated as sum(cost_cell * distance_in_metres) on the
#   ESRI:54034 (equal-area, metre) grid. On-network cells carry cost in
#   {0.621, 1.777, 1.874}; off-network land cells carry cost_land * HMI
#   (~95-773, median ~144). The huge gap lets us separate them with a
#   simple threshold, and the land leg in cost units is d_i(m) * c_land.
#
# READS:
#   data/derived/01_cost_rasters/ucost_actual_1960_s0.tif
#   data/derived/01_cost_rasters/ucost_actual_1986_s0.tif
#   data/derived/03_taus/tau_actual_1960_s0.parquet
#   data/raw/geo/geo2_ar1970_2010.shp
#
# PRODUCES:
#   results/tables/diagnostic_connector_share.csv   (per-district)
#   results/tables/diagnostic_connector_share.txt   (summary)
# ===========================================================================

suppressPackageStartupMessages({
    library(terra)
    library(sf)
    library(arrow)
})

ON_NET_THRESHOLD <- 50      # cost <= 50 => on-network (gap is 1.874 -> ~95)

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)

    centroids <- load_centroids_vect()

    res60 <- leg_for_period("1960", centroids)
    res86 <- leg_for_period("1986", centroids)

    df <- data.frame(
        geolev2     = res60$geolev2,
        d_1960_km   = res60$d_km,
        d_1986_km   = res86$d_km,
        c_local_60  = res60$c_local,
        leg_1960    = res60$leg,        # d * c_repr, cost units
        leg_1986    = res86$leg,
        stringsAsFactors = FALSE
    )
    df$delta_d_km <- df$d_1986_km - df$d_1960_km           # <= 0 (densify)
    df$delta_leg  <- df$leg_1986 - df$leg_1960

    # Per-district median tau (symmetrize the lower-triangle table).
    med_tau <- per_district_median_tau()
    df <- merge(df, med_tau, by = "geolev2", all.x = TRUE)

    # Land share of a TYPICAL pair through district i:
    #   ~ (leg_i + median_leg) / median_tau_i  (origin + a representative dest)
    med_leg_60 <- median(df$leg_1960, na.rm = TRUE)
    df$land_share_pair_60 <- (df$leg_1960 + med_leg_60) / df$median_tau
    # One-ended share (just district i's own leg) for reference.
    df$land_share_oneend_60 <- df$leg_1960 / df$median_tau

    write_outputs(df, c_repr_used = res60$c_repr, c_repr_86 = res86$c_repr)
}

# ---------------------------------------------------------------------------
# Centroids as SpatVector in raster CRS (mirrors 03c load_centroids)
# ---------------------------------------------------------------------------
load_centroids_vect <- function() {
    shp <- sf::st_read(file.path(dir_raw_geo, "geo2_ar1970_2010.shp"),
                       quiet = TRUE)
    shp <- sf::st_make_valid(shp)
    names(shp)[names(shp) == "GEOLEVEL2"] <- "geolev2"
    shp$geolev2 <- sub("^0+", "", as.character(shp$geolev2))
    shp <- shp[!sf::st_is_empty(shp), ]
    shp <- shp[!(shp$geolev2 %in% geolev2_exclude), ]
    shp <- shp[!grepl("0000$", shp$geolev2), ]
    stopifnot(nrow(shp) == 312L, !any(duplicated(shp$geolev2)))
    cents <- suppressWarnings(sf::st_centroid(shp))
    cents <- sf::st_transform(cents, crs = crs_raster)
    terra::vect(cents[, "geolev2"])
}

# ---------------------------------------------------------------------------
# For one period: distance from each centroid to nearest on-network cell,
# the local land cost, the representative land cost, and the leg in cost units
# ---------------------------------------------------------------------------
leg_for_period <- function(period, centroids) {
    rpath <- file.path(dir_derived_rasters,
                       sprintf("ucost_actual_%s_s0.tif", period))
    r <- terra::rast(rpath)

    # On-network = 1, everything else NA, then distance() gives every cell's
    # distance (metres) to the nearest on-network cell.
    on_net <- terra::ifel(r <= ON_NET_THRESHOLD, 1, NA)
    dist_r <- terra::distance(on_net)            # metres (equal-area CRS)

    d_m     <- terra::extract(dist_r, centroids)[, 2]
    c_local <- terra::extract(r, centroids)[, 2]

    # Representative per-(unit-distance) land cost: median over land cells.
    vals    <- terra::values(r)
    vals    <- vals[is.finite(vals)]
    c_repr  <- median(vals[vals > ON_NET_THRESHOLD])

    list(
        geolev2 = as.character(centroids$geolev2),
        d_km    = d_m / 1000,
        c_local = c_local,
        c_repr  = c_repr,
        leg     = d_m * c_repr            # cost units, same accumulation as tau
    )
}

# ---------------------------------------------------------------------------
# Per-district median tau from the 1960 lower-triangle table
# ---------------------------------------------------------------------------
per_district_median_tau <- function() {
    tau <- arrow::read_parquet(
        file.path(dir_derived_taus, "tau_actual_1960_s0.parquet"))
    # Symmetrize: stack (origin->dest) and (dest->origin).
    long <- rbind(
        data.frame(geolev2 = tau$origin_geolev2,      tau = tau$tau),
        data.frame(geolev2 = tau$destination_geolev2, tau = tau$tau)
    )
    long$geolev2 <- as.character(long$geolev2)
    agg <- aggregate(tau ~ geolev2, data = long,
                     FUN = function(x) median(x[is.finite(x)]))
    names(agg)[2] <- "median_tau"
    agg
}

# ---------------------------------------------------------------------------
write_outputs <- function(df, c_repr_used, c_repr_86) {
    out_csv <- file.path(dir_tables, "diagnostic_connector_share.csv")
    write.csv(df, out_csv, row.names = FALSE)

    qd <- function(x) round(quantile(x, c(0, .1, .25, .5, .75, .9, 1),
                                     na.rm = TRUE), 3)
    qs <- function(x) round(quantile(x, c(0, .1, .25, .5, .75, .9, 1),
                                     na.rm = TRUE), 4)

    lines <- c(
        "Connector pre-check — land leg (centroid -> nearest on-network cell)",
        "as a share of tau. Main spec: actual networks, sector 0.",
        sprintf("Representative land cost c_repr: 1960=%.1f, 1986=%.1f",
                c_repr_used, c_repr_86),
        strrep("=", 70),
        "",
        sprintf("Districts: %d", nrow(df)),
        sprintf("On-network threshold (cost units): %d", ON_NET_THRESHOLD),
        "",
        "Distance centroid -> nearest on-network cell, 1960 (km):",
        capture.output(print(qd(df$d_1960_km))),
        "",
        "Distance centroid -> nearest on-network cell, 1986 (km):",
        capture.output(print(qd(df$d_1986_km))),
        "",
        "Change in distance 1960 -> 1986 (km; <=0 means densification):",
        capture.output(print(qd(df$delta_d_km))),
        sprintf("  Districts with d shrinking (delta < 0): %d / %d",
                sum(df$delta_d_km < -1e-6, na.rm = TRUE), nrow(df)),
        sprintf("  Districts already on-network in 1960 (d<1km): %d",
                sum(df$d_1960_km < 1, na.rm = TRUE)),
        "",
        "Land share of a TYPICAL PAIR through district i, 1960",
        "  (leg_i + median_leg) / median_tau_i:",
        capture.output(print(qs(df$land_share_pair_60))),
        "",
        "One-ended land share (leg_i / median_tau_i), 1960:",
        capture.output(print(qs(df$land_share_oneend_60))),
        "",
        "INTERPRETATION GUIDE:",
        "  - If median pair land-share is small (<~5%), the centroid->network",
        "    leg is a minor part of tau: capillarity inflation is limited,",
        "    the full four-case rebuild is low-value.",
        "  - If it is large (>~15-20%) AND distances shrink 1960->1986 in many",
        "    districts, road densification can move Delta log MA materially:",
        "    the rebuild is worth doing.",
        "  - Middle ground: report and decide jointly."
    )
    out_txt <- file.path(dir_tables, "diagnostic_connector_share.txt")
    writeLines(lines, out_txt)
    cat(paste(lines, collapse = "\n"), "\n")
    message("\nSaved: ", out_csv)
    message("Saved: ", out_txt)
}

main()

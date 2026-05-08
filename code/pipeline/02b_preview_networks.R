# ===========================================================================
# 02b_preview_networks.R
#
# PURPOSE: Produce a quick 1x2 visualization of the two MSTs over the
#          construction cost raster for eyeball review. Not part of the
#          main pipeline; run on demand after 02_hypothetical_networks.R.
#
# READS:
#   data/derived/01_cost_rasters/construction_costs.tif
#   data/derived/02_hypothetical_networks/lcp_mst.gpkg
#   data/derived/02_hypothetical_networks/euc_mst.gpkg
#   data/raw/networks_hypo/ciudades_seleccion2.shp
#
# PRODUCES:
#   results/figures/hypothetical_networks_preview.png
#
# NOTES:
#   The cost raster is log-transformed for display (range 1 to 1e6 spans
#   6 orders of magnitude; linear scale would wash out land variation).
# ===========================================================================

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)

    cost <- terra::rast(file.path(dir_derived_rasters,
                                  "construction_costs.tif"))

    out_net <- file.path(dir_derived, "02_hypothetical_networks")
    lcp_mst <- sf::st_read(file.path(out_net, "lcp_mst.gpkg"), quiet = TRUE)
    euc_mst <- sf::st_read(file.path(out_net, "euc_mst.gpkg"), quiet = TRUE)

    cities <- sf::st_read(
        file.path(dir_raw, "networks_hypo", "ciudades_seleccion2.shp"),
        quiet = TRUE
    )
    cities$pop1960 <- suppressWarnings(as.numeric(cities$pop1960))
    cities$capital <- suppressWarnings(as.integer(cities$capital))
    cities <- cities[
        (cities$pop1960 >= 15000 & !is.na(cities$pop1960)) |
        (cities$capital == 1L  & !is.na(cities$capital)),
    ]

    # Log-transform cost for display; keep 0 (burned cities) visible as 0.
    log_cost <- log10(cost + 1)

    out_path <- file.path(dir_figures, "hypothetical_networks_preview.png")
    png(out_path, width = 2000, height = 1600, res = 150)
    par(mfrow = c(1, 2), mar = c(2, 2, 3, 4))

    draw_panel(
        "LCP MST (53 edges) over cost raster", log_cost, lcp_mst, cities,
        line_col = grDevices::rgb(0.1, 0.3, 0.8, 0.95)
    )
    draw_panel(
        "Euclidean MST (53 edges) over cost raster", log_cost, euc_mst,
        cities, line_col = grDevices::rgb(0.7, 0.1, 0.1, 0.95)
    )

    dev.off()
    message(sprintf("Saved: %s", out_path))
}

draw_panel <- function(title, log_cost, net, cities, line_col, lwd = 1.5) {
    # Greyscale background: low cost = light, high cost = dark
    terra::plot(
        log_cost,
        main = title,
        col = grDevices::grey(seq(1, 0.1, length.out = 100)),
        axes = FALSE,
        legend = TRUE,
        plg = list(title = "log10(cost+1)")
    )
    plot(sf::st_geometry(net), col = line_col, lwd = lwd, add = TRUE)
    plot(sf::st_geometry(cities), pch = 20, col = "black", cex = 0.7,
         add = TRUE)
}

main()

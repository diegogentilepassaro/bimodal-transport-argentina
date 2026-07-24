# ===========================================================================
# plot_figure_a4_navigation.R
#
# PURPOSE: Appendix Figure A4 (Cote reading note #19, marked "do not
#          defer"): what the navigation layer of the cost surface
#          actually contains. Answers the question the text left
#          open — which ports/waters are connected: the navigable
#          INLAND waterways flagged by IGN (Parana-Paraguay-Uruguay-
#          Plata system, plus a few Patagonian rivers) and the
#          buffered Strait of Magellan crossing. There is NO
#          open-ocean coastal shipping in the cost surface: Atlantic
#          ports connect through land (or river) legs only.
#
#          The selection replicates rasterize_navigation() in
#          03a_build_cost_raster.R exactly: cursos_de_agua with
#          navegabili == "SI" (1 km buffer in the raster; drawn as
#          lines here) plus cuerpos_de_agua nombre == "DE MAGALLANES"
#          buffered by nav_magellan_buffer_m.
#
# READS:
#   data/raw/networks_hypo/cursos_de_agua.shp
#   data/raw/networks_hypo/cuerpos_de_agua.shp
#   data/raw/geo/geo2_ar1970_2010.shp     (district base layer)
#
# PRODUCES:
#   results/figures/figure_a4_navigation.pdf
#   results/figures/figure_a4_navigation.png
# ===========================================================================

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_diagnostic_helpers.R"),
           echo = FALSE)

    if (!dir.exists(dir_figures)) dir.create(dir_figures, recursive = TRUE)

    districts <- load_district_shapes()

    rivers <- sf::st_make_valid(sf::st_read(
        file.path(dir_raw, "networks_hypo", "cursos_de_agua.shp"),
        quiet = TRUE))
    nav <- rivers[!is.na(rivers$navegabili) &
                  rivers$navegabili == "SI", ]
    stopifnot(nrow(nav) > 0)
    # Geodesic length in the native CRS (repo convention, matches the
    # A3 script; cr-review PR #122: the equal-area transform inflated
    # this to 8,123 and the old Gauss-Kruger audit gave 8,250 -- same
    # selection, different projections).
    nav_km <- round(sum(as.numeric(sf::st_length(nav))) / 1000)

    bodies <- sf::st_make_valid(sf::st_read(
        file.path(dir_raw, "networks_hypo", "cuerpos_de_agua.shp"),
        quiet = TRUE))
    magellan <- bodies[!is.na(bodies$nombre) &
                       bodies$nombre == "DE MAGALLANES", ]
    stopifnot(nrow(magellan) > 0)
    magellan_buf <- sf::st_union(sf::st_buffer(
        sf::st_transform(magellan, crs_raster),
        dist = nav_magellan_buffer_m))
    # Back to the districts' CRS for plotting.
    magellan_plot <- sf::st_transform(magellan_buf,
                                      sf::st_crs(districts))

    for (fmt in c("pdf", "png")) {
        out <- file.path(dir_figures,
                         sprintf("figure_a4_navigation.%s", fmt))
        if (fmt == "pdf") {
            grDevices::pdf(out, width = 8, height = 10)
        } else {
            grDevices::png(out, width = 1600, height = 2000, res = 200)
        }
        par(mar = c(0.5, 0.5, 2, 0.5))

        plot(sf::st_geometry(districts),
             col = "grey95", border = "grey80", lwd = 0.25,
             main = "Navigation layer of the transport-cost surface")

        plot(sf::st_geometry(magellan_plot),
             col = "#9ecae1", border = "#3182bd", lwd = 0.6,
             add = TRUE)
        plot(sf::st_geometry(nav),
             col = "#08519c", lwd = 1.1, lty = 1, add = TRUE)

        legend("bottomright",
               legend = c(
                   sprintf("Navigable waterways, IGN  (%s km)",
                           format(nav_km, big.mark = ",")),
                   sprintf("Strait of Magellan crossing (%d km buffer)",
                           round(nav_magellan_buffer_m / 1000))
               ),
               col = c("#08519c", "#9ecae1"),
               lwd = c(1.1, NA),
               pch = c(NA, 15),
               pt.cex = c(NA, 1.4),
               bty = "n", cex = 0.9, y.intersp = 1.2)

        add_map_furniture()

        dev.off()
        message("Saved: ", out)
    }
}

main()

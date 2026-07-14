# ===========================================================================
# plot_figure_a2_hypothetical_networks.R
#
# PURPOSE: Appendix Figure A2 (task C16) — the four hypothetical road
#          networks used to build the hypothetical-MA instruments, each
#          overlaid on the actual 1986 paved+gravel road network so the
#          reader can see what the instrument does and does not trace.
#
#   Panels: (a) Euclidean bilateral   (b) Euclidean MST
#           (c) LCP bilateral         (d) LCP MST  [main instrument input]
#
# READS:
#   data/derived/02_hypothetical_networks/{euc_network,euc_mst,
#                                          lcp_network,lcp_mst}.gpkg
#   data/raw/networks/comparacion_54_70_86.shp   (actual 1986 roads)
#   data/raw/geo/geo2_ar1970_2010.shp            (district base layer)
#
# PRODUCES:
#   results/figures/figure_a2_hypothetical_networks.pdf
#   results/figures/figure_a2_hypothetical_networks.png
# ===========================================================================

PANELS <- data.frame(
    file  = c("euc_network.gpkg", "euc_mst.gpkg",
              "lcp_network.gpkg", "lcp_mst.gpkg"),
    title = c("(a) Euclidean, bilateral", "(b) Euclidean, MST",
              "(c) Least-cost path, bilateral",
              "(d) Least-cost path, MST (main)")
)

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_diagnostic_helpers.R"),
           echo = FALSE)

    if (!dir.exists(dir_figures)) dir.create(dir_figures, recursive = TRUE)

    districts <- load_district_shapes()

    # Actual 1986 roads: present-in-1986 categories of the comparison layer
    # (type2 in {1, 2, 3, 5}; taxonomy documented in plot_figure_1.R).
    roads <- sf::st_read(file.path(dir_raw_networks,
                                   "comparacion_54_70_86.shp"), quiet = TRUE)
    roads <- sf::st_make_valid(roads)
    roads86 <- roads[roads$type2 %in% c(1, 2, 3, 5), ]

    hypo_dir <- file.path(dir_derived, "02_hypothetical_networks")
    hypo <- lapply(PANELS$file, function(f) {
        g <- sf::st_read(file.path(hypo_dir, f), quiet = TRUE)
        sf::st_make_valid(g)
    })

    for (fmt in c("pdf", "png")) {
        out <- file.path(dir_figures,
                         sprintf("figure_a2_hypothetical_networks.%s", fmt))
        if (fmt == "pdf") {
            grDevices::pdf(out, width = 12, height = 12)
        } else {
            grDevices::png(out, width = 2400, height = 2400, res = 200)
        }
        par(mfrow = c(2, 2),
            mar   = c(0.5, 0.5, 2, 0.5),
            oma   = c(2.5, 0, 0, 0))

        for (k in seq_len(nrow(PANELS))) {
            plot(sf::st_geometry(districts),
                 col = "grey95", border = "grey80", lwd = 0.25,
                 main = PANELS$title[k])
            plot(sf::st_geometry(roads86),
                 col = "grey55", lwd = 0.5, add = TRUE)
            plot(sf::st_geometry(hypo[[k]]),
                 col = "#c00000", lwd = 1.3, add = TRUE)
        }

        par(fig = c(0, 1, 0, 1), oma = c(0, 0, 0, 0),
            mar = c(0, 0, 0, 0), new = TRUE)
        plot.new()
        legend("bottom",
               legend = c("Hypothetical network",
                          "Actual paved+gravel roads, 1986"),
               col    = c("#c00000", "grey55"),
               lwd    = c(1.3, 0.5),
               horiz  = TRUE, bty = "n", cex = 0.95)

        dev.off()
        message("Saved: ", out)
    }
}

main()

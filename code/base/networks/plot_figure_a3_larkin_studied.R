# ===========================================================================
# plot_figure_a3_larkin_studied.R
#
# PURPOSE: Appendix Figure A3 (task C17) — the Larkin Plan discontinuity
#          that instrument 1 exploits: rail segments the plan studied in
#          detail (below its freight-density thresholds; candidates for
#          closure) vs segments it did not study. Single-panel map with
#          km totals in the legend.
#
# NOTE on the studied share: the legend reports km from the digitized
#   lp_1979 geometry. The share implied here (~48.8%) differs from
#   Larkin's own 39.6% figure cited in §2 — basis unresolved, tracked as
#   issue #68. The figure deliberately reports km, not a share, so it
#   stays agnostic until #68 is settled.
#
# READS:
#   data/raw/networks/lp_1979.shp        (studied_co in {0, 1})
#   data/raw/geo/geo2_ar1970_2010.shp    (district base layer)
#
# PRODUCES:
#   results/figures/figure_a3_larkin_studied.pdf
#   results/figures/figure_a3_larkin_studied.png
# ===========================================================================

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_diagnostic_helpers.R"),
           echo = FALSE)

    if (!dir.exists(dir_figures)) dir.create(dir_figures, recursive = TRUE)

    districts <- load_district_shapes()

    rails <- sf::st_read(file.path(dir_raw_networks, "lp_1979.shp"),
                         quiet = TRUE)
    rails <- sf::st_make_valid(rails)
    stopifnot(all(rails$studied_co %in% c(0, 1)))

    km <- sapply(c(1, 0), function(s) {
        sub <- rails[rails$studied_co == s, ]
        round(sum(as.numeric(sf::st_length(sub)) / 1000))
    })

    for (fmt in c("pdf", "png")) {
        out <- file.path(dir_figures,
                         sprintf("figure_a3_larkin_studied.%s", fmt))
        if (fmt == "pdf") {
            grDevices::pdf(out, width = 8, height = 10)
        } else {
            grDevices::png(out, width = 1600, height = 2000, res = 200)
        }
        par(mar = c(0.5, 0.5, 2, 0.5))

        plot(sf::st_geometry(districts),
             col = "grey95", border = "grey80", lwd = 0.25,
             main = "Larkin Plan (1962): studied vs non-studied rail segments")

        # Non-studied beneath, studied on top (the treatment of interest).
        plot(sf::st_geometry(rails[rails$studied_co == 0, ]),
             col = "#1f4e79", lwd = 1.0, add = TRUE)
        plot(sf::st_geometry(rails[rails$studied_co == 1, ]),
             col = "#c00000", lwd = 1.3, add = TRUE)

        legend("bottomright",
               legend = c(
                   sprintf("Studied by the Larkin Plan  (%s km)",
                           format(km[1], big.mark = ",")),
                   sprintf("Not studied  (%s km)",
                           format(km[2], big.mark = ","))
               ),
               col = c("#c00000", "#1f4e79"),
               lwd = c(1.3, 1.0),
               bty = "n", cex = 0.9, y.intersp = 1.2)

        dev.off()
        message("Saved: ", out)
    }
}

main()

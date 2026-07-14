# ===========================================================================
# plot_figure_a1_cost_schedule.R
#
# PURPOSE: Appendix Figure A1 (task C15) — the Baumgartner & Palazzo (1969)
#          transport cost schedule: unit cost per ton-km by mode across the
#          three tabulated cargo-density scenarios. Shows the road/rail
#          crossover that drives the scale-economies mechanism in §2.4:
#          at high density (bulk grain) rail is cheaper than road; at low
#          density (small-batch manufactures) road is cheaper.
#
# READS:   nothing — the B&P parameters live in config.R section 7
#          (cost_road, cost_rail, cost_nav by sector, with the density
#          mapping documented there: manufacturing = 100 t/day,
#          overall = 500, agricultural = 1,000).
#
# PRODUCES:
#   results/figures/figure_a1_cost_schedule.pdf   (paper version)
#   results/figures/figure_a1_cost_schedule.png   (review-diff version)
# ===========================================================================

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)

    if (!dir.exists(dir_figures)) dir.create(dir_figures, recursive = TRUE)

    # Density scenarios (t/day) in increasing order, with the sector each
    # tabulated column represents (config.R section 7).
    density <- c(100, 500, 1000)
    sector  <- c("manufacturing", "overall", "agricultural")
    road <- cost_road[sector]
    rail <- cost_rail[sector]
    nav  <- cost_nav[sector]

    for (fmt in c("pdf", "png")) {
        out <- file.path(dir_figures,
                         sprintf("figure_a1_cost_schedule.%s", fmt))
        if (fmt == "pdf") {
            grDevices::pdf(out, width = 8, height = 6)
        } else {
            grDevices::png(out, width = 1600, height = 1200, res = 200)
        }

        par(mar = c(4.5, 4.5, 1.5, 1))
        plot(density, road,
             type = "b", pch = 19, lwd = 1.6, col = "#c00000",
             log  = "xy",
             xlim = c(90, 1100), ylim = c(0.4, 15),
             xaxt = "n",
             xlab = "Cargo density (tons/day)",
             ylab = "Unit cost (1960 pesos per ton-km, log scale)")
        axis(1, at = density, labels = format(density, big.mark = ","))
        lines(density, rail, type = "b", pch = 17, lwd = 1.6, col = "#1f4e79")
        lines(density, nav,  type = "b", pch = 15, lwd = 1.4, col = "grey45",
              lty = 2)

        # Annotate the crossover region: road cheaper at 100 and (just) at
        # 500; rail cheaper at 1,000.
        abline(v = 700, col = "grey80", lty = 3)
        text(700, 12, "rail overtakes road\nat high density",
             cex = 0.8, col = "grey30", pos = 4)

        # Sector labels under the density points
        mtext(c("(manufacturing)", "(overall)", "(agricultural)"),
              side = 1, line = 2.2, at = density, cex = 0.7, col = "grey40")

        legend("topright",
               legend = c("Road", "Rail", "Navigation (density-invariant)"),
               col    = c("#c00000", "#1f4e79", "grey45"),
               pch    = c(19, 17, 15),
               lwd    = c(1.6, 1.6, 1.4),
               lty    = c(1, 1, 2),
               bty    = "n", cex = 0.9)

        dev.off()
        message("Saved: ", out)
    }
}

main()

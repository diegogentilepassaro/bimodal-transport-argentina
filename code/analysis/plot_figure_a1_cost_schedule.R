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
#          (cost_road, cost_rail, cost_nav and the sector -> cargo-density
#          mapping cost_density: manufacturing = 100 t/day, overall = 500,
#          agricultural = 1,000).
#
# PRODUCES:
#   results/figures/figure_a1_cost_schedule.pdf   (paper version)
#   results/figures/figure_a1_cost_schedule.png   (review-diff version)
# ===========================================================================

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)

    if (!dir.exists(dir_figures)) dir.create(dir_figures, recursive = TRUE)

    # Density scenarios (t/day) from config.R section 7, in increasing
    # order, with the sector each tabulated column represents.
    sector  <- names(sort(cost_density))
    density <- as.numeric(cost_density[sector])
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

        # Shade the interval that brackets the road/rail crossover: road is
        # cheaper up to `lo`, rail from `hi` on. The three tabulated points
        # do not identify where in between the curves cross, so shade the
        # bracketing interval rather than mark a (spurious) point.
        lo <- max(density[road < rail])
        hi <- min(density[rail < road])
        rect(lo, 10^par("usr")[3], hi, 10^par("usr")[4],
             col = grDevices::adjustcolor("grey80", alpha.f = 0.35),
             border = NA)
        text(sqrt(lo * hi), 12,
             sprintf("rail overtakes road\nbetween %s and %s t/day",
                     format(lo, big.mark = ","),
                     format(hi, big.mark = ",")),
             cex = 0.8, col = "grey30")

        # Sector labels under the density points
        mtext(sprintf("(%s)", sector),
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

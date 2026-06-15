# ===========================================================================
# plot_diagnostic_theta_sweep_sectoral.R
#
# PURPOSE: Visual companion to diagnostic_theta_sweep_sectoral.R. Plots the
#          IV-Both elasticity (beta) against theta for each of the six
#          outcomes, with 95% CI bands. Makes the "option 3" robustness
#          reading immediate: the LEVEL of beta is theta-dependent, but the
#          SECTORAL CONTRAST (mfg value/wage responds, agriculture does not)
#          holds across the whole theta grid.
#
#   Standalone diagnostic exhibit. NOT wired into paper.tex — whether we
#   present beta-as-a-function-of-theta at all depends on the pending
#   theta/tau-object decision (memo Decision A).
#
#   Base graphics (no ggplot2): matches the repo's other figure scripts
#   (plot_figure_1/2/...) and the provisioned R environment.
#
# READS:
#   results/tables/diagnostic_theta_sweep_sectoral.csv  (PR #71)
#
# PRODUCES:
#   results/figures/diagnostic_theta_sweep_sectoral.{pdf,png}
#
# DESIGN:
#   - One panel per outcome (3x2 grid), independent y per panel so each
#     panel's significance shape is legible despite very different scales.
#   - 95% CI band = beta +/- 1.96 * SE (HC1), drawn as a shaded polygon.
#   - Horizontal line at 0; vertical dashed line at theta = 4.55 (main),
#     dotted at theta = 8.11 (alt).
#   - Gibbons ~0.3 is a POPULATION benchmark, not sectoral, so it is noted
#     in the caption rather than drawn across every panel.
# ===========================================================================

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)

    if (!dir.exists(dir_figures)) dir.create(dir_figures, recursive = TRUE)

    csv_path <- file.path(dir_tables, "diagnostic_theta_sweep_sectoral.csv")
    stopifnot(file.exists(csv_path))
    d <- read.csv(csv_path, stringsAsFactors = FALSE)

    # 95% CI from HC1 SE.
    d$ci_lo <- d$beta - 1.96 * d$se
    d$ci_hi <- d$beta + 1.96 * d$se

    # Panel order: population, then manufacturing, then agriculture.
    outcome_levels <- c("population", "mfg production value",
                        "mfg wage mass", "mfg establishments",
                        "ag farms", "ag farmed area")

    for (fmt in c("pdf", "png")) {
        out <- file.path(dir_figures,
                         sprintf("diagnostic_theta_sweep_sectoral.%s", fmt))
        open_device(out, fmt)
        draw_panels(d, outcome_levels)
        dev.off()
        message("Saved: ", out)
    }
}

open_device <- function(path, fmt) {
    if (fmt == "pdf") {
        grDevices::pdf(path, width = 7.5, height = 8.5)
    } else {
        grDevices::png(path, width = 1500, height = 1700, res = 200)
    }
}

draw_panels <- function(d, outcome_levels) {
    op <- par(mfrow = c(3, 2), mar = c(4, 4, 2.5, 1),
              oma = c(3, 0, 2.5, 0))
    on.exit(par(op))

    for (oc in outcome_levels) {
        di <- d[d$outcome == oc, ]
        di <- di[order(di$theta), ]
        draw_one(di, oc)
    }

    mtext("Sectoral elasticity vs theta (IV-Both, 95% CI)",
          outer = TRUE, side = 3, cex = 0.95, font = 2)
    mtext(paste("Dashed = main theta (4.55), dotted = alt (8.11).",
                "HC1 SE. Population benchmark Gibbons ~0.3."),
          outer = TRUE, side = 1, cex = 0.7, col = "grey40", line = 1)
}

draw_one <- function(di, oc) {
    yr <- range(c(di$ci_lo, di$ci_hi, 0), na.rm = TRUE)
    plot(di$theta, di$beta, type = "n", ylim = yr,
         xlab = expression(theta), ylab = "IV-Both beta", main = oc)
    # CI band
    polygon(c(di$theta, rev(di$theta)), c(di$ci_lo, rev(di$ci_hi)),
            col = grDevices::adjustcolor("#2166ac", alpha.f = 0.18),
            border = NA)
    abline(h = 0, col = "grey60", lwd = 0.8)
    abline(v = 4.55, lty = 2, col = "grey50")
    abline(v = 8.11, lty = 3, col = "grey50")
    lines(di$theta, di$beta, col = "#2166ac", lwd = 1.6)
    points(di$theta, di$beta, col = "#2166ac", pch = 19, cex = 0.7)
}

main()

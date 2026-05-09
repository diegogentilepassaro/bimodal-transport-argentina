# ===========================================================================
# plot_figure_3.R
#
# PURPOSE: Paper Figure 3 — scatter of Δ(rail km) vs Δ(road km) by
#          district. Visualizes whether the two infrastructure shocks are
#          spatially independent (for the validation package).
#
# READS:
#   data/derived/05_panel/departments_wide_panel.parquet
#
# PRODUCES:
#   results/figures/figure_3_rail_vs_road_change.pdf
#   results/figures/figure_3_rail_vs_road_change.png
# ===========================================================================

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    if (!dir.exists(dir_figures)) dir.create(dir_figures, recursive = TRUE)

    p <- arrow::read_parquet(
        file.path(dir_derived_panel, "departments_wide_panel.parquet")
    )

    x <- p$chg_pav_and_grav_86_54
    y <- p$chg_tot_rails_86_60
    keep <- !is.na(x) & !is.na(y)
    xk <- x[keep]
    yk <- y[keep]
    n  <- sum(keep)
    r  <- suppressWarnings(stats::cor(xk, yk))

    for (fmt in c("pdf", "png")) {
        out <- file.path(
            dir_figures, sprintf("figure_3_rail_vs_road_change.%s", fmt)
        )
        open_device(out, fmt)

        par(mar = c(4.5, 4.5, 3, 1))
        plot(xk, yk,
             pch  = 20,
             col  = grDevices::adjustcolor("black", alpha.f = 0.55),
             cex  = 0.9,
             xlab = "Change in paved+gravel road km, 1954-1986",
             ylab = "Change in rail km, 1960-1986",
             main = sprintf(
                 "Road expansion vs rail loss by district (N=%d, r=%.3f)",
                 n, r
             ))
        graphics::abline(h = 0, col = "grey70", lty = 2)
        graphics::abline(v = 0, col = "grey70", lty = 2)
        # OLS regression line
        fit <- stats::lm(yk ~ xk)
        graphics::abline(fit, col = "#c00000", lwd = 2)
        b <- stats::coef(fit)
        graphics::legend(
            "bottomleft",
            legend = c(
                sprintf("OLS slope = %.4f", b[2]),
                sprintf("Intercept = %.1f", b[1])
            ),
            col = c("#c00000", NA),
            lwd = c(2, NA),
            bty = "n",
            cex = 0.9
        )

        dev.off()
        message("Saved: ", out)
    }
}

open_device <- function(path, fmt) {
    if (fmt == "pdf") {
        grDevices::pdf(path, width = 7, height = 6)
    } else if (fmt == "png") {
        grDevices::png(path, width = 1400, height = 1200, res = 200)
    }
}

main()

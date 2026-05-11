# ===========================================================================
# plot_figure_4.R
#
# PURPOSE: Paper's Figure 4 — two-panel scatter of ΔlogMA against km-level
#          network changes. Shows what drives the variation in the main
#          treatment variable.
#
#   Panel A: Δ logMA (86-60, s0, elow) vs Δ paved+gravel road km (86-54)
#   Panel B: Δ logMA (86-60, s0, elow) vs Δ rail km (86-60)
#
# READS:
#   data/derived/05_panel/departments_wide_panel.parquet
#
# PRODUCES:
#   results/figures/figure_4_infra_vs_ma_scatter.pdf
#   results/figures/figure_4_infra_vs_ma_scatter.png
#
# DESIGN:
#   - Each panel shows the OLS regression line and the correlation r.
#   - Points use semi-transparent black so clusters are visible.
#   - h=0 / v=0 reference lines mark the "no change" baselines.
# ===========================================================================

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    if (!dir.exists(dir_figures)) dir.create(dir_figures, recursive = TRUE)

    d <- arrow::read_parquet(
        file.path(dir_derived_panel, "departments_wide_panel.parquet")
    )

    for (fmt in c("pdf", "png")) {
        out <- file.path(
            dir_figures,
            sprintf("figure_4_infra_vs_ma_scatter.%s", fmt)
        )
        open_device(out, fmt)
        par(mfrow = c(1, 2),
            mar   = c(4.5, 4.5, 3, 1),
            oma   = c(0, 0, 0, 0))

        draw_panel(d,
                   x_col = "chg_pav_and_grav_86_54",
                   y_col = "chg_logMA_86_60_s0_elow",
                   x_lab = "Change in paved+gravel road km, 1954-1986",
                   y_lab = expression(paste(Delta, " log MA, 1960-1986")),
                   title = "(a) Roads")

        draw_panel(d,
                   x_col = "chg_tot_rails_86_60",
                   y_col = "chg_logMA_86_60_s0_elow",
                   x_lab = "Change in rail km, 1960-1986",
                   y_lab = expression(paste(Delta, " log MA, 1960-1986")),
                   title = "(b) Rails")

        dev.off()
        message("Saved: ", out)
    }
}

# ---------------------------------------------------------------------------
# Device helpers
# ---------------------------------------------------------------------------
open_device <- function(path, fmt) {
    if (fmt == "pdf") {
        grDevices::pdf(path, width = 12, height = 6)
    } else if (fmt == "png") {
        grDevices::png(path, width = 2400, height = 1200, res = 200)
    }
}

# ---------------------------------------------------------------------------
# Draw one scatter panel
# ---------------------------------------------------------------------------
draw_panel <- function(d, x_col, y_col, x_lab, y_lab, title) {
    x <- d[[x_col]]
    y <- d[[y_col]]
    keep <- !is.na(x) & !is.na(y)
    x <- x[keep]
    y <- y[keep]
    n <- length(x)
    r <- stats::cor(x, y)

    plot(x, y,
         pch  = 20,
         col  = grDevices::adjustcolor("black", alpha.f = 0.55),
         cex  = 0.9,
         xlab = x_lab,
         ylab = y_lab,
         main = sprintf("%s (N=%d, r=%.3f)", title, n, r))
    graphics::abline(h = 0, col = "grey70", lty = 2)
    graphics::abline(v = 0, col = "grey70", lty = 2)

    fit <- stats::lm(y ~ x)
    graphics::abline(fit, col = "#c00000", lwd = 2)

    b <- stats::coef(fit)
    se <- summary(fit)$coefficients["x", "Std. Error"]
    # Adaptive unit: for rail/road in km, b is very small (e-4 range).
    graphics::legend(
        "topleft",
        legend = c(
            sprintf("OLS slope = %.4g (SE %.4g)", b[2], se),
            sprintf("Intercept = %.2f", b[1])
        ),
        col = c("#c00000", NA),
        lwd = c(2, NA),
        bty = "n",
        cex = 0.85
    )
}

main()

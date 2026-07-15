# ===========================================================================
# plot_figure_c13.R
#
# PURPOSE: Block-2 figure — three-panel choropleth decomposing the
#          1960 → 1986 market-access change into its rail and road
#          components.
#
#   Panel A: ΔlogMA_full        (actual total, chg_logMA_86_60_s0_elow)
#   Panel B: ΔlogMA_only_rail   (rails_86 + roads_54 counterfactual)
#   Panel C: ΔlogMA_only_road   (rails_60 + roads_86 counterfactual)
#
# READS:
#   data/derived/05_panel/departments_wide_panel.parquet
#   data/raw/geo/geo2_ar1970_2010.shp
#
# PRODUCES:
#   results/figures/figure_c13_ma_counterfactual_trio.pdf
#   results/figures/figure_c13_ma_counterfactual_trio.png
#
# DESIGN:
#   - All three panels share the same colour scale so magnitudes are
#     visually comparable across shocks.
#   - Break points are hand-chosen with fine cuts near zero (-0.5,
#     -0.1, -0.05) because the rail-only shock is small and uniformly
#     negative; coarse bins would render panel (b) monochrome.
#   - Diverging palette centered at 0 (blue = MA fell, red = MA rose).
#   - Sector 0, θ_low for the main spec; other specs trivially
#     reproducible by editing the column names.
# ===========================================================================

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_diagnostic_helpers.R"),
           echo = FALSE)  # add_map_furniture()

    if (!dir.exists(dir_figures)) dir.create(dir_figures, recursive = TRUE)

    d <- load_panel_with_geometry()

    # Pooled breakpoints, shared across the three panels so magnitudes are
    # visually comparable. Fine bins near zero are required: the rail-only
    # shock is small and uniformly negative (IQR roughly -0.09 to -0.02),
    # so coarse bins would put ~90% of districts in one class and render
    # panel (b) monochrome.
    breaks <- c(-Inf, -2, -1, -0.5, -0.1, -0.05, 0, 0.5, 1, 2, 4, Inf)
    labels <- c("< -2", "(-2, -1]", "(-1, -0.5]", "(-0.5, -0.1]",
                "(-0.1, -0.05]", "(-0.05, 0]", "(0, 0.5]", "(0.5, 1]",
                "(1, 2]", "(2, 4]", "> 4")
    palette <- c(
        "< -2"          = "#08306b",
        "(-2, -1]"      = "#2171b5",
        "(-1, -0.5]"    = "#4292c6",
        "(-0.5, -0.1]"  = "#6baed6",
        "(-0.1, -0.05]" = "#9ecae1",
        "(-0.05, 0]"    = "#deebf7",
        "(0, 0.5]"      = "#fddbc7",
        "(0.5, 1]"      = "#f4a582",
        "(1, 2]"        = "#d6604d",
        "(2, 4]"        = "#b2182b",
        "> 4"           = "#67001f"
    )

    for (fmt in c("pdf", "png")) {
        out <- file.path(
            dir_figures,
            sprintf("figure_c13_ma_counterfactual_trio.%s", fmt)
        )
        open_device(out, fmt)
        layout(matrix(c(1, 2, 3, 4), nrow = 1),
               widths = c(1, 1, 1, 0.4))
        par(mar = c(0.5, 0.5, 3, 0.5), oma = c(0, 0, 0, 0))

        draw_panel(d, "chg_logMA_86_60_s0_elow",
                   "(a) Total 1960-1986",
                   breaks, labels, palette)
        draw_panel(d, "chg_logMA_only_rail_s0_elow",
                   "(b) Rail-only counterfactual",
                   breaks, labels, palette)
        draw_panel(d, "chg_logMA_only_road_s0_elow",
                   "(c) Road-only counterfactual",
                   breaks, labels, palette)
        add_map_furniture()  # scale bar + north arrow + CRS, last panel

        # Shared legend panel
        par(mar = c(0.5, 0, 3, 0))
        plot.new()
        graphics::legend(
            "center",
            legend = labels,
            fill   = palette,
            bty    = "n",
            cex    = 0.85,
            y.intersp = 1.1,
            title  = expression(paste(Delta, "log MA"))
        )

        dev.off()
        message("Saved: ", out)
    }
}

open_device <- function(path, fmt) {
    if (fmt == "pdf") {
        grDevices::pdf(path, width = 18, height = 8)
    } else if (fmt == "png") {
        grDevices::png(path, width = 3600, height = 1600, res = 200)
    }
}

load_panel_with_geometry <- function() {
    shp <- sf::st_read(
        file.path(dir_raw_geo, "geo2_ar1970_2010.shp"), quiet = TRUE
    )
    shp <- sf::st_make_valid(shp)
    names(shp)[names(shp) == "GEOLEVEL2"] <- "geolev2"
    shp$geolev2 <- sub("^0+", "", as.character(shp$geolev2))
    shp <- shp[!sf::st_is_empty(shp), ]
    shp <- shp[!(shp$geolev2 %in% geolev2_exclude), ]
    shp <- shp[!grepl("0000$", shp$geolev2), ]

    panel <- arrow::read_parquet(
        file.path(dir_derived_panel, "departments_wide_panel.parquet")
    )
    panel <- ensure_geolev2_char(panel)

    cols <- c("geolev2",
              "chg_logMA_86_60_s0_elow",
              "chg_logMA_only_rail_s0_elow",
              "chg_logMA_only_road_s0_elow")
    stopifnot(all(cols %in% names(panel)))
    merge(shp, panel[, cols], by = "geolev2")
}

draw_panel <- function(d, col, title, breaks, labels, palette) {
    x <- d[[col]]
    cat_x <- cut(x, breaks = breaks, labels = labels, right = TRUE)
    cat_x <- factor(as.character(cat_x), levels = labels)

    plot(sf::st_geometry(d),
         col = palette[cat_x],
         border = "grey50",
         lwd = 0.3,
         main = title)

    # Median/IQR, not mean/SD: the rail-only panel has a -11.35 outlier
    # that makes the mean unrepresentative of the distribution's center.
    qs <- stats::quantile(x, c(0.25, 0.5, 0.75), na.rm = TRUE)
    subtitle <- sprintf(
        "Median %+.2f, IQR [%+.2f, %+.2f]", qs[2], qs[1], qs[3]
    )
    graphics::mtext(subtitle, side = 1, line = -1, cex = 0.75)
}

main()

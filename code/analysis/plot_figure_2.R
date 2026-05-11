# ===========================================================================
# plot_figure_2.R
#
# PURPOSE: Paper's Figure 2 — district-level choropleth of the main
#          treatment variable Δlog MA (actual 1986 − actual 1960),
#          sector 0 (overall), θ_low = 4.55.
#
# READS:
#   data/derived/05_panel/departments_wide_panel.parquet  (chg_logMA)
#   data/raw/geo/geo2_ar1970_2010.shp                     (district polygons)
#
# PRODUCES:
#   results/figures/figure_2_ma_change_choropleth.pdf
#   results/figures/figure_2_ma_change_choropleth.png
#
# DESIGN:
#   - Divergent colour scale centered at zero: blue for districts whose
#     market access fell, red for districts whose market access rose.
#     (Almost all districts see MA rise because roads expanded ~190%.)
#   - Break points chosen to show the distribution's shape. Binned map
#     rather than continuous because the long positive tail would
#     otherwise wash out the small-change districts.
#   - 312 districts, all with finite ΔlogMA after Phase 2b navigation.
# ===========================================================================

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    if (!dir.exists(dir_figures)) dir.create(dir_figures, recursive = TRUE)

    d <- load_panel_with_geometry()

    for (fmt in c("pdf", "png")) {
        out <- file.path(
            dir_figures,
            sprintf("figure_2_ma_change_choropleth.%s", fmt)
        )
        open_device(out, fmt)
        par(mar = c(0.5, 0.5, 3, 0.5))
        draw_choropleth(d)
        dev.off()
        message("Saved: ", out)
    }
}

# ---------------------------------------------------------------------------
# Device helpers (matches plot_figure_1.R convention)
# ---------------------------------------------------------------------------
open_device <- function(path, fmt) {
    if (fmt == "pdf") {
        grDevices::pdf(path, width = 7, height = 9)
    } else if (fmt == "png") {
        grDevices::png(path, width = 1400, height = 1800, res = 200)
    }
}

# ---------------------------------------------------------------------------
# Load districts and merge the ΔlogMA column
# ---------------------------------------------------------------------------
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

    merge_cols <- c("geolev2", "chg_logMA_86_60_s0_elow")
    stopifnot(all(merge_cols %in% names(panel)))
    merge(shp, panel[, merge_cols], by = "geolev2")
}

# ---------------------------------------------------------------------------
# Draw the choropleth
# ---------------------------------------------------------------------------
draw_choropleth <- function(d) {
    x <- d$chg_logMA_86_60_s0_elow

    # Diverging binning. Centered at 0.
    breaks <- c(-Inf, -1, 0, 0.5, 1, 2, 4, Inf)
    labels <- c("< -1", "(-1, 0]", "(0, 0.5]", "(0.5, 1]",
                "(1, 2]", "(2, 4]", "> 4")
    # Blue below 0, red above — diverging palette (colorblind-safe)
    palette <- c(
        "< -1"       = "#053061",
        "(-1, 0]"    = "#4393c3",
        "(0, 0.5]"   = "#fddbc7",
        "(0.5, 1]"   = "#f4a582",
        "(1, 2]"     = "#d6604d",
        "(2, 4]"     = "#b2182b",
        "> 4"        = "#67001f"
    )

    d$cat <- cut(x, breaks = breaks, labels = labels, right = TRUE)
    d$cat <- factor(as.character(d$cat), levels = labels)

    plot(sf::st_geometry(d),
         col = palette[d$cat],
         border = "grey50",
         lwd = 0.3,
         main = expression(paste(Delta, "log MA: 1960 ", "\u2192", " 1986")))

    graphics::legend(
        "bottomleft",
        legend = labels,
        fill   = palette,
        bty    = "n",
        cex    = 0.85,
        y.intersp = 1.1,
        title  = expression(paste(Delta, "log MA"))
    )

    # Summary stats on the figure
    ok <- !is.na(x)
    subtitle <- sprintf(
        "N = %d districts. Mean = %+.3f, SD = %.3f. %.0f%% positive.",
        sum(ok), mean(x[ok]), sd(x[ok]),
        100 * mean(x[ok] > 0)
    )
    graphics::mtext(subtitle, side = 1, line = -0.5, cex = 0.75)
}

main()

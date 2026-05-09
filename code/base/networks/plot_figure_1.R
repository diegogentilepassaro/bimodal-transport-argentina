# ===========================================================================
# plot_figure_1.R
#
# PURPOSE: Paper's Figure 1 — two-panel map of the transport infrastructure
#          changes the paper exploits for identification.
#
#   Panel A (rails):
#     - Pre-dictatorship closures (status1979 == 3, closed 1960–1966),
#       shown in light grey.
#     - Dictatorship closures (status1979 == 2, closed 1976–1983),
#       highlighted in red — this is the big spatial shock on the rail side.
#     - Surviving segments (status1979 == 1, active 1979 and 1986), blue.
#
#   Panel B (roads, paved+gravel):
#     - Pre-reform network (type2 ∈ {1, 5, 7}, present in 1954), blue.
#     - Segments added 1954 → 1986 (type2 ∈ {2, 3}), highlighted in red.
#     - Types 4 and 6 (disappearing roads or 1970-only) shown in light grey.
#
# READS:
#   data/raw/networks/lp_1979.shp
#   data/raw/networks/comparacion_54_70_86.shp
#   data/raw/geo/geo2_ar1970_2010.shp
#
# PRODUCES:
#   results/figures/figure_1_network_changes.pdf   (paper version)
#   results/figures/figure_1_network_changes.png   (review-diff version)
# ===========================================================================

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    if (!dir.exists(dir_figures)) dir.create(dir_figures, recursive = TRUE)

    districts <- load_districts()
    rails     <- load_rails()
    roads     <- load_roads()

    for (fmt in c("pdf", "png")) {
        out <- file.path(
            dir_figures, sprintf("figure_1_network_changes.%s", fmt)
        )
        open_device(out, fmt)
        par(mfrow = c(1, 2),
            mar   = c(0.5, 0.5, 2, 0.5),   # no axes, compact panels
            oma   = c(0, 0, 0, 0))

        draw_rail_panel(districts, rails)
        draw_road_panel(districts, roads)

        dev.off()
        message("Saved: ", out)
    }
}

# ---------------------------------------------------------------------------
# Device helpers
# ---------------------------------------------------------------------------
open_device <- function(path, fmt) {
    if (fmt == "pdf") {
        grDevices::pdf(path, width = 14, height = 9)
    } else if (fmt == "png") {
        grDevices::png(path, width = 2800, height = 1800, res = 200)
    } else {
        stop("Unknown format: ", fmt)
    }
}

# ---------------------------------------------------------------------------
# Load the district base layer with 312-row full mainland coverage
# (same conventions as clean_roads.R / clean_railroads.R)
# ---------------------------------------------------------------------------
load_districts <- function() {
    shp_path <- file.path(dir_raw_geo, "geo2_ar1970_2010.shp")
    d <- sf::st_read(shp_path, quiet = TRUE)
    d <- sf::st_make_valid(d)
    names(d)[names(d) == "GEOLEVEL2"] <- "geolev2"
    d$geolev2 <- sub("^0+", "", as.character(d$geolev2))
    d <- d[!sf::st_is_empty(d), ]
    d <- d[!(d$geolev2 %in% geolev2_exclude), ]
    d
}

# ---------------------------------------------------------------------------
# Load rail segments
# ---------------------------------------------------------------------------
load_rails <- function() {
    shp_path <- file.path(dir_raw_networks, "lp_1979.shp")
    rails <- sf::st_read(shp_path, quiet = TRUE)
    rails <- sf::st_make_valid(rails)
    stopifnot(all(rails$status1979 %in% c(1, 2, 3)))
    rails
}

# ---------------------------------------------------------------------------
# Load road segments
# ---------------------------------------------------------------------------
load_roads <- function() {
    shp_path <- file.path(dir_raw_networks, "comparacion_54_70_86.shp")
    roads <- sf::st_read(shp_path, quiet = TRUE)
    roads <- sf::st_make_valid(roads)
    stopifnot(all(roads$type2 %in% 1:7))
    roads
}

# ---------------------------------------------------------------------------
# Panel A: railways 1960 → 1986
# ---------------------------------------------------------------------------
draw_rail_panel <- function(districts, rails) {
    plot(sf::st_geometry(districts),
         col    = "grey95",
         border = "grey75",
         lwd    = 0.3,
         main   = "(a) Railways: 1960 to 1986")

    # Draw order: least-salient first so the dictatorship closures sit on top.
    layers <- list(
        list(sel = rails$status1979 == 3, col = "grey60",  lwd = 1.0),
        list(sel = rails$status1979 == 1, col = "#1f4e79", lwd = 1.2),
        list(sel = rails$status1979 == 2, col = "#c00000", lwd = 1.4)
    )
    for (L in layers) {
        sub <- rails[L$sel, ]
        if (nrow(sub) > 0) {
            plot(sf::st_geometry(sub), col = L$col, lwd = L$lwd, add = TRUE)
        }
    }

    # Counts for the legend (km, not segments — more meaningful)
    km_by_status <- sapply(1:3, function(s) {
        sub <- rails[rails$status1979 == s, ]
        round(sum(as.numeric(sf::st_length(sub)) / 1000))
    })

    legend(
        "bottomright",
        legend = c(
            sprintf("Active 1979 and 1986  (%s km)",
                    format(km_by_status[1], big.mark = ",")),
            sprintf("Closed during dictatorship 1976-83  (%s km)",
                    format(km_by_status[2], big.mark = ",")),
            sprintf("Closed 1960-66, pre-dictatorship  (%s km)",
                    format(km_by_status[3], big.mark = ","))
        ),
        col    = c("#1f4e79", "#c00000", "grey60"),
        lwd    = c(1.2, 1.4, 1.0),
        bty    = "n",
        cex    = 0.85,
        y.intersp = 1.2
    )
}

# ---------------------------------------------------------------------------
# Panel B: roads 1954 → 1986
#
# type2 taxonomy (from clean_roads.R):
#   1 = 1954 + 1970 + 1986    (blue; pre-reform + retained)
#   5 = 1954 + 1986 absent-1970 (blue; cartographic error, 1954-era)
#   7 = 1954 only              (grey; disappeared)
#   2 = new in 1970            (red; part of the expansion)
#   3 = new in 1986            (red; part of the expansion)
#   4 = 1954 + 1970 gone 1986  (grey; disappeared)
#   6 = 1970 only              (grey; transient)
# ---------------------------------------------------------------------------
draw_road_panel <- function(districts, roads) {
    plot(sf::st_geometry(districts),
         col    = "grey95",
         border = "grey75",
         lwd    = 0.3,
         main   = "(b) Paved+gravel roads: 1954 to 1986")

    # Categorize
    roads$cat <- with(roads, ifelse(
        type2 %in% c(1, 5),  "kept",        # in 1954 and retained in 1986
        ifelse(type2 %in% c(2, 3), "added", # new 1954 to 1986
        "other")                             # disappeared or 1970-only
    ))

    layers <- list(
        list(sel = roads$cat == "other", col = "grey60",  lwd = 0.7),
        list(sel = roads$cat == "kept",  col = "#1f4e79", lwd = 1.0),
        list(sel = roads$cat == "added", col = "#c00000", lwd = 1.1)
    )
    for (L in layers) {
        sub <- roads[L$sel, ]
        if (nrow(sub) > 0) {
            plot(sf::st_geometry(sub), col = L$col, lwd = L$lwd, add = TRUE)
        }
    }

    # Total km per category
    km <- sapply(c("kept", "added", "other"), function(k) {
        sub <- roads[roads$cat == k, ]
        round(sum(as.numeric(sf::st_length(sub)) / 1000))
    })

    legend(
        "bottomright",
        legend = c(
            sprintf("Present in 1954 and 1986  (%s km)",
                    format(km["kept"], big.mark = ",")),
            sprintf("Added 1954 to 1986  (%s km)",
                    format(km["added"], big.mark = ",")),
            sprintf("Disappeared or short-lived  (%s km)",
                    format(km["other"], big.mark = ","))
        ),
        col    = c("#1f4e79", "#c00000", "grey60"),
        lwd    = c(1.0, 1.1, 0.7),
        bty    = "n",
        cex    = 0.85,
        y.intersp = 1.2
    )
}

main()

# ===========================================================================
# preview_networks.R
#
# PURPOSE: Eyeball-check figures for the cleaned road and rail networks.
#          Run on demand after clean_roads.R and clean_railroads.R.
#
# READS:
#   data/raw/networks/lp_1979.shp
#   data/raw/networks/comparacion_54_70_86.shp
#   data/raw/geo/geo2_ar1970_2010.shp
#   data/derived/base/networks/rails_by_district.parquet
#   data/derived/base/networks/roads_by_district.parquet
#
# PRODUCES:
#   results/figures/rails_by_status1979.png
#   results/figures/rails_by_studied.png
#   results/figures/rails_chg_8660_choropleth.png
#   results/figures/roads_by_period.png
#   results/figures/roads_chg_8654_choropleth.png
# ===========================================================================

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    if (!dir.exists(dir_figures)) dir.create(dir_figures, recursive = TRUE)

    districts <- load_districts_plain()

    # --- Rails ---
    rails <- sf::st_read(
        file.path(dir_raw_networks, "lp_1979.shp"), quiet = TRUE
    )
    rails <- sf::st_make_valid(rails)
    rails_panel <- arrow::read_parquet(
        file.path(dir_derived_networks, "rails_by_district.parquet")
    )

    plot_rails_by_status(rails, districts)
    plot_rails_by_studied(rails, districts)
    plot_rails_choropleth(rails_panel, districts)

    # --- Roads ---
    roads <- sf::st_read(
        file.path(dir_raw_networks, "comparacion_54_70_86.shp"),
        quiet = TRUE
    )
    roads <- sf::st_make_valid(roads)
    roads_panel <- arrow::read_parquet(
        file.path(dir_derived_networks, "roads_by_district.parquet")
    )

    plot_roads_by_period(roads, districts)
    plot_roads_choropleth(roads_panel, districts)

    message("Done — 5 figures in results/figures/")
}

# ---------------------------------------------------------------------------
# Helper: load districts (no sf-column trickery needed for plotting)
# ---------------------------------------------------------------------------
load_districts_plain <- function() {
    d <- sf::st_read(
        file.path(dir_raw_geo, "geo2_ar1970_2010.shp"), quiet = TRUE
    )
    d <- sf::st_make_valid(d)
    names(d)[names(d) == "GEOLEVEL2"] <- "geolev2"
    d$geolev2 <- sub("^0+", "", as.character(d$geolev2))
    d <- d[!sf::st_is_empty(d), ]
    d <- d[!(d$geolev2 %in% geolev2_exclude), ]
    d
}

# ---------------------------------------------------------------------------
# Figure 1: Rails colored by status1979
# ---------------------------------------------------------------------------
plot_rails_by_status <- function(rails, districts) {
    out <- file.path(dir_figures, "rails_by_status1979.png")
    png(out, width = 1600, height = 2000, res = 150)
    par(mar = c(2, 2, 3, 2))

    plot(sf::st_geometry(districts), col = "grey95", border = "grey70",
         lwd = 0.3,
         main = "Rail segments by status1979")

    # Plot layers in order: active on top so it's most visible
    ord <- c(3, 2, 1)
    cols <- c("1" = "#1f77b4", "2" = "#d62728", "3" = "#7f7f7f")
    lwds <- c("1" = 1.1,        "2" = 1.0,        "3" = 0.9)
    for (s in ord) {
        r <- rails[rails$status1979 == s, ]
        plot(sf::st_geometry(r), col = cols[as.character(s)],
             lwd = lwds[as.character(s)], add = TRUE)
    }

    legend("bottomright",
           legend = c(
               sprintf("1 = active 1979/1986 (n=%d)",
                       sum(rails$status1979 == 1)),
               sprintf("2 = closed in dictadura 1976-83 (n=%d)",
                       sum(rails$status1979 == 2)),
               sprintf("3 = closed pre-dictadura 1960-66 (n=%d)",
                       sum(rails$status1979 == 3))
           ),
           col = cols, lwd = 2, bty = "n", cex = 0.85)

    dev.off()
    message("Saved: ", out)
}

# ---------------------------------------------------------------------------
# Figure 2: Rails colored by studied_co (Larkin instrument)
# ---------------------------------------------------------------------------
plot_rails_by_studied <- function(rails, districts) {
    out <- file.path(dir_figures, "rails_by_studied.png")
    png(out, width = 1600, height = 2000, res = 150)
    par(mar = c(2, 2, 3, 2))

    plot(sf::st_geometry(districts), col = "grey95", border = "grey70",
         lwd = 0.3,
         main = "Rail segments by Larkin-study flag (instrument)")

    r0 <- rails[rails$studied_co == 0, ]
    r1 <- rails[rails$studied_co == 1, ]
    plot(sf::st_geometry(r0), col = "#7f7f7f", lwd = 0.9, add = TRUE)
    plot(sf::st_geometry(r1), col = "#d62728", lwd = 1.2, add = TRUE)

    legend("bottomright",
           legend = c(
               sprintf("0 = not studied (n=%d)", nrow(r0)),
               sprintf("1 = Larkin-studied (n=%d)", nrow(r1))
           ),
           col = c("#7f7f7f", "#d62728"), lwd = 2, bty = "n", cex = 0.85)

    dev.off()
    message("Saved: ", out)
}

# ---------------------------------------------------------------------------
# Figure 3: District choropleth of rail-km change 1960 -> 1986
# ---------------------------------------------------------------------------
plot_rails_choropleth <- function(panel, districts) {
    out <- file.path(dir_figures, "rails_chg_8660_choropleth.png")

    d <- merge(districts[, "geolev2"], panel[, c("geolev2",
                                                 "chg_tot_rails_86_60",
                                                 "tot_rails_1960")],
               by = "geolev2", all.x = TRUE)
    # Districts with zero 1960 rails have chg = 0 — separate them so the
    # colour scale isn't dominated by "no change = no rail"
    d$lost_km <- -d$chg_tot_rails_86_60  # positive = km lost
    d$cat <- cut(
        d$lost_km,
        breaks = c(-Inf, 0.0001, 25, 75, 150, Inf),
        labels = c("No rail / no loss", "(0, 25]", "(25, 75]",
                   "(75, 150]", "150+"),
        right = TRUE
    )
    # Districts that had no rail in 1960 get their own "grey" level
    d$cat <- as.character(d$cat)
    d$cat[is.na(d$tot_rails_1960) | d$tot_rails_1960 < 0.001] <- "No rail in 1960"
    d$cat <- factor(d$cat, levels = c(
        "No rail in 1960", "No rail / no loss",
        "(0, 25]", "(25, 75]", "(75, 150]", "150+"
    ))

    pal <- c("No rail in 1960"   = "grey90",
             "No rail / no loss" = "#ffffcc",
             "(0, 25]"           = "#fed976",
             "(25, 75]"          = "#fd8d3c",
             "(75, 150]"         = "#e31a1c",
             "150+"              = "#800026")

    png(out, width = 1600, height = 2000, res = 150)
    par(mar = c(2, 2, 3, 2))

    plot(sf::st_geometry(d), col = pal[d$cat], border = "grey70", lwd = 0.3,
         main = "Rail km lost 1960 → 1986 (by district)")
    legend("bottomright",
           legend = names(pal),
           fill = pal, bty = "n", cex = 0.85,
           title = "Rail km lost")

    dev.off()
    message("Saved: ", out)
}

# ---------------------------------------------------------------------------
# Figure 4: Roads colored by type2 (period presence)
# ---------------------------------------------------------------------------
plot_roads_by_period <- function(roads, districts) {
    out <- file.path(dir_figures, "roads_by_period.png")
    png(out, width = 1600, height = 2000, res = 150)
    par(mar = c(2, 2, 3, 2))

    plot(sf::st_geometry(districts), col = "grey95", border = "grey70",
         lwd = 0.3,
         main = "Road segments by period (comparacion_54_70_86)")

    cols <- c(
        "1" = "#1f77b4",   # present all three
        "2" = "#2ca02c",   # new in 1970
        "3" = "#d62728",   # new in 1986
        "4" = "#9467bd",   # gone by 1986
        "5" = "#8c564b",   # 1954+1986, absent 1970 (error)
        "6" = "#e377c2",   # 1970 only
        "7" = "#7f7f7f"    # 1954 only
    )
    labs <- c(
        "1" = "1: 1954+1970+1986",
        "2" = "2: new in 1970",
        "3" = "3: new in 1986",
        "4" = "4: gone by 1986",
        "5" = "5: 1954+1986 (absent 1970)",
        "6" = "6: 1970 only",
        "7" = "7: 1954 only"
    )

    # Plot from least-visible (type 7) up so salient types sit on top
    ord <- c("7", "6", "5", "4", "3", "2", "1")
    for (s in ord) {
        r <- roads[as.character(roads$type2) == s, ]
        if (nrow(r) == 0) next
        plot(sf::st_geometry(r), col = cols[s], lwd = 0.9, add = TRUE)
    }

    present <- sort(unique(as.character(roads$type2)))
    legend("bottomright",
           legend = sprintf("%s (n=%d)", labs[present],
                            sapply(present, function(s)
                                sum(as.character(roads$type2) == s))),
           col = cols[present], lwd = 2, bty = "n", cex = 0.8)

    dev.off()
    message("Saved: ", out)
}

# ---------------------------------------------------------------------------
# Figure 5: District choropleth of pav+grav road change 1954 -> 1986
# ---------------------------------------------------------------------------
plot_roads_choropleth <- function(panel, districts) {
    out <- file.path(dir_figures, "roads_chg_8654_choropleth.png")

    d <- merge(districts[, "geolev2"],
               panel[, c("geolev2", "chg_pav_and_grav_86_54",
                         "pav_and_grav_1954")],
               by = "geolev2", all.x = TRUE)

    d$cat <- cut(
        d$chg_pav_and_grav_86_54,
        breaks = c(-Inf, 0, 25, 100, 250, 500, Inf),
        labels = c("Zero/negative", "(0, 25]", "(25, 100]",
                   "(100, 250]", "(250, 500]", "500+"),
        right = TRUE
    )
    d$cat <- as.character(d$cat)
    d$cat[is.na(d$chg_pav_and_grav_86_54)] <- "No data"
    d$cat <- factor(d$cat, levels = c(
        "No data", "Zero/negative", "(0, 25]", "(25, 100]",
        "(100, 250]", "(250, 500]", "500+"
    ))

    pal <- c("No data"       = "grey90",
             "Zero/negative" = "#ffffcc",
             "(0, 25]"       = "#c7e9b4",
             "(25, 100]"     = "#7fcdbb",
             "(100, 250]"    = "#41b6c4",
             "(250, 500]"    = "#1d91c0",
             "500+"          = "#253494")

    png(out, width = 1600, height = 2000, res = 150)
    par(mar = c(2, 2, 3, 2))

    plot(sf::st_geometry(d), col = pal[d$cat], border = "grey70", lwd = 0.3,
         main = "Paved+gravel road km added 1954 → 1986 (by district)")
    legend("bottomright",
           legend = names(pal),
           fill = pal, bty = "n", cex = 0.85,
           title = "Road km added")

    dev.off()
    message("Saved: ", out)
}

main()

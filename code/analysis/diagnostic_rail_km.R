# ===========================================================================
# diagnostic_rail_km.R
#
# PURPOSE: Measure every rail-kilometre and segment-count quantity that
#          Sections 2 and 3 quote, from the one rail source this package
#          has, so that none of them is typed into prose. Before this
#          script the six rail-km figures in Section 3 and the three in
#          Section 2 were literals, and they disagreed with each other
#          (see PROVENANCE below).
#
# WHAT THE RAIL SOURCE IS, AND WHAT IT IS NOT. data/raw/networks/lp_1979.shp
# is a SINGLE 1979 snapshot: 565 segments whose only temporal field is
# status1979 in {1, 2, 3}. There is no closure-date field, and there is no
# second rail map at another date. The 1960 and 1986 rail networks the
# pipeline builds are that one map with status buckets included or excluded:
#   1960     = status 1 + 2 + 3   (assumes the status-3 segments, "closed
#                                  before 1976", were still open in 1960)
#   pre-1976 = status 1 + 2       (the definition of the field)
#   1986     = status 1           (active in 1979 and 1986)
# The multi-period network in this project is the ROAD file
# (comparacion_54_70_86.shp, three cross-sections). Rail is not.
#
# PROVENANCE OF THE DISCREPANCIES THIS RESOLVES (Cote's kms-via note,
# 2026-09-16, reproduced independently here and recorded in
# .kiro/rail_km_sources_note.md):
#   - Section 2 quoted ~43,500 -> ~33,500 while Section 3 quoted
#     42,780 -> 33,303. Both were right for their own source: Section 2 was
#     quoting a national track-kilometre series, Section 3 this map, and the
#     map runs a uniform ~2.5% short of that series. Section 2 now quotes
#     the map, so the paper has one rail source and the gap needs no
#     explaining. The series comparison is recorded in the note above and is
#     NOT cited in the paper: its provenance is unresolved (asked of Cote).
#   - Section 3 called status1979 "documented closure dates" and labelled
#     the status-3 kilometres "closed between 1960 and 1966". The field is a
#     status, and its own documentation (data/raw/networks/readme.md and the
#     clean_railroads manifest) says "closed before 1976". The 1960-1966
#     label also made a claim the national series contradicts: it records
#     1,982 km closed in that window, not 3,914.
#
# METHOD: geodesic length via sf::st_length() on EPSG:4326, which is what
#   clean_railroads.R uses, so these totals are comparable to the manifest's.
#   sf uses s2 great-circle lengths; a WGS-84 ellipsoid calculation
#   (pyproj.Geod, which Cote used) gives 42,785.5 km against our 42,789.9 for
#   the same segments, a difference of 0.01% that moves nothing.
#
# READS:
#   data/raw/networks/lp_1979.shp
#   data/derived/base/networks/rails_by_district.parquet
#
# PRODUCES:
#   results/tables/diagnostic_rail_km.csv   (tidy: quantity, value, source)
#   results/tables/diagnostic_rail_km.txt   (human-readable report)
# ===========================================================================

suppressPackageStartupMessages({
    library(sf)
    library(arrow)
})

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    rails <- sf::st_read(file.path(dir_raw_networks, "lp_1979.shp"),
                         quiet = TRUE)
    stopifnot(nrow(rails) == 565L,
              all(c("status1979", "studied_co") %in% names(rails)),
              !any(grepl("date|fecha|year|anio", names(rails),
                         ignore.case = TRUE)))
    rails$len_km <- as.numeric(sf::st_length(rails)) / 1000

    km_by_status <- tapply(rails$len_km, rails$status1979, sum)
    n_by_status  <- table(rails$status1979)
    stopifnot(identical(sort(names(km_by_status)), c("1", "2", "3")))

    # The clipped district aggregates the paper actually quotes.
    rbd <- arrow::read_parquet(
        file.path(dir_derived_base, "networks", "rails_by_district.parquet"))
    clip_1960 <- sum(rbd$tot_rails_1960)
    clip_1986 <- sum(rbd$tot_rails_1986)
    clip_s2   <- sum(rbd$status1979_2)
    clip_s3   <- sum(rbd$status1979_3)
    n_no_rail <- sum(rbd$tot_rails_1960 == 0)

    unclip_1960 <- sum(km_by_status)
    clip_loss   <- unclip_1960 - clip_1960
    stopifnot("clip must not lose more than 0.1% of the network" =
                  abs(clip_loss) / unclip_1960 < 0.001)

    q <- function(quantity, value, source, note = "") {
        data.frame(quantity = quantity, value = value, source = source,
                   note = note, stringsAsFactors = FALSE)
    }
    df <- rbind(
        q("rail_km_1960", clip_1960, "map, district-clipped",
          "status 1+2+3; assumes status-3 segments open in 1960"),
        q("rail_km_1986", clip_1986, "map, district-clipped", "status 1"),
        q("rail_km_lost", clip_1960 - clip_1986, "map, district-clipped", ""),
        q("rail_km_dictatorship", clip_s2, "map, district-clipped",
          "status 2, closed 1976-1983"),
        q("rail_km_pre1976", clip_s3, "map, district-clipped",
          "status 3, closed before 1976 -- window NOT 1960-1966"),
        q("rail_pct_fall", 100 * (1 - clip_1986 / clip_1960),
          "map, district-clipped", ""),
        q("rail_share_dictatorship", 100 * clip_s2 / (clip_1960 - clip_1986),
          "map, district-clipped", ""),
        q("rail_share_pre1976", 100 * clip_s3 / (clip_1960 - clip_1986),
          "map, district-clipped", ""),
        q("rail_km_1960_unclipped", unclip_1960, "map, segments", ""),
        q("rail_km_pre1976_cut", sum(km_by_status[c("1", "2")]),
          "map, segments", "status 1+2, the field's own definition"),
        q("rail_km_1986_unclipped", km_by_status[["1"]], "map, segments", ""),
        q("rail_km_clip_loss", clip_loss, "map",
          "unclipped minus district-clipped"),
        q("n_segments", nrow(rails), "map", ""),
        q("n_studied", sum(rails$studied_co == 1), "map",
          "Larkin Plan studied segments (the instrument)"),
        q("n_not_studied", sum(rails$studied_co == 0), "map", ""),
        q("n_districts_no_rail", n_no_rail, "map, district-clipped", "")
    )
    write.csv(df, file.path(dir_tables, "diagnostic_rail_km.csv"),
              row.names = FALSE)

    con <- file(file.path(dir_tables, "diagnostic_rail_km.txt"), open = "wt")
    w <- function(...) { l <- sprintf(...); cat(l, "\n")
                         cat(l, "\n", file = con) }
    w("%s", strrep("=", 72))
    w("RAIL KILOMETRES: the quantities Sections 2 and 3 quote")
    w("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
    w("%s", strrep("=", 72))
    w("")
    w("SOURCE: lp_1979.shp, a single 1979 snapshot. status1979 is a STATUS,")
    w("not a date; the file has no date field and there is no second rail")
    w("map. The 1960 and 1986 networks are status buckets of this one map.")
    w("")
    w("Segments by status (unclipped):")
    w("  %-9s %6s %12s  %s", "status", "n", "km", "meaning")
    meaning <- c("1" = "active 1979 and 1986",
                 "2" = "closed during the dictatorship (1976-1983)",
                 "3" = "closed before 1976")
    for (s in c("1", "2", "3")) {
        w("  %-9s %6d %12.1f  %s", s, n_by_status[[s]], km_by_status[[s]],
          meaning[[s]])
    }
    w("")
    w("The three cuts the status field supports:")
    w("  1960     (1+2+3) %12.1f km   by assumption: status 3 open in 1960",
      unclip_1960)
    w("  pre-1976 (1+2)   %12.1f km   clean: the field's own definition",
      sum(km_by_status[c("1", "2")]))
    w("  1986     (1)     %12.1f km   clean", km_by_status[["1"]])
    w("")
    w("District clipping loses %.1f km of %.1f (%.3f%%), so the clipped",
      clip_loss, unclip_1960, 100 * clip_loss / unclip_1960)
    w("totals the paper quotes are the segment totals to three figures.")
    w("")
    w("What the paper quotes (all now AutoFilled from this file):")
    w("  1960 -> 1986: %.0f -> %.0f km, a %.0f%% fall", clip_1960, clip_1986,
      100 * (1 - clip_1986 / clip_1960))
    w("  of %.0f km lost: %.0f (%.0f%%) dictatorship, %.0f (%.0f%%) pre-1976",
      clip_1960 - clip_1986, clip_s2,
      100 * clip_s2 / (clip_1960 - clip_1986), clip_s3,
      100 * clip_s3 / (clip_1960 - clip_1986))
    w("  %d segments, %d studied by the Larkin Plan, %d not",
      nrow(rails), sum(rails$studied_co == 1), sum(rails$studied_co == 0))
    w("  %d districts with no rail in any period", n_no_rail)
    w("")
    w("NOT IN THIS FILE, DELIBERATELY: the national track-kilometre series")
    w("(1857-1989) that Section 2 used to quote. It is a second source, it")
    w("runs ~2.5%% above this map uniformly across all three cuts, and its")
    w("provenance is unresolved, so it is not cited in the paper and not")
    w("read by any script. See .kiro/rail_km_sources_note.md.")
    close(con)
    message("Saved: diagnostic_rail_km.{csv,txt}")
}

main()

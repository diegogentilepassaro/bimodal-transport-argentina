# ===========================================================================
# diagnostic_rail_km.R
#
# PURPOSE: Measure every rail-kilometre, segment-count and studied-share
#          quantity that Sections 2, 3 and 4 quote, from the one rail
#          source this package has, so that none of them is typed into
#          prose. Before this script those figures were literals, and
#          they disagreed with each other (see PROVENANCE below).
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
#     the map, so the paper has one rail source. The series comparison is
#     recorded in the note above and is NOT cited in the paper: its
#     provenance is unresolved (asked of Cote).
#   - Section 3 called status1979 "documented closure dates" and labelled
#     the status-3 kilometres "closed between 1960 and 1966". The field is a
#     status, and its own documentation (data/raw/networks/readme.md and the
#     clean_railroads manifest) says "closed before 1976". The 1960-1966
#     label also made a claim the national series contradicts: it records
#     1,982 km closed in that window, not 3,914.
#   - Section 4's studied-share footnote quoted a recom_code breakdown
#     (2,310 / 14,377 / 5,197 km, giving 16,687 km and 38.4%) that does NOT
#     reproduce from this shapefile under geodesic measurement: the same
#     groups measure 2,202 / 13,743 / 4,922, about 5% shorter. The file is
#     byte-identical to the copy those figures came from (md5
#     89c28feab3535f83a1e49933b43f8404, checked against the coauthor's
#     ledger), so the difference is method, not data -- most likely lengths
#     taken in a single planar CRS, which over Argentina's seven
#     Gauss-Kruger zones inflates by about this much. The footnote now uses
#     the geodesic figures computed here.
#
# METHOD: geodesic length via sf::st_length() on EPSG:4326, the CRS the
#   shapefile is stored in. clean_railroads.R reprojects rails to the
#   district CRS before intersecting; that CRS is also 4326, so the two are
#   comparable in practice, which the agreement with the manifest below
#   confirms. sf uses s2 great-circle lengths; a WGS-84 ellipsoid
#   calculation (pyproj.Geod, which Cote used) gives 42,785.5 km against our
#   42,789.9 for the same segments, a difference of 0.01% that moves
#   nothing.
#
# READS:
#   data/raw/networks/lp_1979.shp
#   data/derived/base/networks/rails_by_district.parquet
#
# PRODUCES:
#   results/tables/diagnostic_rail_km.csv   (quantity, value, source, note)
#   results/tables/diagnostic_rail_km.txt   (human-readable report)
# ===========================================================================

suppressPackageStartupMessages({
    library(sf)
    library(arrow)
})

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)

    rails <- sf::st_read(file.path(dir_raw_networks, "lp_1979.shp"),
                         quiet = TRUE)
    # The no-date-field check is deliberately a check on NAMES: it exists to
    # stop Section 3's claim that the file carries a status rather than a
    # date from going stale if the shapefile is ever replaced by one with a
    # date column. It cannot prove the absence of a date encoded under some
    # other name, and it will fail loudly on a harmless new column called
    # something like update_date -- which is the intended trade: a false
    # alarm is cheap, a false claim in the paper is not.
    stopifnot(nrow(rails) == 565L,
              all(c("status1979", "studied_co", "recom_code") %in%
                      names(rails)),
              !any(grepl("date|fecha|year|anio", names(rails),
                         ignore.case = TRUE)))
    # tapply() and table() drop NA groups silently, and a dropped group
    # would remove its kilometres from every total below without tripping
    # any of the comparisons. Rule the case out rather than hope for it.
    stopifnot("status/studied/recom must have no missing values" =
                  !anyNA(rails$status1979) && !anyNA(rails$studied_co) &&
                  !anyNA(rails$recom_code),
              "status1979 must be exactly {1,2,3}" =
                  setequal(unique(rails$status1979), c(1, 2, 3)),
              "studied_co must be exactly {0,1}" =
                  setequal(unique(rails$studied_co), c(0, 1)))
    rails$len_km <- as.numeric(sf::st_length(rails)) / 1000

    km_by_status <- tapply(rails$len_km, rails$status1979, sum)
    n_by_status  <- table(rails$status1979)
    n_studied     <- sum(rails$studied_co == 1)
    n_not_studied <- sum(rails$studied_co == 0)
    # The partition Section 3 asserts ("237 ... or among the 328"). Also
    # catches an NA in studied_co, which would otherwise reach the paper as
    # the string "NA" through the macro.
    stopifnot("studied + not studied must exhaust the segments" =
                  n_studied + n_not_studied == nrow(rails))

    # Studied-share family, for the Section 4 footnote. Measured on whole
    # segments: the recom_code breakdown exists only in the shapefile, not
    # in the district-clipped table, and clipping moves these shares by
    # less than 0.03pp (see the clip loss below).
    st       <- rails[rails$studied_co == 1, ]
    km_st    <- sum(st$len_km)
    km_new   <- sum(st$len_km[st$recom_code == 3])
    km_st_ex <- km_st - km_new

    rbd_path <- file.path(dir_derived_networks, "rails_by_district.parquet")
    rbd <- arrow::read_parquet(rbd_path)
    need <- c("status1979_2", "status1979_3", "tot_rails_1960",
              "tot_rails_1986")
    stopifnot("rails_by_district must cover the 312 districts" =
                  nrow(rbd) == 312L,
              "rails_by_district is missing a needed column" =
                  all(need %in% names(rbd)))
    clip_1960 <- sum(rbd$tot_rails_1960)
    clip_1986 <- sum(rbd$tot_rails_1986)
    clip_s2   <- sum(rbd$status1979_2)
    clip_s3   <- sum(rbd$status1979_3)
    # Exact-zero comparison would be at the mercy of a clipping change that
    # leaves slivers behind.
    n_no_rail <- sum(rbd$tot_rails_1960 < 1e-6)

    unclip_1960 <- sum(km_by_status)
    clip_loss   <- unclip_1960 - clip_1960
    clip_pct    <- 100 * clip_loss / unclip_1960
    # Per-status clip loss, because the aggregate rate understates the
    # status-3 bucket by a factor of eight and 3,914 is a number the paper
    # prints.
    clip_pct_s3 <- 100 * (km_by_status[["3"]] - clip_s3) / km_by_status[["3"]]
    stopifnot("district clipping must stay under 0.5% on every bucket" =
                  max(clip_pct, clip_pct_s3) < 0.5)

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
        q("rail_km_closed_pre1976", clip_s3, "map, district-clipped",
          "status 3, closed before 1976 -- window NOT 1960-1966"),
        q("rail_pct_fall", 100 * (1 - clip_1986 / clip_1960),
          "map, district-clipped", ""),
        q("rail_share_dictatorship", 100 * clip_s2 / (clip_1960 - clip_1986),
          "map, district-clipped", ""),
        q("rail_share_closed_pre1976", 100 * clip_s3 /
              (clip_1960 - clip_1986), "map, district-clipped", ""),
        q("rail_km_1960_unclipped", unclip_1960, "map, segments", ""),
        q("rail_km_network_pre1976", sum(km_by_status[c("1", "2")]),
          "map, segments",
          "status 1+2 = the network STANDING before 1976, not the km closed"),
        q("rail_km_1986_unclipped", km_by_status[["1"]], "map, segments", ""),
        q("rail_km_clip_loss", clip_loss, "map",
          "unclipped minus district-clipped"),
        q("rail_clip_loss_pct", clip_pct, "map", "on the 1960 total"),
        q("rail_clip_loss_pct_status3", clip_pct_s3, "map",
          "worst bucket; the paper prints this bucket's km"),
        q("rail_km_studied", km_st, "map, segments",
          "studied_co == 1, the instrument's classification"),
        q("rail_km_studied_ex_newstudy", km_st_ex, "map, segments",
          "studied minus recom_code 3 (new study)"),
        q("rail_km_newstudy", km_new, "map, segments",
          "studied and recom_code == 3"),
        q("rail_studied_share", 100 * km_st / unclip_1960, "map, segments",
          ""),
        q("rail_studied_share_ex_newstudy", 100 * km_st_ex / unclip_1960,
          "map, segments", ""),
        q("n_segments", nrow(rails), "map", ""),
        q("n_studied", n_studied, "map",
          "Larkin Plan studied segments (the instrument)"),
        q("n_not_studied", n_not_studied, "map", ""),
        q("n_districts_no_rail", n_no_rail, "map, district-clipped", "")
    )
    write.csv(df, file.path(dir_tables, "diagnostic_rail_km.csv"),
              row.names = FALSE)

    con <- file(file.path(dir_tables, "diagnostic_rail_km.txt"), open = "wt")
    on.exit(close(con), add = TRUE)
    w <- function(...) { l <- sprintf(...); cat(l, "\n", sep = "")
                         cat(l, "\n", file = con, sep = "") }
    w("%s", strrep("=", 72))
    w("RAIL KILOMETRES: the quantities Sections 2, 3 and 4 quote")
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
    w("District clipping loses %.1f km of %.1f, %.3f%% of the 1960 total.",
      clip_loss, unclip_1960, clip_pct)
    w("It is NOT uniform across buckets: the status-3 bucket loses %.2f%%,",
      clip_pct_s3)
    w("about %.0fx the aggregate rate, and status 3 is the bucket whose km",
      clip_pct_s3 / clip_pct)
    w("the paper prints (%.0f clipped against %.0f unclipped).",
      clip_s3, km_by_status[["3"]])
    w("")
    w("What Sections 2 and 3 quote (all AutoFilled from this file):")
    w("  1960 -> 1986: %.0f -> %.0f km, a %.0f%% fall", clip_1960, clip_1986,
      100 * (1 - clip_1986 / clip_1960))
    w("  of %.0f km lost: %.0f (%.0f%%) dictatorship, %.0f (%.0f%%) pre-1976",
      clip_1960 - clip_1986, clip_s2,
      100 * clip_s2 / (clip_1960 - clip_1986), clip_s3,
      100 * clip_s3 / (clip_1960 - clip_1986))
    w("  %d segments, %d studied by the Larkin Plan, %d not",
      nrow(rails), n_studied, n_not_studied)
    w("  %d districts with no rail in any period", n_no_rail)
    w("")
    w("What Section 4's studied-share footnote quotes:")
    w("  studied                      %8.1f km  %5.2f%% of the 1960 network",
      km_st, 100 * km_st / unclip_1960)
    w("  of which new-study (recom 3) %8.1f km", km_new)
    w("  studied excluding new-study  %8.1f km  %5.2f%%",
      km_st_ex, 100 * km_st_ex / unclip_1960)
    w("  Measured on whole segments; clipping moves these shares by less")
    w("  than 0.03pp. The previously reported 2,310 / 14,377 / 5,197 km")
    w("  (16,687 km, 38.4%%) do not reproduce geodesically from this file,")
    w("  which is byte-identical to the copy they came from; the likely")
    w("  cause is lengths taken in a single planar CRS.")
    w("")
    w("NOT IN THIS FILE, DELIBERATELY: the national track-kilometre series")
    w("(1857-1989) that Section 2 used to quote. It is a second source, it")
    w("runs ~2.5%% above this map uniformly across all three cuts, and its")
    w("provenance is unresolved, so it is not cited in the paper and not")
    w("read by any script. See .kiro/rail_km_sources_note.md.")
    message("Saved: diagnostic_rail_km.{csv,txt}")
}

main()

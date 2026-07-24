# ===========================================================================
# diagnostic_roadseg_prep.R
#
# PURPOSE: Preparation for the corridor-timing design instrument
#          (successor to the settlement road-timing design of PR #115,
#          which was balanced but too small a dose: recentered F ~ 1).
#          Move the timing experiment onto the ACTUAL road expansion:
#          comparacion_54_70_86.shp codes each segment's vintage,
#          type2 = 2 (new by 1970, "early") vs type2 = 3 (new by 1986,
#          "late"). Conditional on being paved eventually, WHICH
#          corridor was paved early is the world's sequencing shock --
#          same timing-as-dice logic, with dose = the full expansion.
#
#   1. CORRIDOR CHAINS: group the 982 expansion segments into chains --
#      connected components of contiguous same-vintage segments,
#      split at junctions (endpoint nodes of degree >= 3), mirroring
#      diagnostic_recentering_lines.R for the Larkin rails.
#      SCOPE (cr-review PR #117): junctions are detected among
#      EXPANSION-segment endpoints only; crossings with the
#      pre-existing 1954 network do not split chains. Deliberate --
#      the permutation unit is the newly built corridor, and where it
#      meets the old network is not a construction-sequencing joint.
#   2. STRATA: region x chain-length tercile, thin cells (>=
#      recentering_min_cell early AND late chains) merged by the same
#      converging loop as the settlement design.
#   3. CHAIN-DISTRICT WEIGHTS: length of each chain inside each
#      district (for the balance table: traversed-district
#      predetermined covariates, computed once here so the results
#      script does no spatial work).
#
# NOTE (raster-convention asymmetry, as documented in PR #115): the
# draw network rasterizes actual_1960 roads c(1, 5, 7) + the draw's
# early chains. The "true" 1970 network also contains type2 = 4/6
# (roads later removed); those are excluded to keep the z differencing
# against logMA_actual_1960 exact. Deliberate.
#
# READS:
#   data/raw/networks/comparacion_54_70_86.shp
#   data/raw/geo/geo2_ar1970_2010.shp
#
# PRODUCES (data/derived/07_recentering/roadseg/):
#   chains.gpkg               (chain_id, early, region, length_km,
#                              stratum; merged geometry per chain)
#   chains.parquet            (same, no geometry; key chain_id)
#   chain_districts.parquet   (chain_id x geolev2, km_in_district)
#   prep_report.txt, prep_manifest.log
#
# USAGE:  Rscript code/analysis/diagnostic_roadseg_prep.R
# RUNTIME: ~2-4 min (node graph + district intersection; no LCPs).
# ===========================================================================

suppressPackageStartupMessages({
    library(sf)
    library(arrow)
    library(igraph)
})

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    message("\n", strrep("=", 72))
    message("diagnostic_roadseg_prep.R  |  corridor chains + strata")
    message(strrep("=", 72))

    dir_rs <- file.path(dir_derived_recentering, "roadseg")
    if (!dir.exists(dir_rs)) dir.create(dir_rs, recursive = TRUE)

    # ---- 1. Expansion segments ----------------------------------------------
    roads <- sf::st_make_valid(sf::st_read(
        file.path(dir_raw_networks, "comparacion_54_70_86.shp"),
        quiet = TRUE))
    ex <- roads[roads$type2 %in% c(2L, 3L), ]
    n_seg <- nrow(ex)
    stopifnot(n_seg == 982L)
    ex_p <- sf::st_transform(ex, crs = crs_raster)
    ex_p$seg_row   <- seq_len(n_seg)
    ex_p$early     <- ex_p$type2 == 2L
    ex_p$length_km <- as.numeric(sf::st_length(ex_p)) / 1000
    message(sprintf(
        "[rs] %d expansion segments: %d early (%.0f km) / %d late (%.0f km)",
        n_seg, sum(ex_p$early), sum(ex_p$length_km[ex_p$early]),
        sum(!ex_p$early), sum(ex_p$length_km[!ex_p$early])))

    # ---- 2. Node graph and chains (mirrors the Larkin lines script) ---------
    ends <- lapply(seq_len(n_seg), function(i) {
        cc <- sf::st_coordinates(ex_p[i, ])
        rbind(cc[1, c("X", "Y")], cc[nrow(cc), c("X", "Y")])
    })
    end_pts <- do.call(rbind, ends)
    end_sf  <- sf::st_as_sf(data.frame(seg = rep(seq_len(n_seg), each = 2),
                                       x = end_pts[, 1], y = end_pts[, 2]),
                            coords = c("x", "y"), crs = crs_raster)
    n_end <- nrow(end_sf)
    prox <- sf::st_is_within_distance(end_sf, end_sf,
                                      dist = recentering_snap_tol_m,
                                      sparse = TRUE)
    pe <- do.call(rbind, lapply(seq_len(n_end), function(k) {
        js <- prox[[k]]; js <- js[js > k]
        if (length(js) == 0L) return(NULL)
        cbind(k, js)
    }))
    ge <- igraph::make_empty_graph(n = n_end, directed = FALSE)
    if (!is.null(pe)) ge <- igraph::add_edges(ge, t(pe))
    node_of_end <- igraph::components(ge)$membership
    end_sf$node <- node_of_end

    # Same-chain rule: a node joins two segments iff EXACTLY TWO
    # DISTINCT segments meet there AND they share a vintage. (Precise
    # form of the "degree 2" rule, as in the Larkin lines script: a
    # self-touching ring contributes one distinct segment and never
    # joins; cr-review PR #117 consider 4.)
    node_segs <- split(end_sf$seg, end_sf$node)
    su <- do.call(rbind, lapply(node_segs, function(ss) {
        ss <- unique(ss)
        if (length(ss) != 2L) return(NULL)
        i <- ss[1]; j <- ss[2]
        if (ex_p$early[i] != ex_p$early[j]) return(NULL)
        cbind(i, j)
    }))
    gs <- igraph::make_empty_graph(n = n_seg, directed = FALSE)
    if (!is.null(su)) gs <- igraph::add_edges(gs, t(su))
    comp <- igraph::components(gs)
    ex_p$chain_id <- comp$membership
    n_chains <- comp$no
    chk <- tapply(ex_p$early, ex_p$chain_id,
                  function(x) length(unique(x)))
    stopifnot(all(chk == 1L))
    message(sprintf("[rs] %d corridor chains (junction/vintage-split)",
                    n_chains))

    # ---- 3. Region via province of chain's longest segment ------------------
    dist_shp <- sf::st_make_valid(sf::st_read(
        file.path(dir_raw_geo, "geo2_ar1970_2010.shp"), quiet = TRUE))
    dist_p <- sf::st_transform(dist_shp, crs = crs_raster)
    names(dist_p)[names(dist_p) == "GEOLEVEL2"] <- "geolev2"
    dist_p$geolev2 <- sub("^0+", "", as.character(dist_p$geolev2))
    midpts <- suppressWarnings(sf::st_point_on_surface(ex_p))
    hit  <- sf::st_nearest_feature(midpts, dist_p)
    prov <- as.character(dist_p$PARENT[hit])
    reg  <- region_of_province[prov]
    stopifnot(!any(is.na(reg)))
    ex_p$region_seg <- reg

    chains_df <- data.frame(
        chain_id  = seq_len(n_chains),
        early     = as.logical(tapply(ex_p$early, ex_p$chain_id, unique)),
        n_segments = as.integer(table(ex_p$chain_id)),
        length_km = as.numeric(tapply(ex_p$length_km, ex_p$chain_id, sum)),
        region    = vapply(seq_len(n_chains), function(l) {
            sel <- ex_p$chain_id == l
            ex_p$region_seg[sel][which.max(ex_p$length_km[sel])]
        }, character(1)),
        stringsAsFactors = FALSE
    )

    # ---- 4. Strata: region x length tercile, converging thin-cell merge -----
    qs <- quantile(chains_df$length_km, probs = c(1, 2) / 3)
    chains_df$len_ter <- cut(chains_df$length_km,
                             breaks = c(-Inf, qs, Inf),
                             labels = c("t1", "t2", "t3"))
    chains_df$stratum <- paste(chains_df$region, chains_df$len_ter,
                               sep = ":")

    merges <- character(0)
    cell_ok <- function(df, s) {
        sel <- df$stratum == s
        sum(df$early[sel]) >= recentering_min_cell &&
        sum(!df$early[sel]) >= recentering_min_cell
    }
    # Pass 1: thin region:tercile cell -> drop the tercile split for
    # that region. Pass 2: still-thin region -> POOLED:<tercile>.
    # Pass 3 (converging, as in the settlement prep): a thin POOLED
    # cell absorbs the smallest good cell until all cells pass; a
    # single remaining cell is the fixed point.
    for (r in unique(chains_df$region)) {
        for (t in c("t1", "t2", "t3")) {
            s <- paste(r, t, sep = ":")
            if (s %in% chains_df$stratum && !cell_ok(chains_df, s)) {
                sel <- chains_df$region == r
                chains_df$stratum[sel] <- paste(r, "all", sep = ":")
                merges <- c(merges, sprintf(
                    "region %s: tercile split dropped (thin cell %s)",
                    r, s))
                break
            }
        }
    }
    for (s in unique(chains_df$stratum)) {
        if (!cell_ok(chains_df, s)) {
            sel <- chains_df$stratum == s
            chains_df$stratum[sel] <- paste("POOLED",
                                            chains_df$len_ter[sel],
                                            sep = ":")
            merges <- c(merges, sprintf(
                "stratum %s: merged into POOLED (still thin)", s))
        }
    }
    repeat {
        bad <- Filter(function(s) !cell_ok(chains_df, s),
                      unique(chains_df$stratum))
        if (length(bad) == 0L) break
        if (length(unique(chains_df$stratum)) == 1L) {
            stop("[rs] single stratum still thin -- pool too small")
        }
        s <- bad[1]
        good <- setdiff(unique(chains_df$stratum), bad)
        tgt <- if (length(good) > 0L) {
            good[which.min(vapply(good, function(g)
                sum(chains_df$stratum == g), integer(1)))]
        } else setdiff(unique(chains_df$stratum), s)[1]
        chains_df$stratum[chains_df$stratum %in% c(s, tgt)] <-
            paste("POOLED", "all", sep = ":")
        merges <- c(merges, sprintf(
            "stratum %s: absorbed with %s into POOLED:all", s, tgt))
    }
    for (s in unique(chains_df$stratum)) stopifnot(cell_ok(chains_df, s))

    # ---- 5. Chain-district length weights (for the balance table) -----------
    inter <- suppressWarnings(sf::st_intersection(
        ex_p[, c("chain_id", "seg_row")], dist_p[, "geolev2"]))
    inter$km <- as.numeric(sf::st_length(inter)) / 1000
    cd <- aggregate(km ~ chain_id + geolev2,
                    data = sf::st_drop_geometry(inter), FUN = sum)
    names(cd)[names(cd) == "km"] <- "km_in_district"
    cd <- ensure_geolev2_char(cd)
    # Coverage check: intersection must account for ~all chain length
    # (boundary slivers tolerated).
    cover <- sum(cd$km_in_district) / sum(chains_df$length_km)
    message(sprintf("[rs] chain-district coverage: %.1f%% of chain km",
                    100 * cover))
    stopifnot(cover > 0.98, cover < 1.02)

    # ---- 6. Save + report ----------------------------------------------------
    chains_geom <- aggregate(ex_p[, "length_km"],
                             by = list(chain_id = ex_p$chain_id),
                             FUN = sum)
    chains_out <- merge(chains_geom, chains_df[, c(
        "chain_id", "early", "n_segments", "region", "len_ter",
        "stratum")], by = "chain_id")
    stopifnot(nrow(chains_out) == n_chains,
              !any(duplicated(chains_out$chain_id)),
              abs(sum(chains_out$length_km) -
                  sum(chains_df$length_km)) < 1)
    chains_out <- chains_out[order(chains_out$chain_id), ]
    sf::st_write(chains_out, file.path(dir_rs, "chains.gpkg"),
                 delete_dsn = TRUE, quiet = TRUE)
    arrow::write_parquet(sf::st_drop_geometry(chains_out),
                         file.path(dir_rs, "chains.parquet"))
    cd <- cd[order(cd$chain_id, cd$geolev2), ]
    arrow::write_parquet(cd, file.path(dir_rs, "chain_districts.parquet"))

    rpt <- file.path(dir_rs, "prep_report.txt")
    sink(rpt)
    cat("diagnostic_roadseg_prep.R report\n")
    cat(sprintf("Generated: %s\n\n", Sys.time()))
    cat(sprintf("Segments: %d  |  chains: %d  |  snap tol: %d m\n",
                n_seg, n_chains, recentering_snap_tol_m))
    cat(sprintf("Early chains: %d (%.0f km)  |  late: %d (%.0f km)\n\n",
                sum(chains_df$early),
                sum(chains_df$length_km[chains_df$early]),
                sum(!chains_df$early),
                sum(chains_df$length_km[!chains_df$early])))
    cat("Cells (stratum: early / late chains, early km share):\n")
    for (s in sort(unique(chains_df$stratum))) {
        sel <- chains_df$stratum == s
        cat(sprintf("  %-18s  %3d / %3d   km share early = %.3f\n", s,
                    sum(chains_df$early[sel]), sum(!chains_df$early[sel]),
                    sum(chains_df$length_km[sel & chains_df$early]) /
                        sum(chains_df$length_km[sel])))
    }
    cat("\nMerges applied:\n")
    if (length(merges) == 0L) cat("  (none)\n") else
        for (m in merges) cat("  -", m, "\n")
    sink()

    sink(file.path(dir_rs, "prep_manifest.log"))
    cat("Data file manifest -- diagnostic_roadseg_prep.R\n")
    cat(sprintf("Generated: %s\n\n", Sys.time()))
    cat(sprintf("chains.gpkg / chains.parquet: %d rows, key chain_id\n",
                n_chains))
    cat(sprintf("chain_districts.parquet: %d rows, key chain_id x geolev2\n",
                nrow(cd)))
    sink()

    message(readLines(rpt) |> paste(collapse = "\n"))
}

main()

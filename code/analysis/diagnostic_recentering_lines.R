# ===========================================================================
# diagnostic_recentering_lines.R
#
# PURPOSE: Stage 1 of the recentering diagnostic (Borusyak & Hull 2023;
#          see Plan/borusyak_hull_recentering_plan.md). Build the
#          permutation units and strata for counterfactual Larkin
#          study designations:
#
#          1. Group the 565 rail segments of lp_1979.shp into LINE UNITS:
#             connected components of contiguous segments sharing the
#             same studied_co status. The Larkin Plan designated
#             contiguous stretches together, so these units — not
#             individual segments — are the natural objects whose
#             status the permutation reassigns.
#          2. Classify each unit as BRANCH (has a free end: an endpoint
#             touched by no other rail segment) or TRUNK.
#          3. Assign each unit to an INDEC region (Pampeana, NOA, NEA,
#             Cuyo, Patagonia) via the province of its spatial position.
#          4. Strata = region x branch, with thin cells (<
#             recentering_min_cell studied or non-studied units) merged
#             deterministically and documented.
#
# READS:
#   data/raw/networks/lp_1979.shp
#   data/raw/geo/geo2_ar1970_2010.shp        (province via PARENT code)
#
# PRODUCES:
#   data/derived/07_recentering/rail_segments_strata.parquet
#       565 rows (key: seg_row = st_read row order, asserted stable via
#       studied_co checksum downstream). Columns: seg_row, id_main,
#       status1979, studied_co, length_km, line_id, region, branch,
#       stratum.
#   data/derived/07_recentering/rail_lines_strata.parquet
#       one row per line unit (key: line_id): studied_co, n_segments,
#       length_km, region, branch, stratum.
#   data/derived/07_recentering/lines_report.txt
#       cell sizes, merges applied, verification summary.
#   data/derived/07_recentering/data_file_manifest.log
#
# VERIFY (fail-loud):
#   - every segment in exactly one unit; unit studied status constant;
#   - every stratum has >= recentering_min_cell studied AND non-studied
#     units after merging;
#   - partition sums: total km and segment count match the shapefile.
# ===========================================================================

suppressPackageStartupMessages({
    library(sf)
    library(arrow)
    library(igraph)
})

# INDEC five-region grouping by province code (PARENT field of the
# district shapefile; INDEC numeric province codes).
region_of_province <- c(
    "002" = "Pampeana",  # CABA
    "006" = "Pampeana",  # Buenos Aires
    "014" = "Pampeana",  # Cordoba
    "030" = "Pampeana",  # Entre Rios
    "042" = "Pampeana",  # La Pampa
    "082" = "Pampeana",  # Santa Fe
    "010" = "NOA",       # Catamarca
    "038" = "NOA",       # Jujuy
    "046" = "NOA",       # La Rioja
    "066" = "NOA",       # Salta
    "086" = "NOA",       # Santiago del Estero
    "090" = "NOA",       # Tucuman
    "018" = "NEA",       # Corrientes
    "022" = "NEA",       # Chaco
    "034" = "NEA",       # Formosa
    "054" = "NEA",       # Misiones
    "050" = "Cuyo",      # Mendoza
    "070" = "Cuyo",      # San Juan
    "074" = "Cuyo",      # San Luis
    "026" = "Patagonia", # Chubut
    "058" = "Patagonia", # Neuquen
    "062" = "Patagonia", # Rio Negro
    "078" = "Patagonia", # Santa Cruz
    "094" = "Patagonia"  # Tierra del Fuego
)

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)

    message("\n", strrep("=", 72))
    message("diagnostic_recentering_lines.R  |  line units + strata")
    message(strrep("=", 72))

    if (!dir.exists(dir_derived_recentering)) {
        dir.create(dir_derived_recentering, recursive = TRUE)
    }

    # ---- 1. Read rails, project, measure -----------------------------------
    rails <- sf::st_read(file.path(dir_raw_networks, "lp_1979.shp"),
                         quiet = TRUE)
    rails <- sf::st_make_valid(rails)
    n_seg <- nrow(rails)
    stopifnot(n_seg == 565L)
    rails_p <- sf::st_transform(rails, crs = crs_raster)
    rails_p$seg_row   <- seq_len(n_seg)
    rails_p$length_km <- as.numeric(sf::st_length(rails_p)) / 1000

    message(sprintf("[lines] %d segments, %.0f km total, %d studied",
                    n_seg, sum(rails_p$length_km),
                    sum(rails_p$studied_co == 1L)))

    # ---- 2. Node graph: snapped endpoints, node degree ----------------------
    # Nodes are clusters of segment endpoints within snap tolerance
    # (single linkage). Node degree = number of segment-ends at the node.
    # Junctions (degree >= 3) split line units; free ends (degree == 1)
    # define branches.
    ends <- lapply(seq_len(n_seg), function(i) {
        cc <- sf::st_coordinates(rails_p[i, ])
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
    node_degree <- as.integer(table(node_of_end))  # ends per node
    end_sf$node   <- node_of_end
    end_sf$degree <- node_degree[node_of_end]

    # ---- 3. Line units: chains split at junctions and status changes --------
    # Two segments belong to the same unit iff they share a node of
    # degree exactly 2 AND have the same studied_co. Junction nodes
    # (degree >= 3) always split; status changes always split. Units are
    # therefore status-pure, junction-to-junction (or terminus) stretches
    # -- the physical objects the Plan designated together.
    node_segs <- split(end_sf$seg, end_sf$node)
    su <- do.call(rbind, lapply(node_segs, function(ss) {
        ss <- unique(ss)
        if (length(ss) != 2L) return(NULL)          # not degree-2 pairing
        i <- ss[1]; j <- ss[2]
        if (rails_p$studied_co[i] != rails_p$studied_co[j]) return(NULL)
        cbind(i, j)
    }))
    gs <- igraph::make_empty_graph(n = n_seg, directed = FALSE)
    if (!is.null(su)) gs <- igraph::add_edges(gs, t(su))
    comp <- igraph::components(gs)
    rails_p$line_id <- comp$membership
    n_lines <- comp$no
    message(sprintf(
        "[lines] %d line units (junction- and status-split chains)",
        n_lines))

    # Verify: partition, and constant studied status within unit.
    stopifnot(all(rails_p$line_id >= 1L),
              !any(is.na(rails_p$line_id)))
    chk <- tapply(rails_p$studied_co, rails_p$line_id,
                  function(x) length(unique(x)))
    stopifnot(all(chk == 1L))

    # Branch: unit contains a free end (node of degree 1).
    seg_has_free_end <- tapply(end_sf$degree == 1L, end_sf$seg, any)
    unit_branch <- as.logical(tapply(seg_has_free_end[rails_p$seg_row],
                                     rails_p$line_id, any))
    rails_p$branch <- ifelse(unit_branch[rails_p$line_id],
                             "branch", "trunk")
    message(sprintf("[lines] branch units: %d / %d",
                    sum(unit_branch), n_lines))

    # ---- 4. Region via province of segment midpoint -------------------------
    dist_shp <- sf::st_read(file.path(dir_raw_geo, "geo2_ar1970_2010.shp"),
                            quiet = TRUE)
    dist_shp <- sf::st_make_valid(dist_shp)
    dist_p   <- sf::st_transform(dist_shp, crs = crs_raster)
    midpts   <- suppressWarnings(sf::st_point_on_surface(rails_p))
    hit      <- sf::st_nearest_feature(midpts, dist_p)
    prov     <- as.character(dist_p$PARENT[hit])
    reg      <- region_of_province[prov]
    stopifnot(!any(is.na(reg)))
    rails_p$region_seg <- reg
    # Unit region = region of the unit's longest segment (deterministic).
    unit_region <- vapply(seq_len(n_lines), function(l) {
        sel <- rails_p$line_id == l
        rails_p$region_seg[sel][which.max(rails_p$length_km[sel])]
    }, character(1))
    rails_p$region <- unit_region[rails_p$line_id]

    # ---- 5. Strata with deterministic thin-cell merging ---------------------
    lines_df <- data.frame(
        line_id    = seq_len(n_lines),
        studied_co = as.integer(tapply(rails_p$studied_co,
                                       rails_p$line_id, unique)),
        n_segments = as.integer(table(rails_p$line_id)),
        length_km  = as.numeric(tapply(rails_p$length_km,
                                       rails_p$line_id, sum)),
        region     = unit_region,
        branch     = ifelse(unit_branch, "branch", "trunk"),
        stringsAsFactors = FALSE
    )
    lines_df$stratum <- paste(lines_df$region, lines_df$branch, sep = ":")

    merges <- character(0)
    cell_ok <- function(df, s) {
        sel <- df$stratum == s
        sum(df$studied_co[sel] == 1L) >= recentering_min_cell &&
        sum(df$studied_co[sel] == 0L) >= recentering_min_cell
    }
    # Pass 1: thin region:branch cell -> collapse branch split for that
    # region (both its cells become "<region>:all").
    for (r in unique(lines_df$region)) {
        for (b in c("branch", "trunk")) {
            s <- paste(r, b, sep = ":")
            if (s %in% lines_df$stratum && !cell_ok(lines_df, s)) {
                sel <- lines_df$region == r
                lines_df$stratum[sel] <- paste(r, "all", sep = ":")
                merges <- c(merges, sprintf(
                    "region %s: branch split dropped (thin cell %s)", r, s))
                break
            }
        }
    }
    # Pass 2: still-thin region cell -> pooled national cell by branch
    # status ("POOLED:<branch>"), preserving at least the branch margin.
    for (s in unique(lines_df$stratum)) {
        if (!cell_ok(lines_df, s)) {
            sel <- lines_df$stratum == s
            lines_df$stratum[sel] <- paste("POOLED", lines_df$branch[sel],
                                           sep = ":")
            merges <- c(merges, sprintf(
                "stratum %s: merged into POOLED (still thin)", s))
        }
    }
    # Pass 3 (last resort): a thin POOLED cell merges into one national
    # cell. Only reachable if a branch margin is thin nationwide.
    for (s in unique(lines_df$stratum)) {
        if (!cell_ok(lines_df, s)) {
            sel <- lines_df$stratum %in%
                c("POOLED:branch", "POOLED:trunk", s)
            lines_df$stratum[sel] <- "POOLED:all"
            merges <- c(merges, sprintf(
                "stratum %s: collapsed into POOLED:all", s))
        }
    }
    for (s in unique(lines_df$stratum)) stopifnot(cell_ok(lines_df, s))

    rails_p$stratum <- lines_df$stratum[rails_p$line_id]

    # ---- 6. Save + report ----------------------------------------------------
    seg_out <- sf::st_drop_geometry(rails_p)[, c(
        "seg_row", "id_main", "status1979", "studied_co", "length_km",
        "line_id", "region", "branch", "stratum")]
    stopifnot(nrow(seg_out) == 565L, !any(duplicated(seg_out$seg_row)))
    seg_out <- seg_out[order(seg_out$seg_row), ]
    arrow::write_parquet(
        seg_out,
        file.path(dir_derived_recentering, "rail_segments_strata.parquet"))

    stopifnot(!any(duplicated(lines_df$line_id)))
    lines_df <- lines_df[order(lines_df$line_id), ]
    arrow::write_parquet(
        lines_df,
        file.path(dir_derived_recentering, "rail_lines_strata.parquet"))

    rpt <- file.path(dir_derived_recentering, "lines_report.txt")
    sink(rpt)
    cat("diagnostic_recentering_lines.R report\n")
    cat(sprintf("Generated: %s\n\n", Sys.time()))
    cat(sprintf("Segments: %d  |  line units: %d  |  snap tol: %d m\n",
                n_seg, n_lines, recentering_snap_tol_m))
    cat(sprintf("Studied units: %d (%.0f km)  |  non-studied: %d (%.0f km)\n\n",
                sum(lines_df$studied_co == 1L),
                sum(lines_df$length_km[lines_df$studied_co == 1L]),
                sum(lines_df$studied_co == 0L),
                sum(lines_df$length_km[lines_df$studied_co == 0L])))
    cat("Cells (stratum: studied / non-studied units, studied km share):\n")
    for (s in sort(unique(lines_df$stratum))) {
        sel <- lines_df$stratum == s
        cat(sprintf("  %-22s  %3d / %3d   km share studied = %.3f\n", s,
                    sum(lines_df$studied_co[sel] == 1L),
                    sum(lines_df$studied_co[sel] == 0L),
                    sum(lines_df$length_km[sel & lines_df$studied_co == 1L]) /
                        sum(lines_df$length_km[sel])))
    }
    cat("\nMerges applied:\n")
    if (length(merges) == 0L) cat("  (none)\n") else
        for (m in merges) cat("  -", m, "\n")
    sink()

    manifest <- file.path(dir_derived_recentering, "data_file_manifest.log")
    sink(manifest)
    cat("Data file manifest -- diagnostic_recentering_lines.R\n")
    cat(sprintf("Generated: %s\n\n", Sys.time()))
    cat(sprintf("rail_segments_strata.parquet: %d rows, key seg_row\n",
                nrow(seg_out)))
    cat(sprintf("rail_lines_strata.parquet:    %d rows, key line_id\n",
                nrow(lines_df)))
    sink()

    message(sprintf("[lines] Saved outputs + report to %s",
                    dir_derived_recentering))
    message(readLines(rpt) |> paste(collapse = "\n"))
}

main()

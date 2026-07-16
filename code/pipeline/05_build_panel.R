# ===========================================================================
# 05_build_panel.R
#
# PURPOSE: Merge all base-cleaned sources into a single wide district-level
#          estimation panel (one row per geolev2, 312 rows). Adds log-change
#          variables for the main change-in-change specification.
#
# READS:
#   data/derived/base/geo/geo_controls.parquet                (312)
#   data/derived/base/ipums/ipums_panel.parquet               (312 × 5 years)
#   data/derived/base/census_1947/census_1947_ipums.parquet   (243)
#   data/derived/base/census_1960/census_1960_ipums.parquet   (312)
#   data/derived/base/agricultural/agr_census.parquet         (306 + 300)
#   data/derived/base/industrial/ind_census.parquet           (310 + 311)
#   data/derived/base/networks/roads_by_district.parquet      (309)
#   data/derived/base/networks/rails_by_district.parquet      (312)
#   data/derived/base/networks/hypo_networks_by_district.parquet (312)
#
# PRODUCES:
#   data/derived/05_panel/departments_wide_panel.parquet
#   data/derived/05_panel/data_file_manifest.log
#
# DESIGN (see issue #25):
#   - Wide format, 312 rows, key = geolev2 (character).
#   - All 312 districts (incl. CF and TdF) carried through. Let estimation
#     filter.
#   - Change variable year pairs held in a config list so they're easy to
#     flex later (change period, add/remove a year).
#   - Missing data encoded as NA so regressions auto-drop.
#   - Genuine observed zeros (e.g., TdF rail km) are kept as 0.
#   - Market-access columns are OUT of scope here; follow-up PR adds them.
#
# NOTES:
#   - IPUMS urbpop is structurally missing for 1970 and 2010 in the source
#     microdata. Those years are recoded from 0 to NA at the pipe step.
#   - Change vars use log(x+1) for counts that can be zero (road km, rail
#     km) but log(x) for strictly positive outcomes (population).
# ===========================================================================

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    message("\n", strrep("=", 72))
    message("05_build_panel.R  |  merge base sources → wide panel")
    message(strrep("=", 72))

    # ---- 1. Geo controls as the base spine --------------------------------
    panel <- load_geo()

    # ---- 2. IPUMS (multi-year) wide-merged --------------------------------
    ipums_w <- load_ipums_wide()
    panel   <- merge_left(panel, ipums_w, "IPUMS wide")

    # ---- 3. 1947 and 1960 census ------------------------------------------
    panel <- merge_left(panel, load_census1947(), "census_1947")
    panel <- merge_left(panel, load_census1960(), "census_1960")

    # ---- 4. Agricultural and industrial -----------------------------------
    panel <- merge_left(panel, load_agr(), "agricultural census")
    panel <- merge_left(panel, load_ind(), "industrial census")

    # ---- 5. Networks (rails, roads, hypo) ---------------------------------
    panel <- merge_left(panel, load_rails(), "rails")
    panel <- merge_left(panel, load_roads(), "roads")
    panel <- merge_left(panel, load_hypo(),  "hypo networks")

    # ---- 6. Change variables ----------------------------------------------
    panel <- add_change_vars(panel)

    # ---- 7. Save + manifest -----------------------------------------------
    save_panel(panel)

    message(strrep("=", 72))
    message("05_build_panel.R  |  Complete.")
    message(strrep("=", 72), "\n")
}

# ---------------------------------------------------------------------------
# Year-pair config for change variables. Edit to flex.
# ---------------------------------------------------------------------------
change_pairs <- list(
    pop           = list(pre = 1960, post = 1991),
    urbpop        = list(pre = 1960, post = 1991),
    placebo_pop   = list(pre = 1947, post = 1960),
    nestab        = list(pre = 1954, post = 1985),
    massal        = list(pre = 1954, post = 1985),
    valprod       = list(pre = 1954, post = 1985),
    nexp          = list(pre = 1960, post = 1988),
    areatot_ha    = list(pre = 1960, post = 1988)
)
# NOTE: Industrial variables available in both 1954 and 1985: nestab,
# massal, valprod. 1954 has nemp (workers) and nobr (blue-collar); 1985
# has npers (all persons, different definition) — not directly comparable
# across years so no change variable is built for either.
# `rur` is not available in IPUMS 1991 (only in census_1960_ipums); skip.

# ---------------------------------------------------------------------------
# Helper: left merge with diagnostic log
# ---------------------------------------------------------------------------
merge_left <- function(x, y, label) {
    y <- ensure_geolev2_char(y)
    n_x_before <- nrow(x)
    n_y        <- nrow(y)
    n_match    <- sum(x$geolev2 %in% y$geolev2)
    n_unmatch  <- n_x_before - n_match

    out <- merge(x, y, by = "geolev2", all.x = TRUE)

    message(sprintf(
        "[panel] merge %-25s  x=%d  y=%d  matched=%d  unmatched_in_panel=%d",
        label, n_x_before, n_y, n_match, n_unmatch
    ))
    stopifnot(nrow(out) == n_x_before)      # No duplication, no drops
    stopifnot(!any(duplicated(out$geolev2)))
    out
}

# ---------------------------------------------------------------------------
# Load: geo controls (the spine — all 312 districts)
# ---------------------------------------------------------------------------
load_geo <- function() {
    d <- arrow::read_parquet(
        file.path(dir_derived_geo, "geo_controls.parquet")
    )
    d <- ensure_geolev2_char(d)
    message(sprintf("[panel] geo spine: %d districts", nrow(d)))
    d
}

# ---------------------------------------------------------------------------
# Load: IPUMS panel, recode 0-urbpop for missing years, pivot to wide
# ---------------------------------------------------------------------------
load_ipums_wide <- function() {
    d <- arrow::read_parquet(
        file.path(dir_derived_ipums, "ipums_panel.parquet")
    )
    # Cast year from haven_labelled if needed
    if (inherits(d$year, "haven_labelled")) {
        d$year <- haven::zap_labels(d$year)
    }
    d$year <- as.integer(d$year)
    d <- ensure_geolev2_char(d)

    # IPUMS 1970 and 2010 have no urban/rural variable; recode to NA
    d$urbpop[d$year %in% c(1970, 2010)] <- NA

    # Keep only the columns we export, for clarity
    keep_vars <- c(
        "pop", "urbpop",
        "college", "secondary", "mig5",
        "empstat_1", "empstat_2", "empstat_3"
    )
    keep_vars <- intersect(keep_vars, names(d))
    d <- d[, c("geolev2", "year", keep_vars)]

    # Long → wide via reshape
    w <- stats::reshape(
        as.data.frame(d),
        idvar     = "geolev2",
        timevar   = "year",
        direction = "wide",
        sep       = "_"
    )
    # reshape names things "pop.1970"; normalize to "pop_1970"
    names(w) <- gsub("\\.(\\d+)$", "_\\1", names(w))
    message(sprintf("[panel] IPUMS wide: %d rows, %d cols (years: %s)",
                    nrow(w), ncol(w),
                    paste(sort(unique(d$year)), collapse = ", ")))
    w
}

# ---------------------------------------------------------------------------
# Load: 1947 census (243 districts, 1947 only)
# ---------------------------------------------------------------------------
load_census1947 <- function() {
    d <- arrow::read_parquet(
        file.path(dir_derived_census1947, "census_1947_ipums.parquet")
    )
    d <- ensure_geolev2_char(d)
    # Keep only vars we want at year 1947; add _1947 suffix
    keep <- c("pop", "urbpop", "pop_imputed", "urbpop_imputed", "note")
    keep <- intersect(keep, names(d))
    d <- d[, c("geolev2", keep)]
    names(d)[names(d) != "geolev2"] <- paste0(
        names(d)[names(d) != "geolev2"], "_1947"
    )
    d
}

# ---------------------------------------------------------------------------
# Load: 1960 census (312 districts, 1960 only)
# ---------------------------------------------------------------------------
load_census1960 <- function() {
    d <- arrow::read_parquet(
        file.path(dir_derived_census1960, "census_1960_ipums.parquet")
    )
    d <- ensure_geolev2_char(d)
    keep <- intersect(c("pop", "urbpop", "rur"), names(d))
    d <- d[, c("geolev2", keep)]
    names(d)[names(d) != "geolev2"] <- paste0(
        names(d)[names(d) != "geolev2"], "_1960"
    )
    d
}

# ---------------------------------------------------------------------------
# Load: agricultural census, pivot to wide
# ---------------------------------------------------------------------------
load_agr <- function() {
    d <- arrow::read_parquet(
        file.path(dir_derived_agr, "agr_census.parquet")
    )
    d <- ensure_geolev2_char(d)
    if (inherits(d$year, "haven_labelled")) {
        d$year <- haven::zap_labels(d$year)
    }
    d$year <- as.integer(d$year)
    w <- stats::reshape(
        as.data.frame(d),
        idvar = "geolev2", timevar = "year", direction = "wide", sep = "_"
    )
    names(w) <- gsub("\\.(\\d+)$", "_\\1", names(w))
    w
}

# ---------------------------------------------------------------------------
# Load: industrial census, pivot to wide
# ---------------------------------------------------------------------------
load_ind <- function() {
    d <- arrow::read_parquet(
        file.path(dir_derived_ind, "ind_census.parquet")
    )
    d <- ensure_geolev2_char(d)
    if (inherits(d$year, "haven_labelled")) {
        d$year <- haven::zap_labels(d$year)
    }
    d$year <- as.integer(d$year)
    w <- stats::reshape(
        as.data.frame(d),
        idvar = "geolev2", timevar = "year", direction = "wide", sep = "_"
    )
    names(w) <- gsub("\\.(\\d+)$", "_\\1", names(w))
    w
}

# ---------------------------------------------------------------------------
# Load: rails (already wide, all 312 districts, zeros are real)
# ---------------------------------------------------------------------------
load_rails <- function() {
    arrow::read_parquet(
        file.path(dir_derived_networks, "rails_by_district.parquet")
    )
}

# ---------------------------------------------------------------------------
# Load: roads (already wide, 309 districts; 3 are missing)
# ---------------------------------------------------------------------------
load_roads <- function() {
    arrow::read_parquet(
        file.path(dir_derived_networks, "roads_by_district.parquet")
    )
}

# ---------------------------------------------------------------------------
# Load: hypothetical networks (312, zeros are real)
# ---------------------------------------------------------------------------
load_hypo <- function() {
    arrow::read_parquet(
        file.path(dir_derived_networks, "hypo_networks_by_district.parquet")
    )
}

# ---------------------------------------------------------------------------
# Change variables: log changes from change_pairs config
#
# For a variable `x` with pre-year yy and post-year zz, creates
#   chg_log_<x>_<zz>_<yy> = log(x_<zz>) - log(x_<yy>)
# with NA where either endpoint is NA or non-positive.
# ---------------------------------------------------------------------------
add_change_vars <- function(panel) {
    message("\n[panel] Adding log-change variables")

    for (vnm in names(change_pairs)) {
        cfg  <- change_pairs[[vnm]]
        pre  <- cfg$pre
        post <- cfg$post
        # The actual variable name in the data is often different from
        # the change-pair key (e.g., placebo_pop uses pop_1947/pop_1960)
        base <- if (vnm == "placebo_pop") "pop" else vnm
        v_pre  <- sprintf("%s_%d", base, pre)
        v_post <- sprintf("%s_%d", base, post)
        if (!all(c(v_pre, v_post) %in% names(panel))) {
            message(sprintf("  skip %-15s (missing %s or %s)",
                            vnm, v_pre, v_post))
            next
        }
        out_name <- sprintf("chg_log_%s_%02d_%02d",
                            vnm, post %% 100, pre %% 100)
        x_pre  <- as.numeric(panel[[v_pre]])
        x_post <- as.numeric(panel[[v_post]])
        # log-change only defined where both > 0
        valid <- !is.na(x_pre) & !is.na(x_post) & x_pre > 0 & x_post > 0
        out <- rep(NA_real_, length(x_pre))
        out[valid] <- log(x_post[valid]) - log(x_pre[valid])
        panel[[out_name]] <- out
        message(sprintf("  %-25s N=%d  mean=%.3f  sd=%.3f",
                        out_name, sum(valid),
                        mean(out, na.rm = TRUE),
                        stats::sd(out, na.rm = TRUE)))
    }

    panel
}

# ---------------------------------------------------------------------------
# Save panel and write manifest
# ---------------------------------------------------------------------------
save_panel <- function(panel) {
    message("\n[panel] Saving")

    out_dir <- dir_derived_panel
    if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

    panel$geolev2 <- as.character(panel$geolev2)
    panel <- panel[order(panel$geolev2), ]
    stopifnot(!any(duplicated(panel$geolev2)))
    stopifnot(!any(is.na(panel$geolev2)))
    message(sprintf("[panel]   Key unique, non-missing. Districts: %d",
                    nrow(panel)))

    # Strip haven attributes before writing (arrow complains on labelled)
    for (nm in names(panel)) {
        if (inherits(panel[[nm]], "haven_labelled")) {
            panel[[nm]] <- haven::zap_labels(panel[[nm]])
        }
    }

    out_path <- file.path(out_dir, "departments_wide_panel.parquet")
    arrow::write_parquet(panel, out_path)
    message(sprintf("[panel]   Saved: %s (%d rows, %d cols)",
                    out_path, nrow(panel), ncol(panel)))

    # Manifest
    log_path <- file.path(out_dir, "data_file_manifest.log")
    sink(log_path)
    on.exit(sink(), add = TRUE)
    cat("Data file manifest — 05_build_panel.R\n")
    cat(sprintf("Generated: %s\n", Sys.time()))
    cat(sprintf("rootdir: %s\n\n", rootdir))
    cat(strrep("=", 60), "\n")
    cat("FILE: departments_wide_panel.parquet\n")
    cat(strrep("=", 60), "\n")
    cat(sprintf("Rows: %d\nColumns: %d\nKey: geolev2\n\n",
                nrow(panel), ncol(panel)))

    cat("Summary of every variable:\n")
    for (v in names(panel)) {
        if (v == "geolev2") next
        vals <- panel[[v]]
        if (!is.numeric(vals)) {
            cat(sprintf("  %-30s (class=%s, n_unique=%d, NA=%d)\n",
                        v, class(vals)[1],
                        length(unique(vals)), sum(is.na(vals))))
            next
        }
        n_na <- sum(is.na(vals))
        n_ok <- sum(!is.na(vals))
        if (n_ok == 0) {
            cat(sprintf("  %-30s  ALL NA\n", v))
            next
        }
        cat(sprintf(
            "  %-30s N=%d  mean=%.3g  sd=%.3g  min=%.3g  max=%.3g  NA=%d\n",
            v, n_ok,
            mean(vals, na.rm = TRUE), stats::sd(vals, na.rm = TRUE),
            min(vals, na.rm = TRUE), max(vals, na.rm = TRUE),
            n_na
        ))
    }

    message(sprintf("[panel]   Manifest: %s", log_path))
}

main()

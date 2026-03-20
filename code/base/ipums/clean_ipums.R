# ===========================================================================
# clean_ipums.R
#
# PURPOSE: Clean IPUMS International census microdata for the estimation
#          panel. Aggregates person-level records to district-year level
#          using household weights. Also produces a district name crosswalk
#          used by all other data source cleaning scripts.
#
# READS:
#   data/raw/census/ipumsi_00007.dta  — IPUMS extract (15M person records)
#   data/raw/census/prov_labels.xlsx  — province name labels
#
# PRODUCES:
#   data/derived/base/ipums/ipums_panel.parquet
#       District-year panel. Key: geolev2 + year.
#       Variables: population, urbanization, education, migration,
#       employment by sector/occupation/class.
#
#   data/derived/base/ipums/ipums_districts_for_merge.parquet
#       Crosswalk mapping province+district name strings to geolev2 codes.
#       Key: provmerge + distmerge.
#       Used by: agricultural census, industrial census, other merges.
#
#   data/derived/base/ipums/data_file_manifest.log
#
# REFERENCE: Old data/Train/base/ipums/code/clean_ipums.do
#
# NOTES:
#   - The raw .dta file is ~2GB. haven::read_dta() loads it into memory.
#     Ensure at least 8GB RAM available.
#   - geo2_ar labels contain accented characters that need cleaning for
#     consistent name matching across data sources.
#   - One known typo in IPUMS: geolev2=32006032 (Marcos Paz) is mislabeled
#     as "Maipu" in the geo2_ar variable. Fixed manually.
#   - Malvinas (238094004) and South Georgia (239094003) are excluded.
# ===========================================================================

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)

    message("\n", strrep("=", 72))
    message("clean_ipums.R  |  IPUMS census microdata → district-year panel")
    message(strrep("=", 72))

    # --- 1. Read raw data ---------------------------------------------------
    df <- read_ipums_raw()

    # --- 2. Generate district-level aggregates ------------------------------
    panel <- aggregate_to_districts(df)

    # --- 3. Merge province names --------------------------------------------
    panel <- merge_province_names(panel)

    # --- 4. Exclude non-mainland territories and unassigned codes ----------
    panel <- exclude_territories(panel)

    # --- 5. Validate panel --------------------------------------------------
    validate_panel(panel)

    # --- 6. Save panel ------------------------------------------------------
    panel <- save_panel(panel)

    # --- 7. Build and save crosswalk ----------------------------------------
    crosswalk <- build_crosswalk(df)
    save_crosswalk(crosswalk)

    # --- 8. Write manifest --------------------------------------------------
    write_manifest(panel, crosswalk)

    message(strrep("=", 72))
    message("clean_ipums.R  |  Complete.")
    message(strrep("=", 72), "\n")
}

# ---------------------------------------------------------------------------
# Helper: read raw IPUMS extract
# ---------------------------------------------------------------------------
read_ipums_raw <- function() {
    message("\n[ipums] Step 1 — Reading raw IPUMS extract")

    raw_path <- file.path(dir_raw_census, "ipumsi_00007.dta")
    stopifnot(file.exists(raw_path))

    df <- haven::read_dta(raw_path)
    df <- as.data.frame(df)  # strip haven labelled class for cleaner ops
    message(sprintf("[ipums]   Raw data: %d rows, %d columns", nrow(df), ncol(df)))
    message(sprintf("[ipums]   Years: %s", paste(sort(unique(df$year)), collapse = ", ")))
    message(sprintf("[ipums]   Unique geolev2: %d", length(unique(df$geolev2))))

    df
}

# ---------------------------------------------------------------------------
# Helper: aggregate person-level records to district-year
# ---------------------------------------------------------------------------
aggregate_to_districts <- function(df) {
    message("\n[ipums] Step 2 — Aggregating to district-year level")

    # -- Population: sum of household weights
    # Kept for: main outcome (Table 9), MA denominator
    df$pop_contrib <- df$hhwt

    # -- Urban population: sum of hhwt where urban == 2
    # Kept for: urbanization outcome (Table 9)
    df$urbpop_contrib <- ifelse(df$urban == 2, df$hhwt, 0)
    df$urbpop_contrib[is.na(df$urban)] <- NA

    # -- Education: share with completed tertiary (edattain == 4)
    # Kept for: education outcome (Table 11)
    df$has_college <- ifelse(df$edattain == 4, 1, 0)
    df$has_college[df$edattain %in% c(0, 9)] <- NA  # NIU, unknown

    # -- Education: share with secondary or higher (edattain >= 3)
    df$has_secondary <- ifelse(df$edattain >= 3 & df$edattain <= 4, 1, 0)
    df$has_secondary[df$edattain %in% c(0, 9)] <- NA

    # -- Migration: share of 5-year inter-province migrants
    # Kept for: migration outcome (Table 11)
    # migrate5: 10-12 = same province (stayers), 20 = different province,
    #           30 = abroad. We want the share who MOVED (codes 20-30).
    # NOTE: The old Stata pipeline (clean_ipums.do) coded 10-12 as the
    #   numerator, producing the share of stayers. That was inverted
    #   relative to the variable name "mig5" (migration share). Fixed here
    #   so mig5 = share of people who moved between provinces or from abroad.
    df$has_mig_info <- ifelse(df$migrate5 >= 10 & df$migrate5 <= 30, 1, 0)
    df$is_migrant <- ifelse(df$migrate5 >= 20 & df$migrate5 <= 30, 1, 0)
    df$is_migrant[df$has_mig_info == 0] <- NA
    df$has_mig_info[df$has_mig_info == 0] <- NA

    # -- Industry (indgen): create dummies for each sector
    # Kept for: sectoral employment (Table 10)
    df$indgen_clean <- df$indgen
    df$indgen_clean[df$indgen_clean %in% c(0, 998, 999)] <- NA
    indgen_vals <- sort(unique(df$indgen_clean[!is.na(df$indgen_clean)]))

    for (v in indgen_vals) {
        vname <- sprintf("indgen_%d", v)
        df[[vname]] <- ifelse(df$indgen_clean == v, 1, 0)
        df[[vname]][is.na(df$indgen_clean)] <- NA
    }

    # -- Occupation (occisco): create dummies
    # Kept for: robustness / heterogeneity
    df$occisco_clean <- df$occisco
    df$occisco_clean[df$occisco_clean %in% c(97, 98, 99)] <- NA
    occisco_vals <- sort(unique(df$occisco_clean[!is.na(df$occisco_clean)]))

    for (v in occisco_vals) {
        vname <- sprintf("occisco_%d", v)
        df[[vname]] <- ifelse(df$occisco_clean == v, 1, 0)
        df[[vname]][is.na(df$occisco_clean)] <- NA
    }

    # -- Class of worker (classwk): create dummies
    # Kept for: robustness
    df$classwk_clean <- df$classwk
    df$classwk_clean[df$classwk_clean %in% c(0, 9)] <- NA
    classwk_vals <- sort(unique(df$classwk_clean[!is.na(df$classwk_clean)]))

    for (v in classwk_vals) {
        vname <- sprintf("classwk_%d", v)
        df[[vname]] <- ifelse(df$classwk_clean == v, 1, 0)
        df[[vname]][is.na(df$classwk_clean)] <- NA
    }

    # -- Employment status (empstat): create dummies
    # Kept for: employment outcomes (Table 11)
    df$empstat_clean <- df$empstat
    df$empstat_clean[df$empstat_clean %in% c(0, 9)] <- NA
    empstat_vals <- sort(unique(df$empstat_clean[!is.na(df$empstat_clean)]))

    for (v in empstat_vals) {
        vname <- sprintf("empstat_%d", v)
        df[[vname]] <- ifelse(df$empstat_clean == v, 1, 0)
        df[[vname]][is.na(df$empstat_clean)] <- NA
    }

    # -- Aggregate: weighted means and counts by district-year
    # For shares: weighted mean of dummy (= share of non-missing pop)
    # For counts: weighted sum of dummy * hhwt (= count in that category)
    group_vars <- c("year", "country", "geolev1", "geolev2", "geo2_ar")

    # Identify all dummy columns created above
    indgen_cols  <- grep("^indgen_\\d+$",  names(df), value = TRUE)
    occisco_cols <- grep("^occisco_\\d+$", names(df), value = TRUE)
    classwk_cols <- grep("^classwk_\\d+$", names(df), value = TRUE)
    empstat_cols <- grep("^empstat_\\d+$", names(df), value = TRUE)
    all_dummy_cols <- c(indgen_cols, occisco_cols, classwk_cols, empstat_cols)

    message(sprintf("[ipums]   Dummy columns: %d indgen, %d occisco, %d classwk, %d empstat",
                    length(indgen_cols), length(occisco_cols),
                    length(classwk_cols), length(empstat_cols)))

    # Compute weighted shares for each dummy: sum(hhwt * dummy) / sum(hhwt where dummy not NA)
    # And weighted counts: sum(hhwt * dummy)
    agg <- do.call(rbind, lapply(
        split(df, interaction(df$year, df$geolev2, drop = TRUE)),
        function(g) {
            out <- data.frame(
                year     = g$year[1],
                country  = g$country[1],
                geolev1  = g$geolev1[1],
                geolev2  = g$geolev2[1],
                geo2_ar  = g$geo2_ar[1],
                pop      = sum(g$hhwt, na.rm = TRUE),
                urbpop   = sum(g$urbpop_contrib, na.rm = TRUE),
                stringsAsFactors = FALSE
            )

            # Education shares
            educ_mask <- !is.na(g$has_college)
            educ_den  <- sum(g$hhwt[educ_mask], na.rm = TRUE)
            out$college    <- if (educ_den > 0) sum(g$hhwt[educ_mask] * g$has_college[educ_mask]) / educ_den else NA
            out$ncollege   <- sum(g$hhwt[educ_mask] * g$has_college[educ_mask], na.rm = TRUE)
            out$secondary  <- if (educ_den > 0) sum(g$hhwt[educ_mask] * g$has_secondary[educ_mask]) / educ_den else NA
            out$nsecondary <- sum(g$hhwt[educ_mask] * g$has_secondary[educ_mask], na.rm = TRUE)

            # Migration shares
            mig_mask <- !is.na(g$has_mig_info)
            mig_den  <- sum(g$hhwt[mig_mask], na.rm = TRUE)
            out$mig5  <- if (mig_den > 0) sum(g$hhwt[mig_mask] * g$is_migrant[mig_mask]) / mig_den else NA
            out$nmig5 <- sum(g$hhwt[mig_mask] * g$is_migrant[mig_mask], na.rm = TRUE)

            # All dummy variable shares and counts
            for (col in all_dummy_cols) {
                mask <- !is.na(g[[col]])
                den  <- sum(g$hhwt[mask], na.rm = TRUE)
                out[[col]]              <- if (den > 0) sum(g$hhwt[mask] * g[[col]][mask]) / den else NA
                out[[paste0("n", col)]] <- sum(g$hhwt[mask] * g[[col]][mask], na.rm = TRUE)
            }

            out
        }
    ))

    rownames(agg) <- NULL

    # Set urbpop to NA where urban was all-missing in that district-year
    # (mirrors old Stata: replace urbpop = . if urban == .)
    # We detect this by checking if urbpop == 0 AND pop > 0 — could be
    # genuinely zero urban pop or all-missing. Conservative: keep as-is.
    # The old code set urbpop=NA when urban was missing at person level;
    # our aggregation already handles this since NA urban → 0 contribution.

    message(sprintf("[ipums]   Aggregated: %d district-year observations", nrow(agg)))
    agg
}

# ---------------------------------------------------------------------------
# Helper: merge province names from prov_labels.xlsx
# ---------------------------------------------------------------------------
merge_province_names <- function(panel) {
    message("\n[ipums] Step 3 — Merging province names")

    prov_path <- file.path(dir_raw_census, "prov_labels.xlsx")
    stopifnot(file.exists(prov_path))

    prov <- readxl::read_excel(prov_path)
    prov <- as.data.frame(prov)

    # The xlsx has GEOLEV1 and label columns
    names(prov) <- tolower(names(prov))
    stopifnot("geolev1" %in% names(prov) && "label" %in% names(prov))

    panel <- merge(panel, prov[, c("geolev1", "label")],
                   by = "geolev1", all.x = TRUE)

    # Drop districts where province is "UNKNOWN" (shouldn't happen)
    n_unknown <- sum(panel$label == "UNKNOWN", na.rm = TRUE)
    if (n_unknown > 0) {
        message(sprintf("[ipums]   WARNING: dropping %d obs with UNKNOWN province", n_unknown))
        panel <- panel[panel$label != "UNKNOWN" | is.na(panel$label), ]
    }

    names(panel)[names(panel) == "label"] <- "provname"
    n_matched <- sum(!is.na(panel$provname))
    message(sprintf("[ipums]   Province names matched: %d / %d", n_matched, nrow(panel)))

    panel
}

# ---------------------------------------------------------------------------
# Helper: exclude non-mainland territories
# ---------------------------------------------------------------------------
exclude_territories <- function(panel) {
    message("\n[ipums] Step 4 — Excluding non-mainland territories")

    n_before <- nrow(panel)

    # Exclude Malvinas and South Georgia
    panel <- panel[!(panel$geolev2 %in% geolev2_exclude), ]

    # Exclude unassigned/residual geolev2 codes (e.g., 32030000 = 250 people
    # in Entre Ríos 1970 with no district assignment). These are province-level
    # residuals that can't be matched to any named district.
    # Pattern: codes ending in 0000 that aren't in the standard 312-district set.
    residual_codes <- panel$geolev2[panel$geolev2 %% 10000 == 0]
    if (length(residual_codes) > 0) {
        message(sprintf("[ipums]   Dropping %d obs with residual geolev2 codes: %s",
                        sum(panel$geolev2 %in% residual_codes),
                        paste(unique(residual_codes), collapse = ", ")))
        panel <- panel[!(panel$geolev2 %in% residual_codes), ]
    }

    n_dropped <- n_before - nrow(panel)
    message(sprintf("[ipums]   Dropped %d obs total", n_dropped))
    message(sprintf("[ipums]   Remaining: %d obs, %d unique districts",
                    nrow(panel), length(unique(panel$geolev2))))

    panel
}

# ---------------------------------------------------------------------------
# Helper: validate the panel
# ---------------------------------------------------------------------------
validate_panel <- function(panel) {
    message("\n[ipums] Step 5 — Validating panel")

    # Key must be unique
    key <- paste(panel$geolev2, panel$year)
    n_dup <- sum(duplicated(key))
    stopifnot(n_dup == 0)
    message("[ipums]   Key (geolev2 + year) is unique: OK")

    # Key must be non-missing
    stopifnot(!any(is.na(panel$geolev2)))
    stopifnot(!any(is.na(panel$year)))
    message("[ipums]   Key has no missing values: OK")

    # Expected district count
    n_districts_actual <- length(unique(panel$geolev2))
    message(sprintf("[ipums]   Unique districts: %d (expected: %d)",
                    n_districts_actual, n_districts))

    # Year coverage
    message(sprintf("[ipums]   Years: %s",
                    paste(sort(unique(panel$year)), collapse = ", ")))
    message(sprintf("[ipums]   Total obs: %d", nrow(panel)))
}

# ---------------------------------------------------------------------------
# Helper: save panel with SaveData pattern
# ---------------------------------------------------------------------------
save_panel <- function(panel) {
    message("\n[ipums] Step 6 — Saving panel")

    # Sort by key
    panel <- panel[order(panel$geolev2, panel$year), ]

    # Drop intermediate columns not needed downstream
    drop_cols <- c("country", "geo2_ar")
    panel <- panel[, !(names(panel) %in% drop_cols)]

    out_path <- file.path(dir_derived_ipums, "ipums_panel.parquet")
    arrow::write_parquet(panel, out_path)
    message(sprintf("[ipums]   Saved: %s (%d rows, %d cols)",
                    out_path, nrow(panel), ncol(panel)))

    # Return the cleaned panel (without dropped cols) for manifest
    panel
}

# ---------------------------------------------------------------------------
# Helper: build district name crosswalk from 1991 census geography labels
# ---------------------------------------------------------------------------
build_crosswalk <- function(df) {
    message("\n[ipums] Step 7 — Building district name crosswalk")

    # Use 1991 census year — it has the most complete geography labels
    df91 <- df[df$year == 1991, ]

    # Get unique geolev2 values with their geo2_ar labels
    districts <- unique(df91[, c("geolev1", "geolev2", "geo2_ar")])
    message(sprintf("[ipums]   Unique districts in 1991: %d", nrow(districts)))

    # Decode geo2_ar labels — these are haven labelled integers
    # The labels contain comma-separated district name variants
    geo2_labels <- haven::as_factor(df91$geo2_ar)
    label_map <- data.frame(
        geo2_ar = df91$geo2_ar,
        label   = as.character(geo2_labels),
        stringsAsFactors = FALSE
    )
    label_map <- unique(label_map)

    districts <- merge(districts, label_map, by = "geo2_ar", all.x = TRUE)

    # Split comma-separated labels into separate rows
    # Each geolev2 may have multiple name variants (e.g., "Quilmes, Berazategui")
    rows <- list()
    for (i in seq_len(nrow(districts))) {
        parts <- trimws(strsplit(districts$label[i], ",")[[1]])
        parts <- parts[parts != ""]
        for (p in parts) {
            rows[[length(rows) + 1]] <- data.frame(
                geolev1 = districts$geolev1[i],
                geolev2 = districts$geolev2[i],
                districtIPUMS = p,
                stringsAsFactors = FALSE
            )
        }
    }
    xwalk <- do.call(rbind, rows)

    # Clean district names: remove spaces, punctuation, accents; uppercase
    xwalk$districtIPUMS <- clean_district_name(xwalk$districtIPUMS)

    # Merge province names
    prov_path <- file.path(dir_raw_census, "prov_labels.xlsx")
    prov <- readxl::read_excel(prov_path)
    prov <- as.data.frame(prov)
    names(prov) <- tolower(names(prov))
    xwalk <- merge(xwalk, prov[, c("geolev1", "label")], by = "geolev1", all.x = TRUE)
    names(xwalk)[names(xwalk) == "label"] <- "provname"

    # Create merge keys
    xwalk$provmerge <- xwalk$provname
    xwalk$distmerge <- xwalk$districtIPUMS

    # --- Manual fix: IPUMS typo ---
    # geolev2=32006032 is Marcos Paz, but IPUMS labels it as "Maipu"
    # Discovered during merge validation in old pipeline
    xwalk$distmerge[xwalk$geolev2 == 32006032 & xwalk$distmerge == "MAIPU"] <- "MARCOSPAZ"

    # Check for duplicate provmerge + distmerge
    dup_key <- paste(xwalk$provmerge, xwalk$distmerge)
    n_dup <- sum(duplicated(dup_key))
    if (n_dup > 0) {
        message(sprintf("[ipums]   WARNING: %d duplicate provmerge+distmerge keys", n_dup))
        dups <- xwalk[duplicated(dup_key) | duplicated(dup_key, fromLast = TRUE), ]
        message("[ipums]   Duplicates:")
        print(dups[, c("provmerge", "distmerge", "geolev2")])
    }

    # Keep final columns
    xwalk <- xwalk[, c("provmerge", "distmerge", "geolev2", "provname", "districtIPUMS")]

    message(sprintf("[ipums]   Crosswalk: %d rows, %d unique geolev2",
                    nrow(xwalk), length(unique(xwalk$geolev2))))

    xwalk
}

# ---------------------------------------------------------------------------
# Helper: clean district name string for matching
# ---------------------------------------------------------------------------
clean_district_name <- function(x) {
    # Remove spaces, periods, hyphens, apostrophes
    x <- gsub(" ", "", x)
    x <- gsub("\\.", "", x)
    x <- gsub("-", "", x)
    x <- gsub("'", "", x)

    # Replace accented characters (common in Argentine district names)
    # These appear as UTF-8 in the IPUMS labels
    x <- gsub("\u00e1", "a", x)  # á → a
    x <- gsub("\u00e9", "e", x)  # é → e
    x <- gsub("\u00ed", "i", x)  # í → i
    x <- gsub("\u00f3", "o", x)  # ó → o
    x <- gsub("\u00fa", "u", x)  # ú → u
    x <- gsub("\u00fc", "u", x)  # ü → u
    x <- gsub("\u00f1", "n", x)  # ñ → n
    x <- gsub("\u00c1", "A", x)  # Á → A
    x <- gsub("\u00c9", "E", x)  # É → E
    x <- gsub("\u00cd", "I", x)  # Í → I
    x <- gsub("\u00d3", "O", x)  # Ó → O
    x <- gsub("\u00da", "U", x)  # Ú → U
    x <- gsub("\u00d1", "N", x)  # Ñ → N

    # Also handle the garbled encodings from the old Stata pipeline
    # (ã³ = ó, ã­ = í, etc. — these are UTF-8 bytes misread as Latin-1)
    x <- gsub("\u00e3\u00b3", "o", x)
    x <- gsub("\u00e3\u00ad", "i", x)
    x <- gsub("\u00e3\u00a1", "a", x)
    x <- gsub("\u00e3\u00a9", "e", x)
    x <- gsub("\u00e3\u00b1", "n", x)
    x <- gsub("\u00e3\u00ba", "u", x)
    x <- gsub("\u00e3\u00bc", "u", x)
    # Double-encoded Ñ/ñ: original Ñ → UTF-8 C3 91 → misread as
    # Latin-1 → re-encoded → C3 A3 C2 91 (lowercase) or C3 83 C2 91 (uppercase).
    x <- gsub("\u00e3\u0091", "n", x)  # lowercase ñ double-encoded
    x <- gsub("\u00c3\u0091", "N", x)  # uppercase Ñ double-encoded

    toupper(x)
}

# ---------------------------------------------------------------------------
# Helper: save crosswalk
# ---------------------------------------------------------------------------
save_crosswalk <- function(xwalk) {
    # Sort by key
    xwalk <- xwalk[order(xwalk$provmerge, xwalk$distmerge), ]

    out_path <- file.path(dir_derived_ipums, "ipums_districts_for_merge.parquet")
    arrow::write_parquet(xwalk, out_path)
    message(sprintf("[ipums]   Saved crosswalk: %s (%d rows)",
                    out_path, nrow(xwalk)))
}

# ---------------------------------------------------------------------------
# Helper: write manifest log
# ---------------------------------------------------------------------------
write_manifest <- function(panel, crosswalk) {
    message("\n[ipums] Step 8 — Writing manifest")

    log_path <- file.path(dir_derived_ipums, "data_file_manifest.log")
    sink(log_path)
    on.exit(sink(), add = TRUE)

    cat(sprintf("Data file manifest — clean_ipums.R\n"))
    cat(sprintf("Generated: %s\n", Sys.time()))
    cat(sprintf("rootdir: %s\n\n", rootdir))

    cat(strrep("=", 60), "\n")
    cat("FILE: ipums_panel.parquet\n")
    cat(strrep("=", 60), "\n")
    cat(sprintf("Rows: %d\n", nrow(panel)))
    cat(sprintf("Columns: %d\n", ncol(panel)))
    cat(sprintf("Key: geolev2 + year\n"))
    cat(sprintf("Unique geolev2: %d\n", length(unique(panel$geolev2))))
    cat(sprintf("Years: %s\n", paste(sort(unique(panel$year)), collapse = ", ")))
    cat("\nColumn names:\n")
    cat(paste(" ", names(panel), collapse = "\n"), "\n")
    cat("\nSummary of key variables:\n")
    for (v in c("pop", "urbpop", "college", "secondary", "mig5")) {
        vals <- panel[[v]]
        if (!is.null(vals)) {
            cat(sprintf("  %-15s  N=%d  mean=%.2f  sd=%.2f  min=%.2f  max=%.2f  NA=%d\n",
                        v, sum(!is.na(vals)), mean(vals, na.rm = TRUE),
                        sd(vals, na.rm = TRUE), min(vals, na.rm = TRUE),
                        max(vals, na.rm = TRUE), sum(is.na(vals))))
        }
    }

    cat("\n", strrep("=", 60), "\n")
    cat("FILE: ipums_districts_for_merge.parquet\n")
    cat(strrep("=", 60), "\n")
    cat(sprintf("Rows: %d\n", nrow(crosswalk)))
    cat(sprintf("Columns: %d\n", ncol(crosswalk)))
    cat(sprintf("Key: provmerge + distmerge\n"))
    cat(sprintf("Unique geolev2: %d\n", length(unique(crosswalk$geolev2))))
    cat(sprintf("Unique provmerge: %d\n", length(unique(crosswalk$provmerge))))

    message(sprintf("[ipums]   Manifest written: %s", log_path))
}

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
main()

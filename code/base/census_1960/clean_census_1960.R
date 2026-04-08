# ===========================================================================
# clean_census_1960.R
#
# PURPOSE: Clean digitized 1960 Argentine national census and merge to
#          IPUMS district geography. Reads regional Excel files with
#          locality-level population, classifies urban (pop > 2000) vs
#          rural, collapses to district level, standardizes names, and
#          merges to the IPUMS crosswalk.
#
# READS:
#   data/raw/census/censo1960/1c1960_*.xlsx  — locality-level pop by region
#   data/derived/base/ipums/ipums_districts_for_merge.parquet — crosswalk
#
# PRODUCES:
#   data/derived/05a_census/census_1960_ipums.parquet
#       Key: geolev2. Variables: pop, urbpop, rur, year.
#
#   data/derived/05a_census/census_1960_manifest.log
#
# REFERENCE:
#   Old data/Train/base/census_1960/code/import_c1960.do
#   Old data/Train/derived/census_1960/code/merge_c1960_to_IPUMS.do
#
# NOTES:
#   - The 1960 census is organized by geographic region, each with
#     multiple Excel pages (one per sub-region or province within the
#     region). Part 2 = Capital Federal, Parts 3-9 = rest of country.
#   - Urban is defined as localities with population > 2000 (standard
#     Argentine census definition).
#   - Capital Federal localities are collapsed into a single district
#     "CITYOFBUENOSAIRES" (excluded from estimation sample).
#   - Berazategui was segregated from Quilmes in 1960. Population is
#     split 2/3 Quilmes (32006076) and 1/3 Berazategui (32006087).
#   - The raw Excel files contain many OCR/transcription errors in
#     district names. All fixes are documented inline with sources.
# ===========================================================================

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"))

    message("\n", strrep("=", 72))
    message("clean_census_1960.R  |  1960 census -> geolev2 panel")
    message(strrep("=", 72))

    # --- 1. Read and append all regional Excel files -----------------------
    raw <- read_raw_1960()

    # --- 2. Fix OCR typos in district names -------------------------------
    raw <- fix_district_typos(raw)

    # --- 3. Classify urban/rural and collapse to district level -----------
    districts <- collapse_to_districts(raw)

    # --- 4. Create merge keys with historical name changes ----------------
    districts <- apply_name_changes(districts)

    # --- 5. Merge to IPUMS crosswalk --------------------------------------
    merged <- merge_to_ipums(districts)

    # --- 6. Handle Berazategui split and collapse to geolev2 --------------
    final <- collapse_to_geolev2(merged)

    # --- 7. Validate and save ---------------------------------------------
    save_output(final)

    message(strrep("=", 72))
    message("clean_census_1960.R  |  Complete.")
    message(strrep("=", 72), "\n")
}

# ---------------------------------------------------------------------------
# Step 1: Read raw Excel files
#
# The 1960 census is split into 8 regional parts (2-9). Part 2 is Capital
# Federal (single file). Parts 3-9 each have multiple pages (sub-files).
# ---------------------------------------------------------------------------
read_raw_1960 <- function() {
    message("\n[c1960] Step 1 -- Reading raw Excel files")

    raw_dir <- file.path(dir_raw_census, "censo1960")
    if (!dir.exists(raw_dir)) {
        stop(sprintf("Raw data directory not found: %s", raw_dir))
    }

    all_rows <- list()

    # -- Part 2: Capital Federal (single file)
    p2_file <- file.path(raw_dir, "1c1960_2.xlsx")
    stopifnot(file.exists(p2_file))
    p2 <- readxl::read_excel(p2_file)
    p2 <- as.data.frame(p2)
    names(p2) <- tolower(names(p2))
    p2$provincia <- "CAPITALFEDERAL"
    p2$distrito <- clean_name(p2$partido)
    p2$pop <- as.numeric(p2$pop)
    p2 <- p2[, c("provincia", "distrito", "pop")]
    all_rows[[1]] <- p2

    # -- Parts 3-9: multiple pages each
    # Number of pages per part (from old Stata code)
    pages <- list(
        "3" = 8, "4" = 7, "5" = 3, "6" = 3,
        "7" = 4, "8" = 2, "9" = 2
    )

    for (part in names(pages)) {
        n_pages <- pages[[part]]
        part_rows <- list()

        for (pg in seq_len(n_pages)) {
            fname <- sprintf("1c1960_%s_%d.xlsx", part, pg)
            fpath <- file.path(raw_dir, fname)
            stopifnot(file.exists(fpath))

            pg_df <- readxl::read_excel(fpath)
            pg_df <- as.data.frame(pg_df)
            names(pg_df) <- tolower(names(pg_df))
            pg_df$pop <- as.numeric(pg_df$pop)
            pg_df$distrito <- clean_name(pg_df$distrito)
            pg_df$provincia <- clean_name(pg_df$provincia)

            # Drop empty rows
            pg_df <- pg_df[
                !(is.na(pg_df$distrito) &
                    is.na(pg_df$pop) &
                    is.na(pg_df$provincia)),
            ]

            pg_df <- pg_df[, c("provincia", "distrito", "pop")]
            part_rows[[pg]] <- pg_df
        }

        all_rows[[length(all_rows) + 1]] <- do.call(rbind, part_rows)
    }

    raw <- do.call(rbind, all_rows)
    raw$pop <- as.numeric(raw$pop)

    message(sprintf(
        "[c1960]   Read %d locality rows", nrow(raw)
    ))
    raw
}

# ---------------------------------------------------------------------------
# Step 2: Fix OCR/transcription typos in district names
#
# The digitized Excel files contain many spelling errors from OCR or
# manual transcription. These are organized by region (matching the
# assert blocks in the old Stata code).
# ---------------------------------------------------------------------------
fix_district_typos <- function(df) {
    message("\n[c1960] Step 2 -- Fixing district name typos")

    n_before <- nrow(df)

    # Helper: replace distrito value
    fix <- function(correct, wrong) {
        df$distrito[df$distrito == wrong] <<- correct
    }

    # -- Part 3: Buenos Aires and La Pampa --
    fix("AVELLANEDA", "AVALLANEDA")
    fix("SANCAYETANO", "18NSANCAYETANO")
    fix("BRANDSEN", "BRANSEN")
    fix("BRANDSEN", "BRANDEN")
    fix("CARMENDEARECO", "CARMENDEARACO")
    fix("CARMENDEARECO", "CARMRENDEARECO")
    fix("CHALILEO", "CHALILES")
    fix("GENERALJMADARIAGA", "GENERALJUANMADARIAGA")
    fix("GENERALJMADARIAGA", "GENERALMADARIAGA")
    fix("GENERALJMADARIAGA", "GENERALMADRIAGA")
    fix("GENERALCHAVES", "GENERALCHAVEZ")
    fix("GUAMINI", "GUAMANI")
    fix("LEANDRONALEM", "LEANDROSALEM")
    fix("LINCOLN", "LICOLN")
    fix("REALICO", "REATICO")
    fix("SANANDRESDEGILES", "SANANDRESDEGILESDE")
    fix("SANANTONIODEARECO", "SANANTONIODEARACO")
    fix("SANNICOLAS", "SANNICOLAN")
    fix("TRENQUELAUQUEN", "TRANQUELAUQUEN")
    fix("UTRACAN", "ULTRACAN")
    # NOTE: old data had garbled "X&L¬AC16N..." prefix; current xlsx is clean.
    # Kept as defensive fix in case older data versions are used.
    fix("EXALTACIONDELACRUZ", "XLAC16NEXALTACIONDELACRUZ")
    fix("VILLARINO", "VILLAMARINO")
    fix("VILLARINO", "VILLARINAO")
    fix("GONZALEZCHAVES", "GONZALEZCHAVEZ")
    fix("CASEROS", "CASERES")
    fix("ROJAS", "REJAS")
    fix("LOVENTUE", "LEVENTUE")
    fix("RANCUL", "RACUL")

    # Caseros-La Pampa is actually La Larga in Daireaux, Buenos Aires
    # https://es.wikipedia.org/wiki/Partido_de_Daireaux
    idx_caseros_lp <- df$distrito == "CASEROS" &
        df$provincia == "LAPAMPA"
    df$provincia[idx_caseros_lp] <- "BUENOSAIRES"

    # -- Part 4: Córdoba and Santa Fe --
    fix("PUNILLA", "1PUNILLA")
    fix("CALAMUCHITA", "CALAMACHITA")
    fix("CALAMUCHITA", "CALAMUCHITATA")
    fix("CALAMUCHITA", "CALAZUCHITA")
    fix("CASTELLANOS", "CASETELLANO")
    fix("GENERALROCA", "GENERALBOCA")
    fix("GENERALOBLIGADO", "IGENERALOBLIGADO")
    fix("JUAREZCELMAN", "IJUAREZCELMAN")
    fix("SANCRISTOBAL", "ISANCRISTOBAL")
    fix("ISCHILIN", "ISCHILLIN")
    fix("ISCHILIN", "IACHILIN")
    fix("TERCEROARRIBA", "ITERCEROARRIBA")
    fix("MARCOSJUAREZ", "MARCOJUAREZ")
    fix("PRESIDENTEROQUESAENZPENA", "PRESIDENTEROQUEZSAENZPENA")
    fix("PRESIDENTEROQUESAENZPENA", "PUENTEROQUESAENZPENA")
    fix("PRESIDENTEROQUESAENZPENA", "PRESIDENTROQUESAENZPENA")
    fix("PUNILLA", "PUNILLO")
    fix("PUNILLA", "PUNTILLA")
    fix("SANLORENZO", "SANLORENZOI")
    fix("SANTAMARIA", "SANTAMARIAI")
    fix("SANJAVIER", "SANJAVIERR")
    fix("TERCEROARRIBA", "TERCEROARRIBAL")
    fix("TERCEROARRIBA", "TERCEROARRRIBA")
    fix("9DEJULIO", "NUEVEDEJULIO")

    # Piamonte (Santa Fe) is in San Martín department, not San Miguel
    # Confirmed with GIS shapefiles
    fix("SANMARTIN", "SANMIGUEL")

    # -- Part 5: Corrientes, Entre Ríos, Misiones --
    fix("LIBERTADORGENERALSANMARTIN", "BERTADORGENERALSANMARTIN")
    fix("GUALEGUAY", "GUALAGUAY")
    fix("GUALEGUAYCHU", "GUALEGUAYOBU")
    fix("GUALEGUAYCHU", "GUALEGUYCHU")
    fix("ITUZAINGO", "ITUZAINGU")
    fix("LIBERTADORGENERALSANMARTIN", "LIBERTADORGENERARLSANMARTIN")
    fix("VICTORIA", "VICTORIAI")
    fix("VILLAGUAY", "VILLAGUAS")
    fix("TALA", "TELA")
    fix("SANTOTOME", "SANTOTOMAS")

    # -- Part 6: Chaco, Formosa, Santiago del Estero --
    fix("ATAMISQUI", "ATAMIAQUI")
    fix("LIBERTADORGENERALSANMARTIN", "LIBERALGENERALSANMARTIN")
    fix("GENERALTABOADA", "NERALTABOADA")
    fix("QUEBRACHOS", "BRACHOS")

    # -- Part 7: Catamarca, Jujuy, La Rioja, Salta, Tucumán --
    fix("BURRUYACU", "BURRUCUYU")
    fix("BURRUYACU", "BURRUYACUJ")
    fix("BURRUYACU", "BURRRUYACU")
    fix("CAPAYAN", "CAPAYAM")
    fix("CAPAYAN", "CARAYAN")
    fix("CASTROBARROS", "CASTROPARRON")
    fix("CHICLIGASTA", "CHICLIGASTAI")
    fix("CHICLIGASTA", "CHICLIGASTE")
    fix("HUMAHUACA", "HUMAHUACAI")
    fix("LAVINA", "LAVINAI")
    fix("ANTOFAGASTADELASIERRA", "NTOFAGASTADELASIERRA")
    fix("FAMAILLA", "PAMAILLA")
    fix("FAMAILLA", "PAMPILLA")
    fix("METAN", "MATAN")
    fix("PACLIN", "PAOLIN")
    fix("POMAN", "POPAN")
    fix("RINCONADA", "RINCONADA\u00ae")  # garbled character
    fix("SUSQUES", "SUAQUES")
    fix("SUSQUES", "SUSQUES+")
    fix("VELEZSARSFIELD", "VELESSARSFIELD")
    fix("LACALDERA", "CALDERA")
    fix("GOBERNADORGORDILLO", "GENERALGORDILLO")

    # -- Part 8: Mendoza, San Juan, San Luis --
    fix("CAUCETE", "CAUCETEL")

    # -- Part 9: Chubut, Neuquén, Río Negro, Santa Cruz, Tierra del Fuego --
    fix("ADOLFOALSINA", "ADOLFOALSINAS")
    fix("RIOSENGUER", "RIOSENGUERR")
    fix("RIOSENGUER", "RIOSENGUERRR")
    fix("TEHUELCHES", "TAHUELCHES")

    n_after <- nrow(df)
    stopifnot(n_before == n_after)  # typo fixes should not change row count
    message(sprintf("[c1960]   Typo fixes applied (%d rows unchanged)", n_after))
    df
}

# ---------------------------------------------------------------------------
# Step 3: Classify urban/rural and collapse to district level
#
# Urban = locality with population > 2000 (standard Argentine definition).
# Collapse localities to distrito level, computing total, urban, and
# rural population.
# ---------------------------------------------------------------------------
collapse_to_districts <- function(raw) {
    message("\n[c1960] Step 3 -- Classifying urban/rural, collapsing")

    # Capital Federal: all localities → single district
    raw$distrito[raw$provincia == "CAPITALFEDERAL"] <- "CITYOFBUENOSAIRES"

    # Classify urban (pop > 2000)
    raw$urban <- as.integer(!is.na(raw$pop) & raw$pop > 2000)

    # Collapse: sum pop by provincia + distrito + urban
    agg <- aggregate(
        pop ~ provincia + distrito + urban,
        data = raw, FUN = sum, na.rm = TRUE
    )

    # Reshape wide: pop0 = rural, pop1 = urban
    rur <- agg[agg$urban == 0, c("provincia", "distrito", "pop")]
    urb <- agg[agg$urban == 1, c("provincia", "distrito", "pop")]
    names(rur)[3] <- "rur"
    names(urb)[3] <- "urb"

    districts <- merge(rur, urb,
        by = c("provincia", "distrito"), all = TRUE
    )
    districts$rur[is.na(districts$rur)] <- 0
    districts$urb[is.na(districts$urb)] <- 0
    districts$pop <- districts$rur + districts$urb
    districts$year <- 1960L

    # Rename to match project convention (urbpop, not urb)
    names(districts)[names(districts) == "urb"] <- "urbpop"

    message(sprintf(
        "[c1960]   %d districts after collapsing", nrow(districts)
    ))
    districts
}

# ---------------------------------------------------------------------------
# Step 4: Apply historical name changes
#
# Create provmerge/distmerge keys that match the IPUMS crosswalk (1991
# boundaries). Changes documented with sources.
# ---------------------------------------------------------------------------
apply_name_changes <- function(df) {
    message("\n[c1960] Step 4 -- Applying historical name changes")

    df$provmerge <- df$provincia
    df$distmerge <- df$distrito

    ch <- function(new, old, prov) {
        idx <- df$distmerge == old & df$provmerge == prov
        df$distmerge[idx] <<- new
    }

    # -- BUENOS AIRES --
    pr <- "BUENOSAIRES"
    ch("TRESDEFEBRERO", "3DEFEBRERO", pr)
    # Arrecifes was called Bartolomé Mitre
    ch("ARRECIFES", "BARTOLOMEMITRE", pr)
    ch("CORONELDEMARINELROSALES", "CORONELROSALES", pr)
    ch("ADOLFOGONZALEZCHAVES", "GONZALEZCHAVES", pr)
    ch("GENERALJUANMADARIAGA", "GENERALJMADARIAGA", pr)
    # Daireaux was called Caseros until 1970
    ch("DAIREAUX", "CASEROS", pr)
    ch("BENITOJUAREZ", "JUAREZ", pr)

    # -- CHACO --
    pr <- "CHACO"
    ch("12DEOCTUBRE", "DOCEDEOCTUBRE", pr)
    ch("1DEMAYO", "PRIMERODEMAYO", pr)
    ch("25DEMAYO", "VEINTICINCODEMAYO", pr)
    ch("9DEJULIO", "NUEVEDEJULIO", pr)
    ch("FRAYJUSTOSTAMARIADEORO", "FRAYJSANTAMARIADEORO", pr)
    ch("OHIGGINS", "CAPITANGENERALOHIGGINS", pr)

    # -- CHUBUT --
    ch("PASODELOSINDIOS", "PASODEINDIOS", "CHUBUT")

    # -- FORMOSA --
    ch("RAMONLISTA", "RAMONLIATA", "FORMOSA")

    # -- JUJUY --
    ch("DRMANUELBELGRANO", "CAPITAL", "JUJUY")

    # -- LA PAMPA --
    ch("LEVENTUE", "LOVENTUE", "LAPAMPA")

    # -- LA RIOJA --
    pr <- "LARIOJA"
    ch("CORONELFELIPEVARELA", "GENERALLAVALLE", pr)
    ch("ROSARIOVERAPENALOZA", "GENERALROCA", pr)
    ch("CHAMICAL", "GOBERNADORGORDILLO", pr)
    ch("VINCHINA", "GENERALSARMIENTO", pr)
    ch("GENERALJUANFQUIROGA", "RIVADAVIA", pr)
    ch("GENERALANGELVPENALOZA", "VELEZSARSFIELD", pr)

    # -- MENDOZA --
    ch("LUJANDECUYO", "LUJAN", "MENDOZA")

    # -- MISIONES --
    ch("GENERALMANUELBELGRANO", "GENERALBELGRANO", "MISIONES")

    # -- RIO NEGRO --
    ch("CONESA", "GENERALCONESA", "RIONEGRO")

    # -- SALTA --
    pr <- "SALTA"
    ch("GENERALJOSEDESANMARTIN", "GENERALJDESANMARTIN", pr)
    ch("GENERALGUEMES", "GENERALMARTINMIGUELDEGUEMES", pr)
    ch("LACANDELARIA", "CANDELARIA", pr)

    # -- SAN JUAN --
    pr <- "SANJUAN"
    ch("25DEMAYO", "VEINTICINCODEMAYO", pr)
    ch("9DEJULIO", "NUEVEDEJULIO", pr)
    ch("IGLESIA", "IGLESIAS", pr)
    ch("ULLUM", "ULLUN", pr)

    # -- SAN LUIS --
    ch("LIBERTADORGENERALSANMARTIN", "SANMARTIN", "SANLUIS")

    # -- SANTIAGO DEL ESTERO --
    ch("JUANFIBARRA", "MATARA", "SANTIAGODELESTERO")

    # -- TIERRA DEL FUEGO --
    ch("RIOGRANDE", "SANSEBASTIAN", "TIERRADELFUEGO")

    # -- TUCUMAN --
    pr <- "TUCUMAN"
    ch("GRANERO", "GRANEROS", pr)
    ch("TAFIDELVALLE", "TAFI", pr)

    message(sprintf(
        "[c1960]   %d districts after name changes", nrow(df)
    ))
    df
}

# ---------------------------------------------------------------------------
# Step 5: Merge to IPUMS crosswalk
# ---------------------------------------------------------------------------
merge_to_ipums <- function(df) {
    message("\n[c1960] Step 5 -- Merging to IPUMS crosswalk")

    xwalk_path <- file.path(
        dir_derived, "base", "ipums",
        "ipums_districts_for_merge.parquet"
    )
    stopifnot(file.exists(xwalk_path))
    xwalk <- arrow::read_parquet(xwalk_path)
    xwalk <- as.data.frame(xwalk)
    xwalk <- ensure_geolev2_char(xwalk)

    merged <- merge(
        df, xwalk,
        by.x = c("provmerge", "distmerge"),
        by.y = c("provmerge", "distmerge"),
        all = TRUE
    )

    n_matched     <- sum(!is.na(merged$pop) & !is.na(merged$geolev2))
    n_census_only <- sum(!is.na(merged$pop) & is.na(merged$geolev2))
    n_ipums_only  <- sum(is.na(merged$pop) & !is.na(merged$geolev2))

    message(sprintf("[c1960]   Matched: %d", n_matched))
    message(sprintf("[c1960]   Census only (unmatched): %d", n_census_only))
    message(sprintf("[c1960]   IPUMS only (no 1960 data): %d", n_ipums_only))

    if (n_census_only > 0) {
        unmatched <- merged[!is.na(merged$pop) & is.na(merged$geolev2), ]
        message("[c1960]   Unmatched census districts:")
        for (i in seq_len(nrow(unmatched))) {
            message(sprintf(
                "          %s - %s",
                unmatched$provmerge[i], unmatched$distmerge[i]
            ))
        }
    }

    # Drop redundant IPUMS-only rows (geolev2 already covered by a match)
    matched_geolev2 <- unique(
        merged$geolev2[!is.na(merged$pop) & !is.na(merged$geolev2)]
    )
    redundant <- is.na(merged$pop) &
        !is.na(merged$geolev2) &
        merged$geolev2 %in% matched_geolev2
    n_redundant <- sum(redundant)
    if (n_redundant > 0) {
        message(sprintf(
            "[c1960]   Dropping %d redundant IPUMS-only rows",
            n_redundant
        ))
        merged <- merged[!redundant, ]
    }

    # Handle the one remaining unmatched IPUMS district: Berazategui
    # Berazategui was segregated from Quilmes in 1960
    ipums_only <- merged[is.na(merged$pop) & !is.na(merged$geolev2), ]
    census_rows <- merged[!is.na(merged$pop), ]

    if (nrow(ipums_only) > 0) {
        message("[c1960]   Remaining IPUMS-only districts:")
        for (i in seq_len(nrow(ipums_only))) {
            message(sprintf(
                "          %s - %s (geolev2=%s)",
                ipums_only$provmerge[i],
                ipums_only$distmerge[i],
                ipums_only$geolev2[i]
            ))
        }

        # Map Berazategui to Quilmes
        beraz_idx <- ipums_only$distmerge == "BERAZATEGUI" &
            ipums_only$provmerge == "BUENOSAIRES"
        if (any(beraz_idx)) {
            ipums_only$distmerge[beraz_idx] <- "QUILMES"
        }

        # Re-merge the remapped rows
        remapped <- merge(
            ipums_only[, c(
                "provmerge", "distmerge", "geolev2",
                "provname", "districtIPUMS"
            )],
            census_rows[, c(
                "provmerge", "distmerge",
                "pop", "urbpop", "rur", "year"
            )],
            by = c("provmerge", "distmerge"),
            all.x = TRUE
        )

        # Align columns before rbind to avoid mismatch
        matched <- census_rows[!is.na(census_rows$geolev2), ]
        remapped_ok <- remapped[!is.na(remapped$pop), ]
        common_cols <- intersect(names(matched), names(remapped_ok))
        census_rows <- rbind(
            matched[, common_cols],
            remapped_ok[, common_cols]
        )
    }

    message(sprintf(
        "[c1960]   After merge cleanup: %d rows", nrow(census_rows)
    ))

    census_rows
}

# ---------------------------------------------------------------------------
# Step 6: Handle Quilmes split and collapse to geolev2
#
# Quilmes (Buenos Aires) was split into:
#   geolev2=32006076 = Quilmes proper (~2/3 of pop)
#   geolev2=32006087 = Berazategui (~1/3)
# For all other many-to-one matches, divide equally.
# ---------------------------------------------------------------------------
collapse_to_geolev2 <- function(df) {
    message("\n[c1960] Step 6 -- Allocating splits, collapsing to geolev2")

    # Count how many geolev2 codes each 1960 district maps to
    df$alloc_key <- paste(df$provmerge, df$distmerge, sep = "_")
    alloc_counts <- table(df$alloc_key)
    df$n_alloc <- as.integer(alloc_counts[df$alloc_key])

    # Default: divide equally
    for (v in c("pop", "urbpop", "rur")) {
        df[[v]] <- df[[v]] / df$n_alloc
    }

    # Special case: Quilmes split
    quilmes_idx <- df$geolev2 == "32006076"
    beraz_idx   <- df$geolev2 == "32006087"
    if (any(quilmes_idx) && any(beraz_idx)) {
        for (v in c("pop", "urbpop", "rur")) {
            total <- df[[v]][quilmes_idx] * df$n_alloc[quilmes_idx]
            df[[v]][quilmes_idx] <- total * (2 / 3)
            df[[v]][beraz_idx]   <- total * (1 / 3)
        }
    }

    # Collapse to geolev2
    final <- aggregate(
        cbind(pop, urbpop, rur) ~ geolev2,
        data = df[!is.na(df$geolev2), ],
        FUN = sum, na.rm = TRUE
    )
    final$year <- 1960L

    # Exclude non-mainland territories, Capital Federal, Tierra del Fuego
    geo_cf <- "32002001"
    geo_tdf <- c("32094001", "32094002")  # Ushuaia + Antártida, Río Grande
    excl <- final$geolev2 %in% geolev2_exclude |
        final$geolev2 %in% c(geo_cf, geo_tdf)
    final <- final[!excl, ]

    message(sprintf("[c1960]   Final: %d districts", nrow(final)))
    final
}

# ---------------------------------------------------------------------------
# Step 7: Validate and save (SaveData pattern)
# ---------------------------------------------------------------------------
save_output <- function(final) {
    message("\n[c1960] Step 7 -- Validating and saving")

    # -- Validate key
    stopifnot(!any(is.na(final$geolev2)))
    stopifnot(!any(duplicated(final$geolev2)))
    message("[c1960]   Key (geolev2) is unique and non-missing: OK")

    # -- Sort by key
    final <- final[order(final$geolev2), ]

    # -- Ensure output directory exists
    out_dir <- dir_derived_census1960
    if (!dir.exists(out_dir)) {
        dir.create(out_dir, recursive = TRUE)
    }

    # -- Save parquet
    out_path <- file.path(out_dir, "census_1960_ipums.parquet")
    arrow::write_parquet(final, out_path)
    message(sprintf("[c1960]   Saved: %s (%d rows)", out_path, nrow(final)))

    # -- Write manifest
    log_path <- file.path(out_dir, "data_file_manifest.log")
    sink(log_path)
    on.exit(sink(), add = TRUE)

    cat("Data file manifest -- clean_census_1960.R\n")
    cat(sprintf("Generated: %s\n", Sys.time()))
    cat(sprintf("rootdir: %s\n\n", rootdir))

    cat(strrep("=", 60), "\n")
    cat("FILE: census_1960_ipums.parquet\n")
    cat(strrep("=", 60), "\n")
    cat(sprintf("Rows: %d\n", nrow(final)))
    cat(sprintf("Columns: %d\n", ncol(final)))
    cat(sprintf("Key: geolev2\n"))
    cat(sprintf("Year: %d\n", unique(final$year)))

    cat("\nSummary of key variables:\n")
    for (v in c("pop", "urbpop", "rur")) {
        vals <- final[[v]]
        cat(sprintf(
            "  %-10s  N=%d  mean=%.0f  min=%.0f  max=%.0f  NA=%d\n",
            v, sum(!is.na(vals)),
            mean(vals, na.rm = TRUE),
            min(vals, na.rm = TRUE),
            max(vals, na.rm = TRUE),
            sum(is.na(vals))
        ))
    }

    message(sprintf("[c1960]   Manifest: %s", log_path))
}

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
main()

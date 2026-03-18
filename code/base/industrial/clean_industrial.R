# ===========================================================================
# clean_industrial.R
#
# PURPOSE: Clean industrial/economic census data (1954, 1985) and harmonize
#          to IPUMS geolev2 district codes for the estimation panel.
#
# READS:
#   data/raw/industrial/Industrial1954_*.xlsx       (24 files, numbered 20-66)
#   data/raw/industrial/Economico1985_*.xlsx        (25 files, one per province)
#   data/raw/industrial/Codigo_*.xlsx               (25 files, code-to-name maps)
#   data/derived/base/ipums/ipums_districts_for_merge.parquet  (crosswalk)
#
# PRODUCES:
#   data/derived/base/industrial/ind_census.parquet
#       Key: geolev2 + year.
#       Variables: nestab, nemp (1954) / npers (1985), massal, valprod.
#   data/derived/base/industrial/data_file_manifest.log
#
# REFERENCE:
#   Old data/Train/base/ind_census_1954/code/clean_Industrial1954.do
#   Old data/Train/base/ind_census_1985/code/clean_Economico1985.do
#   Old data/Train/derived/ind_census_1954/code/merge_in1954_to_IPUMS.do
#   Old data/Train/derived/ind_census_1985/code/merge_ec1985_to_IPUMS.do
#
# NOTES:
#   - 1954 files are numbered by province code (20, 22, ..., 66), wide format.
#     Variables: Nestab, Nemp, Nobr, Massal, Valprod.
#   - 1985 uses numeric district codes (Ncodigo) in data files, with separate
#     Codigo_*.xlsx files mapping codes to district names.
#     Variables: Nestab, Npers, Massal, Valprod1, Valprod2.
#   - 1954 has district splits: Camarones → Escalante + Florentino Ameghino,
#     Comodoro Rivadavia → Escalante + Deseado (Chubut military zone).
#   - 1985 has GranBuenosAires as separate province — duplicates with
#     BuenosAires districts must be resolved (drop GranBuenosAires dupes).
#   - Capital Federal dropped from 1985.
#   - Quilmes/Berazategui 1/3-2/3 split applied for 1954.
# ===========================================================================

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)

    message("\n", strrep("=", 72))
    message("clean_industrial.R  |  Industrial census → geolev2 panel")
    message(strrep("=", 72))

    ind54 <- read_and_clean_1954()
    ind85 <- read_and_clean_1985()
    xwalk <- load_crosswalk()
    ind54_merged <- harmonize_and_merge_1954(ind54, xwalk)
    ind85_merged <- harmonize_and_merge_1985(ind85, xwalk)
    stack_validate_save(ind54_merged, ind85_merged)

    message(strrep("=", 72))
    message("clean_industrial.R  |  Complete.")
    message(strrep("=", 72), "\n")
}

# ---------------------------------------------------------------------------
# Helper: read and clean 1954 industrial census
# ---------------------------------------------------------------------------
read_and_clean_1954 <- function() {
    message("\n[ind] Step 1 — Reading 1954 industrial census")

    # Files numbered by province code: 20, 22, 24, ..., 66
    file_nums <- seq(20, 66, by = 2)

    dfs <- lapply(file_nums, function(n) {
        path <- file.path(dir_raw_industrial,
                          sprintf("Industrial1954_%d.xlsx", n))
        stopifnot(file.exists(path))
        df <- readxl::read_excel(path)
        df <- as.data.frame(df)
        names(df) <- tolower(names(df))
        # Replace dashes with NA and destring
        for (v in c("nestab", "nemp", "nobr", "massal", "valprod")) {
            if (v %in% names(df)) {
                df[[v]][grepl("^-+$", df[[v]])] <- NA
                df[[v]] <- as.numeric(df[[v]])
            }
        }
        df
    })

    ind54 <- do.call(rbind, dfs)
    ind54$year <- 1954L

    # Drop empty rows
    ind54 <- ind54[!(is.na(ind54$provincia) & is.na(ind54$distrito)), ]

    # Standardize names
    ind54$provincia <- clean_name(ind54$provincia)
    ind54$distrito  <- clean_name(ind54$distrito)

    message(sprintf("[ind]   1954: %d rows, %d provinces",
                    nrow(ind54), length(unique(ind54$provincia))))
    ind54
}

# ---------------------------------------------------------------------------
# Helper: read and clean 1985 economic census
# ---------------------------------------------------------------------------
read_and_clean_1985 <- function() {
    message("\n[ind] Step 2 — Reading 1985 economic census")

    provinces_1985 <- c(
        "BuenosAires", "Catamarca", "Chaco", "Chubut", "Cordoba",
        "Corrientes", "EntreRios", "Formosa", "Jujuy", "LaPampa",
        "LaRioja", "Mendoza", "Misiones", "Neuquen", "RioNegro",
        "Salta", "SanJuan", "SanLuis", "SantaCruz", "SantaFe",
        "SantiagoDelEstero", "Tucuman",
        "CapitalFederal", "GranBuenosAires", "TierraDelFuego"
    )

    dfs <- lapply(provinces_1985, function(prov) {
        # Read data file
        data_path <- file.path(dir_raw_industrial,
                               sprintf("Economico1985_%s.xlsx", prov))
        stopifnot(file.exists(data_path))
        df <- readxl::read_excel(data_path)
        df <- as.data.frame(df)
        names(df) <- tolower(names(df))

        # Destring numeric columns
        for (v in c("nestab", "npers", "massal", "valprod1", "valprod2")) {
            if (v %in% names(df)) {
                df[[v]][grepl("^-+$", as.character(df[[v]]))] <- NA
                df[[v]] <- as.numeric(df[[v]])
            }
        }

        # Clean provincia, rename distrito to ncodigo
        df$provincia <- gsub(" ", "", df$provincia)
        if ("distrito" %in% names(df)) {
            names(df)[names(df) == "distrito"] <- "ncodigo"
        }
        df <- df[!is.na(df$ncodigo), ]

        # Read code file (maps ncodigo → district name)
        code_path <- file.path(dir_raw_industrial,
                               sprintf("Codigo_%s.xlsx", prov))
        stopifnot(file.exists(code_path))
        codes <- readxl::read_excel(code_path)
        codes <- as.data.frame(codes)
        names(codes) <- tolower(names(codes))
        codes$provincia <- gsub(" ", "", codes$provincia)
        codes <- codes[!is.na(codes$ncodigo), ]

        # Harmonize district column name (some files use "partido")
        if ("partido" %in% names(codes)) {
            names(codes)[names(codes) == "partido"] <- "departamento"
        }
        if ("distrito" %in% names(codes)) {
            names(codes)[names(codes) == "distrito"] <- "departamento"
        }

        # Fix province name inconsistencies in code files
        codes$provincia[codes$provincia == "SantiagodelEstero"] <-
            "SantiagoDelEstero"
        codes$provincia[grepl("TERRITORIONACIONAL", codes$provincia)] <-
            "TierradelFuego"

        # Merge data with codes
        merged <- merge(df, codes[, c("provincia", "ncodigo", "departamento")],
                        by = c("provincia", "ncodigo"), all.x = TRUE)

        # Keep only the columns we need
        keep_cols <- c("provincia", "ncodigo", "departamento",
                       "nestab", "npers", "massal", "valprod1", "valprod2")
        keep_cols <- intersect(keep_cols, names(merged))
        merged[, keep_cols]
    })

    ind85 <- do.call(rbind, dfs)
    ind85$year <- 1985L

    names(ind85)[names(ind85) == "departamento"] <- "distrito"

    # Drop empty rows and unmerged
    ind85 <- ind85[!is.na(ind85$distrito) & ind85$distrito != "", ]

    # Standardize names
    ind85$provincia <- clean_name(ind85$provincia)
    ind85$distrito  <- clean_name(ind85$distrito)

    # Drop Capital Federal
    n_cf <- sum(ind85$provincia == "CAPITALFEDERAL")
    if (n_cf > 0) {
        message(sprintf("[ind]   Dropping %d Capital Federal obs from 1985", n_cf))
        ind85 <- ind85[ind85$provincia != "CAPITALFEDERAL", ]
    }

    # Handle GranBuenosAires duplicates — drop GranBuenosAires entries
    # that duplicate BuenosAires districts
    n_gba <- sum(ind85$provincia == "GRANBUENOSAIRES")
    if (n_gba > 0) {
        message(sprintf("[ind]   Dropping %d GranBuenosAires obs (dupes of BA)", n_gba))
        ind85 <- ind85[ind85$provincia != "GRANBUENOSAIRES", ]
    }

    message(sprintf("[ind]   1985: %d rows, %d provinces",
                    nrow(ind85), length(unique(ind85$provincia))))
    ind85
}

# ---------------------------------------------------------------------------
# Helper: load IPUMS crosswalk
# ---------------------------------------------------------------------------
load_crosswalk <- function() {
    message("\n[ind] Step 3 — Loading IPUMS crosswalk")
    xwalk_path <- file.path(dir_derived_ipums,
                            "ipums_districts_for_merge.parquet")
    stopifnot(file.exists(xwalk_path))
    xwalk <- as.data.frame(arrow::read_parquet(xwalk_path))
    message(sprintf("[ind]   Crosswalk: %d rows, %d unique geolev2",
                    nrow(xwalk), length(unique(xwalk$geolev2))))
    xwalk
}

# ---------------------------------------------------------------------------
# Helper: harmonize and merge 1954
# ---------------------------------------------------------------------------
harmonize_and_merge_1954 <- function(ind, xwalk) {
    message("\n[ind] Step 4 — Harmonizing 1954 names and merging")

    ind$provmerge <- ind$provincia
    ind$distmerge <- ind$distrito
    val_cols <- c("nestab", "nemp", "nobr", "massal", "valprod")

    # --- District splits (before name fixes) ---
    # Camarones → Escalante + Florentino Ameghino (Chubut military zone)
    ind$provmerge[ind$provmerge == "ZONAMILITARDECOMODORORIVADAVIA"] <- "CHUBUT"

    cam_mask <- ind$distmerge == "CAMARONES" & ind$provmerge == "CHUBUT"
    if (any(cam_mask)) {
        cam_row <- ind[cam_mask, ]
        new_row <- cam_row
        new_row$distmerge <- "FLORENTINOAMEGHINO"
        ind$distmerge[cam_mask] <- "ESCALANTE"
        ind <- rbind(ind, new_row)
        # Split equally (old pipeline divides by number of target districts)
        esc_mask <- ind$distmerge == "ESCALANTE" & ind$provmerge == "CHUBUT" &
                    ind$distrito == "CAMARONES"
        fam_mask <- ind$distmerge == "FLORENTINOAMEGHINO" & ind$provmerge == "CHUBUT"
        for (v in val_cols) {
            ind[[v]][esc_mask] <- ind[[v]][esc_mask] / 2
            ind[[v]][fam_mask] <- ind[[v]][fam_mask] / 2
        }
        message("[ind]   Split Camarones → Escalante + Florentino Ameghino")
    }

    # Comodoro Rivadavia → Escalante (Chubut) + Deseado (Santa Cruz)
    cr_mask <- ind$distmerge == "COMODORORIVADAVIA" & ind$provmerge == "CHUBUT"
    if (any(cr_mask)) {
        cr_row <- ind[cr_mask, ]
        new_row <- cr_row
        new_row$distmerge <- "DESEADO"
        new_row$provmerge <- "SANTACRUZ"
        ind$distmerge[cr_mask] <- "ESCALANTE"
        ind <- rbind(ind, new_row)
        esc2_mask <- ind$distmerge == "ESCALANTE" & ind$provmerge == "CHUBUT" &
                     ind$distrito == "COMODORORIVADAVIA"
        des_mask <- ind$distmerge == "DESEADO" & ind$provmerge == "SANTACRUZ" &
                    ind$distrito == "COMODORORIVADAVIA"
        for (v in val_cols) {
            ind[[v]][esc2_mask] <- ind[[v]][esc2_mask] / 2
            ind[[v]][des_mask]  <- ind[[v]][des_mask] / 2
        }
        message("[ind]   Split Comodoro Rivadavia → Escalante + Deseado")
    }

    ind <- apply_name_fixes_1954(ind)
    ind <- apply_district_reassignments_1954(ind, xwalk, val_cols)

    # Quilmes/Berazategui split
    q_mask <- ind$provmerge == "BUENOSAIRES" & ind$distmerge == "QUILMES"
    if (any(q_mask)) {
        q_row <- ind[q_mask, ]
        b_row <- q_row
        b_row$distmerge <- "BERAZATEGUI"
        for (v in val_cols) {
            b_row[[v]] <- q_row[[v]] * (1/3)
            ind[[v]][q_mask] <- q_row[[v]] * (2/3)
        }
        ind <- rbind(ind, b_row)
        message("[ind]   Applied Quilmes/Berazategui 1/3-2/3 split")
    }

    # Merge with crosswalk
    merged <- merge(ind, xwalk[, c("provmerge", "distmerge", "geolev2")],
                    by = c("provmerge", "distmerge"), all.x = TRUE)
    n_matched <- sum(!is.na(merged$geolev2))
    n_unmatched <- sum(is.na(merged$geolev2))
    message(sprintf("[ind]   Matched: %d / %d", n_matched, nrow(ind)))

    if (n_unmatched > 0) {
        unmatched <- merged[is.na(merged$geolev2),
                            c("provmerge", "distmerge")]
        non_excl <- unmatched[!(unmatched$provmerge %in%
                    c("TIERRADELFUEGO", "CAPITALFEDERAL", "CITYOFBUENOSAIRES")), ]
        if (nrow(non_excl) > 0) {
            message("[ind]   WARNING: unmatched districts:")
            print(non_excl)
        }
        merged <- merged[!is.na(merged$geolev2), ]
    }

    # Collapse: equal split for 1-to-many, then sum by geolev2
    # (old pipeline divides by count of target districts per source district)
    merged_count <- ave(rep(1, nrow(merged)),
                        merged$provincia, merged$distrito,
                        FUN = length)
    for (v in val_cols) {
        merged[[v]] <- merged[[v]] / merged_count
    }

    result <- aggregate(
        cbind(nestab, nemp, nobr, massal, valprod) ~ geolev2,
        data = merged, FUN = sum, na.rm = TRUE
    )
    result$year <- 1954L
    message(sprintf("[ind]   After collapse: %d geolev2 districts", nrow(result)))
    result
}

# ---------------------------------------------------------------------------
# Helper: harmonize and merge 1985
# ---------------------------------------------------------------------------
harmonize_and_merge_1985 <- function(ind, xwalk) {
    message("\n[ind] Step 5 — Harmonizing 1985 names and merging")

    ind$provmerge <- ind$provincia
    ind$distmerge <- ind$distrito
    val_cols <- c("nestab", "npers", "massal", "valprod1", "valprod2")

    ind <- apply_name_fixes_1985(ind)

    # Merge
    merged <- merge(ind, xwalk[, c("provmerge", "distmerge", "geolev2")],
                    by = c("provmerge", "distmerge"), all.x = TRUE)
    n_matched <- sum(!is.na(merged$geolev2))
    message(sprintf("[ind]   Matched: %d / %d", n_matched, nrow(ind)))

    n_unmatched <- sum(is.na(merged$geolev2))
    if (n_unmatched > 0) {
        unmatched <- merged[is.na(merged$geolev2),
                            c("provmerge", "distmerge")]
        non_excl <- unmatched[!(unmatched$provmerge %in%
                    c("TIERRADELFUEGO", "CAPITALFEDERAL")), ]
        if (nrow(non_excl) > 0) {
            message("[ind]   WARNING: unmatched districts:")
            print(non_excl)
        }
        merged <- merged[!is.na(merged$geolev2), ]
    }

    # 1985: no 1-to-many splits needed (old pipeline asserts x==1)
    result <- aggregate(
        cbind(nestab, npers, massal, valprod1, valprod2) ~ geolev2,
        data = merged, FUN = sum, na.rm = TRUE
    )
    result$year <- 1985L
    message(sprintf("[ind]   After collapse: %d geolev2 districts", nrow(result)))
    result
}

# ---------------------------------------------------------------------------
# Name fixes for 1954 (ported from merge_in1954_to_IPUMS.do)
# ---------------------------------------------------------------------------
apply_name_fixes_1954 <- function(ind) {
    fix <- function(prov, old, new) {
        mask <- ind$provmerge == prov & ind$distmerge == old
        ind$distmerge[mask] <<- new
    }

    fix("BUENOSAIRES", "BARTOLOMEMITRE", "ARRECIFES")
    fix("BUENOSAIRES", "CNELDEMARINALROSALES", "CORONELDEMARINELROSALES")
    fix("BUENOSAIRES", "GONZALEZCHAVES", "ADOLFOGONZALEZCHAVES")
    fix("BUENOSAIRES", "GENERALMADARIAGA", "GENERALJUANMADARIAGA")
    fix("BUENOSAIRES", "CASEROS", "DAIREAUX")
    fix("BUENOSAIRES", "JUAREZ", "BENITOJUAREZ")
    fix("BUENOSAIRES", "VEINTICINCODEMAYO", "25DEMAYO")
    fix("BUENOSAIRES", "NUEVEJULIO", "9DEJULIO")
    fix("BUENOSAIRES", "LEANDRON", "LEANDRONALEM")
    fix("BUENOSAIRES", "LOMASDE", "LOMASDEZAMORA")
    fix("BUENOSAIRES", "CARMENDEARAUCO", "CARMENDEARECO")
    fix("BUENOSAIRES", "TORQUINST", "TORNQUIST")
    fix("BUENOSAIRES", "CORONELBRANDSEN", "BRANDSEN")

    fix("CATAMARCA", "FRAYMAMERTO", "FRAYMAMERTOESQUIU")

    fix("CHACO", "DOCEDEOCTUBRE", "12DEOCTUBRE")
    fix("CHACO", "PRIMERODEMAYO", "1DEMAYO")
    fix("CHACO", "VEINTICINCODEMAYO", "25DEMAYO")
    fix("CHACO", "NUEVEDEJULIO", "9DEJULIO")
    fix("CHACO", "FRAYJUSTOSANTAMARIADEORO", "FRAYJUSTOSTAMARIADEORO")
    fix("CHACO", "CAPITALGENERALOHIGGINS", "OHIGGINS")
    fix("CHACO", "TAPEGANA", "TAPENAGA")

    # Chubut military zone reassignments already done in harmonize_and_merge_1954
    fix("CHUBUT", "LASHERAS", "DESEADO")
    fix("CHUBUT", "PICOTRUNCADO", "DESEADO")
    ind$provmerge[ind$distmerge == "DESEADO" & ind$provmerge == "CHUBUT"] <- "SANTACRUZ"
    fix("CHUBUT", "ALTORIOSENGUER", "RIOSENGUER")
    ind$provmerge[ind$provmerge == "CHUBUT" &
                  ind$distmerge == "LAGOBUENOSAIRES"] <- "SANTACRUZ"
    fix("CHUBUT", "PASORIOMAYO", "RIOSENGUER")

    fix("JUJUY", "CAPITAL", "DRMANUELBELGRANO")
    fix("LAPAMPA", "LOVENTUEL", "LEVENTUE")

    fix("LARIOJA", "GENERALLAVALLE", "CORONELFELIPEVARELA")
    fix("LARIOJA", "GENERALROCA", "ROSARIOVERAPENALOZA")
    fix("LARIOJA", "GOBERNADORGORDILLO", "CHAMICAL")
    fix("LARIOJA", "SARMIENTO", "VINCHINA")
    fix("LARIOJA", "VELEZSARFIELD", "GENERALANGELVPENALOZA")
    fix("LARIOJA", "SANMARTIN", "GENERALSANMARTIN")

    fix("MENDOZA", "LUJAN", "LUJANDECUYO")
    fix("MISIONES", "FRONTERA", "GENERALMANUELBELGRANO")
    fix("RIONEGRO", "VEINTICINCODEMAYO", "25DEMAYO")

    fix("SALTA", "SANMARTIN", "GENERALJOSEDESANMARTIN")
    fix("SALTA", "GENERALMARTINMDEGUEMES", "GENERALGUEMES")
    fix("SALTA", "CANDELARIA", "LACANDELARIA")
    fix("SALTA", "CALDERA", "LACALDERA")
    fix("SALTA", "ANTE", "ANTA")

    fix("SANJUAN", "VEINTICINCODEMAYO", "25DEMAYO")
    fix("SANJUAN", "IGLESIAS", "IGLESIA")
    fix("SANJUAN", "ULLUN", "ULLUM")

    fix("SANLUIS", "SANMARTIN", "LIBERTADORGENERALSANMARTIN")
    fix("SANLUIS", "GENERALBELGRANO", "BELGRANO")

    fix("SANTAFE", "NUEVEDEJULIO", "9DEJULIO")

    fix("SANTIAGODELESTERO", "MATARA", "JUANFIBARRA")
    fix("SANTIAGODELESTERO", "GENERALATABOADA", "GENERALTABOADA")

    fix("TIERRADELFUEGO", "SANSEBASTIAN", "RIOGRANDE")

    fix("TUCUMAN", "GRANEROS", "GRANERO")
    fix("TUCUMAN", "TAFI", "TAFIDELVALLE")
    fix("TUCUMAN", "BURRUCAYU", "BURRUYACU")

    ind
}

# ---------------------------------------------------------------------------
# District reassignments for 1954 (districts created after 1954)
# These IPUMS districts didn't exist in 1954 — map them back to their
# parent district. Ported from match_districts in merge_in1954_to_IPUMS.do.
# ---------------------------------------------------------------------------
apply_district_reassignments_1954 <- function(ind, xwalk, val_cols) {
    # These are IPUMS districts that were carved out of existing districts
    # after 1954. We need to create entries for them by reassigning from
    # the parent district. The old pipeline does this via a "match" step.
    # Here we add the reassignment mappings to the data before merging.

    reassign <- function(prov, parent, child) {
        # Find the parent row and create a child copy
        mask <- ind$provmerge == prov & ind$distmerge == parent
        if (any(mask)) {
            child_row <- ind[mask, ][1, ]
            child_row$distmerge <- child
            ind <<- rbind(ind, child_row)
        }
    }

    # Buenos Aires: Escobar carved from Pilar + Tigre (1959)
    reassign("BUENOSAIRES", "PILAR", "ESCOBAR")
    # Berisso and Ensenada carved from La Plata (1957)
    reassign("BUENOSAIRES", "LAPLATA", "BERISSO")
    reassign("BUENOSAIRES", "LAPLATA", "ENSENADA")
    # Tres de Febrero carved from General San Martín
    reassign("BUENOSAIRES", "GENERALSANMARTIN", "TRESDEFEBRERO")

    # Formosa: Ramón Lista and Matacos were part of Bermejo
    reassign("FORMOSA", "BERMEJO", "RAMONLISTA")
    reassign("FORMOSA", "BERMEJO", "MATACOS")
    # Bermejo → Patiño (same geolev2 in IPUMS)
    fix_mask <- ind$provmerge == "FORMOSA" & ind$distmerge == "BERMEJO"
    ind$distmerge[fix_mask] <- "PATINO"

    # Misiones: several districts carved from older ones (pre-1954 boundaries)
    reassign("MISIONES", "CANDELARIA", "OBERA")
    reassign("MISIONES", "IGUAZU", "ELDORADO")
    reassign("MISIONES", "CAINGUAS", "LIBERTADORGENERALSANMARTIN")
    reassign("MISIONES", "SANPEDRO", "MONTECARLO")
    reassign("MISIONES", "SANJAVIER", "25DEMAYO")

    ind
}

# ---------------------------------------------------------------------------
# Name fixes for 1985 (ported from merge_ec1985_to_IPUMS.do)
# Very similar to agricultural 1988 fixes — same census publication style.
# ---------------------------------------------------------------------------
apply_name_fixes_1985 <- function(ind) {
    fix <- function(prov, old, new) {
        mask <- ind$provmerge == prov & ind$distmerge == old
        ind$distmerge[mask] <<- new
    }

    fix("BUENOSAIRES", "3DEFEBRERO", "TRESDEFEBRERO")
    fix("BUENOSAIRES", "BARTOLOMEMITRE", "ARRECIFES")
    fix("BUENOSAIRES", "ALTEBROWN", "ALMIRANTEBROWN")
    fix("BUENOSAIRES", "CNELDEMARINALEONARDOROSALES", "CORONELDEMARINELROSALES")
    fix("BUENOSAIRES", "CNELDEMARINALROSALES", "CORONELDEMARINELROSALES")
    fix("BUENOSAIRES", "ADOLFOGONZALESCHAVES", "ADOLFOGONZALEZCHAVES")
    fix("BUENOSAIRES", "GENERALJMADARIAGA", "GENERALJUANMADARIAGA")
    fix("BUENOSAIRES", "GENERALMADARIAGA", "GENERALJUANMADARIAGA")
    fix("BUENOSAIRES", "CASEROS", "DAIREAUX")
    fix("BUENOSAIRES", "JUAREZ", "BENITOJUAREZ")
    fix("BUENOSAIRES", "MATANZA", "LAMATANZA")
    fix("BUENOSAIRES", "VEINTICINCODEMAYO", "25DEMAYO")
    fix("BUENOSAIRES", "NUEVEJULIO", "9DEJULIO")
    fix("BUENOSAIRES", "LEANDRON", "LEANDRONALEM")
    fix("BUENOSAIRES", "LOMASDE", "LOMASDEZAMORA")
    fix("BUENOSAIRES", "CARMENDEARAUCO", "CARMENDEARECO")
    fix("BUENOSAIRES", "TORQUINST", "TORNQUIST")
    fix("BUENOSAIRES", "CORONELBRANDSEN", "BRANDSEN")
    fix("BUENOSAIRES", "MUNICIPIOURBANODELACOSTA", "LACOSTA")
    fix("BUENOSAIRES", "MUNICIPIOURBANODEMONTEHERMOSO", "MONTEHERMOSO")
    fix("BUENOSAIRES", "MUNICIPIOURBANODEPINAMAR", "PINAMAR")
    fix("BUENOSAIRES", "MUNICIPIOURBANODEVILLAGESELL", "VILLAGESELL")
    fix("BUENOSAIRES", "SALIQUELO", "SALLIQUELO")

    fix("CATAMARCA", "FRAYMAMERTO", "FRAYMAMERTOESQUIU")

    fix("CHACO", "DOCEDEOCTUBRE", "12DEOCTUBRE")
    fix("CHACO", "PRIMERODEMAYO", "1DEMAYO")
    fix("CHACO", "VEINTICINCODEMAYO", "25DEMAYO")
    fix("CHACO", "NUEVEDEJULIO", "9DEJULIO")
    fix("CHACO", "FRAYJUSTOSANTAMARIADEORO", "FRAYJUSTOSTAMARIADEORO")
    fix("CHACO", "CAPITANGENERALOHIGGINS", "OHIGGINS")
    fix("CHACO", "CAPITALGENERALOHIGGINS", "OHIGGINS")
    fix("CHACO", "O'HIGGINS", "OHIGGINS")
    fix("CHACO", "TAPEGANA", "TAPENAGA")
    fix("CHACO", "ALTEBROWN", "ALMIRANTEBROWN")
    fix("CHACO", "LIBGENERALSANMARTIN", "LIBERTADORGENERALSANMARTIN")
    fix("CHACO", "CMDTEFERNANDEZ", "COMANDANTEFERNANDEZ")
    fix("CHACO", "PTEDELAPLAZA", "PRESIDENCIADELAPLAZA")

    # Chubut
    ind$provmerge[ind$provmerge == "ZONAMILITARDECOMODORORIVADAVIA"] <- "CHUBUT"
    fix("CHUBUT", "LASHERAS", "DESEADO")
    fix("CHUBUT", "PICOTRUNCADO", "DESEADO")
    ind$provmerge[ind$distmerge == "DESEADO" & ind$provmerge == "CHUBUT"] <- "SANTACRUZ"
    fix("CHUBUT", "PASODEINDIOS", "PASODELOSINDIOS")
    fix("CHUBUT", "ALTORIOSENGUER", "RIOSENGUER")
    fix("CHUBUT", "RIOSENGUERR", "RIOSENGUER")
    ind$provmerge[ind$provmerge == "CHUBUT" &
                  ind$distmerge == "LAGOBUENOSAIRES"] <- "SANTACRUZ"
    fix("CHUBUT", "PASORIOMAYO", "RIOSENGUER")

    fix("CORDOBA", "GRALSANMARTIN", "GENERALSANMARTIN")
    fix("CORDOBA", "PTEROQUESAENZPENA", "PRESIDENTEROQUESAENZPENA")

    fix("ENTRERIOS", "CONCEPCIONDELURUGUAY", "URUGUAY")
    fix("ENTRERIOS", "ROSARIOTALA", "TALA")

    fix("JUJUY", "CAPITAL", "DRMANUELBELGRANO")

    fix("LAPAMPA", "LOVENTUE", "LEVENTUE")
    fix("LAPAMPA", "LOVENTUEL", "LEVENTUE")
    fix("LAPAMPA", "CONHELO", "CONHELLO")

    fix("LARIOJA", "GENERALLAVALLE", "CORONELFELIPEVARELA")
    fix("LARIOJA", "GENERALROCA", "ROSARIOVERAPENALOZA")
    fix("LARIOJA", "GOBERNADORGORDILLO", "CHAMICAL")
    fix("LARIOJA", "GENERALGORDILLO", "CHAMICAL")
    fix("LARIOJA", "GENERALSARMIENTO", "VINCHINA")
    fix("LARIOJA", "SARMIENTO", "VINCHINA")
    fix("LARIOJA", "RIVADAVIA", "GENERALJUANFQUIROGA")
    fix("LARIOJA", "GRALJUANFQUIROGA", "GENERALJUANFQUIROGA")
    fix("LARIOJA", "VELEZSARFIELD", "GENERALANGELVPENALOZA")
    fix("LARIOJA", "GRALANGELVPENALOZA", "GENERALANGELVPENALOZA")
    fix("LARIOJA", "SANMARTIN", "GENERALSANMARTIN")
    fix("LARIOJA", "GRALBELGRANO", "GENERALMANUELBELGRANO")
    fix("LARIOJA", "LIBGRALSANMARTIN", "LIBERTADORGENERALSANMARTIN")

    fix("MENDOZA", "LUJAN", "LUJANDECUYO")

    fix("MISIONES", "FRONTERA", "GENERALMANUELBELGRANO")
    fix("MISIONES", "GRALMBELGRANO", "GENERALMANUELBELGRANO")
    fix("MISIONES", "LIBGRALSANMARTIN", "LIBERTADORGENERALSANMARTIN")

    fix("NEUQUEN", "PEHENCHES", "PEHUENCHES")

    fix("RIONEGRO", "GENERALCONESA", "CONESA")
    fix("RIONEGRO", "VEINTICINCODEMAYO", "25DEMAYO")

    fix("SALTA", "SANMARTIN", "GENERALJOSEDESANMARTIN")
    fix("SALTA", "GRALJDESANMARTIN", "GENERALJOSEDESANMARTIN")
    fix("SALTA", "GENERALMARTINMIGUELDEGUEMES", "GENERALGUEMES")
    fix("SALTA", "GENERALMARTINMDEGUEMES", "GENERALGUEMES")
    fix("SALTA", "CANDELARIA", "LACANDELARIA")
    fix("SALTA", "CALDERA", "LACALDERA")
    fix("SALTA", "ANTE", "ANTA")
    fix("SALTA", "LACAPITAL", "CAPITAL")
    fix("SALTA", "ROSARIODELEPMA", "ROSARIODELERMA")
    fix("SALTA", "GDORDUPUY", "GOBERNADORDUPUY")
    fix("SALTA", "LIBGRALSANMARTIN", "LIBERTADORGENERALSANMARTIN")

    fix("SANJUAN", "VEINTICINCODEMAYO", "25DEMAYO")
    fix("SANJUAN", "NUEVEDEJULIO", "9DEJULIO")
    fix("SANJUAN", "IGLESIAS", "IGLESIA")
    fix("SANJUAN", "ULLUN", "ULLUM")

    fix("SANLUIS", "SANMARTIN", "LIBERTADORGENERALSANMARTIN")
    fix("SANLUIS", "LIBGRALSANMARTIN", "LIBERTADORGENERALSANMARTIN")
    fix("SANLUIS", "PRINGLES", "CORONELPRINGLES")
    fix("SANLUIS", "GOBERNADORVICENTEDUPUY", "GOBERNADORDUPUY")
    fix("SANLUIS", "GDORDUPUY", "GOBERNADORDUPUY")
    fix("SANLUIS", "GENERALBELGRANO", "BELGRANO")

    fix("SANTAFE", "NUEVEDEJULIO", "9DEJULIO")

    fix("SANTIAGODELESTERO", "QUEBRACHO", "QUEBRACHOS")
    fix("SANTIAGODELESTERO", "MATARA", "JUANFIBARRA")
    fix("SANTIAGODELESTERO", "BRIGJFELIPEIBARRA", "JUANFIBARRA")
    fix("SANTIAGODELESTERO", "GENERALATABOADA", "GENERALTABOADA")

    fix("TIERRADELFUEGO", "SANSEBASTIAN", "RIOGRANDE")

    fix("TUCUMAN", "GRANEROS", "GRANERO")
    fix("TUCUMAN", "TAFI", "TAFIDELVALLE")
    fix("TUCUMAN", "BURRUCAYU", "BURRUYACU")
    fix("TUCUMAN", "JUANBALBERDI", "JUANBAUTISTAALBERDI")
    fix("TUCUMAN", "TAFIVIEJO", "TAFIVIAJO")
    fix("TUCUMAN", "SANMIGUELDETUCUMAN", "CAPITAL")

    ind
}

# ---------------------------------------------------------------------------
# Stack, validate, save
# ---------------------------------------------------------------------------
stack_validate_save <- function(ind54, ind85) {
    message("\n[ind] Step 6 — Stacking, validating, saving")

    # Harmonize column names: 1954 has nemp+nobr, 1985 has npers
    # Keep both; downstream panel build can decide which to use
    if (!"npers" %in% names(ind54)) ind54$npers <- NA
    if (!"valprod1" %in% names(ind54)) ind54$valprod1 <- ind54$valprod
    if (!"valprod2" %in% names(ind54)) ind54$valprod2 <- NA
    if (!"nemp" %in% names(ind85)) ind85$nemp <- NA
    if (!"nobr" %in% names(ind85)) ind85$nobr <- NA
    if (!"valprod" %in% names(ind85)) ind85$valprod <- ind85$valprod1

    common_cols <- c("geolev2", "year", "nestab", "nemp", "nobr",
                     "npers", "massal", "valprod", "valprod1", "valprod2")
    panel <- rbind(ind54[, common_cols], ind85[, common_cols])
    panel <- panel[order(panel$geolev2, panel$year), ]

    # Validate
    key <- paste(panel$geolev2, panel$year)
    stopifnot(sum(duplicated(key)) == 0)
    message("[ind]   Key (geolev2 + year) is unique: OK")
    stopifnot(!any(is.na(panel$geolev2)))
    stopifnot(!any(is.na(panel$year)))

    n54 <- length(unique(panel$geolev2[panel$year == 1954]))
    n85 <- length(unique(panel$geolev2[panel$year == 1985]))
    message(sprintf("[ind]   Districts: %d in 1954, %d in 1985", n54, n85))
    message(sprintf("[ind]   Total obs: %d", nrow(panel)))

    # Save
    out_path <- file.path(dir_derived_ind, "ind_census.parquet")
    arrow::write_parquet(panel, out_path)
    message(sprintf("[ind]   Saved: %s", out_path))

    # Manifest
    log_path <- file.path(dir_derived_ind, "data_file_manifest.log")
    sink(log_path)
    on.exit(sink(), add = TRUE)
    cat("Data file manifest — clean_industrial.R\n")
    cat(sprintf("Generated: %s\n", Sys.time()))
    cat(strrep("=", 60), "\n")
    cat(sprintf("Rows: %d\nColumns: %d\nKey: geolev2 + year\n",
                nrow(panel), ncol(panel)))
    cat(sprintf("Districts 1954: %d\nDistricts 1985: %d\n", n54, n85))
    cat("\nSummary by year:\n")
    for (y in c(1954, 1985)) {
        sub <- panel[panel$year == y, ]
        cat(sprintf("\n  Year %d (N=%d):\n", y, nrow(sub)))
        for (v in c("nestab", "nemp", "npers", "massal", "valprod")) {
            vals <- sub[[v]]
            if (!all(is.na(vals))) {
                cat(sprintf("    %-12s mean=%.1f sd=%.1f min=%.1f max=%.1f NA=%d\n",
                            v, mean(vals, na.rm = TRUE), sd(vals, na.rm = TRUE),
                            min(vals, na.rm = TRUE), max(vals, na.rm = TRUE),
                            sum(is.na(vals))))
            }
        }
    }
    message(sprintf("[ind]   Manifest written: %s", log_path))
}

# ---------------------------------------------------------------------------
# Helper: clean name string
# ---------------------------------------------------------------------------
clean_name <- function(x) {
    x <- toupper(x)
    x <- gsub(" ", "", x)
    x <- gsub("-", "", x)
    x <- gsub("\\.", "", x)
    x <- gsub("\u00e1", "A", x)
    x <- gsub("\u00e9", "E", x)
    x <- gsub("\u00ed", "I", x)
    x <- gsub("\u00f3", "O", x)
    x <- gsub("\u00fa", "U", x)
    x <- gsub("\u00f1", "N", x)
    x <- gsub("\u00fc", "U", x)
    x
}

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
main()

# ===========================================================================
# clean_agricultural.R
#
# PURPOSE: Clean agricultural census data (1960, 1988) and harmonize to
#          IPUMS geolev2 district codes for the estimation panel.
#
# READS:
#   data/raw/agricultural/Agropecuario1960_*.xlsx  (22 files, one per province)
#   data/raw/agricultural/Agricola1988_*.xlsx      (23 files, one per province)
#   data/derived/base/ipums/ipums_districts_for_merge.parquet  (crosswalk)
#
# PRODUCES:
#   data/derived/base/agricultural/agr_census.parquet
#       Key: geolev2 + year. Variables: nexp, areatot_ha.
#   data/derived/base/agricultural/data_file_manifest.log
#
# REFERENCE:
#   Old data/Train/base/agr_census_1960/code/clean_agro1960.do
#   Old data/Train/base/agr_census_1988/code/clean_agro1988.do
#   Old data/Train/derived/agr_census_1960/code/merge_ag1960_to_IPUMS.do
#   Old data/Train/derived/agr_census_1988/code/merge_ag1988_to_IPUMS.do
#
# NOTES:
#   - 1960 data is wide format: provincia, distrito, nexp, areatot_ha
#   - 1988 data is long format: provincia, distrito, unidad, valor
#     (needs forward-fill on provincia/distrito, then reshape wide)
#   - District names require extensive harmonization to match IPUMS codes.
#     Name mappings ported from the old Stata merge scripts.
#   - Quilmes/Berazategui split (1960): one census district maps to two
#     IPUMS districts. Allocated 1/3 to Berazategui, 2/3 to Quilmes
#     (approximate population proportions). See old merge script.
#   - Capital Federal dropped from 1988 (not in IPUMS geolev2 system).
#   - "INDETERMINADO" entries in 1988 Buenos Aires dropped (10 farms, ~13k ha).
# ===========================================================================

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"))

    message("\n", strrep("=", 72))
    message("clean_agricultural.R  |  Agricultural census → geolev2 panel")
    message(strrep("=", 72))

    # --- 1. Read and clean 1960 ---------------------------------------------
    ag60 <- read_and_clean_1960()

    # --- 2. Read and clean 1988 ---------------------------------------------
    ag88 <- read_and_clean_1988()

    # --- 3. Load crosswalk --------------------------------------------------
    xwalk <- load_crosswalk()

    # --- 4. Harmonize and merge 1960 ----------------------------------------
    ag60_merged <- harmonize_and_merge(ag60, xwalk, year = 1960L)

    # --- 5. Harmonize and merge 1988 ----------------------------------------
    ag88_merged <- harmonize_and_merge(ag88, xwalk, year = 1988L)

    # --- 6. Stack, validate, save -------------------------------------------
    panel <- stack_validate_save(ag60_merged, ag88_merged)

    message(strrep("=", 72))
    message("clean_agricultural.R  |  Complete.")
    message(strrep("=", 72), "\n")
}

# ---------------------------------------------------------------------------
# Helper: read and clean 1960 agricultural census
# ---------------------------------------------------------------------------
read_and_clean_1960 <- function() {
    message("\n[agr] Step 1 — Reading 1960 agricultural census")

    provinces_1960 <- c(
        "BuenosAires", "Catamarca", "Chaco", "Chubut", "Cordoba",
        "Corrientes", "EntreRios", "Formosa", "Jujuy", "LaPampa",
        "LaRioja", "Mendoza", "Misiones", "Neuquen", "RioNegro",
        "Salta", "SanJuan", "SanLuis", "SantaCruz", "SantaFe",
        "SantiagoDelEstero", "Tucuman"
    )

    dfs <- lapply(provinces_1960, function(prov) {
        path <- file.path(dir_raw_agricultural,
                          sprintf("Agropecuario1960_%s.xlsx", prov))
        stopifnot(file.exists(path))
        df <- readxl::read_excel(path)
        df <- as.data.frame(df)
        # Replace "-" with NA and convert to numeric
        for (v in c("nexp", "areatot_ha")) {
            if (v %in% names(df)) {
                df[[v]][df[[v]] == "-"] <- NA
                df[[v]] <- as.numeric(df[[v]])
            }
        }
        df
    })

    ag60 <- do.call(rbind, dfs)
    names(ag60) <- tolower(names(ag60))
    ag60$year <- 1960L

    # Drop empty rows (all NA)
    ag60 <- ag60[!(is.na(ag60$provincia) & is.na(ag60$distrito) &
                   is.na(ag60$nexp) & is.na(ag60$areatot_ha)), ]

    # Drop column "e" if present (artifact in some files)
    if ("e" %in% names(ag60)) ag60$e <- NULL

    # Standardize name strings: uppercase, remove spaces/punctuation
    ag60$provincia <- clean_name(ag60$provincia)
    ag60$distrito  <- clean_name(ag60$distrito)

    message(sprintf("[agr]   1960: %d rows, %d provinces",
                    nrow(ag60), length(unique(ag60$provincia))))
    ag60
}

# ---------------------------------------------------------------------------
# Helper: read and clean 1988 agricultural census
# ---------------------------------------------------------------------------
read_and_clean_1988 <- function() {
    message("\n[agr] Step 2 — Reading 1988 agricultural census")

    provinces_1988 <- c(
        "BuenosAires", "Catamarca", "Chaco", "Chubut", "Cordoba",
        "Corrientes", "EntreRios", "Formosa", "Jujuy", "LaPampa",
        "LaRioja", "Mendoza", "Misiones", "Neuquen", "RioNegro",
        "Salta", "SanJuan", "SanLuis", "SantaCruz", "SantaFe",
        "SantiagoDelEstero", "TierraDelFuego", "Tucuman"
    )

    dfs <- lapply(provinces_1988, function(prov) {
        path <- file.path(dir_raw_agricultural,
                          sprintf("Agricola1988_%s.xlsx", prov))
        stopifnot(file.exists(path))
        df <- readxl::read_excel(path)
        df <- as.data.frame(df)
        names(df) <- tolower(names(df))

        # Forward-fill provincia and distrito (long format: only first row has them)
        for (i in seq_len(nrow(df))) {
            if (is.na(df$provincia[i]) || df$provincia[i] == "") {
                df$provincia[i] <- df$provincia[i - 1]
            }
            if (is.na(df$distrito[i]) || df$distrito[i] == "") {
                df$distrito[i] <- df$distrito[i - 1]
            }
        }
        df
    })

    ag88 <- do.call(rbind, dfs)
    ag88$year <- 1988L

    # Reshape from long (unidad, valor) to wide (nexp, areatot_ha)
    # unidad values: "EAPs" = number of farms, "ha" = total area
    ag88$provincia <- clean_name(ag88$provincia)
    ag88$distrito  <- clean_name(ag88$distrito)
    ag88$unidad    <- trimws(ag88$unidad)

    # Create district ID for reshape
    ag88$id <- paste(ag88$provincia, ag88$distrito, sep = "_")
    # Some valor entries are non-numeric (e.g., "s" for suppressed) — coerce to NA
    ag88$valor <- suppressWarnings(as.numeric(ag88$valor))

    # Pivot: one row per district with nexp and areatot_ha columns
    wide <- reshape(
        ag88[, c("id", "provincia", "distrito", "unidad", "valor", "year")],
        idvar = c("id", "provincia", "distrito", "year"),
        timevar = "unidad",
        v.names = "valor",
        direction = "wide"
    )

    # Rename reshaped columns
    eap_col <- grep("valor\\.EAP", names(wide), value = TRUE)
    ha_col  <- grep("valor\\.ha",  names(wide), value = TRUE)
    if (length(eap_col) == 1) names(wide)[names(wide) == eap_col] <- "nexp"
    if (length(ha_col) == 1)  names(wide)[names(wide) == ha_col]  <- "areatot_ha"

    # Drop reshape artifacts
    wide$id <- NULL
    drop_cols <- grep("^valor\\.", names(wide), value = TRUE)
    if (length(drop_cols) > 0) wide[, drop_cols] <- NULL

    # Drop Capital Federal (not in IPUMS geolev2 system)
    n_cf <- sum(wide$provincia == "CAPITALFEDERAL")
    if (n_cf > 0) {
        message(sprintf("[agr]   Dropping %d Capital Federal obs from 1988", n_cf))
        wide <- wide[wide$provincia != "CAPITALFEDERAL", ]
    }

    # Drop INDETERMINADO (unassigned farms in Buenos Aires)
    n_indet <- sum(wide$distrito == "INDETERMINADO", na.rm = TRUE)
    if (n_indet > 0) {
        message(sprintf("[agr]   Dropping %d INDETERMINADO obs from 1988", n_indet))
        wide <- wide[wide$distrito != "INDETERMINADO", ]
    }

    message(sprintf("[agr]   1988: %d rows, %d provinces",
                    nrow(wide), length(unique(wide$provincia))))
    wide
}

# ---------------------------------------------------------------------------
# Helper: load IPUMS crosswalk
# ---------------------------------------------------------------------------
load_crosswalk <- function() {
    message("\n[agr] Step 3 — Loading IPUMS crosswalk")
    xwalk_path <- file.path(dir_derived_ipums,
                            "ipums_districts_for_merge.parquet")
    stopifnot(file.exists(xwalk_path))
    xwalk <- arrow::read_parquet(xwalk_path)
    xwalk <- as.data.frame(xwalk)
    message(sprintf("[agr]   Crosswalk: %d rows, %d unique geolev2",
                    nrow(xwalk), length(unique(xwalk$geolev2))))
    xwalk
}

# Helper to get all 312 geolev2 codes (for missing district reporting)
xwalk_geolev2 <- function() {
    xwalk_path <- file.path(dir_derived_ipums,
                            "ipums_districts_for_merge.parquet")
    xwalk <- as.data.frame(arrow::read_parquet(xwalk_path))
    unique(xwalk$geolev2)
}

# ---------------------------------------------------------------------------
# Helper: harmonize district names and merge to IPUMS geolev2
# ---------------------------------------------------------------------------
harmonize_and_merge <- function(ag, xwalk, year) {
    message(sprintf("\n[agr] Step %s — Harmonizing %d names and merging",
                    ifelse(year == 1960, "4", "5"), year))

    ag$provmerge <- ag$provincia
    ag$distmerge <- ag$distrito

    # Apply name corrections (ported from old Stata merge scripts)
    if (year == 1960L) {
        ag <- apply_name_fixes_1960(ag)
    } else {
        ag <- apply_name_fixes_1988(ag)
    }

    # Handle Quilmes/Berazategui split (1960 only)
    # Census district QUILMES maps to two IPUMS districts:
    #   geolev2=32006087 (Berazategui) — gets 1/3
    #   geolev2=32006076 (Quilmes)     — gets 2/3
    # Berazategui was carved out of Quilmes in 1960.
    # Proportions approximate population shares (from old pipeline).
    if (year == 1960L) {
        quilmes_mask <- ag$provmerge == "BUENOSAIRES" &
                        ag$distmerge == "QUILMES"
        if (any(quilmes_mask)) {
            quilmes_row <- ag[quilmes_mask, ]
            # Create Berazategui row with 1/3 of values
            beraz_row <- quilmes_row
            beraz_row$distmerge <- "BERAZATEGUI"
            beraz_row$nexp       <- quilmes_row$nexp * (1/3)
            beraz_row$areatot_ha <- quilmes_row$areatot_ha * (1/3)
            # Reduce Quilmes to 2/3
            ag$nexp[quilmes_mask]       <- quilmes_row$nexp * (2/3)
            ag$areatot_ha[quilmes_mask] <- quilmes_row$areatot_ha * (2/3)
            # Append Berazategui
            ag <- rbind(ag, beraz_row)
            message("[agr]   Applied Quilmes/Berazategui 1/3-2/3 split")
        }
    }

    # Merge with crosswalk
    n_before <- nrow(ag)
    merged <- merge(ag, xwalk[, c("provmerge", "distmerge", "geolev2")],
                    by = c("provmerge", "distmerge"), all.x = TRUE)

    n_matched   <- sum(!is.na(merged$geolev2))
    n_unmatched <- sum(is.na(merged$geolev2))
    message(sprintf("[agr]   Matched: %d / %d", n_matched, n_before))

    if (n_unmatched > 0) {
        unmatched <- merged[is.na(merged$geolev2),
                            c("provmerge", "distmerge")]
        # Tierra del Fuego is expected to be unmatched in some cases
        non_tdf <- unmatched[unmatched$provmerge != "TIERRADELFUEGO", ]
        if (nrow(non_tdf) > 0) {
            message(sprintf("[agr]   WARNING: %d unmatched non-TDF districts:",
                            nrow(non_tdf)))
            print(non_tdf)
        }
        # Drop unmatched (Tierra del Fuego + any remaining)
        message(sprintf("[agr]   Dropping %d unmatched obs", n_unmatched))
        merged <- merged[!is.na(merged$geolev2), ]
    }

    # Collapse to geolev2 level (some census districts merge into one IPUMS district)
    result <- aggregate(
        cbind(nexp, areatot_ha) ~ geolev2,
        data = merged,
        FUN = sum,
        na.rm = TRUE
    )
    result$year <- year

    message(sprintf("[agr]   After collapse: %d geolev2 districts", nrow(result)))
    result
}

# ---------------------------------------------------------------------------
# Helper: district name fixes for 1960
# Ported from: Old data/Train/derived/agr_census_1960/code/merge_ag1960_to_IPUMS.do
# ---------------------------------------------------------------------------
apply_name_fixes_1960 <- function(ag) {
    fix <- function(prov, old, new) {
        mask <- ag$provmerge == prov & ag$distmerge == old
        ag$distmerge[mask] <<- new
    }

    # BUENOSAIRES
    fix("BUENOSAIRES", "BARTOLOMEMITRE", "ARRECIFES")
    # Arrecifes was renamed from Bartolomé Mitre
    fix("BUENOSAIRES", "CNELDEMARINALEONARDOROSALES",
        "CORONELDEMARINELROSALES")
    fix("BUENOSAIRES", "GONZALEZCHAVES", "ADOLFOGONZALEZCHAVES")
    fix("BUENOSAIRES", "CASEROS", "DAIREAUX")
    # Daireaux was renamed from Caseros
    fix("BUENOSAIRES", "JUAREZ", "BENITOJUAREZ")
    fix("BUENOSAIRES", "MATANZA", "LAMATANZA")

    # CHACO
    fix("CHACO", "FRAYJUSTOSANTAMARIADEORO", "FRAYJUSTOSTAMARIADEORO")
    fix("CHACO", "CAPITANGENERALOHIGGINS", "OHIGGINS")

    # CHUBUT
    fix("CHUBUT", "PASODEINDIOS", "PASODELOSINDIOS")
    fix("CHUBUT", "RIOSENGUERR", "RIOSENGUER")

    # ENTRERIOS
    fix("ENTRERIOS", "CONCEPCIONDELURUGUAY", "URUGUAY")
    fix("ENTRERIOS", "ROSARIOTALA", "TALA")

    # JUJUY — capital is in Dr. Manuel Belgrano district
    fix("JUJUY", "CAPITAL", "DRMANUELBELGRANO")

    # LARIOJA — extensive name changes
    fix("LARIOJA", "GENERALLAVALLE", "CORONELFELIPEVARELA")
    fix("LARIOJA", "GENERALROCA", "ROSARIOVERAPENALOZA")
    fix("LARIOJA", "GOBERNADORGORDILLO", "CHAMICAL")
    fix("LARIOJA", "GENERALSARMIENTO", "VINCHINA")
    fix("LARIOJA", "RIVADAVIA", "GENERALJUANFQUIROGA")
    fix("LARIOJA", "VELEZSARSFIELD", "GENERALANGELVPENALOZA")

    # MENDOZA
    fix("MENDOZA", "LUJAN", "LUJANDECUYO")

    # RIONEGRO
    fix("RIONEGRO", "GENERALCONESA", "CONESA")

    # SALTA
    fix("SALTA", "GENERALJDESANMARTIN", "GENERALJOSEDESANMARTIN")
    fix("SALTA", "GENERALMARTINMIGUELDEGUEMES", "GENERALGUEMES")
    fix("SALTA", "CANDELARIA", "LACANDELARIA")
    fix("SALTA", "CALDERA", "LACALDERA")

    # SANJUAN
    fix("SANJUAN", "ULLUN", "ULLUM")

    # SANLUIS
    fix("SANLUIS", "SANMARTIN", "LIBERTADORGENERALSANMARTIN")
    fix("SANLUIS", "PRINGLES", "CORONELPRINGLES")
    fix("SANLUIS", "GOBERNADORVICENTEDUPUY", "GOBERNADORDUPUY")

    # SANTIAGODELESTERO
    fix("SANTIAGODELESTERO", "QUEBRACHO", "QUEBRACHOS")
    fix("SANTIAGODELESTERO", "MATARA", "JUANFIBARRA")
    # Juan Felipe Ibarra was renamed from Matará

    # TIERRADELFUEGO
    fix("TIERRADELFUEGO", "SANSEBASTIAN", "RIOGRANDE")
    # Río Grande was renamed from San Sebastián

    # TUCUMAN
    fix("TUCUMAN", "GRANEROS", "GRANERO")
    fix("TUCUMAN", "TAFI", "TAFIDELVALLE")
    # Tafí del Valle was segregated from the old Tafí department

    ag
}

# ---------------------------------------------------------------------------
# Helper: district name fixes for 1988
# Ported from: Old data/Train/derived/agr_census_1988/code/merge_ag1988_to_IPUMS.do
# The 1988 census has many more abbreviations and typos than 1960.
# ---------------------------------------------------------------------------
apply_name_fixes_1988 <- function(ag) {
    fix <- function(prov, old, new) {
        mask <- ag$provmerge == prov & ag$distmerge == old
        ag$distmerge[mask] <<- new
    }

    # BUENOSAIRES — many abbreviations in the 1988 publication
    fix("BUENOSAIRES", "3DEFEBRERO", "TRESDEFEBRERO")
    fix("BUENOSAIRES", "BARTOLOMEMITRE", "ARRECIFES")
    fix("BUENOSAIRES", "BMEMITRE", "ARRECIFES")
    fix("BUENOSAIRES", "ALTEBROWN", "ALMIRANTEBROWN")
    fix("BUENOSAIRES", "CNELDEMARINALEONARDOROSALES",
        "CORONELDEMARINELROSALES")
    fix("BUENOSAIRES", "CNELDEMARINALROSALES", "CORONELDEMARINELROSALES")
    fix("BUENOSAIRES", "ADOLFOGONZALESCHAVES", "ADOLFOGONZALEZCHAVES")
    fix("BUENOSAIRES", "ADOLFOGCHAVES", "ADOLFOGONZALEZCHAVES")
    fix("BUENOSAIRES", "GENERALJMADARIAGA", "GENERALJUANMADARIAGA")
    fix("BUENOSAIRES", "GENERALMADARIAGA", "GENERALJUANMADARIAGA")
    fix("BUENOSAIRES", "GRALMADARIAGA", "GENERALJUANMADARIAGA")
    fix("BUENOSAIRES", "CASEROS", "DAIREAUX")
    fix("BUENOSAIRES", "JUAREZ", "BENITOJUAREZ")
    fix("BUENOSAIRES", "MATANZA", "LAMATANZA")
    fix("BUENOSAIRES", "VEINTICINCODEMAYO", "25DEMAYO")
    fix("BUENOSAIRES", "NUEVEJULIO", "9DEJULIO")
    fix("BUENOSAIRES", "LEANDRON", "LEANDRONALEM")
    fix("BUENOSAIRES", "LNALEM", "LEANDRONALEM")
    fix("BUENOSAIRES", "LOMASDE", "LOMASDEZAMORA")
    fix("BUENOSAIRES", "CARMENDEARAUCO", "CARMENDEARECO")
    fix("BUENOSAIRES", "TORQUINST", "TORNQUIST")
    fix("BUENOSAIRES", "CORONELBRANDSEN", "BRANDSEN")
    fix("BUENOSAIRES", "MUNICIPIOURBANODELACOSTA", "LACOSTA")
    fix("BUENOSAIRES", "MUNICIPIOURBANODEMONTEHERMOSO", "MONTEHERMOSO")
    fix("BUENOSAIRES", "MUNICIPIOURBANODEPINAMAR", "PINAMAR")
    fix("BUENOSAIRES", "MUNICIPIOURBANODEVILLAGESELL", "VILLAGESELL")
    fix("BUENOSAIRES", "SALIQUELO", "SALLIQUELO")
    fix("BUENOSAIRES", "CAPSARMIENTO", "CAPITANSARMIENTO")
    fix("BUENOSAIRES", "CNELDORREGO", "CORONELDORREGO")
    fix("BUENOSAIRES", "CNELPRINGLES", "CORONELPRINGLES")
    fix("BUENOSAIRES", "CNELSUAREZ", "CORONELSUAREZ")
    fix("BUENOSAIRES", "CNELROSALES", "CORONELROSALES")
    fix("BUENOSAIRES", "CORONELROSALES", "CORONELDEMARINELROSALES")
    fix("BUENOSAIRES", "EDELACRUZ", "EXALTACIONDELACRUZ")
    fix("BUENOSAIRES", "GRALALVARADO", "GENERALALVARADO")
    fix("BUENOSAIRES", "GRALALVEAR", "GENERALALVEAR")
    fix("BUENOSAIRES", "GRALARENALES", "GENERALARENALES")
    fix("BUENOSAIRES", "GRALBELGRANO", "GENERALBELGRANO")
    fix("BUENOSAIRES", "GRALPAZ", "GENERALPAZ")
    fix("BUENOSAIRES", "GRALPUEYRREDON", "GENERALPUEYRREDON")
    fix("BUENOSAIRES", "GRALRODRIGUEZ", "GENERALRODRIGUEZ")
    fix("BUENOSAIRES", "GRALSANMARTIN", "GENERALSANMARTIN")
    fix("BUENOSAIRES", "GRALSARMIENTO", "GENERALSARMIENTO")
    fix("BUENOSAIRES", "GRALVILLEGAS", "GENERALVILLEGAS")
    fix("BUENOSAIRES", "SADEARECO", "SANANTONIODEARECO")
    fix("BUENOSAIRES", "SANDRESDEGILES", "SANANDRESDEGILES")
    fix("BUENOSAIRES", "GRALLAMADRID", "GENERALLAMADRID")
    fix("BUENOSAIRES", "GRALLASHERAS", "GENERALLASHERAS")
    fix("BUENOSAIRES", "GRALLAVALLE", "GENERALLAVALLE")
    fix("BUENOSAIRES", "GRALPINTO", "GENERALPINTO")
    fix("BUENOSAIRES", "GRALGUIDO", "GENERALGUIDO")
    fix("BUENOSAIRES", "GRALVIAMONTE", "GENERALVIAMONTE")

    # CATAMARCA
    fix("CATAMARCA", "FRAYMAMERTO", "FRAYMAMERTOESQUIU")

    # CHACO
    fix("CHACO", "DOCEDEOCTUBRE", "12DEOCTUBRE")
    fix("CHACO", "PRIMERODEMAYO", "1DEMAYO")
    fix("CHACO", "1\u00b0DEMAYO", "1DEMAYO")
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

    # CHUBUT — includes Zona Militar de Comodoro Rivadavia reassignments
    # Some districts in the old military zone now belong to Santa Cruz
    ag$provmerge[ag$provmerge == "ZONAMILITARDECOMODORORIVADAVIA"] <- "CHUBUT"
    fix("CHUBUT", "MARTINES", "MARTIRES")
    fix("CHUBUT", "LASHERAS", "DESEADO")
    fix("CHUBUT", "PICOTRUNCADO", "DESEADO")
    # Las Heras and Pico Truncado (ex-military zone) → Deseado, Santa Cruz
    ag$provmerge[ag$distmerge == "DESEADO" & ag$provmerge == "CHUBUT"] <- "SANTACRUZ"
    fix("CHUBUT", "PASODEINDIOS", "PASODELOSINDIOS")
    fix("CHUBUT", "ALTORIOSENGUER", "RIOSENGUER")
    fix("CHUBUT", "RIOSENGUERR", "RIOSENGUER")
    # Lago Buenos Aires moved from Chubut to Santa Cruz in 1955
    ag$provmerge[ag$provmerge == "CHUBUT" &
                 ag$distmerge == "LAGOBUENOSAIRES"] <- "SANTACRUZ"
    fix("CHUBUT", "PASORIOMAYO", "RIOSENGUER")
    # Paso Río Mayo occupied territory of current Río Senguer

    # CORDOBA
    fix("CORDOBA", "GRALSANMARTIN", "GENERALSANMARTIN")
    fix("CORDOBA", "PTEROQUESAENZPENA", "PRESIDENTEROQUESAENZPENA")
    fix("CORDOBA", "ISCHIL\u00cdN", "ISCHILIN")

    # ENTRERIOS
    fix("ENTRERIOS", "CONCEPCIONDELURUGUAY", "URUGUAY")
    fix("ENTRERIOS", "ROSARIOTALA", "TALA")

    # JUJUY
    fix("JUJUY", "CAPITAL", "DRMANUELBELGRANO")
    fix("JUJUY", "GRALMANUELBELGRANO", "DRMANUELBELGRANO")

    # LAPAMPA
    fix("LAPAMPA", "LOVENTUE", "LEVENTUE")
    fix("LAPAMPA", "LOVENTUEL", "LEVENTUE")
    fix("LAPAMPA", "CONHELO", "CONHELLO")

    # LARIOJA — extensive name changes (same as 1960 plus more abbreviations)
    fix("LARIOJA", "GENERALLAVALLE", "CORONELFELIPEVARELA")
    fix("LARIOJA", "CNELFVARELA", "CORONELFELIPEVARELA")
    fix("LARIOJA", "GENERALROCA", "ROSARIOVERAPENALOZA")
    fix("LARIOJA", "RVPENALOZA", "ROSARIOVERAPENALOZA")
    fix("LARIOJA", "GOBERNADORGORDILLO", "CHAMICAL")
    fix("LARIOJA", "GENERALGORDILLO", "CHAMICAL")
    fix("LARIOJA", "GENERALSARMIENTO", "VINCHINA")
    fix("LARIOJA", "SARMIENTO", "VINCHINA")
    fix("LARIOJA", "GRALSARMIENTO", "VINCHINA")
    fix("LARIOJA", "RIVADAVIA", "GENERALJUANFQUIROGA")
    fix("LARIOJA", "GRALJUANFQUIROGA", "GENERALJUANFQUIROGA")
    fix("LARIOJA", "GRALJFQUIROGA", "GENERALJUANFQUIROGA")
    fix("LARIOJA", "VELEZSARFIELD", "GENERALANGELVPENALOZA")
    fix("LARIOJA", "GRALANGELVPENALOZA", "GENERALANGELVPENALOZA")
    fix("LARIOJA", "GRALAVPENALOZA", "GENERALANGELVPENALOZA")
    fix("LARIOJA", "SANMARTIN", "GENERALSANMARTIN")
    fix("LARIOJA", "GRALSANMARTIN", "GENERALSANMARTIN")
    fix("LARIOJA", "LIBGRALSANMARTIN", "LIBERTADORGENERALSANMARTIN")
    fix("LARIOJA", "GRALBELGRANO", "GENERALBELGRANO")
    fix("LARIOJA", "GRALLAMADRID", "GENERALLAMADRID")
    fix("LARIOJA", "GRALOCAMPO", "GENERALOCAMPO")

    # MENDOZA
    fix("MENDOZA", "LUJAN", "LUJANDECUYO")

    # MISIONES
    fix("MISIONES", "FRONTERA", "GENERALMANUELBELGRANO")
    fix("MISIONES", "GRALMBELGRANO", "GENERALMANUELBELGRANO")
    # Belgrano's capital is Irigoyen, which was part of Frontera in 1947
    fix("MISIONES", "LEANDONALEM", "LEANDRONALEM")
    fix("MISIONES", "LIBGRALSANMARTIN", "LIBERTADORGENERALSANMARTIN")

    # NEUQUEN
    fix("NEUQUEN", "PEHENCHES", "PEHUENCHES")

    # RIONEGRO
    fix("RIONEGRO", "GENERALCONESA", "CONESA")
    fix("RIONEGRO", "VEINTICINCODEMAYO", "25DEMAYO")
    fix("RIONEGRO", "GRALROCA", "GENERALROCA")

    # SALTA
    fix("SALTA", "SANMARTIN", "GENERALJOSEDESANMARTIN")
    fix("SALTA", "GRALJDESANMARTIN", "GENERALJOSEDESANMARTIN")
    fix("SALTA", "GRALJOSEDESANMARTIN", "GENERALJOSEDESANMARTIN")
    fix("SALTA", "GENERALMARTINMIGUELDEGUEMES", "GENERALGUEMES")
    fix("SALTA", "GENERALMARTINMDEGUEMES", "GENERALGUEMES")
    fix("SALTA", "CANDELARIA", "LACANDELARIA")
    fix("SALTA", "CALDERA", "LACALDERA")
    fix("SALTA", "ANTE", "ANTA")
    fix("SALTA", "LACAPITAL", "CAPITAL")
    fix("SALTA", "ROSARIODELEPMA", "ROSARIODELERMA")
    fix("SALTA", "GDORDUPUY", "GOBERNADORDUPUY")
    fix("SALTA", "LIBGRALSANMARTIN", "LIBERTADORGENERALSANMARTIN")

    # SANJUAN
    fix("SANJUAN", "VEINTICINCODEMAYO", "25DEMAYO")
    fix("SANJUAN", "NUEVEDEJULIO", "9DEJULIO")
    fix("SANJUAN", "IGLESIAS", "IGLESIA")
    fix("SANJUAN", "ULLUN", "ULLUM")

    # SANLUIS
    fix("SANLUIS", "SANMARTIN", "LIBERTADORGENERALSANMARTIN")
    fix("SANLUIS", "LIBGRALSANMARTIN", "LIBERTADORGENERALSANMARTIN")
    fix("SANLUIS", "PRINGLES", "CORONELPRINGLES")
    fix("SANLUIS", "GOBERNADORVICENTEDUPUY", "GOBERNADORDUPUY")
    fix("SANLUIS", "GDORDUPUY", "GOBERNADORDUPUY")
    fix("SANLUIS", "GENERALBELGRANO", "BELGRANO")

    # SANTAFE
    fix("SANTAFE", "NUEVEDEJULIO", "9DEJULIO")
    fix("SANTAFE", "GRALLOPEZ", "GENERALLOPEZ")
    fix("SANTAFE", "GRALOBLIGADO", "GENERALOBLIGADO")

    # SANTIAGODELESTERO
    fix("SANTIAGODELESTERO", "QUEBRACHO", "QUEBRACHOS")
    fix("SANTIAGODELESTERO", "MATARA", "JUANFIBARRA")
    fix("SANTIAGODELESTERO", "BRIGJFELIPEIBARRA", "JUANFIBARRA")
    fix("SANTIAGODELESTERO", "GENERALATABOADA", "GENERALTABOADA")
    fix("SANTIAGODELESTERO", "GRALTABOADA", "GENERALTABOADA")

    # TIERRADELFUEGO
    fix("TIERRADELFUEGO", "SANSEBASTIAN", "RIOGRANDE")

    # TUCUMAN
    fix("TUCUMAN", "GRANEROS", "GRANERO")
    fix("TUCUMAN", "TAFI", "TAFIDELVALLE")
    fix("TUCUMAN", "BURRUCAYU", "BURRUYACU")
    fix("TUCUMAN", "JUANBALBERDI", "JUANBAUTISTAALBERDI")
    fix("TUCUMAN", "TAFIVIEJO", "TAFIVIAJO")
    fix("TUCUMAN", "SANMIGUELDETUCUMAN", "CAPITAL")

    ag
}

# ---------------------------------------------------------------------------
# Helper: stack years, validate, save
# ---------------------------------------------------------------------------
stack_validate_save <- function(ag60, ag88) {
    message("\n[agr] Step 6 — Stacking, validating, saving")

    panel <- rbind(ag60, ag88)
    panel <- panel[order(panel$geolev2, panel$year), ]

    # Validate key
    key <- paste(panel$geolev2, panel$year)
    n_dup <- sum(duplicated(key))
    if (n_dup > 0) {
        message(sprintf("[agr]   WARNING: %d duplicate keys", n_dup))
        dups <- panel[duplicated(key) | duplicated(key, fromLast = TRUE), ]
        print(dups[, c("geolev2", "year", "nexp", "areatot_ha")])
    }
    stopifnot(n_dup == 0)
    message("[agr]   Key (geolev2 + year) is unique: OK")

    stopifnot(!any(is.na(panel$geolev2)))
    stopifnot(!any(is.na(panel$year)))
    message("[agr]   Key has no missing values: OK")

    n_districts_60 <- length(unique(panel$geolev2[panel$year == 1960]))
    n_districts_88 <- length(unique(panel$geolev2[panel$year == 1988]))
    message(sprintf("[agr]   Districts: %d in 1960, %d in 1988",
                    n_districts_60, n_districts_88))
    message(sprintf("[agr]   Total obs: %d", nrow(panel)))

    # Save
    out_path <- file.path(dir_derived_agr, "agr_census.parquet")
    arrow::write_parquet(panel, out_path)
    message(sprintf("[agr]   Saved: %s", out_path))

    # Write manifest
    log_path <- file.path(dir_derived_agr, "data_file_manifest.log")
    sink(log_path)
    on.exit(sink(), add = TRUE)

    cat("Data file manifest — clean_agricultural.R\n")
    cat(sprintf("Generated: %s\n", Sys.time()))
    cat(sprintf("rootdir: %s\n\n", rootdir))
    cat(strrep("=", 60), "\n")
    cat("FILE: agr_census.parquet\n")
    cat(strrep("=", 60), "\n")
    cat(sprintf("Rows: %d\n", nrow(panel)))
    cat(sprintf("Columns: %d\n", ncol(panel)))
    cat(sprintf("Key: geolev2 + year\n"))
    cat(sprintf("Years: %s\n", paste(sort(unique(panel$year)), collapse = ", ")))
    cat(sprintf("Districts 1960: %d\n", n_districts_60))
    cat(sprintf("Districts 1988: %d\n", n_districts_88))

    # Document which geolev2 codes are missing (for downstream zero-filling)
    all_geolev2 <- sort(unique(xwalk_geolev2()))
    missing_60 <- setdiff(all_geolev2, unique(panel$geolev2[panel$year == 1960]))
    missing_88 <- setdiff(all_geolev2, unique(panel$geolev2[panel$year == 1988]))
    if (length(missing_60) > 0) {
        cat(sprintf("\nMissing geolev2 in 1960 (%d): %s\n",
                    length(missing_60), paste(missing_60, collapse = ", ")))
    }
    if (length(missing_88) > 0) {
        cat(sprintf("\nMissing geolev2 in 1988 (%d): %s\n",
                    length(missing_88), paste(missing_88, collapse = ", ")))
    }
    cat("\nSummary by year:\n")
    for (y in sort(unique(panel$year))) {
        sub <- panel[panel$year == y, ]
        cat(sprintf("\n  Year %d (N=%d):\n", y, nrow(sub)))
        for (v in c("nexp", "areatot_ha")) {
            vals <- sub[[v]]
            cat(sprintf("    %-15s  mean=%.1f  sd=%.1f  min=%.1f  max=%.1f  NA=%d\n",
                        v, mean(vals, na.rm = TRUE), sd(vals, na.rm = TRUE),
                        min(vals, na.rm = TRUE), max(vals, na.rm = TRUE),
                        sum(is.na(vals))))
        }
    }

    message(sprintf("[agr]   Manifest written: %s", log_path))
    panel
}

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
main()

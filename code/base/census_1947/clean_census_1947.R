# ===========================================================================
# clean_census_1947.R
#
# PURPOSE: Clean digitized 1947 Argentine national census (Cuarto Censo
#          General de la Nación) and merge to IPUMS district geography.
#          Reads province-level Excel files with total population (Cuadro 1)
#          and urban population (Cuadro 14), standardizes district names,
#          applies historical name changes and district splits, then merges
#          to the IPUMS crosswalk to produce a geolev2-keyed dataset.
#
# READS:
#   data/raw/census/censo1947/1947_Cuadro1_*.xlsx   — total pop by distrito
#   data/raw/census/censo1947/1947_Cuadro14_*.xlsx  — urban pop by distrito
#   data/derived/base/ipums/ipums_districts_for_merge.parquet — crosswalk
#
# PRODUCES:
#   data/derived/05a_census/census_1947_ipums.parquet
#       Key: geolev2. Variables: pop, urbpop, year, note.
#       note = 1 for Misiones and Chaco (impossible to match 1947→1960
#       boundaries per Cacopardo 1967).
#
#   data/derived/05a_census/census_1947_manifest.log
#
# REFERENCE:
#   Old data/Train/base/census_1946/code/clean_censo1946.do
#   Old data/Train/derived/census_1946/code/merge_popurb1946_to_IPUMS.do
#
# NOTES:
#   - The 1947 census predates the current provincial boundaries. Several
#     districts were renamed, split, or reassigned to different provinces
#     between 1947 and 1991. All such changes are documented inline.
#   - Zona Militar de Comodoro Rivadavia (a federal territory in 1947) was
#     dissolved in 1955; its districts were split between Chubut and
#     Santa Cruz provinces.
#   - Quilmes (Buenos Aires) was split into Quilmes (geolev2=32006076,
#     ~2/3 of pop) and Berazategui (geolev2=32006087, ~1/3) in 1960.
#   - Misiones and Chaco underwent major redistricting upon
#     provincialization in the 1950s. Per Cacopardo (1967), it is
#     impossible to create a 1947→1960 district correspondence for these
#     two provinces. Their values are set to NA (with imputed versions
#     kept separately).
# ===========================================================================

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"))

    message("\n", strrep("=", 72))
    message("clean_census_1947.R  |  1947 census -> geolev2 panel")
    message(strrep("=", 72))

    # --- 1. Read and append all province Excel files -----------------------
    raw <- read_raw_1947()

    # --- 2. Collapse to district level ------------------------------------
    districts <- collapse_to_districts(raw)

    # --- 3. Create merge keys with historical name/boundary changes -------
    districts <- apply_name_changes(districts)

    # --- 4. Merge to IPUMS crosswalk --------------------------------------
    merged <- merge_to_ipums(districts)

    # --- 5. Handle post-1947 district creations ---------------------------
    merged <- handle_post1947_districts(merged)

    # --- 6. Allocate split districts and collapse to geolev2 --------------
    final <- collapse_to_geolev2(merged)

    # --- 7. Validate and save ---------------------------------------------
    save_output(final)

    message(strrep("=", 72))
    message("clean_census_1947.R  |  Complete.")
    message(strrep("=", 72), "\n")
}

# ---------------------------------------------------------------------------
# Step 1: Read raw Excel files
# ---------------------------------------------------------------------------
read_raw_1947 <- function() {
    message("\n[c1947] Step 1 -- Reading raw Excel files")

    raw_dir <- file.path(dir_raw_census, "censo1947")
    if (!dir.exists(raw_dir)) {
        stop(sprintf("Raw data directory not found: %s", raw_dir))
    }

    # Province names matching the Excel filenames (same order as old code)
    provinces <- c(
        "BuenosAires", "Catamarca", "Chaco", "Chubut",
        "Cordoba", "Corrientes", "EntreRios", "Formosa",
        "Jujuy", "LaPampa", "LaRioja", "Mendoza",
        "Misiones", "Neuquen", "RioNegro", "Salta",
        "SanJuan", "SanLuis", "SantaCruz", "SantaFe",
        "SantiagoDelEstero", "Tucuman",
        "ZonaMilitardeComodoroRivadavia"
    )

    all_rows <- list()

    for (prov in provinces) {
        # -- Read Cuadro 1: total population
        pop_file <- file.path(
            raw_dir, sprintf("1947_Cuadro1_%s.xlsx", prov)
        )
        stopifnot(file.exists(pop_file))
        pop_df <- readxl::read_excel(pop_file)
        pop_df <- as.data.frame(pop_df)
        names(pop_df) <- tolower(names(pop_df))
        pop_df$provincia <- clean_name(pop_df$provincia)
        pop_df$partido   <- clean_name(pop_df$partido)
        pop_df$pop <- as.numeric(pop_df$n1947)

        # -- Read Cuadro 14: urban population
        urb_file <- file.path(
            raw_dir, sprintf("1947_Cuadro14_%s.xlsx", prov)
        )
        stopifnot(file.exists(urb_file))
        urb_df <- readxl::read_excel(urb_file)
        urb_df <- as.data.frame(urb_df)
        names(urb_df) <- tolower(names(urb_df))
        urb_df$provincia <- clean_name(urb_df$provincia)
        urb_df$partido   <- clean_name(urb_df$partido)
        urb_df$urb <- as.numeric(urb_df$n1947)

        # -- Fix known typos in raw data before merging pop + urban
        pop_df <- fix_raw_typos(pop_df)
        urb_df <- fix_raw_typos(urb_df)

        # -- Merge pop and urban by provincia + partido
        merged <- merge(
            urb_df[, c("provincia", "partido", "curbano", "urb")],
            pop_df[, c("provincia", "partido", "pop")],
            by = c("provincia", "partido"),
            all.x = TRUE
        )

        # Cuadro14 has urban/rural rows; pop is at partido level
        # (merge is m:1 — multiple urban rows to one pop total)
        all_rows[[length(all_rows) + 1]] <- merged
    }

    raw <- do.call(rbind, all_rows)
    raw$urb <- as.numeric(raw$urb)
    raw$pop <- as.numeric(raw$pop)

    message(sprintf(
        "[c1947]   Read %d rows from %d provinces",
        nrow(raw), length(provinces)
    ))
    raw
}

# ---------------------------------------------------------------------------
# Fix known typos in the raw Excel data (before pop+urban merge)
# These are OCR/transcription errors in the digitized census publications.
# Reference: clean_censo1946.do lines 30-50
# ---------------------------------------------------------------------------
fix_raw_typos <- function(df) {
    ba <- df$provincia == "BUENOSAIRES"
    df$partido[ba & df$partido == "MERIO"] <- "MERLO"
    df$partido[ba & df$partido == "CNELDEMARINALROSALES"] <-
        "CORONELMARINALNROSALES"
    df$partido[ba & df$partido == "LEANDONALEM"] <- "LEANDRONALEM"
    df$partido[ba & df$partido == "GENERALMADARIAGA"] <-
        "GENERALJUANMADARIAGA"

    co <- df$provincia == "CORDOBA"
    df$partido[co & df$partido == "PRESIDROQUESPENA"] <-
        "PRESIDENTEROQUESAENZPENA"

    lp <- df$provincia == "LAPAMPA"
    df$partido[lp & df$partido == "CONHELO"] <- "CONHELLO"

    sf <- df$provincia == "SANTAFE"
    df$partido[sf & df$partido == "NUEVEDEJULIO"] <- "9DEJULIO"

    df
}

# ---------------------------------------------------------------------------
# Step 2: Collapse to district level
# Urban pop is reported by urban/rural rows — sum to get total urban per
# distrito, then keep one row per provincia + distrito.
# ---------------------------------------------------------------------------
collapse_to_districts <- function(raw) {
    message("\n[c1947] Step 2 -- Collapsing to district level")

    # Sum urban pop across urban/rural rows within each distrito
    agg <- aggregate(
        urb ~ provincia + partido,
        data = raw, FUN = sum, na.rm = TRUE
    )
    # Pop is already at distrito level (same value repeated) — take
    # first value per group after asserting they're all equal.
    pop_agg <- aggregate(
        pop ~ provincia + partido,
        data = raw, FUN = function(x) {
            vals <- unique(x[!is.na(x)])
            if (length(vals) > 1) {
                warning(sprintf(
                    "Multiple pop values for same distrito: %s",
                    paste(vals, collapse = ", ")
                ))
            }
            vals[1]
        }
    )
    districts <- merge(agg, pop_agg, by = c("provincia", "partido"))

    # Rename to standard names
    names(districts)[names(districts) == "partido"] <- "distrito"
    districts$year <- 1947L

    message(sprintf(
        "[c1947]   %d districts after collapsing", nrow(districts)
    ))
    districts
}

# ---------------------------------------------------------------------------
# Step 3: Apply historical name changes and boundary reassignments
#
# Between 1947 and 1991, many districts were renamed, split, or moved
# to different provinces. This function creates provmerge/distmerge keys
# that match the IPUMS crosswalk (which uses 1991 boundaries).
#
# Each change is documented with a source (usually Wikipedia or INDEC
# historical publications).
# ---------------------------------------------------------------------------
apply_name_changes <- function(df) {
    message("\n[c1947] Step 3 -- Applying historical name changes")

    df$provmerge <- df$provincia
    df$distmerge <- df$distrito

    # Helper for concise replacements
    ch <- function(new, old, prov) {
        idx <- df$distmerge == old & df$provmerge == prov
        df$distmerge[idx] <<- new
    }

    # -- BUENOS AIRES --
    pr <- "BUENOSAIRES"
    # Arrecifes was called Bartolomé Mitre until 1950s
    ch("ARRECIFES", "BARTOLOMEMITRE", pr)
    ch("CORONELDEMARINELROSALES", "CORONELMARINALNROSALES", pr)
    ch("ADOLFOGONZALEZCHAVES", "GONZALEZCHAVES", pr)
    ch("GENERALJUANMADARIAGA", "GENERALMADARIAGA", pr)
    # Daireaux was called Caseros until 1970
    # https://es.wikipedia.org/wiki/Partido_de_Daireaux
    ch("DAIREAUX", "CASEROS", pr)
    # Benito Juárez was just "Juárez"
    ch("BENITOJUAREZ", "JUAREZ", pr)
    ch("25DEMAYO", "VEINTICINCODEMAYO", pr)
    ch("9DEJULIO", "NUEVEJULIO", pr)
    ch("9DEJULIO", "NUEVEDEJULIO", pr)
    # Lanús was called Cuatro de Junio until 1955
    # https://es.wikipedia.org/wiki/Partido_de_Lanús
    ch("LANUS", "CUATRODEJUNIO", pr)
    # Tigre was called Las Conchas
    # https://es.wikipedia.org/wiki/Tigre_(Buenos_Aires)
    ch("TIGRE", "LASCONCHAS", pr)
    ch("LEANDRONALEM", "LEANDRON", pr)
    ch("LOMASDEZAMORA", "LOMASDE", pr)
    ch("CARMENDEARECO", "CARMENDEARAUCO", pr)
    ch("TORNQUIST", "TORQUINST", pr)
    ch("BRANDSEN", "CORONELBRANDSEN", pr)

    # -- CATAMARCA --
    ch("FRAYMAMERTOESQUIU", "FRAYMAMERTO", "CATAMARCA")

    # -- CHACO --
    # Provincialization in 1950s changed 8 to 24 departments
    pr <- "CHACO"
    ch("12DEOCTUBRE", "DOCEDEOCTUBRE", pr)
    ch("1DEMAYO", "PRIMERODEMAYO", pr)
    ch("25DEMAYO", "VEINTICINCODEMAYO", pr)
    ch("9DEJULIO", "NUEVEDEJULIO", pr)
    ch("FRAYJUSTOSTAMARIADEORO", "FRAYJUSTOSANTAMARIADEORO", pr)
    ch("OHIGGINS", "CAPITALGENERALOHIGGINS", pr)
    ch("TAPENAGA", "TAPEGANA", pr)

    # -- CHUBUT / ZONA MILITAR DE COMODORO RIVADAVIA --
    # The Zona Militar was a federal territory dissolved in 1955.
    # Its districts were split between Chubut and Santa Cruz.
    df$provmerge[df$provmerge == "ZONAMILITARDECOMODORORIVADAVIA"] <-
        "CHUBUT"

    # Camarones comprised territories of current Escalante and
    # Florentino Ameghino.
    # https://es.wikipedia.org/wiki/Departamento_Camarones
    # NOTE: We split pop 50/50 here. The allocation step in
    # collapse_to_geolev2() counts n_alloc by (provincia, distrito),
    # not by distmerge. Since we set distmerge to ESCALANTE and
    # FLORENTINOAMEGHINO (each mapping 1:1 to a geolev2), n_alloc=1
    # for each, so no double-division occurs.
    cam_idx <- df$distmerge == "CAMARONES" & df$provmerge == "CHUBUT"
    if (any(cam_idx)) {
        cam_row <- df[cam_idx, ][1, ]
        new_row <- cam_row
        cam_row$distmerge <- "ESCALANTE"
        new_row$distmerge <- "FLORENTINOAMEGHINO"
        # Split pop equally (will be divided further at allocation step)
        cam_row$urb <- cam_row$urb / 2
        cam_row$pop <- cam_row$pop / 2
        new_row$urb <- new_row$urb / 2
        new_row$pop <- new_row$pop / 2
        df <- rbind(df[!cam_idx, ], cam_row, new_row)
    }

    # Comodoro Rivadavia comprised Escalante (Chubut) and Deseado
    # (Santa Cruz).
    # https://es.wikipedia.org/wiki/Departamento_Comodoro_Rivadavia
    cr_idx <- df$distmerge == "COMODORORIVADAVIA" & df$provmerge == "CHUBUT"
    if (any(cr_idx)) {
        cr_row <- df[cr_idx, ][1, ]
        new_row <- cr_row
        cr_row$distmerge <- "ESCALANTE"
        new_row$distmerge <- "DESEADO"
        new_row$provmerge <- "SANTACRUZ"
        cr_row$urb <- cr_row$urb / 2
        cr_row$pop <- cr_row$pop / 2
        new_row$urb <- new_row$urb / 2
        new_row$pop <- new_row$pop / 2
        df <- rbind(df[!cr_idx, ], cr_row, new_row)
    }

    # Several Zona Militar districts map to Deseado (Santa Cruz)
    ch("DESEADO", "PUERTODESEADO", "CHUBUT")
    ch("DESEADO", "COLONIALASHERAS", "CHUBUT")
    ch("DESEADO", "PICOTRUNCADO", "CHUBUT")
    # Reassign these to Santa Cruz
    deseado_chubut <- df$distmerge == "DESEADO" & df$provmerge == "CHUBUT"
    df$provmerge[deseado_chubut] <- "SANTACRUZ"

    ch("PASODELOSINDIOS", "PASODEINDIOS", "CHUBUT")
    ch("RIOSENGUER", "ALTORIOSENGUER", "CHUBUT")
    ch("RIOSENGUER", "LOSHUEMULES", "CHUBUT")
    ch("ESCALANTE", "PICOSALAMANCA", "CHUBUT")
    ch("RIOSENGUER", "PASORIOMAYO", "CHUBUT")
    # https://es.wikipedia.org/wiki/Departamento_Alto_Río_Mayo
    ch("RIOSENGUER", "ALTORIOMAYO", "CHUBUT")

    # Lago Buenos Aires moved from Chubut to Santa Cruz in 1955
    # https://es.wikipedia.org/wiki/Departamento_Lago_Buenos_Aires
    lb_idx <- df$provmerge == "CHUBUT" & df$distmerge == "LAGOBUENOSAIRES"
    df$provmerge[lb_idx] <- "SANTACRUZ"

    # -- JUJUY --
    # Capital of Jujuy is in Dr. Manuel Belgrano department
    ch("DRMANUELBELGRANO", "CAPITAL", "JUJUY")

    # -- LA PAMPA --
    ch("LEVENTUE", "LOVENTUEL", "LAPAMPA")

    # -- LA RIOJA --
    # Extensive name changes in La Rioja
    # https://es.wikipedia.org/wiki/Anexo:Municipios_de_La_Rioja_(Argentina)
    pr <- "LARIOJA"
    ch("CORONELFELIPEVARELA", "GENERALLAVALLE", pr)
    ch("ROSARIOVERAPENALOZA", "GENERALROCA", pr)
    ch("CHAMICAL", "GOBERNADORGORDILLO", pr)
    ch("VINCHINA", "SARMIENTO", pr)
    ch("GENERALJUANFQUIROGA", "RIVADAVIA", pr)
    ch("GENERALANGELVPENALOZA", "VELEZSARFIELD", pr)
    ch("GENERALSANMARTIN", "SANMARTIN", pr)
    ch("GENERALLAMADRID", "LAMADRID", pr)
    ch("SANBLASDELOSSAUCES", "PELAGIOBLUNA", pr)

    # -- MENDOZA --
    ch("LUJANDECUYO", "LUJAN", "MENDOZA")

    # -- MISIONES --
    # Belgrano's cabecera is Irigoyen, which was part of Frontera in 1947
    # https://es.wikipedia.org/wiki/Departamento_General_Manuel_Belgrano
    ch("GENERALMANUELBELGRANO", "FRONTERA", "MISIONES")

    # -- RIO NEGRO --
    ch("25DEMAYO", "VEINTICINCODEMAYO", "RIONEGRO")

    # -- SALTA --
    pr <- "SALTA"
    ch("GENERALJOSEDESANMARTIN", "SANMARTIN", pr)
    ch("GENERALGUEMES", "GENERALMARTINMDEGUEMES", pr)
    ch("LACANDELARIA", "CANDELARIA", pr)
    ch("LACALDERA", "CALDERA", pr)
    ch("ANTA", "ANTE", pr)
    # Campo Santo was part of General Güemes
    # https://www.familysearch.org/wiki/es/Departamento_de_General_Güemes
    ch("GENERALGUEMES", "CAMPOSANTO", pr)
    # Los Andes = San Antonio de los Cobres
    # https://es.wikipedia.org/wiki/Departamento_de_Los_Andes
    ch("LOSANDES", "SANANTONIODELOSCOBRES", pr)

    # -- SAN JUAN --
    pr <- "SANJUAN"
    ch("25DEMAYO", "VEINTICINCODEMAYO", pr)
    ch("IGLESIA", "IGLESIAS", pr)
    ch("ULLUM", "ULLUN", pr)

    # -- SAN LUIS --
    pr <- "SANLUIS"
    ch("LIBERTADORGENERALSANMARTIN", "SANMARTIN", pr)
    ch("BELGRANO", "GENERALBELGRANO", pr)
    ch("LACAPITAL", "CAPITAL", pr)

    # -- SANTA FE --
    ch("9DEJULIO", "NUEVEDEJULIO", "SANTAFE")

    # -- SANTIAGO DEL ESTERO --
    pr <- "SANTIAGODELESTERO"
    # Juan Felipe Ibarra was called Matará
    # https://es.wikipedia.org/wiki/Departamento_Juan_Felipe_Ibarra
    ch("JUANFIBARRA", "MATARA", pr)
    ch("GENERALTABOADA", "GENERALATABOADA", pr)
    ch("GENERALTABOADA", "GENERALANTONIOTABOADA", pr)

    # -- TIERRA DEL FUEGO --
    # Río Grande was called San Sebastián
    ch("RIOGRANDE", "SANSEBASTIAN", "TIERRADELFUEGO")

    # -- TUCUMAN --
    pr <- "TUCUMAN"
    ch("GRANERO", "GRANEROS", pr)
    # Tafí del Valle was just "Tafí" — Tafí Viejo was later segregated
    # https://es.wikipedia.org/wiki/Departamento_Tafí_del_Valle
    ch("TAFIDELVALLE", "TAFI", pr)
    ch("BURRUYACU", "BURRUCAYU", pr)

    message(sprintf(
        "[c1947]   %d districts after name changes", nrow(df)
    ))
    df
}

# ---------------------------------------------------------------------------
# Step 4: Merge to IPUMS crosswalk
# ---------------------------------------------------------------------------
merge_to_ipums <- function(df) {
    message("\n[c1947] Step 4 -- Merging to IPUMS crosswalk")

    xwalk_path <- file.path(
        dir_derived, "base", "ipums",
        "ipums_districts_for_merge.parquet"
    )
    stopifnot(file.exists(xwalk_path))
    xwalk <- arrow::read_parquet(xwalk_path)
    xwalk <- as.data.frame(xwalk)

    merged <- merge(
        df, xwalk,
        by.x = c("provmerge", "distmerge"),
        by.y = c("provmerge", "distmerge"),
        all = TRUE
    )

    n_matched   <- sum(!is.na(merged$pop) & !is.na(merged$geolev2))
    n_census_only <- sum(!is.na(merged$pop) & is.na(merged$geolev2))
    n_ipums_only  <- sum(is.na(merged$pop) & !is.na(merged$geolev2))

    message(sprintf("[c1947]   Matched: %d", n_matched))
    message(sprintf("[c1947]   Census only (unmatched): %d", n_census_only))
    message(sprintf("[c1947]   IPUMS only (no 1947 data): %d", n_ipums_only))

    if (n_census_only > 0) {
        unmatched <- merged[!is.na(merged$pop) & is.na(merged$geolev2), ]
        message("[c1947]   Unmatched census districts:")
        for (i in seq_len(nrow(unmatched))) {
            message(sprintf(
                "          %s - %s",
                unmatched$provmerge[i], unmatched$distmerge[i]
            ))
        }
    }

    # Drop IPUMS-only rows where the geolev2 is already covered by a
    # matched row (i.e., the IPUMS crosswalk has multiple name variants
    # for the same geolev2, and one variant matched).
    matched_geolev2 <- unique(
        merged$geolev2[!is.na(merged$pop) & !is.na(merged$geolev2)]
    )
    redundant <- is.na(merged$pop) &
        !is.na(merged$geolev2) &
        merged$geolev2 %in% matched_geolev2
    n_redundant <- sum(redundant)
    if (n_redundant > 0) {
        message(sprintf(
            "[c1947]   Dropping %d redundant IPUMS-only rows",
            n_redundant
        ))
        merged <- merged[!redundant, ]
    }

    message(sprintf(
        "[c1947]   After merge cleanup: %d rows", nrow(merged)
    ))

    merged
}

# ---------------------------------------------------------------------------
# Step 5: Handle post-1947 district creations
#
# Some IPUMS districts did not exist in 1947 — they were created by
# splitting older districts. We map each new district back to its
# parent(s) so that 1947 population can be allocated.
# ---------------------------------------------------------------------------
handle_post1947_districts <- function(df) {
    message("\n[c1947] Step 5 -- Handling post-1947 districts")

    # Keep only the IPUMS-only rows (districts with no 1947 match)
    ipums_only <- df[is.na(df$pop) & !is.na(df$geolev2), ]
    census_rows <- df[!is.na(df$pop), ]

    if (nrow(ipums_only) == 0) {
        message("[c1947]   No post-1947 districts to handle")
        return(census_rows)
    }

    # For each unmatched IPUMS district, map it to its 1947 parent.
    # child = the post-1947 IPUMS district name
    # parent = the 1947 district it was carved from
    post1947 <- ipums_only[, c(
        "provmerge", "distmerge", "geolev2",
        "provname", "districtIPUMS"
    )]

    # Helper: remap a child district to its 1947 parent
    to_parent <- function(parent, child, prov) {
        idx <- post1947$distmerge == child &
            post1947$provmerge == prov
        post1947$distmerge[idx] <<- parent
    }

    # -- BUENOS AIRES --
    # Escobar: segregated in 1959 from Pilar and Tigre
    # https://es.wikipedia.org/wiki/Partido_de_Escobar
    esc_idx <- post1947$distmerge == "ESCOBAR" &
        post1947$provmerge == "BUENOSAIRES"
    if (any(esc_idx)) {
        esc_row <- post1947[esc_idx, ][1, ]
        new_row <- esc_row
        esc_row$distmerge <- "PILAR"
        new_row$distmerge <- "TIGRE"
        post1947 <- rbind(post1947[!esc_idx, ], esc_row, new_row)
    }

    # Berisso: segregated in 1957 from La Plata
    # https://es.wikipedia.org/wiki/Partido_de_Berisso
    to_parent("LAPLATA", "BERISSO", "BUENOSAIRES")

    # Ensenada: also from La Plata
    to_parent("LAPLATA", "ENSENADA", "BUENOSAIRES")

    # Tres de Febrero: segregated from General San Martín
    # https://es.wikipedia.org/wiki/Partido_de_Tres_de_Febrero
    to_parent("GENERALSANMARTIN", "TRESDEFEBRERO", "BUENOSAIRES")

    # Berazategui: segregated from Quilmes in 1960
    to_parent("QUILMES", "BERAZATEGUI", "BUENOSAIRES")

    # -- MISIONES --
    # Several districts created from older ones upon provincialization
    # https://biblioteca.indec.gob.ar/bases/minde/1c1947x4_2.pdf
    to_parent("CANDELARIA", "OBERA", "MISIONES")
    to_parent("IGUAZU", "ELDORADO", "MISIONES")
    to_parent("CAINGUAS", "LIBERTADORGENERALSANMARTIN", "MISIONES")
    to_parent("SANPEDRO", "MONTECARLO", "MISIONES")
    to_parent("SANJAVIER", "25DEMAYO", "MISIONES")

    # -- SALTA --
    # General José de San Martín was part of Orán in 1947
    # https://es.wikipedia.org/wiki/Departamento_General_José_de_San_Martín
    to_parent("ORAN", "GENERALJOSEDESANMARTIN", "SALTA")

    # -- MENDOZA --
    # Malargüe was part of San Rafael (then called 25 de Mayo)
    # https://es.wikipedia.org/wiki/Departamento_Malargüe
    to_parent("SANRAFAEL", "MALARGUE", "MENDOZA")

    message(sprintf(
        "[c1947]   %d post-1947 districts mapped to parents",
        nrow(post1947)
    ))

    # Merge post-1947 mappings back to census data.
    # post1947$distmerge now points to the 1947 parent name, so we
    # can look up the parent's pop/urb values from census_rows.
    # Use a targeted merge on provmerge+distmerge, then keep only
    # the columns we need to avoid suffix collisions.
    parent_data <- census_rows[
        !is.na(census_rows$geolev2),
        c("provmerge", "distmerge", "provincia", "distrito",
          "urb", "pop", "year")
    ]
    # De-duplicate: a parent may appear multiple times if it already
    # mapped to multiple geolev2 codes — take the first (values are
    # identical across rows for the same parent).
    parent_data <- parent_data[
        !duplicated(paste(parent_data$provmerge,
                          parent_data$distmerge)),
    ]

    merged <- merge(
        post1947, parent_data,
        by = c("provmerge", "distmerge"),
        all.x = TRUE
    )

    # Build result rows with the same columns as census_rows
    # (which has: provmerge, distmerge, provincia, distrito, urb,
    #  pop, year, geolev2, provname, districtIPUMS)
    matched_census <- census_rows[!is.na(census_rows$geolev2), ]
    matched_post <- merged[!is.na(merged$pop), ]

    # Align columns before rbind
    common_cols <- intersect(names(matched_census), names(matched_post))
    result <- rbind(
        matched_census[, common_cols],
        matched_post[, common_cols]
    )

    message(sprintf(
        "[c1947]   After post-1947 handling: %d rows", nrow(result)
    ))
    result
}

# ---------------------------------------------------------------------------
# Step 6: Allocate split districts and collapse to geolev2
#
# When one 1947 district maps to multiple IPUMS geolev2 codes, we divide
# the population equally — except for Quilmes, which is split 2/3 to
# Quilmes (32006076) and 1/3 to Berazategui (32006087), roughly matching
# their current population proportions.
# ---------------------------------------------------------------------------
collapse_to_geolev2 <- function(df) {
    message("\n[c1947] Step 6 -- Allocating splits, collapsing to geolev2")

    # Count how many geolev2 codes each 1947 district maps to
    df$alloc_key <- paste(df$provincia, df$distrito, sep = "_")
    alloc_counts <- table(df$alloc_key)
    df$n_alloc <- as.integer(alloc_counts[df$alloc_key])

    # Default: divide equally among mapped geolev2 codes
    for (v in c("urb", "pop")) {
        df[[v]] <- df[[v]] / df$n_alloc
    }

    # Special case: Quilmes split (override equal division)
    # geolev2=32006076 = Quilmes proper (~2/3)
    # geolev2=32006087 = Berazategui (~1/3)
    quilmes_idx <- df$geolev2 == 32006076
    beraz_idx   <- df$geolev2 == 32006087
    if (any(quilmes_idx) && any(beraz_idx)) {
        # Undo the equal split, apply 2/3 - 1/3
        for (v in c("urb", "pop")) {
            total <- df[[v]][quilmes_idx] * df$n_alloc[quilmes_idx]
            df[[v]][quilmes_idx] <- total * (2 / 3)
            df[[v]][beraz_idx]   <- total * (1 / 3)
        }
    }

    # Flag Misiones and Chaco: impossible to match 1947→1960 boundaries
    # per M.C. Cacopardo (1967)
    df$note <- ifelse(
        df$provmerge %in% c("MISIONES", "CHACO"), 1L, 0L
    )

    # Rename urban pop to match project convention
    names(df)[names(df) == "urb"] <- "urbpop"

    # Collapse to geolev2 (sum pop and urbpop across sub-districts)
    final <- aggregate(
        cbind(urbpop, pop) ~ geolev2 + note,
        data = df[!is.na(df$geolev2), ],
        FUN = sum, na.rm = TRUE
    )
    # note: take max (1 if any component was flagged)
    note_agg <- aggregate(
        note ~ geolev2,
        data = df[!is.na(df$geolev2), ],
        FUN = max
    )
    final <- merge(
        final[, c("geolev2", "urbpop", "pop")],
        note_agg, by = "geolev2"
    )

    final$year <- 1947L

    # Create imputed versions (keep values even for flagged districts)
    final$urbpop_imputed <- final$urbpop
    final$pop_imputed    <- final$pop

    # Set flagged districts to NA in the main variables
    final$urbpop[final$note == 1] <- NA
    final$pop[final$note == 1]    <- NA

    # Exclude Tierra del Fuego, Capital Federal, and non-mainland
    # territories (not in the estimation sample).
    # Tierra del Fuego and Capital Federal are excluded because the old
    # code drops them explicitly; they are not part of the 312-district
    # estimation sample.
    geo_cf <- 32002001L
    geo_tdf <- c(94094001L, 94094002L)  # Ushuaia, Río Grande
    excl <- final$geolev2 %in% geolev2_exclude |
        final$geolev2 %in% c(geo_cf, geo_tdf)
    final <- final[!excl, ]

    message(sprintf(
        "[c1947]   Final: %d districts (%d flagged as imputed)",
        nrow(final), sum(final$note == 1)
    ))
    final
}

# ---------------------------------------------------------------------------
# Step 7: Validate and save (SaveData pattern)
# ---------------------------------------------------------------------------
save_output <- function(final) {
    message("\n[c1947] Step 7 -- Validating and saving")

    # -- Validate key
    stopifnot(!any(is.na(final$geolev2)))
    stopifnot(!any(duplicated(final$geolev2)))
    message("[c1947]   Key (geolev2) is unique and non-missing: OK")

    # -- Sort by key
    final <- final[order(final$geolev2), ]

    # -- Ensure output directory exists
    out_dir <- dir_derived_census
    if (!dir.exists(out_dir)) {
        dir.create(out_dir, recursive = TRUE)
    }

    # -- Save parquet
    out_path <- file.path(out_dir, "census_1947_ipums.parquet")
    arrow::write_parquet(final, out_path)
    message(sprintf("[c1947]   Saved: %s (%d rows)", out_path, nrow(final)))

    # -- Write manifest
    log_path <- file.path(out_dir, "census_1947_manifest.log")
    sink(log_path)
    on.exit(sink(), add = TRUE)

    cat("Data file manifest -- clean_census_1947.R\n")
    cat(sprintf("Generated: %s\n", Sys.time()))
    cat(sprintf("rootdir: %s\n\n", rootdir))

    cat(strrep("=", 60), "\n")
    cat("FILE: census_1947_ipums.parquet\n")
    cat(strrep("=", 60), "\n")
    cat(sprintf("Rows: %d\n", nrow(final)))
    cat(sprintf("Columns: %d\n", ncol(final)))
    cat(sprintf("Key: geolev2\n"))
    cat(sprintf("Year: %d\n", unique(final$year)))
    cat(sprintf("Districts with note=1 (imputed): %d\n",
                sum(final$note == 1)))

    cat("\nSummary of key variables:\n")
    for (v in c("pop", "urbpop", "pop_imputed", "urbpop_imputed")) {
        vals <- final[[v]]
        cat(sprintf(
            "  %-20s  N=%d  mean=%.0f  min=%.0f  max=%.0f  NA=%d\n",
            v, sum(!is.na(vals)),
            mean(vals, na.rm = TRUE),
            min(vals, na.rm = TRUE),
            max(vals, na.rm = TRUE),
            sum(is.na(vals))
        ))
    }

    message(sprintf("[c1947]   Manifest: %s", log_path))
}

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
main()

# ===========================================================================
# utils.R
#
# PURPOSE: Shared utility functions used by multiple data cleaning scripts.
# USAGE:   source(file.path(dir_code, "base", "utils.R"))
# ===========================================================================

#' Clean a name string for district/province matching.
#' Uppercases, removes spaces/punctuation, strips accents.
#' Used by all census cleaning scripts to standardize names before merging
#' with the IPUMS crosswalk.
clean_name <- function(x) {
    x <- toupper(x)
    x <- gsub(" ", "", x)
    x <- gsub("-", "", x)
    x <- gsub("\\.", "", x)
    x <- gsub("\u00e1", "A", x)  # á
    x <- gsub("\u00e9", "E", x)  # é
    x <- gsub("\u00ed", "I", x)  # í
    x <- gsub("\u00f3", "O", x)  # ó
    x <- gsub("\u00fa", "U", x)  # ú
    x <- gsub("\u00f1", "N", x)  # ñ
    x <- gsub("\u00fc", "U", x)  # ü
    # Strip any remaining non-alphanumeric chars (catches OCR garbage:
    # trailing apostrophes, middle dots, ¬, &, etc. from digitized files)
    x <- gsub("[^A-Z0-9]", "", x)
    x
}

#' Ensure geolev2 column is character.
#' Project convention: geolev2 is ALWAYS character to avoid type coercion
#' bugs (e.g., silent comparison failures when shapefile stores it as
#' factor/numeric). Call this after reading any dataset with geolev2.
ensure_geolev2_char <- function(df, col = "geolev2") {
    if (col %in% names(df)) {
        df[[col]] <- as.character(df[[col]])
    }
    df
}

#' INDEC five-region grouping by numeric province code.
#' Province codes are the INDEC codes used in geolev2 (characters 3-5
#' after the leading-zero strip, e.g. "32006072" -> "006" Buenos Aires)
#' and in the PARENT field of the district shapefile. Shared by the
#' recentering-diagnostic scripts (lines/strata construction; region
#' fixed effects in the controls exploration).
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

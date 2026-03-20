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
    x
}

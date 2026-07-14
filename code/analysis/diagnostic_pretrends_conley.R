# ===========================================================================
# diagnostic_pretrends_conley.R
#
# PURPOSE: Task C37 / memo Decision E (pre-trends). The pre-trends placebo
#          (Table 7) is not a clean null: IV-Both beta = 0.078, p = 0.06
#          on the 235-district placebo subset, HC1 SE. Because districts
#          are spatially arranged and MA is constructed from a shared
#          network, residuals are plausibly spatially correlated, and HC1
#          may overstate precision. This diagnostic re-computes the
#          standard errors of BOTH the pre-trends placebo (Table 7 spec)
#          and the headline population IV (Table 9 spec) with Conley
#          spatial HAC standard errors at several distance cutoffs.
#
#          Point estimates are unchanged by construction (same fits as
#          the tables, via fit_iv_quad); only the vcov is recomputed at
#          summary time with fixest's built-in conley().
#
# READING: if the placebo's p-value rises well above 0.10 under spatial
#          SEs, the pre-trends critique softens (the apparent pre-trend
#          is not distinguishable from spatially correlated noise). If it
#          survives at all cutoffs, the limitation is real and the paper
#          should own it (memo Decision E: own-it vs add-sensitivity).
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#   data/raw/geo/geo2_ar1970_2010.shp        (district centroids -> lat/lon)
#
# PRODUCES:
#   results/tables/diagnostic_pretrends_conley.txt
#   results/tables/diagnostic_pretrends_conley.csv
#
# NOTE: no new dependencies — Conley SEs via fixest::conley() (>= 0.10),
#       recomputed with summary(model, vcov = ...); no refitting, no
#       Dijkstra, runs in seconds.
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
    library(sf)
})

# Conley distance cutoffs (km). Argentina spans ~3,700 km N-S; districts
# are large (median ~2,000 km2). 100 km ~ neighboring districts;
# 1,000 km ~ region-wide correlation.
CONLEY_CUTOFFS_KM <- c(100, 250, 500, 1000)

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)

    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)

    report_path <- file.path(dir_tables, "diagnostic_pretrends_conley.txt")
    con <- file(report_path, open = "wt")
    rep <- function(...) { line <- sprintf(...); cat(line, "\n")
                           cat(line, "\n", file = con) }

    rep("%s", strrep("=", 74))
    rep("CONLEY SPATIAL-SE DIAGNOSTIC (task C37 / memo Decision E, pre-trends)")
    rep("Same point estimates as Tables 7 and 9; SEs recomputed with")
    rep("fixest::conley() at cutoffs %s km",
        paste(CONLEY_CUTOFFS_KM, collapse = " / "))
    rep("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
    rep("%s", strrep("=", 74))

    # ---- Data: estimation sample + district lat/lon ------------------------
    d <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))
    d <- ensure_geolev2_char(d)
    coords <- load_district_latlon()
    d <- merge(d, coords, by = "geolev2")
    stopifnot(nrow(d) > 0, all(c("lat", "lon") %in% names(d)))
    rep("\nSample: %d districts with coordinates", nrow(d))

    # ---- Fit both outcomes with the canonical table specs ------------------
    specs <- list(
        list(label = "Pre-trends placebo (Table 7): d log pop 1947-1960",
             y     = "chg_log_placebo_pop_60_47"),
        list(label = "Headline population (Table 9): d log pop 1960-1991",
             y     = "chg_log_pop_91_60")
    )

    csv_rows <- list()
    for (sp in specs) {
        rep("\n%s", strrep("-", 74))
        rep("[%s]", sp$label)
        rep("%s", strrep("-", 74))

        fits <- fit_iv_quad(
            y = sp$y, data = d,
            endog      = "chg_logMA_86_60_s0_elow",
            lp_instr   = "chg_logMA_stu_s0_elow",
            hypo_instr = main_hypo_instrument,
            ctrls_vec  = geo_controls_main
        )

        for (nm in names(fits)) {
            m <- fits[[nm]]
            cname <- if (nm == "OLS") "chg_logMA_86_60_s0_elow"
                     else "fit_chg_logMA_86_60_s0_elow"

            # HC1 (the tables' SE) as the reference row
            co_hc1 <- safe_coef(m, cname)
            rep("  %-8s  beta = %+.3f   HC1: SE %.3f p %.3f   N=%d",
                nm, co_hc1$est, co_hc1$se, co_hc1$p, nobs(m))
            csv_rows[[length(csv_rows) + 1L]] <- data.frame(
                outcome = sp$y, spec = nm, vcov = "HC1",
                cutoff_km = NA_real_, estimate = co_hc1$est,
                std_err = co_hc1$se, p_value = co_hc1$p, n_obs = nobs(m),
                vcov_psd_fixed = FALSE)

            # Conley at each cutoff: same fit, vcov recomputed at summary.
            # fixest's uniform-kernel spatial HAC can produce a non-PSD
            # vcov in small samples; fixest "fixes" it by eigenvalue
            # truncation, which can bias SEs downward. Capture that
            # warning per cell and flag it — flagged magnitudes are
            # unreliable even if the qualitative direction holds.
            for (ck in CONLEY_CUTOFFS_KM) {
                psd_fixed <- FALSE
                sm <- withCallingHandlers(
                    summary(m, vcov = conley(ck, distance = "spherical")),
                    warning = function(w) {
                        if (grepl("positive semi-definite",
                                  conditionMessage(w))) {
                            psd_fixed <<- TRUE
                            invokeRestart("muffleWarning")
                        }
                    })
                ct <- sm$coeftable
                if (!(cname %in% rownames(ct))) next
                se_c <- ct[cname, 2]; p_c <- ct[cname, 4]
                flag <- if (psd_fixed) "  [vcov PSD-fixed]" else ""
                rep("  %-8s               Conley %4d km: SE %.3f p %.3f%s",
                    "", ck, se_c, p_c, flag)
                csv_rows[[length(csv_rows) + 1L]] <- data.frame(
                    outcome = sp$y, spec = nm, vcov = "Conley",
                    cutoff_km = ck, estimate = co_hc1$est,
                    std_err = se_c, p_value = p_c, n_obs = nobs(m),
                    vcov_psd_fixed = psd_fixed)
            }
        }
    }

    csv_df <- do.call(rbind, csv_rows)
    out_csv <- file.path(dir_tables, "diagnostic_pretrends_conley.csv")
    write.csv(csv_df, out_csv, row.names = FALSE)
    rep("\nSaved: %s", out_csv)

    rep("\n%s", strrep("=", 74))
    rep("READING: point estimates are identical to the tables by construction;")
    rep("only the SEs change. For the PLACEBO, a p-value rising above ~0.10")
    rep("at plausible cutoffs means the apparent pre-trend is not")
    rep("distinguishable from spatially correlated noise (softens the")
    rep("critique). If it stays near or below 0.06 across cutoffs, the")
    rep("pre-trend concern is robust to spatial correlation and the paper")
    rep("should own the limitation. For the HEADLINE spec, Conley SEs are")
    rep("the referee-proofing answer to 'are your SEs too small given")
    rep("spatial correlation?' (task C37).")
    rep("")
    rep("CAVEAT: cells flagged [vcov PSD-fixed] had a non-positive-semi-")
    rep("definite spatial HAC matrix repaired by eigenvalue truncation")
    rep("(fixest default). For those cells the SE magnitude is unreliable")
    rep("(often biased small); read them as 'no evidence of softening,'")
    rep("not as 'the pre-trend sharpened to p<0.01'. Unflagged cells are")
    rep("the trustworthy ones.")
    rep("%s", strrep("=", 74))

    close(con)
    message("\nSaved report: ", report_path)
}

# ---------------------------------------------------------------------------
# District centroids in lat/lon (EPSG:4326) for Conley distances.
# Same district-filtering rules as load_centroids() in 03c.
# ---------------------------------------------------------------------------
load_district_latlon <- function() {
    shp <- sf::st_read(file.path(dir_raw_geo, "geo2_ar1970_2010.shp"),
                       quiet = TRUE)
    shp <- sf::st_make_valid(shp)
    names(shp)[names(shp) == "GEOLEVEL2"] <- "geolev2"
    shp$geolev2 <- sub("^0+", "", as.character(shp$geolev2))
    shp <- shp[!sf::st_is_empty(shp), ]
    shp <- shp[!(shp$geolev2 %in% geolev2_exclude), ]
    shp <- shp[!grepl("0000$", shp$geolev2), ]
    stopifnot(nrow(shp) == 312L, !any(duplicated(shp$geolev2)))

    cents <- suppressWarnings(sf::st_centroid(shp))
    cents <- sf::st_transform(cents, crs = "EPSG:4326")
    xy <- sf::st_coordinates(cents)
    data.frame(geolev2 = cents$geolev2,
               lon = xy[, 1], lat = xy[, 2])
}

main()

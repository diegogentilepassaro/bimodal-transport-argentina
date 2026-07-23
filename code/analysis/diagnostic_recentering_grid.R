# ===========================================================================
# diagnostic_recentering_grid.R
#
# PURPOSE: Completeness grid for the recentered-instrument control
#          exploration (approved by Diego 2026-07-22): the FULL
#          factorial of control blocks, reported DESCRIPTIVELY -- what
#          each control/FE combination does to the recentered first
#          stage and to the mu-spanning R^2. No selection is performed
#          and no outcome is estimated here (the outcome-blind
#          selection exercise lives in diagnostic_recentering_controls.R).
#
# GRID: blocks {mu} x {no FE, region FE, province FE} x {rail
#       baselines} x {lat/lon quadratic} = 2 x 3 x 2 x 2 = 24 sets,
#       each on top of geo_controls_main. (Region and province FE are
#       not crossed with each other: province nests region.)
#
# READS:   draws + estimation sample via _recentering_helpers.R
# PRODUCES:
#   results/tables/diagnostic_recentering_grid.csv
#   results/tables/diagnostic_recentering_grid.txt
#
# USAGE:  Rscript code/analysis/diagnostic_recentering_grid.R
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
    library(sf)
})

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_recentering_helpers.R"),
           echo = FALSE)

    message("\n", strrep("=", 72))
    message("diagnostic_recentering_grid.R  |  full control-block factorial")
    message(strrep("=", 72))

    rc <- load_recentering_data(sector = "s0")
    d2 <- rc$d2; S_perm <- rc$S_perm

    endog   <- "chg_logMA_86_60_s0_elow"
    base    <- geo_controls_main
    ll_quad <- c("lat", "lon", "lat2", "lon2", "latlon")
    rails   <- c("rail_km_1960", "rail_dens_1960")

    grid <- expand.grid(mu = c(FALSE, TRUE),
                        fe = c("none", "region", "province"),
                        rails = c(FALSE, TRUE),
                        latlon = c(FALSE, TRUE),
                        stringsAsFactors = FALSE)

    rows <- list()
    message(sprintf("[grid] %d combinations", nrow(grid)))
    for (k in seq_len(nrow(grid))) {
        g <- grid[k, ]
        ctrl <- c(base,
                  if (g$mu) "mu",
                  if (g$rails) rails,
                  if (g$latlon) ll_quad)
        fe <- if (g$fe == "none") NULL else g$fe

        # Direct first stage (no outcome object; robust Wald F on the
        # single instrument = squared robust t).
        f1 <- as.formula(sprintf("%s ~ z_rec + %s%s", endog,
            paste(ctrl, collapse = " + "),
            if (is.null(fe)) "" else sprintf(" | %s", fe)))
        m1 <- feols(f1, data = d2, vcov = "hetero")
        tz <- safe_coef(m1, "z_rec")
        F_rob <- (tz$est / tz$se)^2

        # How much of mu does the non-mu part of the set span?
        m_mu <- feols(as.formula(paste(
            "mu ~", paste(setdiff(ctrl, "mu"), collapse = " + "),
            if (is.null(fe)) "" else sprintf("| %s", fe))),
            data = d2, vcov = "hetero")
        r2_mu <- r2(m_mu, type = if (is.null(fe)) "r2" else "wr2")

        rows[[k]] <- data.frame(
            mu = g$mu, fe = g$fe, rails = g$rails, latlon = g$latlon,
            F_robust_wald = F_rob, r2_mu = r2_mu, N = nobs(m1))
        message(sprintf(
            "[grid]  mu=%d fe=%-8s rails=%d latlon=%d   F=%6.2f  R2(mu)=%.3f",
            g$mu, g$fe, g$rails, g$latlon, F_rob, r2_mu))
    }

    res <- do.call(rbind, rows)
    res$S_perm <- S_perm
    csv_path <- file.path(dir_tables, "diagnostic_recentering_grid.csv")
    write.csv(res, csv_path, row.names = FALSE)

    txt_path <- file.path(dir_tables, "diagnostic_recentering_grid.txt")
    sink(txt_path)
    cat("Recentered-instrument control grid (full factorial, descriptive)\n")
    cat(sprintf("Generated: %s  |  draws: %d\n\n", Sys.time(), S_perm))
    cat("All sets include geo_controls_main. F = robust Wald on z_rec\n")
    cat("in the direct first stage; R2(mu) = mu on the set's non-mu part.\n\n")
    o <- res[order(-res$F_robust_wald), ]
    cat(sprintf("%-4s %-9s %-6s %-7s %8s %8s\n",
                "mu", "fe", "rails", "latlon", "F", "R2(mu)"))
    for (i in seq_len(nrow(o))) {
        cat(sprintf("%-4d %-9s %-6d %-7d %8.2f %8.3f\n",
                    o$mu[i], o$fe[i], o$rails[i], o$latlon[i],
                    o$F_robust_wald[i], o$r2_mu[i]))
    }
    sink()
    message(sprintf("[grid] Saved: %s and .txt", csv_path))
}

main()

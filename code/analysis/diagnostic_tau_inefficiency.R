# ===========================================================================
# diagnostic_tau_inefficiency.R
#
# PURPOSE: Decision A, Option 1b (route-inefficiency tau) — the third
#          and last column of the tau-object comparison, computable with
#          NO external data (.kiro/decision_a_option1_scoping.md,
#          Construction 1b):
#
#              tau'_ij = cost_ij / (c_min * geodesic_ij)
#
#          with c_min the cheapest-mode unit cost (navigation, 0.621
#          pesos/ton-km, config cost_nav) and geodesic_ij the
#          great-circle distance between the same district centroids
#          03c used for tau extraction. Dimensionless, >= 1 up to
#          navigation availability, PAIR-VARYING — a genuinely
#          different object from the 1a scalar-V sweep.
#
#          ANALYTICAL NOTE (stated once, used throughout): c_min and
#          the raster-unit conversion are global constants, so they
#          cancel in Delta log MA — the regression object is
#          effectively cost-per-geodesic-km. The constants only make
#          the reported tau' band interpretable as a route-inefficiency
#          multiple (>= 1 when the least-cost path is no cheaper than a
#          straight navigation line).
#
# DESIGN: MA machinery as diagnostic_ma_iceberg_sectoral.R (loaders
#   copied with provenance; diagnostics self-contained by convention).
#   Transform each cached s0 tau matrix pairwise by 1/D_ij (D fixed
#   across cases/periods), recompute MA with 1960 weights, rebuild
#   treatment + both instrument deltas, run the Table 9/10 spec for
#   SIX outcomes (population + five sectoral), IV-Both AND IV-LP with
#   first-stage Fs (the A x C interaction is the meeting question).
#   theta = 4.55 and 8.22. Baseline logMA control = the 1b-object 1960
#   logMA (internal consistency, as the iceberg diagnostics).
#
# VERIFICATION (asserted in code, row-count-guarded):
#   - BA (32002001) - Rosario (32082001) centroid geodesic must
#     reproduce the scoping note's quoted 265.9 km.
#   - Distance matrix: symmetric, zero self-distance, positive
#     off-diagonal, 312 districts.
#   - Sample sizes must match Tables 9/10 (311/310/309/297).
#   - The share of pairs with tau' < 1 is reported (expected ~ 0:
#     navigation is the cost floor).
#   NOTE: no raw-anchor nesting exists for 1b (it is not a limit of
#   the pipeline object), so anchors are construction-level.
#
# READS:
#   data/derived/03_taus/tau_{actual_1960,actual_1986,
#                              instrument_stu,instrument_lcp_mst}_s0.parquet
#   data/raw/geo/geo2_ar1970_2010.shp            (centroids, as 03c)
#   data/derived/base/census_1960/census_1960_ipums.parquet
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES (diagnostic only; no paper exhibit, no main.R wiring):
#   results/tables/diagnostic_tau_inefficiency.txt
#   results/tables/diagnostic_tau_inefficiency.csv
# ===========================================================================
suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
    library(sf)
})

CASES <- c("actual_1960", "actual_1986", "instrument_stu", "instrument_lcp_mst")
THETAS <- c(4.55, 8.22)

OUTCOMES <- list(
    list(var = "chg_log_pop_91_60",        lab = "population"),
    list(var = "chg_log_valprod_85_54",    lab = "mfg value"),
    list(var = "chg_log_massal_85_54",     lab = "mfg wage mass"),
    list(var = "chg_log_nestab_85_54",     lab = "mfg establish."),
    list(var = "chg_log_nexp_88_60",       lab = "ag farms"),
    list(var = "chg_log_areatot_ha_88_60", lab = "ag farmed area")
)

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)

    dist_df <- geodesic_pairs()          # geolev2 pair -> km
    pop <- load_pop_1960()
    est <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))
    est <- ensure_geolev2_char(est)

    geo_ctrls <- setdiff(geo_controls_main,
                         c("logMA_actual_1960_s0_elow", "log_pop_1960"))
    ctrls <- c(geo_ctrls, "logMA_ineff_1960", "log_pop_1960")

    # Transformed (pair-varying) tau tables, one per case; also collect
    # the 1960 tau' distribution for the report.
    sym <- lapply(CASES, load_tau_ineff, pop = pop, dist_df = dist_df)
    names(sym) <- CASES
    t60 <- sym[["actual_1960"]]
    band <- quantile(t60$tau_ineff[t60$ok & !t60$dup],
                     c(0.10, 0.50, 0.90), names = FALSE)
    share_lt1 <- mean(t60$tau_ineff[t60$ok & !t60$dup] < 1)
    message(sprintf(
        "[1b] tau' (1960) p10/p50/p90 = %.2f/%.2f/%.2f | share < 1 = %.2f%%",
        band[1], band[2], band[3], 100 * share_lt1))

    rows <- list()
    desc <- list()
    for (th in THETAS) {
        res <- run_theta(th, sym, est, ctrls)
        rows[[length(rows) + 1L]] <- res$rows
        desc[[length(desc) + 1L]] <- res$desc
    }
    df   <- do.call(rbind, rows)
    dsc  <- do.call(rbind, desc)

    verify(df)
    write_outputs(df, dsc, band, share_lt1)
}

# ---------------------------------------------------------------------------
# Geodesic distances between the 03c centroids (WGS84 great-circle)
# ---------------------------------------------------------------------------
geodesic_pairs <- function() {
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
    cents <- sf::st_transform(cents, crs = 4326)

    D <- sf::st_distance(cents)                      # meters, geodesic
    D <- matrix(as.numeric(D), nrow = nrow(shp)) / 1000  # km
    stopifnot(isTRUE(all.equal(D, t(D))),
              all(diag(D) == 0),
              all(D[upper.tri(D)] > 0))

    # Anchor: BA - Rosario centroid geodesic = 265.9 km (scoping note,
    # verified against the cached tau matrices on 2026-07-14).
    i <- match("32002001", cents$geolev2)
    j <- match("32082001", cents$geolev2)
    stopifnot(!is.na(i), !is.na(j))
    stopifnot(abs(D[i, j] - 265.9) < 0.5)
    message(sprintf("[verify] BA-Rosario geodesic = %.1f km (note: 265.9)",
                    D[i, j]))

    g <- cents$geolev2
    data.frame(
        origin_geolev2      = rep(g, times = length(g)),
        destination_geolev2 = rep(g, each  = length(g)),
        dist_km             = as.vector(D),
        stringsAsFactors = FALSE
    )
}

# ---------------------------------------------------------------------------
# One theta: MA per case on the transformed tau, deltas, six outcomes
# ---------------------------------------------------------------------------
run_theta <- function(th, sym, est, ctrls) {
    lma <- lapply(sym, function(s) {
        w <- ifelse(s$ok, 1 / s$tau_ineff^th, 0)
        ma <- rowsum(w * s$pop_dest, group = s$origin_geolev2)
        data.frame(geolev2 = rownames(ma), logMA = log(ma[, 1]),
                   stringsAsFactors = FALSE)
    })
    d <- Reduce(function(a, b) merge(a, b, by = "geolev2"), list(
        setNames(lma[["actual_1960"]],        c("geolev2", "logMA_ineff_1960")),
        setNames(lma[["actual_1986"]],        c("geolev2", "lma86")),
        setNames(lma[["instrument_stu"]],     c("geolev2", "lma_stu")),
        setNames(lma[["instrument_lcp_mst"]], c("geolev2", "lma_lcp"))
    ))
    d$chg_ineff <- d$lma86   - d$logMA_ineff_1960
    d$z_stu     <- d$lma_stu - d$logMA_ineff_1960
    d$z_lcp     <- d$lma_lcp - d$logMA_ineff_1960

    m <- merge(est,
               d[, c("geolev2", "logMA_ineff_1960",
                     "chg_ineff", "z_stu", "z_lcp")],
               by = "geolev2", all.x = FALSE)
    stopifnot(nrow(m) == nrow(est))

    # Descriptives incl. correlation with the raw-object treatment
    desc <- data.frame(
        theta = th,
        dlm_gainshare = mean(d$chg_ineff > 0),
        dlm_mean      = mean(d$chg_ineff),
        dlm_sd        = sd(d$chg_ineff),
        cor_with_raw  = cor(m$chg_ineff, m[[main_treatment]],
                            use = "complete.obs"),
        stringsAsFactors = FALSE
    )

    out <- list()
    for (o in OUTCOMES) {
        fits <- fit_iv_quad(
            y          = o$var,
            data       = m,
            endog      = "chg_ineff",
            lp_instr   = "z_stu",
            hypo_instr = "z_lcp",
            ctrls_vec  = ctrls
        )
        co  <- safe_coef(fits[["IV-B"]],  "fit_chg_ineff")
        clp <- safe_coef(fits[["IV-LP"]], "fit_chg_ineff")
        # MOP effective F + critical values on the 1b first stages
        # (cr-review PR #137 consider 1: the classical-F vs effective-F
        # distinction is load-bearing at F ~ 8-12).
        mop_b  <- mop_check(m, o$var, "chg_ineff", c("z_stu", "z_lcp"),
                            ctrls)
        mop_lp <- mop_check(m, o$var, "chg_ineff", "z_stu", ctrls)
        out[[length(out) + 1L]] <- data.frame(
            theta = th, outcome = o$var, label = o$lab,
            ivb_beta  = co$est,  ivb_se  = co$se,  ivb_p  = co$p,
            ivb_F     = fitstat_F(fits[["IV-B"]]),
            ivb_Feff  = mop_b$F_eff,  ivb_cv10  = mop_b$cv10,
            ivb_pass10  = mop_b$F_eff > mop_b$cv10,
            ivb_cv20  = mop_b$cv20,
            ivb_pass20  = mop_b$F_eff > mop_b$cv20,
            ivlp_beta = clp$est, ivlp_se = clp$se, ivlp_p = clp$p,
            ivlp_F    = fitstat_F(fits[["IV-LP"]]),
            ivlp_Feff = mop_lp$F_eff, ivlp_cv10 = mop_lp$cv10,
            ivlp_pass10 = mop_lp$F_eff > mop_lp$cv10,
            ivlp_cv20 = mop_lp$cv20,
            ivlp_pass20 = mop_lp$F_eff > mop_lp$cv20,
            n_obs     = nobs(fits[["OLS"]]),
            stringsAsFactors = FALSE
        )
    }
    res <- do.call(rbind, out)
    message(sprintf(
        "[1b] th=%.2f | pop IVB %+0.3f (p=%.2f) F=%.1f | mfg val p=%.3f | F(LP)=%.1f",
        th, res$ivb_beta[1], res$ivb_p[1], res$ivb_F[1],
        res$ivb_p[res$outcome == "chg_log_valprod_85_54"],
        res$ivlp_F[1]))
    list(rows = res, desc = desc)
}

# ---------------------------------------------------------------------------
# Verification (construction-level; no raw-anchor nesting exists for 1b)
# ---------------------------------------------------------------------------
verify <- function(df) {
    stopifnot(nrow(df) == length(THETAS) * length(OUTCOMES))
    n_by_out <- df$n_obs[df$theta == THETAS[1]]
    names(n_by_out) <- df$outcome[df$theta == THETAS[1]]
    expect <- c(chg_log_pop_91_60 = 311L, chg_log_valprod_85_54 = 310L,
                chg_log_massal_85_54 = 309L, chg_log_nestab_85_54 = 310L,
                chg_log_nexp_88_60 = 297L, chg_log_areatot_ha_88_60 = 297L)
    stopifnot(identical(as.integer(n_by_out[names(expect)]),
                        as.integer(expect)))
    message("[verify] sample sizes match Tables 9/10 per outcome")
}

# ---------------------------------------------------------------------------
# Loaders — tau transform folded into the load (pair merge with 1/D)
# ---------------------------------------------------------------------------
load_pop_1960 <- function() {
    d <- arrow::read_parquet(
        file.path(dir_derived_census1960, "census_1960_ipums.parquet"))
    d <- ensure_geolev2_char(d)
    data.frame(geolev2 = d$geolev2, pop = as.numeric(d$pop),
               stringsAsFactors = FALSE)
}

load_tau_ineff <- function(case, pop, dist_df) {
    tau_df <- arrow::read_parquet(
        file.path(dir_derived_taus, sprintf("tau_%s_s0.parquet", case)))
    tau_df <- ensure_geolev2_char(tau_df, "origin_geolev2")
    tau_df <- ensure_geolev2_char(tau_df, "destination_geolev2")
    s <- rbind(
        data.frame(origin_geolev2      = tau_df$origin_geolev2,
                   destination_geolev2 = tau_df$destination_geolev2,
                   tau = tau_df$tau, dup = FALSE, stringsAsFactors = FALSE),
        data.frame(origin_geolev2      = tau_df$destination_geolev2,
                   destination_geolev2 = tau_df$origin_geolev2,
                   tau = tau_df$tau, dup = TRUE, stringsAsFactors = FALSE)
    )
    n0 <- nrow(s)
    s <- merge(s, dist_df,
               by = c("origin_geolev2", "destination_geolev2"),
               all.x = TRUE)
    stopifnot(nrow(s) == n0, !any(is.na(s$dist_km)), all(s$dist_km > 0))
    s <- merge(s,
               data.frame(destination_geolev2 = pop$geolev2,
                          pop_dest = pop$pop, stringsAsFactors = FALSE),
               by = "destination_geolev2", all.x = TRUE)
    s$pop_dest[is.na(s$pop_dest)] <- 0
    s$ok <- is.finite(s$tau) & s$tau > 0
    # tau' = cost / (c_min * D). Denominator converted to raster units:
    # cost_nav [pesos/ton-km] x dist [km] gives pesos/ton, and
    # tau_units_to_pesos converts pesos/ton to raster units (config 7b).
    denom <- cost_nav[["overall"]] * tau_units_to_pesos * s$dist_km
    s$tau_ineff <- ifelse(s$ok, s$tau / denom, Inf)
    s[order(s$origin_geolev2, s$destination_geolev2), ]
}

# ---------------------------------------------------------------------------
# MOP effective F + Patnaik critical values on the 1b first stage.
# Machinery copied from diagnostic_mop_critical.R (PR #136; reduced-form
# residuals per MOP's setup; exact bias bound B(W); alpha = 5%).
# ---------------------------------------------------------------------------
mop_check <- function(m, yvar, endog, instrs, ctrls) {
    vars <- c(yvar, endog, instrs, ctrls)
    d <- m[complete.cases(m[, vars]), vars]
    X <- as.matrix(cbind(1, d[, ctrls]))
    r <- function(v) as.numeric(qr.resid(qr(X), v))
    Dt <- r(d[[endog]])
    Yt <- r(d[[yvar]])
    Zt <- sapply(instrs, function(z) r(d[[z]]))
    Zt <- matrix(Zt, ncol = length(instrs))
    n <- nrow(d); k <- ncol(Zt)
    qz <- qr(Zt)
    pi_ <- qr.coef(qz, Dt)
    v2 <- qr.resid(qz, Dt)
    v1 <- qr.resid(qz, Yt)
    hc1 <- n / (n - ncol(X) - k)
    Q   <- crossprod(Zt)
    eQ  <- eigen(Q, symmetric = TRUE)
    Qih <- eQ$vectors %*% diag(1 / sqrt(eQ$values), k) %*% t(eQ$vectors)
    meat <- function(a, b) hc1 * crossprod(Zt * a, Zt * b)
    W1  <- Qih %*% meat(v1, v1) %*% Qih
    W2  <- Qih %*% meat(v2, v2) %*% Qih
    W12 <- Qih %*% meat(v1, v2) %*% Qih
    F_eff <- as.numeric(t(pi_) %*% Q %*% pi_ / sum(diag(W2)))
    B <- B_of_W(W1, W2, W12)
    list(F_eff = F_eff,
         cv10 = patnaik_cv(W2, B / 0.10),
         cv20 = patnaik_cv(W2, B / 0.20))
}

B_of_W <- function(W1, W2, W12) {
    trW2 <- sum(diag(W2))
    ratio_at <- function(b) {
        S12 <- W12 - b * W2
        S1  <- W1 - b * (W12 + t(W12)) + b^2 * W2
        sym <- (S12 + t(S12)) / 2
        ev  <- eigen(sym, symmetric = TRUE, only.values = TRUE)$values
        num <- max(abs(sum(diag(S12)) - 2 * min(ev)),
                   abs(sum(diag(S12)) - 2 * max(ev))) / trW2
        den <- sqrt(max(sum(diag(S1)), .Machine$double.eps) / trW2)
        num / den
    }
    evW2 <- eigen((W2 + t(W2)) / 2, symmetric = TRUE,
                  only.values = TRUE)$values
    lim  <- max(abs(sum(diag(W2)) - 2 * min(evW2)),
                abs(sum(diag(W2)) - 2 * max(evW2))) / trW2
    grid <- tan(seq(-pi / 2 + 1e-3, pi / 2 - 1e-3, length.out = 2001))
    vals <- vapply(grid, ratio_at, numeric(1))
    i    <- which.max(vals)
    ref  <- optimize(ratio_at, lower = grid[max(1, i - 1)],
                     upper = grid[min(length(grid), i + 1)],
                     maximum = TRUE)
    max(vals[i], ref$objective, lim)
}

patnaik_cv <- function(W2, d_tau) {
    trW2  <- sum(diag(W2))
    trW22 <- sum(diag(crossprod(W2)))
    lmax  <- max(eigen((W2 + t(W2)) / 2, symmetric = TRUE,
                       only.values = TRUE)$values)
    k_eff <- trW2^2 * (1 + 2 * d_tau) /
             (trW22 + 2 * d_tau * trW2 * lmax)
    qchisq(0.95, df = k_eff, ncp = k_eff * d_tau) / k_eff
}

# ---------------------------------------------------------------------------
# Report + CSV
# ---------------------------------------------------------------------------
write_outputs <- function(df, dsc, band, share_lt1) {
    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)
    csv_path <- file.path(dir_tables, "diagnostic_tau_inefficiency.csv")
    write.csv(df, csv_path, row.names = FALSE)

    txt_path <- file.path(dir_tables, "diagnostic_tau_inefficiency.txt")
    con <- file(txt_path, open = "wt")
    wline <- function(...) { line <- sprintf(...); cat(line, "\n")
                             cat(line, "\n", file = con) }
    star <- function(p) ifelse(p < 0.01, "***",
                       ifelse(p < 0.05, "**",
                       ifelse(p < 0.10, "*", "")))
    wline("%s", strrep("=", 92))
    wline("ROUTE-INEFFICIENCY TAU (Decision A option 1b; scoping note")
    wline("Construction 1b). tau' = cost / (c_min x geodesic distance):")
    wline("dimensionless, pair-varying, no external data. c_min and unit")
    wline("constants cancel in Delta log MA (the regression object is")
    wline("cost-per-geodesic-km); they only scale the reported tau' band.")
    wline("Table 9/10 spec, 1b-object baseline logMA control, HC1.")
    wline("Stars: * p<.10  ** p<.05  *** p<.01 ; HC1 SE")
    wline("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
    wline("%s", strrep("=", 92))
    wline("")
    wline("tau' (1960) p10/p50/p90 = %.2f / %.2f / %.2f ; share < 1 = %.2f%%",
          band[1], band[2], band[3], 100 * share_lt1)
    for (i in seq_len(nrow(dsc))) {
        r <- dsc[i, ]
        wline(paste0("theta %.2f: Delta log MA gain %.1f%%, mean %+.3f, ",
                     "sd %.3f; cor with raw-object treatment %.3f"),
              r$theta, 100 * r$dlm_gainshare, r$dlm_mean, r$dlm_sd,
              r$cor_with_raw)
    }
    for (spec in list(list(lab = "IV-Both", b = "ivb_beta", p = "ivb_p",
                           f = "ivb_F"),
                      list(lab = "IV-LP", b = "ivlp_beta", p = "ivlp_p",
                           f = "ivlp_F"))) {
        for (th in unique(df$theta)) {
            sub <- df[df$theta == th, ]
            wline("")
            wline("---- theta = %.2f (%s)  [F shown per outcome]", th, spec$lab)
            wline("%-16s %12s %8s %8s", "Outcome", "beta", "p", "F")
            for (i in seq_len(nrow(sub))) {
                r <- sub[i, ]
                wline("%-16s %9.3f%-3s %8.3f %8.1f",
                      r$label, r[[spec$b]], star(r[[spec$p]]),
                      r[[spec$p]], r[[spec$f]])
            }
        }
    }
    # Computed verdict
    wline("")
    wline("Verdict (computed):")
    for (th in unique(df$theta)) {
        sub <- df[df$theta == th, ]
        wline(paste0("- theta %.2f IV-Both: mfg value p = %.3f, wage p = ",
                     "%.3f; establishments/ag min p = %.3f; population ",
                     "p = %.3f, F = %.1f."),
              th,
              sub$ivb_p[sub$outcome == "chg_log_valprod_85_54"],
              sub$ivb_p[sub$outcome == "chg_log_massal_85_54"],
              min(sub$ivb_p[sub$outcome %in%
                  c("chg_log_nestab_85_54", "chg_log_nexp_88_60",
                    "chg_log_areatot_ha_88_60")]),
              sub$ivb_p[sub$outcome == "chg_log_pop_91_60"],
              sub$ivb_F[sub$outcome == "chg_log_pop_91_60"])
    }
    wline("")
    wline("MOP weak-instrument test on the 1b first stages (effective F vs")
    wline("Patnaik critical values, exact bias bound, alpha = 5%%; machinery")
    wline("from diagnostic_mop_critical.R):")
    for (th in unique(df$theta)) {
        sub <- df[df$theta == th, ]
        wline(paste0("- theta %.2f IV-B: F_eff %.1f-%.1f vs cv(10%%) ",
                     "%.1f-%.1f -> %d/%d outcomes pass at 10%%, %d/%d at ",
                     "20%%."),
              th, min(sub$ivb_Feff), max(sub$ivb_Feff),
              min(sub$ivb_cv10), max(sub$ivb_cv10),
              sum(sub$ivb_pass10), nrow(sub),
              sum(sub$ivb_pass20), nrow(sub))
        wline(paste0("- theta %.2f IV-LP: F_eff %.1f-%.1f vs cv(10%%) ",
                     "%.1f-%.1f -> %d/%d pass at 10%%, %d/%d at 20%%."),
              th, min(sub$ivlp_Feff), max(sub$ivlp_Feff),
              min(sub$ivlp_cv10), max(sub$ivlp_cv10),
              sum(sub$ivlp_pass10), nrow(sub),
              sum(sub$ivlp_pass20), nrow(sub))
    }
    wline("")
    wline("Full columns: diagnostic_tau_inefficiency.csv")
    close(con)
    message("\nSaved: ", txt_path)
    message("Saved: ", csv_path)
}

main()

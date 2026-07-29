# ===========================================================================
# diagnostic_crossobject_checks.R
#
# PURPOSE: the two cross-decision checks still open before the
#          2026-07-29 meeting, which decides the tau object (A), the
#          Table 7 placebo spec (B), and the instrument set (C) in the
#          same sitting:
#
#   PART 1 (A x B): does the placebo result survive on the candidate tau
#     objects? Run on the spec Table 7 ACTUALLY ADOPTED — "pop47": six
#     standardized geo + log pop 1947 + the baseline MA. PR #120's
#     ledger entry says "revisit after each MA-definition change", and
#     Decision A may still change the MA definition. Rows: raw anchor
#     (theta 4.55; must reproduce the committed pop47 numbers exactly),
#     decay theta = 0.5, iceberg V = 4,400 and 20,000 pesos/ton at
#     theta 8.22, and the route-inefficiency object at both thetas.
#
#     WAS full47 UNTIL 2026-07-29. full47 (same set, but dropping the MA
#     baseline) was the adoption candidate when this script was written,
#     and it had the convenience of removing any object-baseline choice.
#     PR #143 showed its clean null comes FROM dropping the MA baseline
#     rather than from the 1947 population swap, which is not a
#     defensible reason to drop it, so Table 7 adopted pop47 and this
#     part follows. The baseline is now object-consistent (`l60` from
#     ma_deltas), matching Part 2's convention, so the object choice is
#     handled rather than avoided. Note the consequence: the placebo is
#     NOT a clean null under the adopted spec, so Part 1 is now asking
#     whether the MARGINAL rejection is stable across objects rather
#     than whether a null survives.
#
#   PART 2 (A x C): the missing cell of the object x instrument
#     matrix — sectoral outcomes under IV-LP on the DECAY object.
#     PR #127 ran IV-Both only; the iceberg (PR #135) and 1b (PR #137)
#     cells are measured. Grid theta in {0.25, 0.5, 0.75}, six
#     outcomes, IV-B (anchor: must reproduce
#     diagnostic_theta_gibbons.csv) + IV-LP (the new content), with
#     the MOP weak-instrument test on each IV-LP cell (cr-review
#     PR #139 B1: 1b was judged by the MOP standard, so decay must be
#     too; at K=1 the exact bias bound is 1, so cv(10%) = 23.11 and
#     cv(20%) = 15.06 for every cell).
#
# DESIGN: MA recomputed per object from the four cached s0 tau
#   parquets with 1960 pop weights (machinery as the iceberg/1b
#   diagnostics, copied with provenance; diagnostics self-contained
#   by convention). Object transforms:
#     raw       tau                      (theta as specified)
#     decay     tau                      (theta = 0.5; the object is
#                                         the (raw cost, low theta) pair)
#     iceberg   1 + tau / (V * tau_units_to_pesos)
#     ineff     tau / (cost_nav * tau_units_to_pesos * geodesic_km)
#   Estimation via fit_iv_quad; controls per part (Part 1: pop47 set,
#   six geo + log_pop_1947 + object-consistent baseline logMA;
#   Part 2: six geo + log_pop_1960 + object-consistent baseline logMA,
#   matching the gibbons variant's convention). HC1 throughout.
#
# VERIFICATION (asserted in code, row-count-guarded):
#   - Part 1 raw anchor reproduces diagnostic_placebo_1947.csv's
#     pop47 rows (all four estimators, coef/se/F; N = 237).
#   - The raw object's recomputed l60 and chg equal the pipeline's
#     logMA_actual_1960_s0_elow and chg_logMA_86_60_s0_elow to 1e-12.
#     Asserted directly: the coefficient anchor alone cannot tell l60
#     from an affine transform of it.
#   - Part 2 IV-B reproduces diagnostic_theta_gibbons.csv beta/se
#     per (outcome, theta) on all 18 cells.
#   - BA-Rosario geodesic = 265.9 km (1b machinery anchor).
#
# READS:
#   data/derived/03_taus/tau_{actual_1960,actual_1986,
#                              instrument_stu,instrument_lcp_mst}_s0.parquet
#   data/raw/geo/geo2_ar1970_2010.shp
#   data/derived/base/census_1960/census_1960_ipums.parquet
#   data/derived/06_analysis/estimation_sample.parquet
#   results/tables/diagnostic_placebo_1947.csv     (anchor)
#   results/tables/diagnostic_theta_gibbons.csv    (anchor)
#
# PRODUCES (diagnostic only; no paper exhibit, no main.R wiring):
#   results/tables/diagnostic_crossobject_checks.txt
#   results/tables/diagnostic_crossobject_checks.csv
# ===========================================================================
suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
    library(sf)
})

CASES <- c("actual_1960", "actual_1986", "instrument_stu", "instrument_lcp_mst")

PLACEBO_OBJECTS <- list(
    list(id = "raw (anchor)",    kind = "raw",     theta = 4.55, V = NA),
    list(id = "decay th=0.5",    kind = "raw",     theta = 0.50, V = NA),
    list(id = "iceberg V=4400",  kind = "iceberg", theta = 8.22, V = 4400),
    list(id = "iceberg V=20000", kind = "iceberg", theta = 8.22, V = 20000),
    list(id = "ineff th=4.55",   kind = "ineff",   theta = 4.55, V = NA),
    list(id = "ineff th=8.22",   kind = "ineff",   theta = 8.22, V = NA)
)

DECAY_GRID <- c(0.25, 0.50, 0.75)

OUTCOMES <- list(
    list(var = "chg_log_pop_91_60",        lab = "population"),
    list(var = "chg_log_valprod_85_54",    lab = "mfg production value"),
    list(var = "chg_log_massal_85_54",     lab = "mfg wage mass"),
    list(var = "chg_log_nestab_85_54",     lab = "mfg establishments"),
    list(var = "chg_log_nexp_88_60",       lab = "ag farms"),
    list(var = "chg_log_areatot_ha_88_60", lab = "ag farmed area")
)

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)

    pop <- load_pop_1960()
    sym <- lapply(CASES, load_tau_sym, pop = pop)
    names(sym) <- CASES
    dist_df <- geodesic_pairs()
    est <- ensure_geolev2_char(as.data.frame(arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))))
    est$log_pop_1947 <- ifelse(!is.na(est$pop_1947) & est$pop_1947 > 0,
                               log(est$pop_1947), NA_real_)
    geo6 <- setdiff(geo_controls_main,
                    c("logMA_actual_1960_s0_elow", "log_pop_1960"))

    p1 <- run_placebo(sym, dist_df, est, geo6)
    p2 <- run_decay_ivlp(sym, est, geo6)

    verify_placebo(p1)
    verify_decay(p2)
    write_outputs(p1, p2)
}

# ---------------------------------------------------------------------------
# Object-transformed MA deltas: geolev2, chg, z_stu, z_lcp, l60
# ---------------------------------------------------------------------------
ma_deltas <- function(obj, sym, dist_df) {
    transform <- function(s) {
        if (obj$kind == "raw") return(s$tau)
        if (obj$kind == "iceberg") {
            return(1 + s$tau / (obj$V * tau_units_to_pesos))
        }
        # ineff: pair-varying denominator via the geodesic distances
        key <- paste(s$origin_geolev2, s$destination_geolev2)
        d   <- dist_df$dist_km[match(key, dist_df$key)]
        stopifnot(!any(is.na(d)))
        s$tau / (cost_nav[["overall"]] * tau_units_to_pesos * d)
    }
    lma <- lapply(sym, function(s) {
        tt <- transform(s)
        w  <- ifelse(s$ok, 1 / tt^obj$theta, 0)
        ma <- rowsum(w * s$pop_dest, group = s$origin_geolev2)
        data.frame(geolev2 = rownames(ma), logMA = log(ma[, 1]),
                   stringsAsFactors = FALSE)
    })
    d <- Reduce(function(a, b) merge(a, b, by = "geolev2"), list(
        setNames(lma[["actual_1960"]],        c("geolev2", "l60")),
        setNames(lma[["actual_1986"]],        c("geolev2", "l86")),
        setNames(lma[["instrument_stu"]],     c("geolev2", "lstu")),
        setNames(lma[["instrument_lcp_mst"]], c("geolev2", "llcp"))
    ))
    d$chg   <- d$l86  - d$l60
    d$z_stu <- d$lstu - d$l60
    d$z_lcp <- d$llcp - d$l60
    d[, c("geolev2", "chg", "z_stu", "z_lcp", "l60")]
}

# ---------------------------------------------------------------------------
# Part 1: the ADOPTED (pop47) placebo spec on each candidate object
# ---------------------------------------------------------------------------
run_placebo <- function(sym, dist_df, est, geo6) {
    y <- "chg_log_placebo_pop_60_47"
    rows <- list()
    for (obj in PLACEBO_OBJECTS) {
        ma <- ma_deltas(obj, sym, dist_df)
        m  <- merge(est, ma, by = "geolev2", all.x = FALSE)
        stopifnot(nrow(m) == nrow(est))
        # The raw object at the published theta must reproduce the
        # pipeline's own MA columns exactly -- the recomputation here is
        # only trustworthy for the transformed objects if it is exact on
        # the untransformed one. Asserted directly rather than inferred
        # from the coefficient anchor, which cannot distinguish l60 from
        # an affine transform of it (cr-review PR #154).
        if (obj$id == "raw (anchor)") {
            stopifnot(
                max(abs(m$l60 - m$logMA_actual_1960_s0_elow)) < 1e-12,
                max(abs(m$chg - m$chg_logMA_86_60_s0_elow))   < 1e-12
            )
        }
        fits <- fit_iv_quad(
            y          = y,
            data       = m,
            endog      = "chg",
            lp_instr   = "z_stu",
            hypo_instr = "z_lcp",
            # pop47, the spec Table 7 adopted: six geo + the 1947
            # population baseline + the baseline MA. The MA baseline is
            # `l60`, recomputed FROM THIS OBJECT by ma_deltas(), not the
            # pipeline's fixed logMA_actual_1960_s0_elow -- the same
            # object-consistency convention Part 2 uses. Using the fixed
            # s0 raw column would control for a baseline built from a
            # different tau object than the treatment.
            ctrls_vec  = c(geo6, "log_pop_1947", "l60")
        )
        for (k in c("OLS", "IV-LP", "IV-H", "IV-B")) {
            cn <- if (k == "OLS") "chg" else "fit_chg"
            co <- safe_coef(fits[[k]], cn)
            rows[[length(rows) + 1L]] <- data.frame(
                part = "placebo_pop47", object = obj$id,
                theta = obj$theta, spec = k,
                beta = co$est, se = co$se, p = co$p,
                F = if (k == "OLS") NA_real_ else fitstat_F(fits[[k]]),
                n_obs = nobs(fits[[k]]),
                stringsAsFactors = FALSE
            )
        }
        r <- rows[[length(rows)]]
        message(sprintf(
            "[p1] %-16s IV-B %+7.4f (p=%.3f) F=%5.1f N=%d",
            obj$id, r$beta, r$p, r$F, r$n_obs))
    }
    do.call(rbind, rows)
}

# ---------------------------------------------------------------------------
# Part 2: decay grid, six outcomes, IV-B (anchor) + IV-LP (new)
# ---------------------------------------------------------------------------
run_decay_ivlp <- function(sym, est, geo6) {
    rows <- list()
    for (th in DECAY_GRID) {
        obj <- list(kind = "raw", theta = th, V = NA)
        ma <- ma_deltas(obj, sym, dist_df = NULL)
        m  <- merge(est, ma, by = "geolev2", all.x = FALSE)
        stopifnot(nrow(m) == nrow(est))
        for (o in OUTCOMES) {
            fits <- fit_iv_quad(
                y          = o$var,
                data       = m,
                endog      = "chg",
                lp_instr   = "z_stu",
                hypo_instr = "z_lcp",
                ctrls_vec  = c(geo6, "log_pop_1960", "l60")
            )
            co  <- safe_coef(fits[["IV-B"]],  "fit_chg")
            clp <- safe_coef(fits[["IV-LP"]], "fit_chg")
            # Same-sample guard: fit_iv_quad NA-drops per model
            # (cr-review PR #139 consider 3).
            stopifnot(nobs(fits[["IV-LP"]]) == nobs(fits[["IV-B"]]))
            mop <- mop_check(m, o$var, "chg", "z_stu",
                             c(geo6, "log_pop_1960", "l60"))
            rows[[length(rows) + 1L]] <- data.frame(
                part = "decay_ivlp", outcome = o$lab, theta = th,
                ivb_beta = co$est, ivb_se = co$se, ivb_p = co$p,
                ivb_F = fitstat_F(fits[["IV-B"]]),
                ivlp_beta = clp$est, ivlp_se = clp$se, ivlp_p = clp$p,
                ivlp_F = fitstat_F(fits[["IV-LP"]]),
                ivlp_Feff = mop$F_eff,
                ivlp_cv10 = mop$cv10, ivlp_pass10 = mop$F_eff > mop$cv10,
                ivlp_cv20 = mop$cv20, ivlp_pass20 = mop$F_eff > mop$cv20,
                n_obs = nobs(fits[["IV-B"]]),
                stringsAsFactors = FALSE
            )
        }
        sub <- rows[(length(rows) - 5):length(rows)]
        v <- do.call(rbind, sub)
        message(sprintf(
            "[p2] th=%.2f | IV-LP mfg val p=%.3f wage p=%.3f | F(LP)=%.1f",
            th, v$ivlp_p[v$outcome == "mfg production value"],
            v$ivlp_p[v$outcome == "mfg wage mass"], v$ivlp_F[1]))
    }
    do.call(rbind, rows)
}

# ---------------------------------------------------------------------------
# MOP effective F + Patnaik critical values — copied from
# diagnostic_tau_inefficiency.R (PR #137; itself per diagnostic_
# mop_critical.R, PR #136). At K=1 the bias bound B is exactly 1, so
# the cvs reduce to the fixed MOP K=1 thresholds (23.11 / 15.06).
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
# Verification anchors
# ---------------------------------------------------------------------------
verify_placebo <- function(p1) {
    tol <- 1e-9
    ref <- read.csv(file.path(dir_tables, "diagnostic_placebo_1947.csv"))
    # pop47, not full47: Table 7 adopted the pop47 spec (agenda item B).
    # This anchor is stronger than the full47 one it replaces -- full47
    # carries no MA baseline, so reproducing it said nothing about the MA
    # recomputation in ma_deltas(), whereas pop47 controls for `l60`.
    # BUT NOT AS STRONG AS "l60 EQUALS THE PIPELINE COLUMN" (cr-review
    # PR #154): coefficients are invariant to an affine reparametrization
    # of a control and to adding any linear combination of the other
    # regressors, so l60' = a + b*logMA_1960 + 3*elev_mean_std would pass
    # this test identically. It pins l60 only up to an affine transform
    # within the span of the remaining controls. The exact equality is
    # asserted separately in run_placebo(), where the raw object's l60 is
    # compared with the pipeline column directly.
    ref <- ref[ref$variant == "pop47", ]
    ours <- p1[p1$object == "raw (anchor)", ]
    stopifnot(nrow(ours) == 4L)
    for (k in c("OLS", "IV-LP", "IV-H", "IV-B")) {
        o <- ours[ours$spec == k, ]
        rc <- ref$value[ref$spec == k & ref$stat == "coef"]
        rs <- ref$value[ref$spec == k & ref$stat == "se"]
        rn <- ref$value[ref$spec == k & ref$stat == "N"]
        rf <- ref$value[ref$spec == k & ref$stat == "F"]
        stopifnot(length(rc) == 1L, length(rs) == 1L, length(rn) == 1L,
                  length(rf) == 1L)
        stopifnot(abs(o$beta - rc) < tol, abs(o$se - rs) < tol,
                  o$n_obs == rn)
        # F was previously unanchored while the write-ups quoted it
        # (cr-review PR #154). NA for OLS on both sides.
        stopifnot(isTRUE(all.equal(o$F, rf, tolerance = 1e-8)))
    }
    # Common-sample check across all six objects (cr-review PR #139
    # consider 1; the reference script enforced this explicitly).
    stopifnot(all(p1$n_obs == 237L))
    message("[verify] raw placebo anchor == diagnostic_placebo_1947 pop47")
}

verify_decay <- function(p2) {
    tol <- 1e-9
    ref <- read.csv(file.path(dir_tables, "diagnostic_theta_gibbons.csv"))
    chk <- merge(p2, ref[, c("outcome", "theta", "beta", "se", "n_obs")],
                 by = c("outcome", "theta"), suffixes = c("", "_ref"))
    stopifnot(nrow(chk) == nrow(p2), nrow(p2) == 18L)
    stopifnot(max(abs(chk$ivb_beta - chk$beta)) < tol,
              max(abs(chk$ivb_se   - chk$se))   < tol,
              all(chk$n_obs == chk$n_obs_ref))
    message("[verify] decay IV-B == diagnostic_theta_gibbons (18 cells)")
}

# ---------------------------------------------------------------------------
# Loaders — copied from diagnostic_tau_inefficiency.R (provenance)
# ---------------------------------------------------------------------------
load_pop_1960 <- function() {
    d <- arrow::read_parquet(
        file.path(dir_derived_census1960, "census_1960_ipums.parquet"))
    d <- ensure_geolev2_char(d)
    data.frame(geolev2 = d$geolev2, pop = as.numeric(d$pop),
               stringsAsFactors = FALSE)
}

load_tau_sym <- function(case, pop) {
    tau_df <- arrow::read_parquet(
        file.path(dir_derived_taus, sprintf("tau_%s_s0.parquet", case)))
    tau_df <- ensure_geolev2_char(tau_df, "origin_geolev2")
    tau_df <- ensure_geolev2_char(tau_df, "destination_geolev2")
    s <- rbind(
        data.frame(origin_geolev2      = tau_df$origin_geolev2,
                   destination_geolev2 = tau_df$destination_geolev2,
                   tau = tau_df$tau, stringsAsFactors = FALSE),
        data.frame(origin_geolev2      = tau_df$destination_geolev2,
                   destination_geolev2 = tau_df$origin_geolev2,
                   tau = tau_df$tau, stringsAsFactors = FALSE)
    )
    s <- merge(s,
               data.frame(destination_geolev2 = pop$geolev2,
                          pop_dest = pop$pop, stringsAsFactors = FALSE),
               by = "destination_geolev2", all.x = TRUE)
    s$pop_dest[is.na(s$pop_dest)] <- 0
    s$ok <- is.finite(s$tau) & s$tau > 0
    s[order(s$origin_geolev2, s$destination_geolev2), ]
}

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
    D <- sf::st_distance(cents)
    D <- matrix(as.numeric(D), nrow = nrow(shp)) / 1000
    i <- match("32002001", cents$geolev2)
    j <- match("32082001", cents$geolev2)
    stopifnot(!is.na(i), !is.na(j), abs(D[i, j] - 265.9) < 0.5)
    message(sprintf("[verify] BA-Rosario geodesic = %.1f km (note: 265.9)",
                    D[i, j]))
    g <- cents$geolev2
    out <- data.frame(
        origin_geolev2      = rep(g, times = length(g)),
        destination_geolev2 = rep(g, each  = length(g)),
        dist_km             = as.vector(D),
        stringsAsFactors = FALSE
    )
    out$key <- paste(out$origin_geolev2, out$destination_geolev2)
    out
}

# ---------------------------------------------------------------------------
# Report + CSV
# ---------------------------------------------------------------------------
write_outputs <- function(p1, p2) {
    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)
    csv_path <- file.path(dir_tables, "diagnostic_crossobject_checks.csv")
    # Column-union rbind via merge: safe only while the 'part' values
    # are disjoint — assert no row multiplication (cr-review PR #139 SF3).
    all_rows <- merge(p1, p2, all = TRUE)
    stopifnot(nrow(all_rows) == nrow(p1) + nrow(p2))
    write.csv(all_rows, csv_path, row.names = FALSE)

    txt_path <- file.path(dir_tables, "diagnostic_crossobject_checks.txt")
    con <- file(txt_path, open = "wt")
    wline <- function(...) { line <- sprintf(...); cat(line, "\n")
                             cat(line, "\n", file = con) }
    star <- function(p) ifelse(p < 0.01, "***",
                       ifelse(p < 0.05, "**",
                       ifelse(p < 0.10, "*", "")))
    wline("%s", strrep("=", 92))
    wline("CROSS-DECISION CHECKS BEFORE THE 2026-07-29 MEETING")
    wline("Part 1 (A x B): the adopted pop47 placebo spec on each candidate tau")
    wline("object. Part 2 (A x C): sectoral outcomes under IV-LP on the")
    wline("decay object (completes the object x instrument matrix).")
    wline("Stars: * p<.10  ** p<.05  *** p<.01 ; HC1 SE")
    wline("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
    wline("%s", strrep("=", 92))
    wline("")
    wline("PART 1 — placebo (1947-60 growth), pop47 controls, by object:")
    wline("%-17s %5s  %-17s %-17s %-17s %-17s",
          "Object", "theta", "OLS", "IV-LP", "IV-H", "IV-B")
    for (obj in unique(p1$object)) {
        sub <- p1[p1$object == obj, ]
        cell <- function(k) {
            r <- sub[sub$spec == k, ]
            sprintf("%+.3f%s p=%.2f", r$beta, star(r$p), r$p)
        }
        # %-17s not %-15s: pop47 produces significance stars, and
        # "+0.588*** p=0.01" is 16 characters (cr-review PR #154).
        wline("%-17s %5.2f  %-17s %-17s %-17s %-17s",
              obj, sub$theta[1],
              cell("OLS"), cell("IV-LP"), cell("IV-H"), cell("IV-B"))
    }
    fb <- p1[p1$spec == "IV-B", ]
    wline("First-stage F by object, IV-B: %s",
          paste(sprintf("%s %.1f", fb$object, fb$F), collapse = "; "))
    flp <- p1[p1$spec == "IV-LP", ]
    wline("First-stage F by object, IV-LP: %s",
          paste(sprintf("%s %.1f", flp$object, flp$F), collapse = "; "))
    wline("")
    # VERDICT. Rewritten for pop47 (cr-review PR #154): the previous text
    # summarised IV-B by its MAXIMUM p, which is the right statistic for a
    # "the null survives everywhere" claim and the wrong one now that the
    # question is whether a rejection is stable. Under pop47 it printed
    # "far from conventional levels" directly above a p of 0.042. Every
    # number below is computed, and the object names are looked up rather
    # than hard-coded, so this block cannot go stale against the table
    # again.
    ivb <- p1[p1$spec == "IV-B", ]
    n10 <- sum(ivb$p < 0.10); n05 <- sum(ivb$p < 0.05)
    wline("Part 1 verdict: the placebo rejection is NOT dissolved by the")
    wline("tau object. IV-B rejects at 10%% on %d of %d objects and at 5%%",
          n10, nrow(ivb))
    wline("on %d (min p = %.3f, on %s; max p = %.3f, on %s).",
          n05, min(ivb$p), ivb$object[which.min(ivb$p)],
          max(ivb$p), ivb$object[which.max(ivb$p)])
    wline("Coefficient MAGNITUDES are not comparable across objects (the")
    wline("tau scales differ); the p-values are.")
    wline("")
    wline("CAVEAT 1 — first stages are weaker than under full47. IV-B F by")
    wline("object runs %.1f-%.1f here against 21.5-61.1 under full47, and",
          min(ivb$F), max(ivb$F))
    wline("%d of the %d rejections at 10%% sit on F < 10, so read them as",
          sum(ivb$p < 0.10 & ivb$F < 10), n10)
    wline("suggestive rather than firm.")
    wline("CAVEAT 2 — the weakest IV-LP first stages are %s (F = %.1f)",
          flp$object[order(flp$F)][1], sort(flp$F)[1])
    wline("and %s (F = %.1f); IV-LP results on those two objects are",
          flp$object[order(flp$F)][2], sort(flp$F)[2])
    wline("low-power non-results either way.")
    wline("")
    wline("PART 2 — decay object, IV-LP (new) vs IV-B (PR #127 anchor):")
    for (th in unique(p2$theta)) {
        sub <- p2[p2$theta == th, ]
        wline("")
        wline("---- theta = %.2f   [beta, p, F]", th)
        wline("%-22s %14s %22s", "Outcome", "IV-B", "IV-LP")
        for (i in seq_len(nrow(sub))) {
            r <- sub[i, ]
            wline("%-22s %8.3f%-3s p=%.3f %8.3f%-3s p=%.3f  F(LP)=%.1f",
                  r$outcome, r$ivb_beta, star(r$ivb_p), r$ivb_p,
                  r$ivlp_beta, star(r$ivlp_p), r$ivlp_p, r$ivlp_F)
        }
    }
    mfg <- p2[p2$outcome %in% c("mfg production value", "mfg wage mass"), ]
    wline("")
    wline("Part 2 verdict: under IV-LP on the decay object, mfg value/wage")
    wline("max p = %.3f across the grid (IV-B max p = %.3f); F(LP) range",
          max(mfg$ivlp_p), max(mfg$ivb_p))
    wline("%.1f-%.1f vs F(IV-B) %.1f-%.1f.",
          min(p2$ivlp_F), max(p2$ivlp_F), min(p2$ivb_F), max(p2$ivb_F))
    wline("")
    wline("MOP test on the decay IV-LP cells (same standard that judged")
    wline("1b; K=1 so cv(10%%) = %.2f and cv(20%%) = %.2f for every cell):",
          p2$ivlp_cv10[1], p2$ivlp_cv20[1])
    wline("F_eff range %.1f-%.1f -> %d/%d cells pass at 10%%, %d/%d at",
          min(p2$ivlp_Feff), max(p2$ivlp_Feff),
          sum(p2$ivlp_pass10), nrow(p2), sum(p2$ivlp_pass20), nrow(p2))
    wline("20%%. READ TOGETHER: decay is the least-weak IV-LP candidate —")
    wline("classical 5%% significance holds across its whole grid, unlike")
    wline("the iceberg (low V only) and 1b (nowhere) — but by the MOP")
    wline("standard that disqualified 1b, decay IV-LP is also weak. No")
    wline("candidate object makes IV-LP-only MOP-strong.")
    wline("")
    wline("Full columns: diagnostic_crossobject_checks.csv")
    close(con)
    message("\nSaved: ", txt_path)
    message("Saved: ", csv_path)
}

main()

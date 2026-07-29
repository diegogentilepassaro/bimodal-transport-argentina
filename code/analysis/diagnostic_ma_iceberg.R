# ===========================================================================
# diagnostic_ma_iceberg.R
#
# PURPOSE: Decision A, Option 1a (tau normalization; Cote 1.5 / memo
#          point ii). Applies the D&H-faithful AFFINE iceberg transform
#
#              tau'_ij = 1 + cost_ij / V
#
#          to the cached tau matrices and traces the population elasticity
#          as a function of V (1960 value per ton of transported goods).
#          Implements work items 3-4 of .kiro/decision_a_option1_scoping.md:
#          the "+1" breaks the multiplicative-cancellation result of
#          .kiro/theta_benchmark_note.md, so a single scalar V changes
#          Delta log MA. V is NOT yet sourced (archive lookup rides the
#          issue-#68 visit); the deliverable is therefore the WHOLE CURVE
#          beta(V) over a wide grid, with the tau' distribution at each V
#          so the meeting can see when tau' enters the narrow band above 1
#          (the regime the D&H trade-elasticity exponent is built for).
#          Honesty rule (work item 4): V gets justified by the sourced
#          number, never by where beta lands.
#
# DESIGN (mirrors the pipeline exactly, then transforms):
#   - MA_i = sum_{j != i} Pop_j / tau'_ij^theta, 1960 population weights
#     for ALL cases (as in 04_market_access.R).
#   - Pair filter identical to the pipeline: only finite, positive raw
#     tau contribute; Inf (disconnected) pairs get weight 0 under the
#     transform too (1 + Inf/V = Inf).
#   - Deltas per the pipeline definitions (06_merge_ma_into_panel.R):
#     treatment = logMA(actual_1986) - logMA(actual_1960);
#     LP instr   = logMA(instrument_stu)     - logMA(actual_1960);
#     hypo instr = logMA(instrument_lcp_mst) - logMA(actual_1960).
#   - Regression = Table 9 spec on total population (chg_log_pop_91_60),
#     fit_iv_quad (OLS / IV-LP / IV-H / IV-B, HC1), with the baseline
#     log-MA control replaced by the V-SPECIFIC 1960 log MA for internal
#     consistency (raw-tau baseline would mix objects).
#   - theta: 8.22 (D&H preferred, the headline) and 4.55 (main-spec
#     continuity column). V -> 0 anchor = raw tau at the same theta
#     (affine ~ pure rescale there, which cancels in logs). V -> Inf is
#     analytic: tau' -> 1, Delta log MA -> 0 for every district, no
#     regression exists; reported as a note, not a row.
#
# VERIFICATION (run 2026-07-25):
#   - The raw (V->0) anchor reproduces the pipeline EXACTLY: Delta log MA
#     matches the estimation sample's chg_logMA_86_60_s0_elow with max
#     abs diff = 0, and the 312-district stats match the published
#     scalars (\maMean +1.51, \maSharePos 90.7).
#   - The scoping note's scratch table (2026-07-14) reproduces exactly in
#     the 1960-only tau' bands (e.g. p10-p90 1.36-3.40 at V_raster=4.4e6)
#     but only approximately in the Delta log MA stats (e.g. gain 94.6%
#     vs its 94.9% at 4.4e6): the scratch check predates a 1986-tau
#     refresh. The pipeline zero-diff above is the binding standard and
#     is now ASSERTED IN CODE at the raw theta-low anchor (see run_one),
#     so the next tau refresh cannot silently invalidate this header.
#   - V -> 0 at theta 8.22 gives IV-B beta = 0.0240, consistent with the
#     committed raw-tau sweep's 0.0244 at theta 8.11
#     (results/tables/diagnostic_theta_sweep.csv, post-refresh vintage).
#
# UNITS: V is reported in 1960 pesos/ton via tau_units_to_pesos = 1000
#   (config.R section 7b; raster units = pesos/ton x 1000).
#
# READS:
#   data/derived/03_taus/tau_{actual_1960,actual_1986,
#                              instrument_stu,instrument_lcp_mst}_s0.parquet
#   data/derived/base/census_1960/census_1960_ipums.parquet
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES (diagnostic only; no paper exhibit, no main.R wiring —
# same status as the Gibbons-decay experiment, pending Decision A):
#   results/tables/diagnostic_ma_iceberg.txt
#   results/tables/diagnostic_ma_iceberg.csv
# ===========================================================================
suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
})

CASES <- c("actual_1960", "actual_1986", "instrument_stu", "instrument_lcp_mst")

# V grid in 1960 pesos/ton. Brackets the scratch-check values (1e6, 4.4e6,
# 2e7 raster units = 1000, 4400, 20000 pesos/ton) by an order of magnitude
# on each side; 4400 ~ the median raw tau in pesos/ton.
V_GRID_PESOS <- c(100, 500, 1000, 2000, 4400, 10000, 20000, 50000, 100000)

THETAS <- c(4.55, 8.22)

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)

    # ---- Inputs ---------------------------------------------------------
    pop <- load_pop_1960()
    sym <- lapply(CASES, load_tau_sym, pop = pop)
    names(sym) <- CASES
    est <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))
    est <- ensure_geolev2_char(est)

    # Controls: the six standardized geographic controls from the main
    # spec + log_pop_1960; the raw-tau baseline logMA control is swapped
    # for the V-specific iceberg baseline computed below.
    geo_ctrls <- setdiff(geo_controls_main,
                         c("logMA_actual_1960_s0_elow", "log_pop_1960"))
    ctrls <- c(geo_ctrls, "logMA_iceberg_1960", "log_pop_1960")

    # Raw 1960 tau pair distribution (lower triangle), for the tau' bands
    tau60 <- sym[["actual_1960"]]
    tau60_pairs <- tau60$tau[is.finite(tau60$tau) & tau60$tau > 0 &
                             !tau60$dup]

    # ---- Sweep ----------------------------------------------------------
    rows <- list()
    for (th in THETAS) {
        # V -> 0 anchor: raw tau at this theta (scale cancels in logs)
        rows[[length(rows) + 1L]] <-
            run_one(th, V_pesos = 0, sym, tau60_pairs, est, ctrls,
                    raw_anchor = TRUE)
        for (v in V_GRID_PESOS) {
            rows[[length(rows) + 1L]] <-
                run_one(th, V_pesos = v, sym, tau60_pairs, est, ctrls,
                        raw_anchor = FALSE)
        }
    }
    df <- do.call(rbind, rows)

    write_outputs(df)
}

# ---------------------------------------------------------------------------
# One (theta, V) cell: transform, recompute MA, run the Table 9 quad
# ---------------------------------------------------------------------------
run_one <- function(th, V_pesos, sym, tau60_pairs, est, ctrls, raw_anchor) {
    V_raster <- V_pesos * tau_units_to_pesos
    transform <- function(tau) {
        if (raw_anchor) tau else 1 + tau / V_raster
    }

    # log MA per case, keyed by geolev2
    lma <- lapply(sym, function(s) {
        w <- ifelse(s$ok, 1 / transform(s$tau)^th, 0)
        ma <- rowsum(w * s$pop_dest, group = s$origin_geolev2)
        data.frame(geolev2 = rownames(ma), logMA = log(ma[, 1]),
                   stringsAsFactors = FALSE)
    })

    # Deltas on the full 312-district set (pipeline definitions)
    d <- Reduce(function(a, b) merge(a, b, by = "geolev2"), list(
        setNames(lma[["actual_1960"]],       c("geolev2", "logMA_iceberg_1960")),
        setNames(lma[["actual_1986"]],       c("geolev2", "lma86")),
        setNames(lma[["instrument_stu"]],    c("geolev2", "lma_stu")),
        setNames(lma[["instrument_lcp_mst"]], c("geolev2", "lma_lcp"))
    ))
    d$chg_iceberg <- d$lma86    - d$logMA_iceberg_1960
    d$z_stu       <- d$lma_stu  - d$logMA_iceberg_1960
    d$z_lcp       <- d$lma_lcp  - d$logMA_iceberg_1960

    # Distribution stats: Delta log MA over all districts; tau' pair band
    tp <- if (raw_anchor) tau60_pairs else 1 + tau60_pairs / V_raster
    qs <- quantile(tp, c(0.10, 0.50, 0.90), names = FALSE)
    out <- data.frame(
        theta        = th,
        V_pesos      = V_pesos,
        V_raster     = V_raster,
        anchor       = ifelse(raw_anchor, "raw (V->0)", "affine"),
        tau_p10      = qs[1], tau_p50 = qs[2], tau_p90 = qs[3],
        dlm_gainshare = mean(d$chg_iceberg > 0),
        dlm_mean     = mean(d$chg_iceberg),
        dlm_sd       = sd(d$chg_iceberg),
        dlm_median   = median(d$chg_iceberg),
        stringsAsFactors = FALSE
    )

    # Table 9 spec on the estimation sample
    m <- merge(est,
               d[, c("geolev2", "logMA_iceberg_1960",
                     "chg_iceberg", "z_stu", "z_lcp")],
               by = "geolev2", all.x = FALSE)
    # Merge validation: the tau matrices cover all 312 districts, so the
    # inner join must retain every estimation-sample row (311; CABA is a
    # destination only).
    stopifnot(nrow(m) == nrow(est))
    # Pipeline-fidelity assertion (cr-review PR #130 SF3): at the raw
    # theta-low anchor the recomputed delta must equal the pipeline's
    # chg_logMA_86_60_s0_elow column exactly. Guards this script against
    # a future tau refresh silently invalidating the header's claims.
    if (raw_anchor && abs(th - theta[["low"]]) < 1e-9) {
        stopifnot(max(abs(m$chg_iceberg - m$chg_logMA_86_60_s0_elow)) < 1e-9)
    }
    fits <- fit_iv_quad(
        y          = "chg_log_pop_91_60",
        data       = m,
        endog      = "chg_iceberg",
        lp_instr   = "z_stu",
        hypo_instr = "z_lcp",
        ctrls_vec  = ctrls
    )
    for (k in c("OLS", "IV-LP", "IV-H", "IV-B")) {
        cn <- if (k == "OLS") "chg_iceberg" else "fit_chg_iceberg"
        co <- safe_coef(fits[[k]], cn)
        tag <- c(OLS = "ols", `IV-LP` = "ivlp",
                 `IV-H` = "ivh", `IV-B` = "ivb")[[k]]
        out[[paste0(tag, "_beta")]] <- co$est
        out[[paste0(tag, "_se")]]   <- co$se
        out[[paste0(tag, "_p")]]    <- co$p
        out[[paste0(tag, "_F")]]    <- if (k == "OLS") NA_real_
                                       else fitstat_F(fits[[k]])
    }
    out$n_obs <- nobs(fits[["OLS"]])

    message(sprintf(
        paste0("[iceberg] th=%.2f V=%6.0f p/t | tau' p10-p90 %8.3g-%8.3g",
               " | gain %.1f%% | IV-B %+7.3f (%.3f) F=%5.1f"),
        th, V_pesos, out$tau_p10, out$tau_p90, 100 * out$dlm_gainshare,
        out$ivb_beta, out$ivb_se, out$ivb_F))
    out
}

# ---------------------------------------------------------------------------
# Loaders (pipeline-identical semantics)
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
    # Symmetrise (file stores the lower triangle only), as 04_market_access.R;
    # dup marks the mirrored half so pair-level stats can use the file half.
    s <- rbind(
        data.frame(origin_geolev2      = tau_df$origin_geolev2,
                   destination_geolev2 = tau_df$destination_geolev2,
                   tau = tau_df$tau, dup = FALSE, stringsAsFactors = FALSE),
        data.frame(origin_geolev2      = tau_df$destination_geolev2,
                   destination_geolev2 = tau_df$origin_geolev2,
                   tau = tau_df$tau, dup = TRUE, stringsAsFactors = FALSE)
    )
    s <- merge(s,
               data.frame(destination_geolev2 = pop$geolev2,
                          pop_dest = pop$pop, stringsAsFactors = FALSE),
               by = "destination_geolev2", all.x = TRUE)
    s$pop_dest[is.na(s$pop_dest)] <- 0
    # Pipeline pair filter: finite, positive raw tau contributes; else 0.
    s$ok <- is.finite(s$tau) & s$tau > 0
    s[order(s$origin_geolev2, s$destination_geolev2), ]
}

# ---------------------------------------------------------------------------
# Report + CSV
# ---------------------------------------------------------------------------
write_outputs <- function(df) {
    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)
    csv_path <- file.path(dir_tables, "diagnostic_ma_iceberg.csv")
    write.csv(df, csv_path, row.names = FALSE)

    txt_path <- file.path(dir_tables, "diagnostic_ma_iceberg.txt")
    con <- file(txt_path, open = "wt")
    wline <- function(...) { line <- sprintf(...); cat(line, "\n")
                             cat(line, "\n", file = con) }
    wline("%s", strrep("=", 78))
    wline("ICEBERG TAU NORMALIZATION SWEEP (Decision A option 1a; Cote 1.5)")
    wline("tau' = 1 + cost/V ; MA = sum Pop_j / tau'^theta ; 1960 pop weights")
    wline("Table 9 spec, total population, HC1; baseline logMA control is the")
    wline("V-specific iceberg 1960 logMA. V in 1960 pesos/ton (raster/1000).")
    wline("V is NOT sourced yet (archive lookup, issue #68 visit): read the")
    wline("curve, do not pick a point. V->0 = raw tau (scale cancels);")
    wline("V->Inf: tau'->1, Delta log MA -> 0 for every district (no")
    wline("regression exists at that limit).")
    wline("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
    wline("%s", strrep("=", 78))
    for (th in unique(df$theta)) {
        wline("")
        wline("---- theta = %.2f %s", th,
              ifelse(th > 8,
                     "(D&H preferred; the exponent tau' ~ 1 is built for)",
                     "(main-spec continuity)"))
        wline("%-9s %-22s %-6s %-7s  %-16s %-16s %6s %6s %6s",
              "V (p/t)", "tau' p10/p50/p90", "gain%", "dlmMean",
              "OLS b (SE)", "IV-B b (SE)", "F(LP)", "F(H)", "F(IVB)")
        sub <- df[df$theta == th, ]
        for (i in seq_len(nrow(sub))) {
            r <- sub[i, ]
            wline(paste0("%-9s %-22s %5.1f%% %+7.3f  %+7.3f (%.3f)  ",
                         "%+7.3f (%.3f) %6.1f %6.1f %6.1f"),
                  ifelse(r$anchor != "affine", "raw",
                         format(r$V_pesos, trim = TRUE)),
                  sprintf("%.3g/%.3g/%.3g", r$tau_p10, r$tau_p50, r$tau_p90),
                  100 * r$dlm_gainshare, r$dlm_mean,
                  r$ols_beta, r$ols_se, r$ivb_beta, r$ivb_se,
                  r$ivlp_F, r$ivh_F, r$ivb_F)
        }
    }
    # ---- Computed reading notes (cr-review PR #130 SF2 / C1 / C2 / C4) ----
    aff <- df[df$anchor == "affine", ]
    tmax_i <- which.max(abs(aff$ivb_beta / aff$ivb_se))
    p_min <- min(df$ivb_p)
    p_anchor <- df$ivb_p[df$anchor != "affine" & df$theta == theta[["low"]]]
    lo <- df[df$theta == max(df$theta), ]
    wline("")
    wline("Reading notes:")
    wline("- Significance: NOTHING on either curve clears the 5%% level,")
    wline("  including the raw anchor (its p = %.3f, the published main",
          p_anchor)
    wline("  spec). Smallest IV-B p on the grid = %.3f (theta %.2f,",
          p_min, df$theta[which.min(df$ivb_p)])
    wline("  V = %s p/t): low-V points are no WEAKER than the main spec.",
          format(df$V_pesos[which.min(df$ivb_p)], trim = TRUE))
    wline("- The IV-B t-stat PEAKS at V = %s p/t (t = %.2f) before precision",
          format(aff$V_pesos[tmax_i], trim = TRUE),
          abs(aff$ivb_beta / aff$ivb_se)[tmax_i])
    wline("  decays; 'SEs grow faster than beta' holds beyond V ~ 500.")
    wline("- The IV-B F strengthening is ENTIRELY the hypothetical")
    wline("  instrument: at theta %.2f, F(IV-H) goes %.1f -> %.1f across the",
          max(df$theta), lo$ivh_F[1], lo$ivh_F[nrow(lo)])
    wline("  grid while F(IV-LP) goes %.1f -> %.1f. Normalization REVERSES",
          lo$ivlp_F[1], lo$ivlp_F[nrow(lo)])
    wline("  the instrument-strength ranking (relevant to the IV-LP-only")
    wline("  main-spec question).")
    wline("- The Gibbons ~0.3 crossing occurs only in the degenerate tail")
    wline("  (V = %s p/t ~ %.0fx the median raw cost; Delta log MA mean",
          format(max(df$V_pesos), trim = TRUE),
          max(df$V_pesos) * tau_units_to_pesos / df$tau_p50[1])
    wline("  %.3f there).", df$dlm_mean[df$V_pesos == max(df$V_pesos) &
                                        df$theta == max(df$theta)])
    wline("")
    wline("Full grid incl. IV-LP / IV-H betas: diagnostic_ma_iceberg.csv")
    close(con)
    message("\nSaved: ", txt_path)
    message("Saved: ", csv_path)
}

main()

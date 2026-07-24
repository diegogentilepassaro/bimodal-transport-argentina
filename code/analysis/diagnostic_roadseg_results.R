# ===========================================================================
# diagnostic_roadseg_results.R
#
# PURPOSE: Consume the corridor-timing permutation draws (engine =
#          diagnostic_roadseg_draws.R) and report the design
#          diagnostics. The randomized margin is the world's paving
#          sequence over the ACTUAL road expansion (early = by 1970 vs
#          late = by 1986), so -- unlike the settlement design of PR
#          #115 -- the dose is the full expansion and recentered
#          relevance is a real test, not a foregone collapse.
#          DIAGNOSTIC ONLY: nothing in the paper reads these outputs.
#
#   (bal) Balance: chain-level early vs late within strata (stratum FE)
#         on predetermined covariates of TRAVERSED districts,
#         aggregated with km-in-district weights (from prep); SEs
#         clustered by the chain's modal district. Includes the
#         1947-60 placebo growth -- the credibility headline.
#   (a)   Backbone/variance: R2 of z_obs on mu; sd comparison.
#   (b)   Do geo_controls_main span mu?
#   (c)   BH spec test: recentered z on controls, RI p from draws.
#   (d)   Estimates: endog in {total 86-60, road-only} x four outcomes
#         x three variants (unadjusted / recentered / mu-control);
#         reduced-form RI p with leave-one-out mu.
#   (x)   Context correlations with treatments and the other
#         instruments.
#
# DEFINITIONS:
#   z_i^(s) = logMA^(s)_i - logMA_actual_1960_i (draw network = 1960
#             rails + 1954-convention roads + draw's early chains).
#   mu_i    = mean over the S PERMUTED draws (identity rs000 excluded).
#   z_obs   = z from the identity draw (observed early chains). No
#             panel column exists for this case; the engine's physical
#             identity gate covers the machinery check.
#   z_rec   = z_obs - mu.
#
# READS:
#   data/derived/07_recentering/draws_roadseg/z_rc*.parquet
#   data/derived/07_recentering/roadseg/{chains.parquet,
#       chain_districts.parquet}
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/diagnostic_roadseg.csv   (tidy, all numbers)
#   results/tables/diagnostic_roadseg.txt   (human-readable report)
#
# USAGE:
#   Rscript code/analysis/diagnostic_roadseg_results.R [variant]
#   variant = "base" (default) or "growth" (growth-stratified repair;
#   reads roadseg_growth/ + draws_roadseg_growth/, writes
#   diagnostic_roadseg_growth.{csv,txt}; growth balance is then a
#   by-construction mechanical check, flagged in the report).
#   Runs on however many draws are present (>= 10 enforced; the full
#   diagnostic uses S = 100 -- the report stamps S_perm everywhere).
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
})

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)

    args <- commandArgs(trailingOnly = TRUE)
    variant <- if (length(args) >= 1) args[1] else "base"
    stopifnot(variant %in% c("base", "growth"))
    vsuf <- if (variant == "growth") "_growth" else ""

    message("\n", strrep("=", 72))
    message(sprintf(
        "diagnostic_roadseg_results.R  |  corridor-timing diagnostics (%s)",
        variant))
    message(strrep("=", 72))

    # ---- 1. Load draws, chains, estimation sample ---------------------------
    dir_draws <- file.path(dir_derived_recentering,
                           paste0("draws_roadseg", vsuf))
    files <- sort(list.files(dir_draws, pattern = "^z_rc\\d+\\.parquet$",
                             full.names = TRUE))
    stopifnot(length(files) >= 1L)
    draws <- do.call(rbind, lapply(files, function(f)
        as.data.frame(arrow::read_parquet(f))))
    draws <- ensure_geolev2_char(draws)
    S_perm <- length(unique(draws$draw[draws$draw > 0L]))
    message(sprintf("[rsres] %d draw files (%d permuted + identity)",
                    length(files), S_perm))
    if (S_perm < 10L) {
        stop("[rsres] fewer than 10 permuted draws; ",
             "run diagnostic_roadseg_draws.R first")
    }
    if (S_perm < 100L) {
        message(sprintf(
            "[rsres] WARNING: PROVISIONAL run on %d draws (full = 100); ",
            S_perm), "mu is noisy -- do not over-read recentered columns")
    }

    dir_rs <- file.path(dir_derived_recentering,
                        paste0("roadseg", vsuf))
    ch <- as.data.frame(arrow::read_parquet(
        file.path(dir_rs, "chains.parquet")))
    cd <- ensure_geolev2_char(as.data.frame(arrow::read_parquet(
        file.path(dir_rs, "chain_districts.parquet"))))
    stopifnot(nrow(ch) == 893L, sum(ch$early) == 228L)

    d <- as.data.frame(arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet")))
    d <- ensure_geolev2_char(d)
    stopifnot(nrow(d) == 311L)

    # ---- 2. Build z^(s), mu, recentered instrument --------------------------
    zw <- reshape(draws[, c("geolev2", "draw", "logMA")],
                  idvar = "geolev2", timevar = "draw", direction = "wide")
    d2 <- merge(d, zw, by = "geolev2", all.x = TRUE)
    stopifnot(nrow(d2) == 311L)

    perm_ids <- sort(unique(draws$draw[draws$draw > 0L]))
    perm_cols <- sprintf("logMA.%d", perm_ids)
    stopifnot(all(perm_cols %in% names(d2)), "logMA.0" %in% names(d2))
    base_col <- "logMA_actual_1960_s0_elow"
    zmat <- as.matrix(d2[, perm_cols]) - d2[[base_col]]
    stopifnot(!anyNA(zmat))

    # Identity draw must merge completely before the sign check: with
    # na.rm the min() would silently pass over an NA-producing merge
    # (cr-review PR #117 should-fix 3).
    stopifnot(!anyNA(d2$logMA.0))
    d2$z_obs <- d2$logMA.0 - d2[[base_col]]
    n_early_id <- unique(draws$n_early[draws$draw == 0L])
    stopifnot(identical(as.integer(n_early_id), sum(ch$early)))
    stopifnot(min(d2$z_obs) > -1e-9)

    d2$mu    <- rowMeans(zmat)
    d2$z_rec <- d2$z_obs - d2$mu
    mu_loo <- (S_perm * d2$mu - zmat) / (S_perm - 1)

    out_rows <- list()
    add_row <- function(...) out_rows[[length(out_rows) + 1L]] <<-
        data.frame(..., stringsAsFactors = FALSE)

    # ---- 3. (bal) Balance: early vs late chains within strata ----------------
    # Chain-level (n = 893): predetermined covariates of traversed
    # districts, km-weighted across each chain's districts. Stratum FE;
    # SEs clustered by the chain's modal district (chains crossing the
    # same districts share covariate values; cr-review PR #115 pattern).
    d$log_pop_1947 <- ifelse(d$pop_1947 > 0, log(d$pop_1947), NA)
    d$rail_dens_1960 <- d$tot_rails_1960 / d$area_km2
    bal_covs <- c("log_pop_1947", "log_pop_1960",
                  "chg_log_placebo_pop_60_47", "rail_dens_1960",
                  "elev_mean_std", "rugged_mea_std", "wheat_std",
                  "dist_to_BA_std", "logMA_actual_1960_s0_elow")
    cdm <- merge(cd, d[, c("geolev2", bal_covs)], by = "geolev2",
                 all.x = TRUE)
    n_unmatched_km <- sum(cdm$km_in_district[is.na(cdm$log_pop_1960)])
    message(sprintf(
        "[rsres] (bal) %.0f of %.0f chain-km unmatched to estimation sample",
        n_unmatched_km, sum(cd$km_in_district)))
    add_row(block = "bal", outcome = "match", spec = "chains",
            stat = "unmatched_km", value = n_unmatched_km)
    wmean <- function(x, w) {
        ok <- !is.na(x) & !is.na(w)
        if (!any(ok)) return(NA_real_)
        sum(x[ok] * w[ok]) / sum(w[ok])
    }
    agg <- do.call(rbind, lapply(split(cdm, cdm$chain_id), function(g) {
        vals <- vapply(bal_covs, function(cv)
            wmean(g[[cv]], g$km_in_district), numeric(1))
        md <- g$geolev2[which.max(g$km_in_district)]
        data.frame(chain_id = g$chain_id[1], modal_district = md,
                   t(vals), stringsAsFactors = FALSE)
    }))
    cb <- merge(ch, agg, by = "chain_id", all.x = TRUE)
    stopifnot(nrow(cb) == 893L)
    for (cv in bal_covs) {
        ok <- !is.na(cb[[cv]])
        m <- feols(as.formula(paste(cv, "~ early | stratum")),
                   data = cb[ok, ], cluster = ~modal_district)
        cc <- safe_coef(m, "earlyTRUE")
        add_row(block = "bal", outcome = cv, spec = "early_vs_late",
                stat = "coef", value = cc$est)
        add_row(block = "bal", outcome = cv, spec = "early_vs_late",
                stat = "se", value = cc$se)
        add_row(block = "bal", outcome = cv, spec = "early_vs_late",
                stat = "p", value = cc$p)
        add_row(block = "bal", outcome = cv, spec = "early_vs_late",
                stat = "N", value = nobs(m))
        message(sprintf(
            "[rsres] (bal) %-28s b=%+.4f  se=%.4f  p=%.3f  N=%d",
            cv, cc$est, cc$se, cc$p, nobs(m)))
    }
    # Chain length is a design covariate (stratified on terciles):
    # report it too as a mechanical check.
    m_len <- feols(log(length_km) ~ early | stratum, data = cb,
                   cluster = ~modal_district)
    cc <- safe_coef(m_len, "earlyTRUE")
    add_row(block = "bal", outcome = "log_length_km",
            spec = "early_vs_late", stat = "coef", value = cc$est)
    add_row(block = "bal", outcome = "log_length_km",
            spec = "early_vs_late", stat = "se", value = cc$se)
    add_row(block = "bal", outcome = "log_length_km",
            spec = "early_vs_late", stat = "p", value = cc$p)
    add_row(block = "bal", outcome = "log_length_km",
            spec = "early_vs_late", stat = "N", value = nobs(m_len))
    message(sprintf(
        "[rsres] (bal) %-28s b=%+.4f  se=%.4f  p=%.3f  N=%d",
        "log_length_km", cc$est, cc$se, cc$p, nobs(m_len)))

    # ---- 4. (a) Backbone / variance decomposition ---------------------------
    m_a <- feols(z_obs ~ mu, data = d2, vcov = "hetero")
    r2_a <- r2(m_a, type = "r2")
    message(sprintf(
        "[rsres] (a) z_obs on mu: R2 = %.3f, slope = %.3f",
        r2_a, coef(m_a)[["mu"]]))
    add_row(block = "a_variance", outcome = "z_obs", spec = "on_mu",
            stat = "r2", value = r2_a)
    add_row(block = "a_variance", outcome = "z_obs", spec = "on_mu",
            stat = "slope", value = unname(coef(m_a)[["mu"]]))
    add_row(block = "a_variance", outcome = "z_obs", spec = "sd",
            stat = "sd_z", value = sd(d2$z_obs, na.rm = TRUE))
    add_row(block = "a_variance", outcome = "z_rec", spec = "sd",
            stat = "sd_zrec", value = sd(d2$z_rec, na.rm = TRUE))
    ek <- draws$early_km[draws$geolev2 == draws$geolev2[1] &
                         draws$draw > 0L]
    add_row(block = "a_variance", outcome = "early_km", spec = "draws",
            stat = "mean", value = mean(ek))
    add_row(block = "a_variance", outcome = "early_km", spec = "draws",
            stat = "sd", value = sd(ek))

    # ---- 5. (b) Do the controls span mu? ------------------------------------
    ctrls_expr <- paste(geo_controls_main, collapse = " + ")
    m_b <- feols(as.formula(paste("mu ~", ctrls_expr)),
                 data = d2, vcov = "hetero")
    r2_b <- r2(m_b, type = "r2")
    message(sprintf("[rsres] (b) mu on geo_controls_main: R2 = %.3f", r2_b))
    add_row(block = "b_span", outcome = "mu", spec = "on_controls",
            stat = "r2", value = r2_b)

    # ---- 6. (c) BH specification test ----------------------------------------
    ssf <- function(zv) {
        ok <- complete.cases(d2[, geo_controls_main]) & !is.na(zv)
        m <- feols(as.formula(paste("zv ~", ctrls_expr)),
                   data = cbind(d2, zv = zv)[ok, ], vcov = "hetero")
        sum(fitted(m)^2)
    }
    t_obs <- ssf(d2$z_rec)
    t_null <- vapply(seq_len(ncol(zmat)), function(s) {
        ssf(zmat[, s] - mu_loo[, s])
    }, numeric(1))
    p_ri <- (1 + sum(t_null >= t_obs)) / (length(t_null) + 1)
    message(sprintf(
        "[rsres] (c) spec test: T_obs = %.3f, RI p = %.3f (S = %d)",
        t_obs, p_ri, length(t_null)))
    add_row(block = "c_spec_test", outcome = "z_rec", spec = "joint",
            stat = "T_obs", value = t_obs)
    add_row(block = "c_spec_test", outcome = "z_rec", spec = "joint",
            stat = "ri_p", value = p_ri)
    m_c <- feols(as.formula(paste("z_rec ~", ctrls_expr)),
                 data = d2, vcov = "hetero")
    add_row(block = "c_spec_test", outcome = "z_rec", spec = "on_controls",
            stat = "r2", value = r2(m_c, type = "r2"))
    m_c0 <- feols(as.formula(paste("z_obs ~", ctrls_expr)),
                  data = d2, vcov = "hetero")
    add_row(block = "c_spec_test", outcome = "z_obs", spec = "on_controls",
            stat = "r2", value = r2(m_c0, type = "r2"))

    # ---- 7. (d) Estimates: two endogs x four outcomes x three variants ------
    endogs <- list(total = "chg_logMA_86_60_s0_elow",
                   road_only = "chg_logMA_only_road_s0_elow")
    outcomes <- list(
        c("chg_log_pop_91_60",         "population"),
        c("chg_log_valprod_85_54",     "mfg_valprod"),
        c("chg_log_massal_85_54",      "mfg_wagemass"),
        c("chg_log_placebo_pop_60_47", "placebo_pretrend")
    )

    fit_variant <- function(y, endog, instr, extra_ctrl = NULL) {
        cv <- c(geo_controls_main, extra_ctrl)
        f <- as.formula(sprintf("%s ~ %s | %s ~ %s",
                                y, paste(cv, collapse = " + "),
                                endog, instr))
        feols(f, data = d2, vcov = "hetero")
    }

    for (en in names(endogs)) {
        endog <- endogs[[en]]
        for (oc in outcomes) {
            y <- oc[1]; lbl <- sprintf("%s.%s", en, oc[2])
            specs <- list(
                unadjusted = fit_variant(y, endog, "z_obs"),
                recentered = fit_variant(y, endog, "z_rec"),
                mu_control = fit_variant(y, endog, "z_obs",
                                         extra_ctrl = "mu")
            )
            for (sp in names(specs)) {
                m <- specs[[sp]]
                cc <- safe_coef(m, paste0("fit_", endog))
                add_row(block = "d_estimates", outcome = lbl, spec = sp,
                        stat = "coef", value = cc$est)
                add_row(block = "d_estimates", outcome = lbl, spec = sp,
                        stat = "se",   value = cc$se)
                add_row(block = "d_estimates", outcome = lbl, spec = sp,
                        stat = "p",    value = cc$p)
                add_row(block = "d_estimates", outcome = lbl, spec = sp,
                        stat = "F",    value = fitstat_F(m))
                add_row(block = "d_estimates", outcome = lbl, spec = sp,
                        stat = "N",    value = nobs(m))
                message(sprintf(
                    "[rsres] (d) %-28s %-10s b=%+.4f se=%.4f p=%.3f F=%.1f N=%d",
                    lbl, sp, cc$est, cc$se, cc$p, fitstat_F(m), nobs(m)))
            }

            # NOTE: the reduced form does not involve the endogenous
            # variable, so this RI p is identical across the two endog
            # blocks; kept per-block for report symmetry (cr-review
            # PR #117 consider 8).
            rf_coef <- function(zv) {
                dd <- cbind(d2, zv = zv)
                m <- feols(as.formula(paste(y, "~ zv +", ctrls_expr)),
                           data = dd, vcov = "hetero")
                unname(coef(m)[["zv"]])
            }
            b_obs <- rf_coef(d2$z_rec)
            b_null <- vapply(seq_len(ncol(zmat)), function(s) {
                rf_coef(zmat[, s] - mu_loo[, s])
            }, numeric(1))
            p_rf <- (1 + sum(abs(b_null) >= abs(b_obs))) /
                (length(b_null) + 1)
            add_row(block = "d_estimates", outcome = lbl,
                    spec = "reduced_form", stat = "ri_p", value = p_rf)
            message(sprintf("[rsres] (d) %-28s reduced-form RI p = %.3f",
                            lbl, p_rf))
        }
    }

    # ---- 8. (x) Context correlations ----------------------------------------
    ctx <- c(total = "chg_logMA_86_60_s0_elow",
             road_only = "chg_logMA_only_road_s0_elow",
             larkin = "chg_logMA_stu_s0_elow",
             hypo = main_hypo_instrument)
    for (nm in names(ctx)) {
        for (zc in c("z_obs", "z_rec")) {
            v <- cor(d2[[zc]], d2[[ctx[nm]]], use = "complete.obs")
            add_row(block = "x_context", outcome = nm, spec = zc,
                    stat = "cor", value = v)
            message(sprintf("[rsres] (x) cor(%s, %s) = %+.3f",
                            zc, nm, v))
        }
    }

    # ---- 9. Write outputs -----------------------------------------------------
    res <- do.call(rbind, out_rows)
    res$S_perm <- S_perm
    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)
    csv_path <- file.path(dir_tables,
        sprintf("diagnostic_roadseg%s.csv", vsuf))
    write.csv(res, csv_path, row.names = FALSE)

    txt_path <- file.path(dir_tables,
        sprintf("diagnostic_roadseg%s.txt", vsuf))
    sink(txt_path)
    cat(sprintf("Corridor-timing design instrument diagnostic (%s)\n",
                variant))
    cat(sprintf("Generated: %s  |  permuted draws: %d%s\n\n",
                Sys.time(), S_perm,
                if (S_perm < 100L) "  [PROVISIONAL]" else ""))
    if (variant == "base") {
        cat("VERDICT: real dose, loaded dice. Recentered relevance rises",
            "an\norder of magnitude over the settlement design (F 3-4.5",
            "vs ~1)\nbut early-paved corridors traverse districts with",
            "faster\n1947-60 placebo growth (balance failure below): the",
            "paving\nsequence tracked demand, so timing is not usable",
            "as-good-as-\nrandom conditional on these strata.\n\n")
    } else {
        cat("GROWTH-STRATIFIED REPAIR of the base design's failed\n")
        cat("balance margin: early status permuted within region x\n")
        cat("length x 1947-60 growth strata, so the placebo-growth\n")
        cat("balance row below is a BY-CONSTRUCTION mechanical check,\n")
        cat("not a test. The informative numbers: recentered F (does\n")
        cat("timing entropy survive growth stratification?) and the\n")
        cat("placebo reduced-form RI p.\n\n")
    }
    cat("(bal) Balance, early vs late chains within strata",
        "(SEs clustered by modal district):\n")
    for (cv in c(bal_covs, "log_length_km")) {
        sel <- res$block == "bal" & res$outcome == cv
        g <- function(st_) res$value[sel & res$stat == st_]
        cat(sprintf("    %-28s b=%+.4f  se=%.4f  p=%.3f  N=%d\n",
                    cv, g("coef"), g("se"), g("p"), as.integer(g("N"))))
    }
    cat(sprintf("\n(a) z_obs on mu: R2 = %.3f\n", r2_a))
    cat(sprintf("    sd(z) = %.3f -> sd(z - mu) = %.3f\n",
                sd(d2$z_obs, na.rm = TRUE), sd(d2$z_rec, na.rm = TRUE)))
    cat(sprintf("(b) mu on geo_controls_main: R2 = %.3f\n", r2_b))
    cat(sprintf("(c) Spec test (recentered z on controls): RI p = %.3f\n\n",
                p_ri))
    cat("(d) IV estimates (coef / se / p / first-stage F):\n")
    cat("    CAVEAT (cr-review PR #117): with slope(z_obs, mu) = ",
        sprintf("%.2f", coef(m_a)[["mu"]]),
        " and R2 = ", sprintf("%.2f", r2_a), ",\n", sep = "")
    cat("    controlling for mu soaks up the instrument (F = 0.2-0.4);\n")
    cat("    the mu_control rows are uninformative noise here and are\n")
    cat("    NOT a recentering equivalent (unlike the settlement\n")
    cat("    design). Read the recentered rows.\n")
    for (en in names(endogs)) {
        for (oc in outcomes) {
            lbl <- sprintf("%s.%s", en, oc[2])
            cat(sprintf("  %s\n", lbl))
            for (sp in c("unadjusted", "recentered", "mu_control")) {
                sel <- res$block == "d_estimates" &
                    res$outcome == lbl & res$spec == sp
                g <- function(st_) res$value[sel & res$stat == st_]
                cat(sprintf(
                    "    %-11s  b=%+.4f  se=%.4f  p=%.3f  F=%.1f  N=%d\n",
                    sp, g("coef"), g("se"), g("p"), g("F"),
                    as.integer(g("N"))))
            }
            cat(sprintf("    reduced-form RI p = %.3f\n",
                        res$value[res$block == "d_estimates" &
                                  res$outcome == lbl &
                                  res$spec == "reduced_form" &
                                  res$stat == "ri_p"]))
        }
    }
    sink()

    message(sprintf("[rsres] Saved: %s and .txt", csv_path))
}

main()

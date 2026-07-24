# ===========================================================================
# diagnostic_roadtiming_results.R
#
# PURPOSE: Consume the road-timing permutation draws (issue #114;
#          engine = diagnostic_roadtiming_draws.R) and report the
#          diagnostics for the road-timing design instrument. Unlike
#          the hypo node-permutation families (researcher's dice), the
#          randomized margin here is the WORLD's shock -- which
#          1954-unconnected settlements got a road early (by 1970) vs
#          late/never -- so these draws support both a design-based
#          balance test and a recentered instrument whose relevance is
#          a substantive finding, not an artifact. DIAGNOSTIC ONLY:
#          nothing in the paper reads these outputs.
#
#   (bal) Balance: settlement-level early vs not-early within strata
#         (stratum FE, SEs clustered by containing district) on
#         predetermined district covariates of the
#         containing district, incl. the 1947-60 placebo growth -- the
#         headline credibility test for treating timing as good as
#         random within strata.
#   (a)   Backbone/variance: R2 of z_obs on mu (direct comparison with
#         the hypo families' 0.93-0.99), sd(z_obs) vs sd(z_rec).
#   (b)   Do geo_controls_main span mu?
#   (c)   BH spec test: recentered z on controls, RI p from the draws.
#   (d)   Estimates: endog in {total 86-60, road-only} x four outcomes
#         (pop, mfg valprod, mfg wage mass, placebo) x three variants
#         (unadjusted / recentered / mu-control); first-stage F is the
#         relevance headline. Reduced-form RI p uses leave-one-out mu
#         for the null draws (deep-dive refinement, PR #112).
#   (x)   Context: correlations of z_obs / z_rec with the treatments
#         and with the existing Larkin and hypo instruments.
#
# DEFINITIONS:
#   z_i^(s) = logMA^(s)_i - logMA_actual_1960_i. The draw network is
#             1960 rails + 1954 roads + predicted links of the draw's
#             early set, so z is the MA gain induced by early links.
#   mu_i    = mean over the S PERMUTED draws (identity rt000 excluded).
#   z_obs   = z from the identity draw (observed early set). No panel
#             column exists for this case; the engine's physical
#             identity gate (links only cheapen cells) plays the role
#             the identity-vs-panel check plays in the Larkin chain.
#   z_rec   = z_obs - mu.
#
# READS:
#   data/derived/07_recentering/draws_roadtiming/z_rc*.parquet
#   data/derived/07_recentering/roadtiming/settlements.parquet
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/diagnostic_roadtiming.csv   (tidy, all numbers)
#   results/tables/diagnostic_roadtiming.txt   (human-readable report)
#
# USAGE:
#   Rscript code/analysis/diagnostic_roadtiming_results.R
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

    message("\n", strrep("=", 72))
    message("diagnostic_roadtiming_results.R  |  road-timing diagnostics")
    message(strrep("=", 72))

    # ---- 1. Load draws, settlements, estimation sample ----------------------
    dir_draws <- file.path(dir_derived_recentering, "draws_roadtiming")
    files <- sort(list.files(dir_draws, pattern = "^z_rc\\d+\\.parquet$",
                             full.names = TRUE))
    stopifnot(length(files) >= 1L)
    draws <- do.call(rbind, lapply(files, function(f)
        as.data.frame(arrow::read_parquet(f))))
    draws <- ensure_geolev2_char(draws)
    S_perm <- length(unique(draws$draw[draws$draw > 0L]))
    message(sprintf("[rtres] %d draw files (%d permuted + identity)",
                    length(files), S_perm))
    if (S_perm < 10L) {
        stop("[rtres] fewer than 10 permuted draws; ",
             "run diagnostic_roadtiming_draws.R first")
    }
    if (S_perm < 100L) {
        message(sprintf(
            "[rtres] WARNING: PROVISIONAL run on %d draws (full = 100); ",
            S_perm), "mu is noisy -- do not over-read recentered columns")
    }

    st <- as.data.frame(arrow::read_parquet(file.path(
        dir_derived_recentering, "roadtiming", "settlements.parquet")))
    st <- ensure_geolev2_char(st)
    stopifnot(nrow(st) == 216L, sum(st$early) == 60L)

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
    stopifnot(!anyNA(zmat))  # partial draw set fails loudly

    # Identity-draw integrity: the engine's physical gate already
    # validated the raster; here assert the identity draw carries the
    # observed early count and induces a nonnegative MA gain (adding
    # links cannot reduce market access).
    d2$z_obs <- d2$logMA.0 - d2[[base_col]]
    n_early_id <- unique(draws$n_early[draws$draw == 0L])
    stopifnot(identical(as.integer(n_early_id), sum(st$early)))
    stopifnot(min(d2$z_obs, na.rm = TRUE) > -1e-9)

    d2$mu    <- rowMeans(zmat)
    d2$z_rec <- d2$z_obs - d2$mu
    # Leave-one-out mu for null draws (mu contains draw s; LOO removes
    # the mechanical correlation): mu_loo(s) = (S*mu - z^(s)) / (S-1).
    mu_loo <- (S_perm * d2$mu - zmat) / (S_perm - 1)

    out_rows <- list()
    add_row <- function(...) out_rows[[length(out_rows) + 1L]] <<-
        data.frame(..., stringsAsFactors = FALSE)

    # ---- 3. (bal) Balance: early vs not-early within strata ------------------
    # Settlement-level (n = 216): covariates of the containing district
    # + settlement distance to the 1954 network. Stratum FE; SEs
    # clustered by containing district (cr-review PR #115: district
    # covariates are constant within the 128 districts, so HC1 at the
    # settlement level would understate them). The strata are the
    # randomization cells, so this is the design-based balance table.
    d$log_pop_1947 <- ifelse(d$pop_1947 > 0, log(d$pop_1947), NA)
    d$rail_dens_1960 <- d$tot_rails_1960 / d$area_km2
    bal_covs <- c("dist54_km", "log_pop_1947", "log_pop_1960",
                  "chg_log_placebo_pop_60_47", "rail_dens_1960",
                  "elev_mean_std", "rugged_mea_std", "wheat_std",
                  "dist_to_BA_std", "logMA_actual_1960_s0_elow")
    sb <- merge(st, d[, c("geolev2", setdiff(bal_covs, "dist54_km"))],
                by = "geolev2", all.x = TRUE)
    n_unmatched <- sum(!sb$geolev2 %in% d$geolev2)
    message(sprintf(
        "[rtres] (bal) %d of %d settlements unmatched to estimation sample",
        n_unmatched, nrow(sb)))
    add_row(block = "bal", outcome = "match", spec = "settlements",
            stat = "n_unmatched", value = n_unmatched)
    for (cv in bal_covs) {
        ok <- !is.na(sb[[cv]])
        m <- feols(as.formula(paste(cv, "~ early | stratum")),
                   data = sb[ok, ], cluster = ~geolev2)
        cc <- safe_coef(m, "earlyTRUE")
        add_row(block = "bal", outcome = cv, spec = "early_vs_rest",
                stat = "coef", value = cc$est)
        add_row(block = "bal", outcome = cv, spec = "early_vs_rest",
                stat = "se", value = cc$se)
        add_row(block = "bal", outcome = cv, spec = "early_vs_rest",
                stat = "p", value = cc$p)
        add_row(block = "bal", outcome = cv, spec = "early_vs_rest",
                stat = "N", value = nobs(m))
        message(sprintf(
            "[rtres] (bal) %-28s b=%+.4f  se=%.4f  p=%.3f  N=%d",
            cv, cc$est, cc$se, cc$p, nobs(m)))
    }

    # ---- 4. (a) Backbone / variance decomposition ---------------------------
    m_a <- feols(z_obs ~ mu, data = d2, vcov = "hetero")
    r2_a <- r2(m_a, type = "r2")
    message(sprintf(
        "[rtres] (a) z_obs on mu: R2 = %.3f, slope = %.3f  (hypo: 0.93-0.99)",
        r2_a, coef(m_a)[["mu"]]))
    add_row(block = "a_variance", outcome = "z_obs", spec = "on_mu",
            stat = "r2", value = r2_a)
    add_row(block = "a_variance", outcome = "z_obs", spec = "on_mu",
            stat = "slope", value = unname(coef(m_a)[["mu"]]))
    add_row(block = "a_variance", outcome = "z_obs", spec = "sd",
            stat = "sd_z", value = sd(d2$z_obs, na.rm = TRUE))
    add_row(block = "a_variance", outcome = "z_rec", spec = "sd",
            stat = "sd_zrec", value = sd(d2$z_rec, na.rm = TRUE))
    lk <- draws$links_km[draws$geolev2 == draws$geolev2[1] & draws$draw > 0L]
    add_row(block = "a_variance", outcome = "links_km", spec = "draws",
            stat = "mean", value = mean(lk))
    add_row(block = "a_variance", outcome = "links_km", spec = "draws",
            stat = "sd", value = sd(lk))

    # ---- 5. (b) Do the controls span mu? ------------------------------------
    ctrls_expr <- paste(geo_controls_main, collapse = " + ")
    m_b <- feols(as.formula(paste("mu ~", ctrls_expr)),
                 data = d2, vcov = "hetero")
    r2_b <- r2(m_b, type = "r2")
    message(sprintf("[rtres] (b) mu on geo_controls_main: R2 = %.3f", r2_b))
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
        "[rtres] (c) spec test: T_obs = %.3f, RI p = %.3f (S = %d)",
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
                    "[rtres] (d) %-28s %-10s b=%+.4f se=%.4f p=%.3f F=%.1f N=%d",
                    lbl, sp, cc$est, cc$se, cc$p, fitstat_F(m), nobs(m)))
            }

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
            message(sprintf("[rtres] (d) %-28s reduced-form RI p = %.3f",
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
            message(sprintf("[rtres] (x) cor(%s, %s) = %+.3f",
                            zc, nm, v))
        }
    }

    # ---- 9. Write outputs -----------------------------------------------------
    res <- do.call(rbind, out_rows)
    res$S_perm <- S_perm
    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)
    csv_path <- file.path(dir_tables, "diagnostic_roadtiming.csv")
    write.csv(res, csv_path, row.names = FALSE)

    txt_path <- file.path(dir_tables, "diagnostic_roadtiming.txt")
    sink(txt_path)
    cat("Road-timing design instrument diagnostic (issue #114)\n")
    cat(sprintf("Generated: %s  |  permuted draws: %d%s\n\n",
                Sys.time(), S_perm,
                if (S_perm < 100L) "  [PROVISIONAL]" else ""))
    cat("(bal) Balance, early vs not-early within strata",
        "(SEs clustered by district):\n")
    for (cv in bal_covs) {
        sel <- res$block == "bal" & res$outcome == cv
        g <- function(st_) res$value[sel & res$stat == st_]
        cat(sprintf("    %-28s b=%+.4f  se=%.4f  p=%.3f  N=%d\n",
                    cv, g("coef"), g("se"), g("p"), as.integer(g("N"))))
    }
    cat(sprintf("\n(a) z_obs on mu: R2 = %.3f (hypo families: 0.93-0.99)\n",
                r2_a))
    cat(sprintf("    sd(z) = %.3f -> sd(z - mu) = %.3f\n",
                sd(d2$z_obs, na.rm = TRUE), sd(d2$z_rec, na.rm = TRUE)))
    cat(sprintf("(b) mu on geo_controls_main: R2 = %.3f\n", r2_b))
    cat(sprintf("(c) Spec test (recentered z on controls): RI p = %.3f\n\n",
                p_ri))
    cat("(d) IV estimates (coef / se / p / first-stage F):\n")
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

    message(sprintf("[rtres] Saved: %s and .txt", csv_path))
}

main()

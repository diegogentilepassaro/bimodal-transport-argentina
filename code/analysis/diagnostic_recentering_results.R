# ===========================================================================
# diagnostic_recentering_results.R
#
# PURPOSE: Stage 1 of the recentering diagnostic (Borusyak & Hull 2023):
#          consume the permutation draws and report the four diagnostics
#          the plan specifies. DIAGNOSTIC ONLY: nothing in the paper
#          reads these outputs until Stage 2 is approved.
#
#   (a) Variance decomposition: how much of the observed Larkin
#       instrument z_i = chg_logMA_stu is "expected" given geography --
#       R^2 and slope of z on the expected instrument mu_i.
#   (b) Do the main-spec controls span mu_i? R^2 of mu on
#       geo_controls_main (which includes baseline log MA and log pop).
#   (c) BH specification test (their Table II analogue): regress the
#       RECENTERED instrument z - mu on the same controls; report the
#       joint sum-of-squared-fitted-values statistic with a
#       randomization-inference p-value from the draws.
#   (d) Estimates: population, mfg production value, mfg wage mass, and
#       the pre-trends placebo, each in three variants --
#         unadjusted   IV-LP exactly as in Tables 7/9/10;
#         recentered   instrument = z - mu;
#         mu-control   instrument = z, mu added to controls.
#       Reduced-form RI p-values (sharp null beta = 0) from the draws.
#
# DEFINITIONS:
#   z_i^(s)   = logMA^(s)_i - logMA_actual_1960_i  (draw s counterfactual
#               instrument; baseline term common to all draws).
#   mu_i      = mean over the S PERMUTED draws (s = 1..S). The identity
#               draw rc000 is excluded from mu: it is the machinery
#               check, and its z equals the observed instrument.
#   z~_i      = z_i - mu_i (recentered instrument).
#
# READS:
#   data/derived/07_recentering/draws/z_rc*.parquet
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/diagnostic_recentering.csv   (tidy, all numbers)
#   results/tables/diagnostic_recentering.txt   (human-readable report)
#
# USAGE:
#   Rscript code/analysis/diagnostic_recentering_results.R
#   Runs on however many draws are present (>= 10 enforced; the full
#   diagnostic uses S = 100).
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
    message("diagnostic_recentering_results.R  |  BH recentering diagnostics")
    message(strrep("=", 72))

    # ---- 1. Load draws and estimation sample --------------------------------
    dir_draws <- file.path(dir_derived_recentering, "draws")
    files <- sort(list.files(dir_draws, pattern = "^z_rc\\d+\\.parquet$",
                             full.names = TRUE))
    stopifnot(length(files) >= 1L)
    draws <- do.call(rbind, lapply(files, function(f) {
        as.data.frame(arrow::read_parquet(f))
    }))
    draws <- ensure_geolev2_char(draws)
    S_perm <- length(unique(draws$draw[draws$draw > 0L]))
    message(sprintf("[res] %d draw files (%d permuted + identity)",
                    length(files), S_perm))
    if (S_perm < 10L) {
        stop("[res] fewer than 10 permuted draws present; ",
             "run diagnostic_recentering_draws.R first")
    }

    d <- as.data.frame(arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet")))
    d <- ensure_geolev2_char(d)
    stopifnot(nrow(d) == 311L)

    # ---- 2. Build z^(s), mu, and the recentered instrument ------------------
    # Wide matrix of draw logMA over the estimation sample (311 districts;
    # the draws carry 312 incl. CABA, dropped by the merge).
    zw <- reshape(draws[, c("geolev2", "draw", "logMA")],
                  idvar = "geolev2", timevar = "draw", direction = "wide")
    d2 <- merge(d, zw, by = "geolev2", all.x = TRUE)
    stopifnot(nrow(d2) == 311L)

    perm_cols <- sprintf("logMA.%d", sort(unique(draws$draw[draws$draw > 0L])))
    stopifnot(all(perm_cols %in% names(d2)))
    zmat <- as.matrix(d2[, perm_cols]) - d2$logMA_actual_1960_s0_elow

    # Identity cross-check: z from rc000 must equal the observed
    # instrument column in the estimation sample (float tolerance). This
    # validates that the draw pipeline and the panel pipeline construct
    # the same object.
    z_id <- d2$logMA.0 - d2$logMA_actual_1960_s0_elow
    max_dev <- max(abs(z_id - d2$chg_logMA_stu_s0_elow), na.rm = TRUE)
    message(sprintf("[res] identity vs panel instrument: max dev = %.2e",
                    max_dev))
    stopifnot(max_dev < 1e-6)

    d2$mu    <- rowMeans(zmat)
    d2$z_obs <- d2$chg_logMA_stu_s0_elow
    d2$z_rec <- d2$z_obs - d2$mu

    out_rows <- list()
    add_row <- function(...) out_rows[[length(out_rows) + 1L]] <<- data.frame(
        ..., stringsAsFactors = FALSE)

    # ---- 3. (a) Variance decomposition --------------------------------------
    m_a <- feols(z_obs ~ mu, data = d2, vcov = "hetero")
    r2_a <- r2(m_a, type = "r2")
    message(sprintf(
        "[res] (a) z on mu: R2 = %.3f, slope = %.3f  (share 'expected')",
        r2_a, coef(m_a)[["mu"]]))
    add_row(block = "a_variance", outcome = "z_obs", spec = "on_mu",
            stat = "r2", value = r2_a)
    add_row(block = "a_variance", outcome = "z_obs", spec = "on_mu",
            stat = "slope", value = unname(coef(m_a)[["mu"]]))
    add_row(block = "a_variance", outcome = "z_obs", spec = "sd",
            stat = "sd_z", value = sd(d2$z_obs, na.rm = TRUE))
    add_row(block = "a_variance", outcome = "z_rec", spec = "sd",
            stat = "sd_zrec", value = sd(d2$z_rec, na.rm = TRUE))

    # ---- 4. (b) Do the controls span mu? ------------------------------------
    ctrls_expr <- paste(geo_controls_main, collapse = " + ")
    m_b <- feols(as.formula(paste("mu ~", ctrls_expr)),
                 data = d2, vcov = "hetero")
    r2_b <- r2(m_b, type = "r2")
    message(sprintf("[res] (b) mu on geo_controls_main: R2 = %.3f", r2_b))
    add_row(block = "b_span", outcome = "mu", spec = "on_controls",
            stat = "r2", value = r2_b)

    # ---- 5. (c) BH specification test ----------------------------------------
    # Statistic: sum of squared fitted values from regressing the
    # recentered instrument on the controls (BH fn. 29). Null
    # distribution: same statistic with each permuted draw's recentered
    # z in place of the observed one.
    ssf <- function(zv) {
        ok <- complete.cases(d2[, geo_controls_main]) & !is.na(zv)
        m <- feols(as.formula(paste("zv ~", ctrls_expr)),
                   data = cbind(d2, zv = zv)[ok, ], vcov = "hetero")
        sum(fitted(m)^2)
    }
    t_obs <- ssf(d2$z_rec)
    t_null <- vapply(seq_len(ncol(zmat)), function(s) {
        ssf(zmat[, s] - d2$mu)
    }, numeric(1))
    p_ri <- mean(t_null >= t_obs)
    message(sprintf(
        "[res] (c) spec test: T_obs = %.3f, RI p = %.3f (S = %d)",
        t_obs, p_ri, length(t_null)))
    add_row(block = "c_spec_test", outcome = "z_rec", spec = "joint",
            stat = "T_obs", value = t_obs)
    add_row(block = "c_spec_test", outcome = "z_rec", spec = "joint",
            stat = "ri_p", value = p_ri)
    # Also both R2s for context.
    m_c <- feols(as.formula(paste("z_rec ~", ctrls_expr)),
                 data = d2, vcov = "hetero")
    add_row(block = "c_spec_test", outcome = "z_rec", spec = "on_controls",
            stat = "r2", value = r2(m_c, type = "r2"))
    m_c0 <- feols(as.formula(paste("z_obs ~", ctrls_expr)),
                  data = d2, vcov = "hetero")
    add_row(block = "c_spec_test", outcome = "z_obs", spec = "on_controls",
            stat = "r2", value = r2(m_c0, type = "r2"))

    # ---- 6. (d) Estimates: three variants x four outcomes -------------------
    endog <- "chg_logMA_86_60_s0_elow"
    outcomes <- list(
        c("chg_log_pop_91_60",      "population"),
        c("chg_log_valprod_85_54",  "mfg_valprod"),
        c("chg_log_massal_85_54",   "mfg_wagemass"),
        c("chg_log_placebo_pop_60_47", "placebo_pretrend")
    )

    fit_variant <- function(y, instr, extra_ctrl = NULL) {
        cv <- c(geo_controls_main, extra_ctrl)
        f <- as.formula(sprintf("%s ~ %s | %s ~ %s",
                                y, paste(cv, collapse = " + "),
                                endog, instr))
        feols(f, data = d2, vcov = "hetero")
    }

    for (oc in outcomes) {
        y <- oc[1]; lbl <- oc[2]
        specs <- list(
            unadjusted = fit_variant(y, "z_obs"),
            recentered = fit_variant(y, "z_rec"),
            mu_control = fit_variant(y, "z_obs", extra_ctrl = "mu")
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
                "[res] (d) %-16s %-10s  b=%+.4f  se=%.4f  p=%.3f  F=%.1f  N=%d",
                lbl, sp, cc$est, cc$se, cc$p, fitstat_F(m), nobs(m)))
        }

        # Reduced-form RI p (sharp null beta = 0): coefficient of y on
        # the recentered instrument + controls, compared with the same
        # coefficient across permuted-draw recentered instruments.
        rf_coef <- function(zv) {
            dd <- cbind(d2, zv = zv)
            m <- feols(as.formula(paste(y, "~ zv +", ctrls_expr)),
                       data = dd, vcov = "hetero")
            unname(coef(m)[["zv"]])
        }
        b_obs <- rf_coef(d2$z_rec)
        b_null <- vapply(seq_len(ncol(zmat)), function(s) {
            rf_coef(zmat[, s] - d2$mu)
        }, numeric(1))
        p_rf <- mean(abs(b_null) >= abs(b_obs))
        add_row(block = "d_estimates", outcome = lbl, spec = "reduced_form",
                stat = "ri_p", value = p_rf)
        message(sprintf("[res] (d) %-16s reduced-form RI p = %.3f",
                        lbl, p_rf))
    }

    # ---- 7. Write outputs -----------------------------------------------------
    res <- do.call(rbind, out_rows)
    res$S_perm <- S_perm
    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)
    csv_path <- file.path(dir_tables, "diagnostic_recentering.csv")
    write.csv(res, csv_path, row.names = FALSE)

    txt_path <- file.path(dir_tables, "diagnostic_recentering.txt")
    sink(txt_path)
    cat("Recentering diagnostic (Borusyak-Hull 2023) -- Stage 1 report\n")
    cat(sprintf("Generated: %s  |  permuted draws: %d\n\n",
                Sys.time(), S_perm))
    cat(sprintf("(a) Observed instrument on expected instrument mu:\n"))
    cat(sprintf("    R2 = %.3f   sd(z) = %.3f -> sd(z - mu) = %.3f\n",
                r2_a, sd(d2$z_obs, na.rm = TRUE),
                sd(d2$z_rec, na.rm = TRUE)))
    cat(sprintf("(b) mu on geo_controls_main: R2 = %.3f\n", r2_b))
    cat(sprintf("(c) Spec test (recentered z on controls): RI p = %.3f\n\n",
                p_ri))
    cat("(d) IV estimates (coef / se / p / first-stage F):\n")
    for (oc in outcomes) {
        lbl <- oc[2]
        cat(sprintf("  %s\n", lbl))
        for (sp in c("unadjusted", "recentered", "mu_control")) {
            sel <- res$outcome == lbl & res$spec == sp
            g <- function(st) res$value[sel & res$stat == st]
            cat(sprintf("    %-11s  b=%+.4f  se=%.4f  p=%.3f  F=%.1f  N=%d\n",
                        sp, g("coef"), g("se"), g("p"), g("F"),
                        as.integer(g("N"))))
        }
        cat(sprintf("    reduced-form RI p = %.3f\n",
                    res$value[res$outcome == lbl &
                              res$spec == "reduced_form" &
                              res$stat == "ri_p"]))
    }
    sink()

    message(sprintf("[res] Saved: %s and .txt", csv_path))
}

main()

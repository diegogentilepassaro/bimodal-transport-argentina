# ===========================================================================
# diagnostic_recentering_hypo_results.R
#
# PURPOSE: Part C results: characterize the hypothetical-road instrument
#          via its node-permutation draws. EPISTEMIC STATUS: the
#          randomness is the researcher's (which marginal band cities
#          the network connects), so this CHARACTERIZES the instrument
#          -- generic backbone geography (mu_hypo) vs marginal node
#          choices -- rather than certifying design-based validity.
#
#   (a) Variance decomposition: R^2 of the observed hypo instrument
#       (chg_logMA_lcp_mst_s0_elow) on mu_hypo. A high share means the
#       instrument's content is common to all node choices, i.e. the
#       generic economic geography the exclusion argument must carry.
#   (b) mu_hypo on geo_controls_main.
#   (c) Spec-test analogue on the recentered hypo instrument (RI p).
#   (d) Estimates: total-MA and only-road treatments; variants
#       unadjusted IV-H (Table 13 Panel C convention for only_road),
#       recentered, mu-control. First-stage robust Wald F per variant.
#
# READS:   draws_hypo + estimation sample via _recentering_helpers.R
# PRODUCES:
#   results/tables/diagnostic_recentering_hypo.csv / .txt
#
# USAGE:  Rscript code/analysis/diagnostic_recentering_hypo_results.R
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
    message("diagnostic_recentering_hypo_results.R  |  Part C")
    message(strrep("=", 72))

    rc <- load_recentering_data(sector = "s0", instrument = "lcp_mst")
    d2 <- rc$d2; zmat <- rc$zmat; S_perm <- rc$S_perm

    ctrls <- geo_controls_main
    ctrls_expr <- paste(ctrls, collapse = " + ")

    out_rows <- list()
    add_row <- function(..., treatment = "") out_rows[[
        length(out_rows) + 1L]] <<-
        data.frame(treatment = treatment, ..., stringsAsFactors = FALSE)

    # (a) variance decomposition
    m_a <- feols(z_obs ~ mu, data = d2, vcov = "hetero")
    r2_a <- r2(m_a, type = "r2")
    message(sprintf("[hyr] (a) z_hypo on mu_hypo: R2 = %.3f, slope = %.3f",
                    r2_a, coef(m_a)[["mu"]]))
    add_row(block = "a_variance", outcome = "z_obs", spec = "on_mu",
            stat = "r2", value = r2_a)
    add_row(block = "a_variance", outcome = "z_obs", spec = "on_mu",
            stat = "slope", value = unname(coef(m_a)[["mu"]]))
    add_row(block = "a_variance", outcome = "z_obs", spec = "sd",
            stat = "sd_z", value = sd(d2$z_obs, na.rm = TRUE))
    add_row(block = "a_variance", outcome = "z_rec", spec = "sd",
            stat = "sd_zrec", value = sd(d2$z_rec, na.rm = TRUE))

    # (b) controls span mu_hypo?
    m_b <- feols(as.formula(paste("mu ~", ctrls_expr)),
                 data = d2, vcov = "hetero")
    r2_b <- r2(m_b, type = "r2")
    message(sprintf("[hyr] (b) mu_hypo on controls: R2 = %.3f", r2_b))
    add_row(block = "b_span", outcome = "mu", spec = "on_controls",
            stat = "r2", value = r2_b)

    # (c) spec-test analogue with RI p
    ssf <- function(zv) {
        ok <- complete.cases(d2[, ctrls]) & !is.na(zv)
        m <- feols(as.formula(paste("zv ~", ctrls_expr)),
                   data = cbind(d2, zv = zv)[ok, ], vcov = "hetero")
        sum(fitted(m)^2)
    }
    t_obs <- ssf(d2$z_rec)
    t_null <- vapply(seq_len(ncol(zmat)), function(s)
        ssf(zmat[, s] - d2$mu), numeric(1))
    p_ri <- (1 + sum(t_null >= t_obs)) / (length(t_null) + 1)
    message(sprintf("[hyr] (c) spec test: RI p = %.3f", p_ri))
    add_row(block = "c_spec_test", outcome = "z_rec", spec = "joint",
            stat = "ri_p", value = p_ri)

    # (d) estimates: total + only_road treatments
    treatments <- list(
        c("chg_logMA_86_60_s0_elow",     "total"),
        c("chg_logMA_only_road_s0_elow", "only_road")
    )
    outcomes <- list(
        c("chg_log_pop_91_60",         "population"),
        c("chg_log_valprod_85_54",     "mfg_valprod"),
        c("chg_log_massal_85_54",      "mfg_wagemass"),
        c("chg_log_placebo_pop_60_47", "placebo_pretrend")
    )
    for (tr in treatments) {
        endog <- tr[1]; tlbl <- tr[2]
        for (iv in c("z_obs", "z_rec")) {
            f1 <- as.formula(sprintf("%s ~ %s + %s", endog, iv, ctrls_expr))
            m1 <- feols(f1, data = d2, vcov = "hetero")
            tz <- safe_coef(m1, iv)
            add_row(block = "first_stage", treatment = tlbl, outcome = "",
                    spec = iv, stat = "F_robust_wald",
                    value = (tz$est / tz$se)^2)
            message(sprintf("[hyr] %-10s first stage %-6s F = %6.2f",
                            tlbl, iv, (tz$est / tz$se)^2))
        }
        for (oc in outcomes) {
            specs <- list(
                unadjusted = list(instr = "z_obs", ctrl = ctrls),
                recentered = list(instr = "z_rec", ctrl = ctrls),
                mu_control = list(instr = "z_obs", ctrl = c(ctrls, "mu"))
            )
            for (sp in names(specs)) {
                s <- specs[[sp]]
                f <- as.formula(sprintf("%s ~ %s | %s ~ %s", oc[1],
                                        paste(s$ctrl, collapse = " + "),
                                        endog, s$instr))
                m <- feols(f, data = d2, vcov = "hetero")
                cc <- safe_coef(m, paste0("fit_", endog))
                for (st in c("coef", "se", "p")) {
                    add_row(block = "d_estimates", treatment = tlbl,
                            outcome = oc[2], spec = sp, stat = st,
                            value = cc[[if (st == "coef") "est" else st]])
                }
                add_row(block = "d_estimates", treatment = tlbl,
                        outcome = oc[2], spec = sp, stat = "F_ivf",
                        value = fitstat_F(m))
                add_row(block = "d_estimates", treatment = tlbl,
                        outcome = oc[2], spec = sp, stat = "N",
                        value = nobs(m))
                message(sprintf(
                    "[hyr] %-10s %-16s %-11s b=%+.4f se=%.4f p=%.3f F=%.1f",
                    tlbl, oc[2], sp, cc$est, cc$se, cc$p, fitstat_F(m)))
            }
        }
    }

    res <- do.call(rbind, out_rows)
    res$S_perm <- S_perm
    csv_path <- file.path(dir_tables, "diagnostic_recentering_hypo.csv")
    write.csv(res, csv_path, row.names = FALSE)
    txt_path <- file.path(dir_tables, "diagnostic_recentering_hypo.txt")
    sink(txt_path)
    cat("Hypo-instrument node-permutation characterization (Part C)\n")
    cat(sprintf("Generated: %s  |  draws: %d\n\n", Sys.time(), S_perm))
    cat("EPISTEMIC NOTE: researcher randomization -- characterization,\n")
    cat("not design-based validity.\n\n")
    cat(sprintf("(a) R2(z_hypo on mu_hypo) = %.3f\n", r2_a))
    cat(sprintf("(b) R2(mu_hypo on controls) = %.3f\n", r2_b))
    cat(sprintf("(c) spec-test RI p = %.3f\n\n", p_ri))
    cat("(d) see CSV for all cells.\n")
    sink()
    message(sprintf("[hyr] Saved: %s and .txt", csv_path))
}

main()

# ===========================================================================
# diagnostic_recentering_hypo_curve.R
#
# PURPOSE: The backbone-share CURVE for the hypothetical-road
#          instrument (approved by Diego 2026-07-23). The backbone
#          share -- R^2 of the observed instrument on its expected
#          value over node permutations -- is mechanically decreasing
#          in the width of the counterfactual family, so no single
#          number is meaningful alone. This script reports the share
#          across the three families of increasing width:
#
#            band       marginal-band permutation (narrow: which
#                       10-25k towns are connected)
#            threshold  researcher-threshold randomization
#                       (T ~ U[10k, 50k]; network extent varies)
#            capsub     capital-subsampling probe (mechanical bound;
#                       counterfactuals leave the family of plausible
#                       construction rules)
#
#          Per family: backbone R^2, sd(z) vs sd(z - mu), recentered
#          first-stage robust Wald F vs the total-MA treatment, and
#          R^2 of mu on geo_controls_main.
#
# READS:   draws_hypo{,_threshold,_capsub} via _recentering_helpers.R
# PRODUCES:
#   results/tables/diagnostic_recentering_hypo_curve.csv / .txt
#
# USAGE:  Rscript code/analysis/diagnostic_recentering_hypo_curve.R
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
    message("diagnostic_recentering_hypo_curve.R  |  backbone-share curve")
    message(strrep("=", 72))

    endog <- "chg_logMA_86_60_s0_elow"
    ctrls_expr <- paste(geo_controls_main, collapse = " + ")

    rows <- list()
    for (dsg in c("band", "threshold", "capsub")) {
        rc <- load_recentering_data(sector = "s0",
                                    instrument = "lcp_mst",
                                    hypo_design = dsg)
        d2 <- rc$d2

        r2_bb <- r2(feols(z_obs ~ mu, data = d2, vcov = "hetero"),
                    type = "r2")
        r2_mu <- r2(feols(as.formula(paste("mu ~", ctrls_expr)),
                          data = d2, vcov = "hetero"), type = "r2")
        f1 <- feols(as.formula(sprintf("%s ~ z_rec + %s",
                                       endog, ctrls_expr)),
                    data = d2, vcov = "hetero")
        tz <- safe_coef(f1, "z_rec")
        F_rec <- (tz$est / tz$se)^2

        rows[[dsg]] <- data.frame(
            design = dsg,
            backbone_r2 = r2_bb,
            sd_z = sd(d2$z_obs, na.rm = TRUE),
            sd_zrec = sd(d2$z_rec, na.rm = TRUE),
            F_rec_robust = F_rec,
            r2_mu_on_controls = r2_mu,
            S_perm = rc$S_perm)
        message(sprintf(
            "[curve] %-9s backbone R2 = %.3f  sd(z)=%.2f->%.2f  F_rec=%.2f",
            dsg, r2_bb, sd(d2$z_obs, na.rm = TRUE),
            sd(d2$z_rec, na.rm = TRUE), F_rec))
    }

    res <- do.call(rbind, rows)
    csv_path <- file.path(dir_tables,
                          "diagnostic_recentering_hypo_curve.csv")
    write.csv(res, csv_path, row.names = FALSE)

    txt_path <- file.path(dir_tables,
                          "diagnostic_recentering_hypo_curve.txt")
    sink(txt_path)
    cat("Backbone-share curve for the hypothetical-road instrument\n")
    cat(sprintf("Generated: %s\n\n", Sys.time()))
    cat("The share is FAMILY-RELATIVE (mechanically decreasing in\n")
    cat("randomization width); read the curve, not any single point.\n")
    cat("capsub counterfactuals leave the family of plausible\n")
    cat("construction rules: mechanical bound only. Researcher\n")
    cat("randomization throughout -- characterization, not validity.\n\n")
    cat(sprintf("%-10s %10s %8s %8s %8s %8s\n", "design",
                "backboneR2", "sd(z)", "sd(zrec)", "F_rec", "R2mu"))
    for (i in seq_len(nrow(res))) {
        cat(sprintf("%-10s %10.3f %8.2f %8.2f %8.2f %8.3f\n",
                    res$design[i], res$backbone_r2[i], res$sd_z[i],
                    res$sd_zrec[i], res$F_rec_robust[i],
                    res$r2_mu_on_controls[i]))
    }
    sink()
    message(sprintf("[curve] Saved: %s and .txt", csv_path))
}

main()

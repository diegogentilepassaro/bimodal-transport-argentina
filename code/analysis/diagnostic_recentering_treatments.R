# ===========================================================================
# diagnostic_recentering_treatments.R
#
# PURPOSE: Recentered Larkin instrument against the paper's OTHER
#          emphasized treatment objects (approved by Diego 2026-07-22).
#          The Stage 1 diagnostic used the total-MA treatment (Table 9's
#          headline); this script asks how the recentered design fares
#          on:
#            only_rail  Delta log MA^only-rail (Table 13 Panel B: the
#                       paper's best-identified object, LP-instrumented)
#            only_road  Delta log MA^only-road (Table 13 Panel C;
#                       LP-instrumented here for symmetry -- the paper
#                       uses the hypo instrument for this panel, so the
#                       unadjusted column is NOT a paper column)
#            total      Delta log MA^full (Stage 1 reference row)
#
#          Sector argument (default s0) prepares the same grid for the
#          matched-schedule treatments once s1/s2 draw sets exist
#          (chg_logMA_86_60_<sector>_elow with the sector-matched
#          instrument draws).
#
# VARIANTS per treatment x outcome: unadjusted IV-LP (z_obs),
# recentered (z_rec), mu-control (z_obs + mu). Controls: the
# sector-consistent geo_controls_main analogue (baseline logMA of the
# same sector replaces the s0 one for s1/s2).
#
# READS:   draws + estimation sample via _recentering_helpers.R
# PRODUCES:
#   results/tables/diagnostic_recentering_treatments_<sector>.csv / .txt
#
# USAGE:  Rscript code/analysis/diagnostic_recentering_treatments.R [sector]
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

    args <- commandArgs(trailingOnly = TRUE)
    sector <- if (length(args) >= 1) args[1] else "s0"
    stopifnot(sector %in% c("s0", "s1", "s2"))

    message("\n", strrep("=", 72))
    message(sprintf(
        "diagnostic_recentering_treatments.R  |  sector %s", sector))
    message(strrep("=", 72))

    rc <- load_recentering_data(sector = sector)
    d2 <- rc$d2; S_perm <- rc$S_perm

    # Sector-consistent controls: swap the s0 baseline logMA for the
    # matching sector's baseline where applicable.
    ctrls <- geo_controls_main
    if (sector != "s0") {
        ctrls[ctrls == "logMA_actual_1960_s0_elow"] <-
            sprintf("logMA_actual_1960_%s_elow", sector)
    }
    ctrls_mu <- c(ctrls, "mu")

    treatments <- list(
        c(sprintf("chg_logMA_86_60_%s_elow", sector),     "total"),
        c(sprintf("chg_logMA_only_rail_%s_elow", sector), "only_rail"),
        c(sprintf("chg_logMA_only_road_%s_elow", sector), "only_road")
    )
    outcomes <- list(
        c("chg_log_pop_91_60",         "population"),
        c("chg_log_valprod_85_54",     "mfg_valprod"),
        c("chg_log_massal_85_54",      "mfg_wagemass"),
        c("chg_log_placebo_pop_60_47", "placebo_pretrend")
    )

    out_rows <- list()
    add_row <- function(...) out_rows[[length(out_rows) + 1L]] <<-
        data.frame(..., stringsAsFactors = FALSE)

    for (tr in treatments) {
        endog <- tr[1]; tlbl <- tr[2]
        stopifnot(endog %in% names(d2))

        # Direct first stage per instrument variant (robust Wald F).
        for (iv in c("z_obs", "z_rec")) {
            f1 <- as.formula(sprintf("%s ~ %s + %s", endog, iv,
                                     paste(ctrls, collapse = " + ")))
            m1 <- feols(f1, data = d2, vcov = "hetero")
            tz <- safe_coef(m1, iv)
            add_row(block = "first_stage", treatment = tlbl,
                    outcome = "", spec = iv, stat = "F_robust_wald",
                    value = (tz$est / tz$se)^2)
            message(sprintf("[trt] %-10s first stage %-6s  F = %6.2f",
                            tlbl, iv, (tz$est / tz$se)^2))
        }

        for (oc in outcomes) {
            y <- oc[1]; olbl <- oc[2]
            specs <- list(
                unadjusted = list(instr = "z_obs", ctrl = ctrls),
                recentered = list(instr = "z_rec", ctrl = ctrls),
                mu_control = list(instr = "z_obs", ctrl = ctrls_mu)
            )
            for (sp in names(specs)) {
                s <- specs[[sp]]
                f <- as.formula(sprintf("%s ~ %s | %s ~ %s",
                                        y, paste(s$ctrl, collapse = " + "),
                                        endog, s$instr))
                m <- feols(f, data = d2, vcov = "hetero")
                cc <- safe_coef(m, paste0("fit_", endog))
                add_row(block = "estimates", treatment = tlbl,
                        outcome = olbl, spec = sp, stat = "coef",
                        value = cc$est)
                add_row(block = "estimates", treatment = tlbl,
                        outcome = olbl, spec = sp, stat = "se",
                        value = cc$se)
                add_row(block = "estimates", treatment = tlbl,
                        outcome = olbl, spec = sp, stat = "p",
                        value = cc$p)
                add_row(block = "estimates", treatment = tlbl,
                        outcome = olbl, spec = sp, stat = "F_ivf",
                        value = fitstat_F(m))
                add_row(block = "estimates", treatment = tlbl,
                        outcome = olbl, spec = sp, stat = "N",
                        value = nobs(m))
                message(sprintf(
                    "[trt] %-10s %-16s %-11s b=%+.4f se=%.4f p=%.3f F=%.1f",
                    tlbl, olbl, sp, cc$est, cc$se, cc$p, fitstat_F(m)))
            }
        }
    }

    res <- do.call(rbind, out_rows)
    res$S_perm <- S_perm; res$sector <- sector
    csv_path <- file.path(dir_tables,
        sprintf("diagnostic_recentering_treatments_%s.csv", sector))
    write.csv(res, csv_path, row.names = FALSE)

    txt_path <- file.path(dir_tables,
        sprintf("diagnostic_recentering_treatments_%s.txt", sector))
    sink(txt_path)
    cat("Recentered Larkin instrument x alternative treatments\n")
    cat(sprintf("Generated: %s  |  sector %s  |  draws: %d\n\n",
                Sys.time(), sector, S_perm))
    cat("NOTE: only_road's unadjusted column is LP-instrumented here for\n")
    cat("symmetry; the paper's Table 13 Panel C uses the hypo instrument.\n\n")
    fs <- res[res$block == "first_stage", ]
    cat("First-stage robust Wald F (full sample):\n")
    for (i in seq_len(nrow(fs))) {
        cat(sprintf("  %-10s %-6s  F = %6.2f\n",
                    fs$treatment[i], fs$spec[i], fs$value[i]))
    }
    cat("\nEstimates: see CSV for all cells.\n")
    sink()
    message(sprintf("[trt] Saved: %s and .txt", csv_path))
}

main()

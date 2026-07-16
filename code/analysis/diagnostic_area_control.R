# ===========================================================================
# diagnostic_area_control.R
#
# PURPOSE: Referee-proofing for the Section 4.5 balance findings. Both
#          instruments correlate with log district area (Table 6:
#          LP p<0.001, Hypo p=0.008 post issue #22), and log area is
#          NOT in geo_controls_main — so the conditional-independence
#          argument does not cover it. This diagnostic refits the two
#          headline specifications with log(area_km2) ADDED to the
#          control set and reports how the estimates move.
#
#          A specification change (promoting area into the main control
#          set) is a coauthor decision; this script only produces the
#          evidence. See .kiro/tasks.md deferred ledger.
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/diagnostic_area_control.csv
#   results/tables/diagnostic_area_control.txt
#
# USAGE: Rscript code/analysis/diagnostic_area_control.R
#        (after build_estimation_sample.R; not a main.R step yet)
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
})

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)

    message("\n", strrep("=", 72))
    message("diagnostic_area_control.R  |  log(area) added to controls")
    message(strrep("=", 72))

    d <- as.data.frame(arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet")
    ))
    # Same derivation as table_6_pre_balance.R.
    d$log_area_km2 <- log(d$area_km2)
    stopifnot(sum(!is.na(d$log_area_km2)) == nrow(d))

    ctrls_base <- geo_controls_main
    ctrls_area <- c(geo_controls_main, "log_area_km2")

    specs <- list(
        list(y = "chg_log_pop_91_60",          lbl = "population"),
        list(y = "chg_log_placebo_pop_60_47",  lbl = "placebo"),
        list(y = "chg_log_valprod_85_54",      lbl = "mfg_valprod"),
        list(y = "chg_log_massal_85_54",       lbl = "mfg_wagemass")
    )

    rows <- list()
    for (s in specs) {
        for (cs in list(list(id = "baseline",   ctrls = ctrls_base),
                        list(id = "with_area",  ctrls = ctrls_area))) {
            fits <- fit_iv_quad(
                y          = s$y,
                data       = d,
                endog      = "chg_logMA_86_60_s0_elow",
                lp_instr   = "chg_logMA_stu_s0_elow",
                hypo_instr = main_hypo_instrument,
                ctrls_vec  = cs$ctrls
            )
            for (spec_name in names(fits)) {
                cf <- safe_coef(fits[[spec_name]],
                                "chg_logMA_86_60_s0_elow")
                # fixest names the instrumented regressor fit_<endog>
                if (is.na(cf$est)) {
                    cf <- safe_coef(fits[[spec_name]],
                                    "fit_chg_logMA_86_60_s0_elow")
                }
                Fst <- if (spec_name == "OLS") NA_real_ else {
                    fitstat_F(fits[[spec_name]])
                }
                rows[[length(rows) + 1]] <- data.frame(
                    outcome  = s$lbl,
                    controls = cs$id,
                    spec     = spec_name,
                    estimate = cf$est,
                    std_err  = cf$se,
                    p_value  = cf$p,
                    first_stage_F = Fst,
                    n_obs    = fits[[spec_name]]$nobs
                )
            }
        }
    }
    res <- do.call(rbind, rows)

    out_csv <- file.path(dir_tables, "diagnostic_area_control.csv")
    write.csv(res, out_csv, row.names = FALSE)
    message(sprintf("Saved: %s", out_csv))

    out_txt <- file.path(dir_tables, "diagnostic_area_control.txt")
    sink(out_txt); on.exit(sink(), add = TRUE)
    cat(strrep("=", 74), "\n")
    cat("AREA-CONTROL DIAGNOSTIC (Section 4.5 balance follow-up)\n")
    cat("Both instruments correlate with log district area, which is not\n")
    cat("in geo_controls_main. Rows compare the headline specs with and\n")
    cat("without log(area_km2) as a control. HC1 SE.\n")
    cat(sprintf("Generated: %s\n", Sys.time()))
    cat(strrep("=", 74), "\n\n")
    for (lbl in unique(res$outcome)) {
        cat(sprintf("[%s]\n", lbl))
        sub <- res[res$outcome == lbl, ]
        for (i in seq_len(nrow(sub))) {
            cat(sprintf(
                "  %-10s %-9s beta=%+.4f  se=%.4f  p=%.3f  F=%s  N=%d\n",
                sub$controls[i], sub$spec[i], sub$estimate[i],
                sub$std_err[i], sub$p_value[i],
                ifelse(is.na(sub$first_stage_F[i]), "--",
                       sprintf("%.1f", sub$first_stage_F[i])),
                sub$n_obs[i]
            ))
        }
        cat("\n")
    }
    message(sprintf("Saved: %s", out_txt))

    message(strrep("=", 72))
    message("diagnostic_area_control.R  |  Complete.")
    message(strrep("=", 72), "\n")
}

main()

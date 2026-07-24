# ===========================================================================
# diagnostic_placebo_1947baseline.R
#
# PURPOSE: Cote's identification experiment 1.1 (email 2026-07-24):
#          the pre-trends placebo (Table 7) conditions on 1960
#          baselines (log pop 1960, log MA 1960), but for a placebo
#          whose outcome is 1947-60 growth one can argue the baselines
#          should be 1947 values. This diagnostic reruns the exact
#          Table 7 specification under alternative baseline sets.
#          DIAGNOSTIC ONLY: Table 7 in the paper is unchanged.
#
#          LIMITATION (documented, substantive): a 1947 market access
#          does not exist -- no 1947 network was digitized, and MA is
#          defined only for the 1960 and 1986 networks. Cote's "use
#          1947 baselines" is therefore implementable only for
#          population; the MA baseline can be kept (1960) or dropped.
#
#          SCOPE CAVEAT (cr-review PR #120): the post-outcome-
#          conditioning critique is SPECIFIC TO THE PLACEBO, whose
#          outcome window (1947-60) ends at the baseline date; for
#          the main 1960-91 regressions the 1960 baselines are
#          pre-outcome and remain legitimate. Do not read this
#          diagnostic as indicting geo_controls_main in Tables 8-11.
#          The F-strengthening when the MA baseline drops (11.8 ->
#          23.2 here) is the same absorption mechanism documented for
#          the main spec in .kiro/baseline_ma_control_note.md
#          (13.6 -> 29.9); that note's recommendation to KEEP the
#          baseline-MA control in the main spec coexists with this
#          finding via the timing argument above.
#
# VARIANTS (all: DV = chg_log_placebo_pop_60_47, endog =
#          chg_logMA_86_60_s0_elow, instruments as in Table 7,
#          estimators OLS / IV-LP / IV-Hypo / IV-Both via
#          fit_iv_quad, HC1):
#   t7        geo_controls_main as-is (log pop 1960 + log MA 1960);
#             reproduction anchor -- must match Table 7.
#   pop47     log_pop_1947 replaces log_pop_1960; MA 1960 kept.
#   full47    log_pop_1947 replaces log_pop_1960; MA baseline DROPPED
#             (the most 1947-consistent implementable set).
#   nobase    neither pop nor MA baseline (six geo controls only);
#             bounds the role of the baselines jointly.
#
# SAMPLE: identical across variants by construction (the DV requires
#         pop_1947 > 0, so log_pop_1947 is defined wherever the DV
#         is); asserted below.
#
# READS:   data/derived/06_analysis/estimation_sample.parquet
# PRODUCES:
#   results/tables/diagnostic_placebo_1947.csv / .txt
#
# USAGE:   Rscript code/analysis/diagnostic_placebo_1947baseline.R
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
    message("diagnostic_placebo_1947baseline.R  |  Cote 1.1")
    message(strrep("=", 72))

    d <- ensure_geolev2_char(as.data.frame(arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))))
    stopifnot(nrow(d) == 311L)
    d$log_pop_1947 <- ifelse(!is.na(d$pop_1947) & d$pop_1947 > 0,
                             log(d$pop_1947), NA_real_)

    y <- "chg_log_placebo_pop_60_47"
    # The DV requires 1947 population; assert the shared-sample claim.
    stopifnot(all(!is.na(d$log_pop_1947[!is.na(d[[y]])])))

    base_geo <- setdiff(geo_controls_main,
                        c("logMA_actual_1960_s0_elow", "log_pop_1960"))
    variants <- list(
        t7     = geo_controls_main,
        pop47  = c(base_geo, "logMA_actual_1960_s0_elow",
                   "log_pop_1947"),
        full47 = c(base_geo, "log_pop_1947"),
        nobase = base_geo
    )

    # One estimation sample across variants: complete cases on the
    # union of all controls + DV + endog + instruments.
    all_vars <- unique(c(y, "chg_logMA_86_60_s0_elow",
                         "chg_logMA_stu_s0_elow", main_hypo_instrument,
                         unlist(variants)))
    dd <- d[complete.cases(d[, all_vars]), ]
    message(sprintf("[p47] common estimation sample: N = %d", nrow(dd)))

    out_rows <- list()
    add_row <- function(...) out_rows[[length(out_rows) + 1L]] <<-
        data.frame(..., stringsAsFactors = FALSE)

    # Self-enforcing anchor (cr-review PR #120): the t7 variant must
    # reproduce the committed Table 7 numbers; fail loudly on drift.
    t7_ref <- read.csv(file.path(dir_tables, "table_7_pre_trends.csv"),
                       stringsAsFactors = FALSE)
    spec_map <- c("OLS" = "OLS", "IV-LP" = "IV-LP",
                  "IV-H" = "IV-Hypo", "IV-B" = "IV-Both")
    check_anchor <- function(sp, cc, Fv, n) {
        r <- t7_ref[t7_ref$spec == spec_map[[sp]], ]
        stopifnot(nrow(r) == 1L,
                  abs(cc$est - r$estimate) < 1e-8,
                  abs(cc$se - r$std_err) < 1e-8,
                  n == r$n_obs,
                  is.na(r$first_stage_F) ||
                      abs(Fv - r$first_stage_F) < 1e-6)
    }

    for (vn in names(variants)) {
        fits <- fit_iv_quad(
            y = y, data = dd,
            endog = "chg_logMA_86_60_s0_elow",
            lp_instr = "chg_logMA_stu_s0_elow",
            hypo_instr = main_hypo_instrument,
            ctrls_vec = variants[[vn]]
        )
        for (sp in names(fits)) {
            m <- fits[[sp]]
            cn <- if (sp == "OLS") "chg_logMA_86_60_s0_elow" else
                "fit_chg_logMA_86_60_s0_elow"
            cc <- safe_coef(m, cn)
            Fv <- if (sp == "OLS") NA_real_ else fitstat_F(m)
            if (vn == "t7") check_anchor(sp, cc, Fv, nobs(m))
            add_row(variant = vn, spec = sp, stat = "coef",
                    value = cc$est)
            add_row(variant = vn, spec = sp, stat = "se",
                    value = cc$se)
            add_row(variant = vn, spec = sp, stat = "p",
                    value = cc$p)
            add_row(variant = vn, spec = sp, stat = "F", value = Fv)
            add_row(variant = vn, spec = sp, stat = "N",
                    value = nobs(m))
            message(sprintf(
                "[p47] %-7s %-7s b=%+.4f se=%.4f p=%.3f F=%s N=%d",
                vn, sp, cc$est, cc$se, cc$p,
                ifelse(is.na(Fv), "--", sprintf("%.1f", Fv)),
                nobs(m)))
        }
    }

    res <- do.call(rbind, out_rows)
    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)
    csv_path <- file.path(dir_tables, "diagnostic_placebo_1947.csv")
    write.csv(res, csv_path, row.names = FALSE)

    g <- function(vn, sp, st_) res$value[res$variant == vn &
                                         res$spec == sp & res$stat == st_]
    sink(file.path(dir_tables, "diagnostic_placebo_1947.txt"))
    cat("Placebo baseline experiment (Cote 1.1, email 2026-07-24)\n")
    cat(sprintf("Generated: %s  |  common N = %d\n\n",
                Sys.time(), nrow(dd)))
    cat("DV = chg_log_placebo_pop_60_47; endog/instruments as Table 7.\n")
    cat("NOTE: no 1947 MA exists (no 1947 network); 'full47' is the\n")
    cat("most 1947-consistent implementable set.\n")
    cat("SCOPE: the post-outcome-conditioning critique is specific to\n")
    cat("the placebo (outcome window 1947-60 ends at the baseline\n")
    cat("date). For the main 1960-91 regressions the 1960 baselines\n")
    cat("are pre-outcome and remain legitimate; see\n")
    cat(".kiro/baseline_ma_control_note.md for the related absorption\n")
    cat("mechanism in the main spec.\n")
    cat("ATTRIBUTION: the 1960 MA baseline is the dominant driver --\n")
    cat("swapping pop to 1947 alone only moves IV-B p from 0.034 to\n")
    cat("0.085; dropping the MA baseline cleans the placebo fully.\n\n")
    for (vn in names(variants)) {
        cat(sprintf("%s  (controls: %s)\n", vn,
                    paste(variants[[vn]], collapse = ", ")))
        for (sp in c("OLS", "IV-LP", "IV-H", "IV-B")) {
            cat(sprintf(
                "  %-6s b=%+.4f  se=%.4f  p=%.3f  F=%s  N=%d\n", sp,
                g(vn, sp, "coef"), g(vn, sp, "se"), g(vn, sp, "p"),
                ifelse(is.na(g(vn, sp, "F")), "--",
                       sprintf("%.1f", g(vn, sp, "F"))),
                as.integer(g(vn, sp, "N"))))
        }
        cat("\n")
    }
    sink()
    message(sprintf("[p47] Saved: %s and .txt", csv_path))
}

main()

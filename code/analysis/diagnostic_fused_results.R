# ===========================================================================
# diagnostic_fused_results.R
#
# PURPOSE: Consume the fused-instrument permutation draws (BH-2026
#          efficient-IV test; engine = diagnostic_recentering_draws.R
#          variant "fused") and answer ONE question: does fusing the
#          Larkin rail variation with the LCP-MST hypothetical road
#          world extract more RECENTERED first-stage strength than the
#          plain Larkin instrument? The permutations are the same
#          (same seed stream, same line strata), so draw s of the two
#          designs shares its studied_perm assignment and the
#          comparison is paired. DIAGNOSTIC ONLY.
#
# DEFINITIONS (both designs, s0/elow):
#   stu:   z^(s) = logMA(non-studied-perm rails + 1954 roads) -
#          logMA_actual_1960; draws/ dir; z_obs = panel column
#          chg_logMA_stu_s0_elow (identity cross-check).
#   fused: z^(s) = logMA(non-studied-perm rails + LCP-MST roads) -
#          logMA_actual_1960; draws_fused/ dir; no panel column, so
#          z_obs = identity draw (engine validated it against the
#          committed ma_instrument_fused baseline at 0.00e+00).
#
# READS:
#   data/derived/07_recentering/draws{,_fused}/z_rc*.parquet
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/diagnostic_fused.csv / .txt
#
# USAGE:
#   Rscript code/analysis/diagnostic_fused_results.R
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
    message("diagnostic_fused_results.R  |  fused vs stu, recentered")
    message(strrep("=", 72))

    d <- ensure_geolev2_char(as.data.frame(arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))))
    stopifnot(nrow(d) == 311L)
    base_col <- "logMA_actual_1960_s0_elow"

    load_design <- function(dirname) {
        files <- sort(list.files(
            file.path(dir_derived_recentering, dirname),
            pattern = "^z_rc\\d+\\.parquet$", full.names = TRUE))
        stopifnot(length(files) >= 11L)
        dr <- ensure_geolev2_char(do.call(rbind, lapply(files, function(f)
            as.data.frame(arrow::read_parquet(f)))))
        zw <- reshape(dr[, c("geolev2", "draw", "logMA")],
                      idvar = "geolev2", timevar = "draw",
                      direction = "wide")
        m <- merge(d[, "geolev2", drop = FALSE], zw, by = "geolev2",
                   all.x = TRUE)
        m <- m[match(d$geolev2, m$geolev2), ]
        ids <- sort(unique(dr$draw[dr$draw > 0L]))
        list(zmat = as.matrix(m[, sprintf("logMA.%d", ids)]) -
                 d[[base_col]],
             z_id = m$logMA.0 - d[[base_col]],
             S = length(ids))
    }
    stu   <- load_design("draws")
    fused <- load_design("draws_fused")
    stopifnot(stu$S >= 100L, fused$S >= 100L,
              !anyNA(stu$zmat), !anyNA(fused$zmat),
              !anyNA(stu$z_id), !anyNA(fused$z_id))
    message(sprintf("[fres] draws: stu S=%d, fused S=%d", stu$S, fused$S))

    # Identity cross-checks: stu against the panel column; fused's
    # engine-level check ran against the committed baseline, assert
    # again defensively here that stu's holds (validates the loader).
    dev <- max(abs(stu$z_id - d$chg_logMA_stu_s0_elow), na.rm = TRUE)
    message(sprintf("[fres] stu identity vs panel: max dev = %.2e", dev))
    stopifnot(dev < 1e-6)

    mk <- function(x) {
        mu <- rowMeans(x$zmat)
        list(mu = mu, z_obs = x$z_id, z_rec = x$z_id - mu,
             mu_loo = (x$S * mu - x$zmat) / (x$S - 1), zmat = x$zmat,
             S = x$S)
    }
    S_ <- list(stu = mk(stu), fused = mk(fused))

    out_rows <- list()
    add_row <- function(...) out_rows[[length(out_rows) + 1L]] <<-
        data.frame(..., stringsAsFactors = FALSE)

    ctrls_expr <- paste(geo_controls_main, collapse = " + ")
    endogs <- list(total = "chg_logMA_86_60_s0_elow",
                   rail_only = "chg_logMA_only_rail_s0_elow")
    outcomes <- list(
        c("chg_log_pop_91_60",         "population"),
        c("chg_log_valprod_85_54",     "mfg_valprod"),
        c("chg_log_massal_85_54",      "mfg_wagemass"),
        c("chg_log_placebo_pop_60_47", "placebo_pretrend"))

    for (nm in names(S_)) {
        ds <- S_[[nm]]
        dd <- cbind(d, mu = ds$mu, z_obs = ds$z_obs, z_rec = ds$z_rec)

        m_a <- feols(z_obs ~ mu, data = dd, vcov = "hetero")
        add_row(design = nm, block = "a", outcome = "z_obs",
                spec = "on_mu", stat = "r2",
                value = r2(m_a, type = "r2"))
        add_row(design = nm, block = "a", outcome = "z_obs",
                spec = "on_mu", stat = "slope",
                value = unname(coef(m_a)[["mu"]]))
        message(sprintf("[fres] (%s) backbone R2 = %.3f, slope = %.3f",
                        nm, r2(m_a, "r2"), coef(m_a)[["mu"]]))

        for (en in names(endogs)) {
            f <- as.formula(sprintf(
                "%s ~ %s | %s ~ z_rec",
                outcomes[[1]][1], ctrls_expr, endogs[[en]]))
            m <- feols(f, data = dd, vcov = "hetero")
            add_row(design = nm, block = "F", outcome = en,
                    spec = "recentered", stat = "F",
                    value = fitstat_F(m))
            message(sprintf("[fres] (%s) recentered F vs %-9s = %.2f",
                            nm, en, fitstat_F(m)))
        }

        for (oc in outcomes) {
            y <- oc[1]; lbl <- oc[2]
            f <- as.formula(sprintf("%s ~ %s | %s ~ z_rec",
                                    y, ctrls_expr, endogs$total))
            m <- feols(f, data = dd, vcov = "hetero")
            cc <- safe_coef(m, paste0("fit_", endogs$total))
            add_row(design = nm, block = "d", outcome = lbl,
                    spec = "recentered", stat = "coef", value = cc$est)
            add_row(design = nm, block = "d", outcome = lbl,
                    spec = "recentered", stat = "se", value = cc$se)
            add_row(design = nm, block = "d", outcome = lbl,
                    spec = "recentered", stat = "p", value = cc$p)

            rf_coef <- function(zv) {
                m2 <- feols(as.formula(paste(y, "~ zv +", ctrls_expr)),
                            data = cbind(d, zv = zv), vcov = "hetero")
                unname(coef(m2)[["zv"]])
            }
            b_obs <- rf_coef(ds$z_rec)
            b_null <- vapply(seq_len(ncol(ds$zmat)), function(s)
                rf_coef(ds$zmat[, s] - ds$mu_loo[, s]), numeric(1))
            p_rf <- (1 + sum(abs(b_null) >= abs(b_obs))) /
                (length(b_null) + 1)
            add_row(design = nm, block = "d", outcome = lbl,
                    spec = "reduced_form", stat = "ri_p", value = p_rf)
            message(sprintf(
                "[fres] (%s) %-16s recentered b=%+.4f se=%.4f RI p=%.3f",
                nm, lbl, cc$est, cc$se, p_rf))
        }
    }

    # Head-to-head: paired draws (same permutations), so the difference
    # in recentered instruments is attributable to the road world.
    v <- cor(S_$stu$z_rec, S_$fused$z_rec, use = "complete.obs")
    add_row(design = "both", block = "x", outcome = "z_rec",
            spec = "stu_vs_fused", stat = "cor", value = v)
    message(sprintf("[fres] cor(z_rec stu, z_rec fused) = %+.3f", v))

    res <- do.call(rbind, out_rows)
    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)
    csv_path <- file.path(dir_tables, "diagnostic_fused.csv")
    write.csv(res, csv_path, row.names = FALSE)

    g <- function(dn, bl, oc, sp, st_) res$value[
        res$design == dn & res$block == bl & res$outcome == oc &
        res$spec == sp & res$stat == st_]
    sink(file.path(dir_tables, "diagnostic_fused.txt"))
    cat("Fused-instrument diagnostic (BH-2026 efficiency test)\n")
    cat(sprintf("Generated: %s  |  S = %d (paired permutations)\n\n",
                Sys.time(), S_$stu$S))
    cat("Question: does network-consistent weighting (LCP-MST road\n")
    cat("world) extract more recentered first-stage strength from the\n")
    cat("same permuted Larkin variation than the plain instrument?\n\n")
    for (nm in c("stu", "fused")) {
        cat(sprintf("%s:\n", nm))
        cat(sprintf("  backbone R2 = %.3f  slope = %.3f\n",
                    g(nm, "a", "z_obs", "on_mu", "r2"),
                    g(nm, "a", "z_obs", "on_mu", "slope")))
        cat(sprintf("  recentered F: total = %.2f, rail_only = %.2f\n",
                    g(nm, "F", "total", "recentered", "F"),
                    g(nm, "F", "rail_only", "recentered", "F")))
        for (oc in outcomes) {
            cat(sprintf(
                "  %-18s b=%+.4f  se=%.4f  p=%.3f  RI p=%.3f\n", oc[2],
                g(nm, "d", oc[2], "recentered", "coef"),
                g(nm, "d", oc[2], "recentered", "se"),
                g(nm, "d", oc[2], "recentered", "p"),
                g(nm, "d", oc[2], "reduced_form", "ri_p")))
        }
        cat("\n")
    }
    cat(sprintf("cor(z_rec stu, z_rec fused) = %+.3f\n",
                g("both", "x", "z_rec", "stu_vs_fused", "cor")))
    sink()
    message(sprintf("[fres] Saved: %s and .txt", csv_path))
}

main()

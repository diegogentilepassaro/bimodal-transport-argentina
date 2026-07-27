# ===========================================================================
# diagnostic_placebo_ma1947.R
#
# PURPOSE: two checks that agenda item B (adopt a 1947-consistent placebo
#          spec as Table 7?) turns on. Both were missing from PR #120 /
#          #139. DIAGNOSTIC ONLY: no paper exhibit changes.
#
# PART 1 — is `full47` defensible, or did we drop a control until the
#          test passed?
#   The placebo's clean null comes from DROPPING baseline log MA, not
#   from swapping the population baseline to 1947
#   (diagnostic_placebo_1947.txt: pop47 moves IV-B p only 0.034 ->
#   0.085; full47 takes beta to -0.004, p = 0.893). The stated reason
#   for dropping it is post-outcome conditioning: MA_i(1960) =
#   sum_{j != i} Pop_j(1960) / tau_ij^theta, so it is a distance-
#   weighted average of OTHER districts' 1960 population, and 1960
#   population embeds growth realized over the placebo window itself.
#
#   The test: build a baseline-MA level that keeps the spatial structure
#   but removes the realized-1947-60 population content, by re-weighting
#   the SAME 1960 tau matrix with 1947 population, and rerun the placebo
#   with it. The 1960-weighted counterpart on the same destination set is
#   estimated alongside to separate the population-year effect from the
#   destination-set effect.
#
#   POWER LIMIT (cr-review PR #143, measured below, not assumed): the
#   two constructed levels are near-collinear, so the ma47-vs-ma60
#   contrast cannot discriminate between them by construction. That is
#   not a failure of the test, it IS the finding — MA(1960)'s post-1947
#   population content is negligible, so "it is post-outcome" cannot
#   carry the weight of dropping it. The conclusion rests on the
#   correlation, NOT on comparing the two point estimates (whose CIs
#   overlap heavily; reported below).
#
#   NETWORK CAVEAT: tau is the 1960 network in both MA levels (no 1947
#   network was digitized), so the two differ ONLY in population year.
#   Residual asymmetry: the network geometry postdates the placebo
#   window. Argentine rail was substantially complete decades before
#   1947, so this is a weak channel, but not zero.
#
#   DESTINATION SET (design decision, approved 2026-07-27): 1947
#   population exists for 237 of the 311 estimation-sample districts.
#   Setting the missing ones to 0 (the pipeline's default for absent
#   destinations) would understate MA for districts near uncovered
#   ones, and coverage is spatially clustered, so the control would
#   carry 1947-census-coverage geography. Imputing 1947 population from
#   1960 would reintroduce the content being removed. Instead BOTH new
#   levels sum over the same 237 covered destinations.
#   DISCLOSURE (cr-review PR #143): the pipeline control sums over 312
#   destinations, not 311 — the tau file includes 32002001 (Capital
#   Federal), which is absent from the estimation sample. The restricted
#   set therefore drops 75 destinations, Capital Federal among them, and
#   Capital Federal is a large share of many districts' MA. The dropped
#   share is COMPUTED and reported below rather than asserted to be
#   small. It drops out of the ma47/ma60 pair equally, so that contrast
#   is unaffected; comparisons against t7 (which uses the pipeline's
#   312-destination control) mix a destination-set difference with the
#   population-year difference, which is why pop47 is also estimated.
#
# PART 2 — does pre-1960 growth drive the headline estimates?
#   Part 1 and Table 7 are the INDIRECT test (does the treatment predict
#   pre-period growth?). The direct test conditions the main 1960-91
#   regression on 1947-60 growth. Never run before this script.
#
#   Adding the control also changes the sample (237 of 311), so three
#   rows per outcome:
#     (1) full 311, published spec            -- anchor
#     (2) 237 subsample, published spec       -- isolates SELECTION
#     (3) 237 subsample + 1947-60 growth      -- isolates the CONFOUND
#   (2)-(1) is selection; (3)-(2) is the confound.
#
#   WHAT ROW 3 ACTUALLY IS (cr-review PR #143): algebraically a
#   log_pop_1947 control, not a growth-trend control.
#   chg_log_placebo_pop_60_47 == log_pop_1960 - log_pop_1947 exactly,
#   and log_pop_1960 is already in geo_controls_main, so adding the
#   growth term spans the same space as adding log_pop_1947. Row 3 is
#   therefore a CONVERGENCE control: "does the estimate survive
#   conditioning on the 1947 level as well as the 1960 level?" The
#   treatment coefficient is identical either way — asserted below by
#   estimating row 3b with log_pop_1947 in place of the growth term —
#   and the reported "pre-trend coefficient" equals MINUS the
#   log_pop_1947 coefficient. The estimates and the substantive reading
#   are unaffected; the label was wrong.
#
#   INTERPRETATION LIMIT: conditioning on a lagged level/growth carries
#   its own mean-reversion mechanics, so row 3 is a robustness statement
#   ("the estimate survives"), NOT cleaner identification.
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#   data/derived/03_taus/tau_actual_1960_s0.parquet
#   data/derived/base/census_1960/census_1960_ipums.parquet  (gate only)
#   results/tables/table_7_pre_trends.csv          (t7 anchor)
#   results/tables/diagnostic_placebo_1947.csv     (pop47/full47 anchors)
#   results/tables/table_9_population_iv.csv       (part 2 row 1 anchor)
#   results/tables/table_10_sectoral_iv.csv        (part 2 row 1 anchor)
#
# PRODUCES:
#   results/tables/diagnostic_placebo_ma1947.csv / .txt
#
# USAGE: Rscript code/analysis/diagnostic_placebo_ma1947.R
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
})

# ---------------------------------------------------------------------------
# build_ma_level(tau_df, pop_df, theta_val, dest_keep)
#
# MA_i = sum_{j != i, j in dest_keep} Pop_j / tau_ij^theta, then log.
# Symmetrisation and the Inf-tau -> weight 0 rule are copied from
# code/pipeline/04_market_access.R:compute_ma_one_case() (diagnostics stay
# self-contained by repo convention). Two deliberate differences: (a)
# destinations are restricted to dest_keep instead of using every district
# in the tau file, and (b) absent destination population is an ERROR here
# rather than being coerced to 0 -- the point of the restriction is that
# no destination silently enters with pop 0.
#
# Equivalence with the pipeline is not asserted in prose: gate_pipeline()
# below calls this with the full destination set and the 1960 census and
# requires an EXACT match against the committed control.
# ---------------------------------------------------------------------------
build_ma_level <- function(tau_df, pop_df, theta_val, dest_keep) {
    sym <- rbind(
        data.frame(origin = tau_df$origin_geolev2,
                   dest   = tau_df$destination_geolev2,
                   tau    = tau_df$tau),
        data.frame(origin = tau_df$destination_geolev2,
                   dest   = tau_df$origin_geolev2,
                   tau    = tau_df$tau)
    )
    sym <- sym[sym$dest %in% dest_keep & sym$origin != sym$dest, ]

    sym <- merge(sym,
                 data.frame(dest = pop_df$geolev2, pop_dest = pop_df$pop),
                 by = "dest", all.x = TRUE)
    stopifnot(!any(is.na(sym$pop_dest)), all(sym$pop_dest > 0))

    sym$w <- ifelse(is.finite(sym$tau) & sym$tau > 0,
                    1 / (sym$tau^theta_val), 0)
    ma <- aggregate(list(MA = sym$w * sym$pop_dest),
                    by = list(geolev2 = sym$origin), FUN = sum)
    ma$logMA <- log(ma$MA)
    stopifnot(all(is.finite(ma$logMA)))
    ma[, c("geolev2", "MA", "logMA")]
}

# One value from a long-format reference table, or a hard failure. Plain
# "==" subsetting returns numeric(0) on a lookup miss, and
# stopifnot(abs(x - numeric(0)) < tol) passes SILENTLY, so an anchor built
# that way verifies nothing (cr-review PR #143, blocking 1).
pick1 <- function(x, allow_na = FALSE) {
    stopifnot(length(x) == 1L)
    if (!allow_na) stopifnot(!is.na(x))
    x[[1L]]
}

# ---------------------------------------------------------------------------
# gate_pipeline(): the constructed MA must reproduce the committed control
# EXACTLY when given the same inputs the pipeline uses. Replaces a
# correlation gate that was nearly non-binding -- cr-review PR #143
# measured corr = 0.9965 under the WRONG theta and 0.9931 with a RANDOM
# destination set, both of which would have passed a >0.95 threshold.
# ---------------------------------------------------------------------------
gate_pipeline <- function(tau, census60, theta_val, d) {
    all_dest <- unique(c(tau$origin_geolev2, tau$destination_geolev2))
    stopifnot(all(all_dest %in% census60$geolev2))
    ma_full <- build_ma_level(tau, census60, theta_val, all_dest)
    m <- merge(d[, c("geolev2", "logMA_actual_1960_s0_elow")],
               ma_full[, c("geolev2", "logMA")], by = "geolev2")
    stopifnot(nrow(m) == nrow(d))
    dmax <- max(abs(m$logMA - m$logMA_actual_1960_s0_elow))
    message(sprintf("[b] GATE max|constructed - pipeline control| = %.3e",
                    dmax))
    stopifnot(dmax < 1e-10)
    list(n_dest = length(all_dest), ma_full = ma_full, dmax = dmax)
}

# Share of each district's pipeline MA sum contributed by the destinations
# the 237-district restriction drops. Computed, not assumed (cr-review
# PR #143): Capital Federal is among them and is large.
dropped_share <- function(ma_full, tau, census60, theta_val, covered,
                          caba_id) {
    ma_cov <- build_ma_level(tau, census60, theta_val, covered)
    m <- merge(ma_full[, c("geolev2", "MA")],
               ma_cov[, c("geolev2", "MA")], by = "geolev2",
               suffixes = c("_all", "_cov"))
    share <- 1 - m$MA_cov / m$MA_all
    ma_caba <- build_ma_level(tau, census60, theta_val, caba_id)
    mc <- merge(ma_full[, c("geolev2", "MA")],
                ma_caba[, c("geolev2", "MA")], by = "geolev2",
                suffixes = c("_all", "_caba"))
    list(med = median(share), max = max(share),
         caba_med = median(mc$MA_caba / mc$MA_all),
         caba_max = max(mc$MA_caba / mc$MA_all))
}

# ---------------------------------------------------------------------------
# Part 1: the placebo under five control sets.
# ---------------------------------------------------------------------------
run_part1 <- function(dd, y_plac, variants, refs) {
    rows <- list()
    for (vn in names(variants)) {
        fits <- fit_iv_quad(
            y = y_plac, data = dd, endog = main_treatment,
            lp_instr = main_lp_instrument,
            hypo_instr = main_hypo_instrument,
            ctrls_vec = variants[[vn]]
        )
        for (sp in names(fits)) {
            m  <- fits[[sp]]
            cn <- if (sp == "OLS") main_treatment
                  else paste0("fit_", main_treatment)
            cc <- safe_coef(m, cn)
            Fv <- if (sp == "OLS") NA_real_ else fitstat_F(m)
            # Anchors: drift is a bug, not a finding. Every lookup goes
            # through pick1() so a miss fails instead of passing silently.
            # PR #145 repointed these: Table 7 now holds the pop47
            # spec, so pop47 anchors to it and t7 anchors to ladder
            # row (1). Anchoring t7 to Table 7 is what broke this
            # script when the paper spec changed.
            if (vn == "pop47") {
                r <- refs$t7[refs$t7$spec == refs$map[[sp]], ]
                stopifnot(nrow(r) == 1L,
                          abs(cc$est - r$estimate) < 1e-8,
                          abs(cc$se - r$std_err) < 1e-8,
                          nobs(m) == r$n_obs)
            }
            if (vn == "t7" && sp %in% c("OLS", "IV-B")) {
                r <- refs$lad[refs$lad$control_set ==
                              "(1) 1960 baselines", ]
                stopifnot(nrow(r) == 1L)
                est <- if (sp == "OLS") r$ols_est else r$ivb_est
                se  <- if (sp == "OLS") r$ols_se  else r$ivb_se
                stopifnot(abs(cc$est - est) < 1e-8,
                          abs(cc$se - se) < 1e-8)
            }
            if (vn %in% c("pop47", "full47")) {
                gp <- function(st_) pick1(refs$p47$value[
                    refs$p47$variant == vn & refs$p47$spec == sp &
                    refs$p47$stat == st_])
                stopifnot(abs(cc$est - gp("coef")) < 1e-8,
                          abs(cc$se  - gp("se"))   < 1e-8,
                          nobs(m) == gp("N"))
            }
            rows[[length(rows) + 1L]] <- data.frame(
                part = "1_placebo", variant = vn, outcome = y_plac,
                spec = sp, estimate = cc$est, std_err = cc$se,
                p_value = cc$p, first_stage_F = Fv, n_obs = nobs(m),
                stringsAsFactors = FALSE)
            message(sprintf(
                "[b1] %-9s %-6s b=%+.4f se=%.4f p=%.3f F=%s N=%d",
                vn, sp, cc$est, cc$se, cc$p,
                ifelse(is.na(Fv), "--", sprintf("%.1f", Fv)), nobs(m)))
        }
    }
    do.call(rbind, rows)
}

# ---------------------------------------------------------------------------
# Part 2: pre-trend / convergence control in the main spec.
# Row 3b exists only to assert the algebraic equivalence documented in
# the header; it is reported so the claim is checkable, not just stated.
# ---------------------------------------------------------------------------
run_part2 <- function(d, covered, y_plac, outcomes2, refs) {
    rows <- list()
    anchor2 <- function(yvar, sp, cc, n) {
        ref <- if (yvar %in% refs$t9$outcome) refs$t9 else refs$t10
        r <- ref[ref$outcome == yvar & ref$spec == sp, ]
        stopifnot(nrow(r) == 1L,
                  abs(cc$est - r$estimate) < 1e-8,
                  abs(cc$se - r$std_err) < 1e-8, n == r$n_obs)
    }
    for (o in outcomes2) {
        y <- o$var
        specs <- list(
            list(tag = "1_full311",   ctrls = geo_controls_main,
                 sub = FALSE),
            list(tag = "2_sub237",    ctrls = geo_controls_main,
                 sub = TRUE),
            list(tag = "3_sub237_pt", ctrls = c(geo_controls_main, y_plac),
                 sub = TRUE),
            list(tag = "3b_sub237_p47",
                 ctrls = c(geo_controls_main, "log_pop_1947"), sub = TRUE)
        )
        keep_n <- c()
        beta3 <- c()
        for (s in specs) {
            dat <- if (s$sub) d[d$geolev2 %in% covered, ] else d
            dat <- dat[complete.cases(dat[, unique(c(
                y, main_treatment, main_lp_instrument,
                main_hypo_instrument, s$ctrls))]), ]
            fits <- fit_iv_quad(
                y = y, data = dat, endog = main_treatment,
                lp_instr = main_lp_instrument,
                hypo_instr = main_hypo_instrument,
                ctrls_vec = s$ctrls
            )
            for (sp in c("OLS", "IV-B")) {
                m  <- fits[[sp]]
                cn <- if (sp == "OLS") main_treatment
                      else paste0("fit_", main_treatment)
                cc <- safe_coef(m, cn)
                Fv <- if (sp == "OLS") NA_real_ else fitstat_F(m)
                if (s$tag == "1_full311") anchor2(y, sp, cc, nobs(m))
                if (sp == "IV-B") {
                    keep_n[s$tag] <- nobs(m)
                    beta3[s$tag]  <- cc$est
                }
                rows[[length(rows) + 1L]] <- data.frame(
                    part = "2_maineffect", variant = s$tag,
                    outcome = o$lab, spec = sp, estimate = cc$est,
                    std_err = cc$se, p_value = cc$p,
                    first_stage_F = Fv, n_obs = nobs(m),
                    stringsAsFactors = FALSE)
                message(sprintf(
                    "[b2] %-18s %-14s %-6s b=%+.4f se=%.4f p=%.3f N=%d",
                    o$lab, s$tag, sp, cc$est, cc$se, cc$p, nobs(m)))
            }
            if (s$tag == "3_sub237_pt") {
                cpt <- safe_coef(fits[["IV-B"]], y_plac)
                stopifnot(!is.na(cpt$est), !is.na(cpt$p))
                rows[[length(rows) + 1L]] <- data.frame(
                    part = "2_maineffect", variant = "3_pretrend_coef",
                    outcome = o$lab, spec = "IV-B", estimate = cpt$est,
                    std_err = cpt$se, p_value = cpt$p,
                    first_stage_F = NA_real_,
                    n_obs = nobs(fits[["IV-B"]]), stringsAsFactors = FALSE)
                message(sprintf("[b2] %-18s convergence ctrl b=%+.4f p=%.3f",
                                o$lab, cpt$est, cpt$p))
            }
        }
        # The decomposition is only clean if rows 2 and 3 share a sample,
        # and row 3 is only a relabelling if it equals row 3b.
        stopifnot(keep_n[["2_sub237"]] == keep_n[["3_sub237_pt"]],
                  keep_n[["3_sub237_pt"]] == keep_n[["3b_sub237_p47"]],
                  abs(beta3[["3_sub237_pt"]] -
                      beta3[["3b_sub237_p47"]]) < 1e-10)
    }
    do.call(rbind, rows)
}

# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
write_report <- function(res, meta, variants, outcomes2, path) {
    # first_stage_F is legitimately NA on OLS rows; every other column
    # must be present, so the NA allowance is scoped to that column only.
    g1 <- function(vn, sp, col) pick1(res[[col]][
        res$part == "1_placebo" & res$variant == vn & res$spec == sp],
        allow_na = identical(col, "first_stage_F"))
    g2 <- function(lab, vn, sp, col) pick1(res[[col]][
        res$part == "2_maineffect" & res$outcome == lab &
        res$variant == vn & res$spec == sp],
        allow_na = identical(col, "first_stage_F"))
    ci <- function(vn) {
        e <- g1(vn, "IV-B", "estimate"); s <- g1(vn, "IV-B", "std_err")
        sprintf("[%+.4f, %+.4f]", e - 1.96 * s, e + 1.96 * s)
    }

    sink(path)
    cat(strrep("=", 78), "\n")
    cat("AGENDA ITEM B: is a 1947-consistent placebo defensible, and does\n")
    cat("pre-1960 growth drive the headline estimates?\n")
    cat(sprintf("Generated: %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
    cat(strrep("=", 78), "\n\n")

    cat("SETUP AND WHAT IS VERIFIED\n")
    cat(sprintf("  The tau file has %d districts; the estimation sample has\n",
                meta$n_dest))
    cat(sprintf("  %d. The extra one is %s (Capital Federal), so the\n",
                meta$n_est, meta$caba_id))
    cat(sprintf("  pipeline control sums over %d destinations.\n",
                meta$n_dest))
    cat("  GATE: the MA builder here reproduces the committed\n")
    cat(sprintf("  logMA_actual_1960_s0_elow exactly, max abs diff %.1e.\n",
                meta$dmax))
    cat(sprintf("  1947 population covers %d districts, exactly the placebo\n",
                meta$n_cov))
    cat("  sample. Both constructed levels re-weight the SAME 1960 tau\n")
    cat(sprintf("  matrix (theta = %.2f) over those %d destinations, so they\n",
                meta$theta, meta$n_cov))
    cat("  differ ONLY in population year.\n")
    cat("  Destinations dropped by that restriction carry a median\n")
    cat(sprintf("  %.1f%% (max %.1f%%) of each district's pipeline MA sum;\n",
                100 * meta$drop_med, 100 * meta$drop_max))
    cat(sprintf("  Capital Federal alone is median %.1f%% (max %.1f%%). It\n",
                100 * meta$caba_med, 100 * meta$caba_max))
    cat("  drops out of both constructed levels equally, so the ma47-vs-\n")
    cat("  ma60 contrast is unaffected; pop47 (which keeps the pipeline\n")
    cat("  control) is estimated so the comparison against t7 does not\n")
    cat("  rest on the restricted set.\n")
    cat(sprintf("  corr(pop47 level, pop60 level)      = %.4f\n",
                meta$cor_years))
    cat(sprintf("  partial corr after full47 controls  = %.4f\n\n",
                meta$pcor_years))

    cat("PART 1 - placebo (DV = 1947-60 population growth), N = ",
        meta$n_p1, "\n", sep = "")
    cat("  t7        = published Table 7 (log pop 1960 + log MA 1960)\n")
    cat("  pop47     = log pop 1947, pipeline MA 1960 control KEPT\n")
    cat("  full47    = log pop 1947, NO baseline MA (the proposal)\n")
    cat("  ma47_ctrl = full47 + constructed MA, 1947 population weights\n")
    cat("  ma60_ctrl = full47 + constructed MA, 1960 population weights\n\n")
    cat(sprintf("%-10s %-7s %10s %9s %8s %7s\n",
                "Variant", "Spec", "beta", "SE", "p", "F"))
    for (vn in names(variants)) {
        for (sp in c("OLS", "IV-LP", "IV-H", "IV-B")) {
            Fv <- g1(vn, sp, "first_stage_F")
            cat(sprintf("%-10s %-7s %+10.4f %9.4f %8.3f %7s\n",
                        vn, sp, g1(vn, sp, "estimate"),
                        g1(vn, sp, "std_err"), g1(vn, sp, "p_value"),
                        ifelse(is.na(Fv), "--", sprintf("%.1f", Fv))))
        }
    }

    cat("\nHOW TO READ PART 1 (fixed before the numbers were seen):\n")
    cat("  clean with ma47_ctrl, dirty with ma60_ctrl -> the post-outcome\n")
    cat("    story holds: the 1960 POPULATION content revives the\n")
    cat("    rejection, not the presence of a baseline-MA level.\n")
    cat("  clean with BOTH -> no baseline-MA level revives the rejection,\n")
    cat("    so dropping it is not what produces the null.\n")
    cat("  dirty with BOTH -> the post-outcome framing is wrong; the\n")
    cat("    honest headline placebo is pop47.\n\n")

    cat("RECONCILIATION (cr-review PR #143, blocking 2). Read literally on\n")
    cat("p-values the outcome is the SECOND scenario: neither ma47_ctrl\n")
    cat(sprintf("(p = %.3f) nor ma60_ctrl (p = %.3f) rejects at 10%%. But the\n",
                g1("ma47_ctrl", "IV-B", "p_value"),
                g1("ma60_ctrl", "IV-B", "p_value")))
    cat("second scenario's CONCLUSION does not follow, for three reasons\n")
    cat("that the p-values alone hide:\n")
    cat(sprintf("  (a) Near-collinearity. The two levels correlate %.4f raw\n",
                meta$cor_years))
    cat(sprintf("      and %.4f after the full47 controls, so this contrast\n",
                meta$pcor_years))
    cat("      NEVER had the power to separate them. Removing the post-1947\n")
    cat("      population content changes nothing because there was almost\n")
    cat("      nothing to remove -- which is itself the answer to whether\n")
    cat("      'MA 1960 is post-outcome' can justify dropping it.\n")
    cat("  (b) The coefficient, not the p-value, is what moves. With any\n")
    cat(sprintf("      baseline-MA control it is %+.4f (t7), %+.4f (pop47),\n",
                g1("t7", "IV-B", "estimate"), g1("pop47", "IV-B",
                                                 "estimate")))
    cat(sprintf("      %+.4f (1947 wts), %+.4f (1960 wts); without one,\n",
                g1("ma47_ctrl", "IV-B", "estimate"),
                g1("ma60_ctrl", "IV-B", "estimate")))
    cat(sprintf("      %+.4f. ma47/ma60 sit above 0.10 because log pop 1947\n",
                g1("full47", "IV-B", "estimate")))
    cat(sprintf("      is a noisier baseline (SE %.4f -> %.4f), not because\n",
                g1("t7", "IV-B", "std_err"),
                g1("ma47_ctrl", "IV-B", "std_err")))
    cat("      the association went away.\n")
    cat("  (c) Two confounds in the with-vs-without comparison, so it is\n")
    cat("      suggestive and not a test. The CIs overlap:\n")
    cat(sprintf("      ma47_ctrl %s vs full47 %s.\n", ci("ma47_ctrl"),
                ci("full47")))
    cat(sprintf("      And the first stage differs (F %.1f with vs %.1f\n",
                g1("ma47_ctrl", "IV-B", "first_stage_F"),
                g1("full47", "IV-B", "first_stage_F")))
    cat("      without): the baseline-MA control absorbs instrument\n")
    cat("      variation, the mechanism documented for the main spec in\n")
    cat("      .kiro/baseline_ma_control_note.md (13.6 vs 29.9 there).\n")
    cat("  NET: the post-outcome justification for dropping the baseline-MA\n")
    cat("  control is UNSUPPORTED -- on (a), not refuted by (b)/(c). The\n")
    cat("  defensible reading is pop47: keep the airtight fix (log\n")
    cat("  pop 1960 is a component of the DV), keep the MA baseline,\n")
    cat(sprintf("  and report beta = %+.4f, p = %.3f as a marginal rejection\n",
                g1("pop47", "IV-B", "estimate"),
                g1("pop47", "IV-B", "p_value")))
    cat("  at 10%, not as a clean null.\n\n")

    cat("PART 2 - does pre-1960 growth drive the headline estimates?\n")
    cat("  1_full311     published spec, all 311 districts\n")
    cat("  2_sub237      same spec, 1947-covered subsample   [SELECTION]\n")
    cat("  3_sub237_pt   + 1947-60 growth as a control       [CONFOUND]\n")
    cat("  3b_sub237_p47 + log pop 1947 instead              [IDENTICAL]\n")
    cat("  (2)-(1) is selection; (3)-(2) is the confound.\n")
    cat("  Row 3b is the algebra check: chg_log_placebo_pop_60_47 =\n")
    cat("  log_pop_1960 - log_pop_1947 and log_pop_1960 is already a\n")
    cat("  control, so row 3 IS a 1947-level convergence control. Rows 3\n")
    cat("  and 3b agree to 1e-10 (asserted), and the 'convergence ctrl'\n")
    cat("  coefficient below is minus the log_pop_1947 coefficient.\n\n")
    for (o in outcomes2) {
        cat(sprintf("%s\n", toupper(o$lab)))
        cat(sprintf("  %-14s %-6s %10s %9s %8s %6s\n",
                    "Row", "Spec", "beta", "SE", "p", "N"))
        for (vn in c("1_full311", "2_sub237", "3_sub237_pt",
                     "3b_sub237_p47")) {
            for (sp in c("OLS", "IV-B")) {
                cat(sprintf("  %-14s %-6s %+10.4f %9.4f %8.3f %6d\n",
                            vn, sp, g2(o$lab, vn, sp, "estimate"),
                            g2(o$lab, vn, sp, "std_err"),
                            g2(o$lab, vn, sp, "p_value"),
                            as.integer(g2(o$lab, vn, sp, "n_obs"))))
            }
        }
        cat(sprintf("  convergence control coefficient (IV-B): %+.4f (p = %.3f)\n\n",
                    g2(o$lab, "3_pretrend_coef", "IV-B", "estimate"),
                    g2(o$lab, "3_pretrend_coef", "IV-B", "p_value")))
    }
    cat("INTERPRETATION LIMIT: conditioning on a lagged level or growth\n")
    cat("carries its own mean-reversion mechanics, so row 3 is a robustness\n")
    cat("statement ('the estimate survives'), NOT cleaner identification. A\n")
    cat("convergence-control coefficient that is itself insignificant also\n")
    cat("means row 3 is a weak test: little pre-period signal to absorb.\n")
    sink()
}

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)

    message("\n", strrep("=", 72))
    message("diagnostic_placebo_ma1947.R  |  agenda item B")
    message(strrep("=", 72))

    d <- ensure_geolev2_char(as.data.frame(arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))))
    stopifnot(nrow(d) == 311L)
    d$log_pop_1947 <- ifelse(!is.na(d$pop_1947) & d$pop_1947 > 0,
                             log(d$pop_1947), NA_real_)

    y_plac  <- "chg_log_placebo_pop_60_47"
    covered <- d$geolev2[!is.na(d$pop_1947) & d$pop_1947 > 0]
    message(sprintf("[b] districts with 1947 population: %d of %d",
                    length(covered), nrow(d)))
    # The placebo DV needs 1947 population, so the covered set and the
    # placebo sample must coincide; anything else means the DV was built
    # from a different 1947 source than pop_1947.
    stopifnot(setequal(covered, d$geolev2[!is.na(d[[y_plac]])]))

    tau <- as.data.frame(arrow::read_parquet(
        file.path(dir_derived_taus, "tau_actual_1960_s0.parquet")))
    tau <- ensure_geolev2_char(tau, "origin_geolev2")
    tau <- ensure_geolev2_char(tau, "destination_geolev2")
    census60 <- ensure_geolev2_char(as.data.frame(arrow::read_parquet(
        file.path(dir_derived_base, "census_1960",
                  "census_1960_ipums.parquet"))))
    census60 <- data.frame(geolev2 = census60$geolev2,
                           pop = as.numeric(census60$pop))

    th   <- theta[["low"]]     # 4.55, matches logMA_actual_1960_s0_elow
    gate <- gate_pipeline(tau, census60, th, d)
    caba_id <- setdiff(gate$ma_full$geolev2, d$geolev2)
    stopifnot(length(caba_id) == 1L)
    drop <- dropped_share(gate$ma_full, tau, census60, th, covered, caba_id)
    message(sprintf("[b] dropped destinations carry median %.1f%% of MA (CABA %.1f%%)",
                    100 * drop$med, 100 * drop$caba_med))

    # ---- the two constructed baseline-MA levels ------------------------
    pop_of <- function(col) data.frame(
        geolev2 = covered, pop = d[[col]][match(covered, d$geolev2)])
    ma47 <- build_ma_level(tau, pop_of("pop_1947"), th, covered)
    ma60 <- build_ma_level(tau, pop_of("pop_1960"), th, covered)
    d <- merge(d, data.frame(geolev2 = ma47$geolev2,
                             logMA1960net_pop47 = ma47$logMA),
               by = "geolev2", all.x = TRUE)
    d <- merge(d, data.frame(geolev2 = ma60$geolev2,
                             logMA1960net_pop60 = ma60$logMA),
               by = "geolev2", all.x = TRUE)
    # Only DESTINATIONS are restricted; every origin still gets a level
    # (its market access TO the covered set), so all 311 are non-missing
    # even though the placebo uses 237.
    stopifnot(nrow(d) == 311L, !any(is.na(d$logMA1960net_pop47)),
              !any(is.na(d$logMA1960net_pop60)))

    # ---- specs ---------------------------------------------------------
    base_geo <- setdiff(geo_controls_main,
                        c("logMA_actual_1960_s0_elow", "log_pop_1960"))
    variants <- list(
        t7        = geo_controls_main,
        pop47     = c(base_geo, "logMA_actual_1960_s0_elow",
                      "log_pop_1947"),
        full47    = c(base_geo, "log_pop_1947"),
        ma47_ctrl = c(base_geo, "log_pop_1947", "logMA1960net_pop47"),
        ma60_ctrl = c(base_geo, "log_pop_1947", "logMA1960net_pop60")
    )
    all_vars <- unique(c(y_plac, main_treatment, main_lp_instrument,
                         main_hypo_instrument, unlist(variants)))
    dd <- d[complete.cases(d[, all_vars]), ]
    message(sprintf("[b] part 1 common sample: N = %d", nrow(dd)))
    stopifnot(nrow(dd) == length(covered))

    # Raw and partial correlation of the two levels. The partial version
    # is the one that says whether the ma47-vs-ma60 contrast could have
    # discriminated at all (cr-review PR #143).
    cor_years <- cor(dd$logMA1960net_pop47, dd$logMA1960net_pop60)
    resid_on <- function(v) residuals(lm(
        as.formula(paste(v, "~", paste(c(base_geo, "log_pop_1947"),
                                       collapse = " + "))), data = dd))
    pcor_years <- cor(resid_on("logMA1960net_pop47"),
                      resid_on("logMA1960net_pop60"))
    message(sprintf("[b] corr = %.4f | partial corr = %.4f",
                    cor_years, pcor_years))

    refs <- list(
        t7  = read.csv(file.path(dir_tables, "table_7_pre_trends.csv"),
                       stringsAsFactors = FALSE),
        p47 = read.csv(file.path(dir_tables,
                                 "diagnostic_placebo_1947.csv"),
                       stringsAsFactors = FALSE),
        lad = read.csv(file.path(dir_tables,
                                 "table_b2_placebo_ladder.csv"),
                       stringsAsFactors = FALSE),
        t9  = read.csv(file.path(dir_tables, "table_9_population_iv.csv"),
                       stringsAsFactors = FALSE),
        t10 = read.csv(file.path(dir_tables, "table_10_sectoral_iv.csv"),
                       stringsAsFactors = FALSE),
        map = c("OLS" = "OLS", "IV-LP" = "IV-LP", "IV-H" = "IV-Hypo",
                "IV-B" = "IV-Both")
    )

    res1 <- run_part1(dd, y_plac, variants, refs)

    # Population plus the two manufacturing outcomes the paper's findings
    # rest on; establishments and both agricultural outcomes are null in
    # Table 10 and add nothing here.
    outcomes2 <- list(
        list(var = "chg_log_pop_91_60",     lab = "population"),
        list(var = "chg_log_valprod_85_54", lab = "mfg prod. value"),
        list(var = "chg_log_massal_85_54",  lab = "mfg wage mass")
    )
    res2 <- run_part2(d, covered, y_plac, outcomes2, refs)

    res <- rbind(res1, res2)
    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)
    csv_path <- file.path(dir_tables, "diagnostic_placebo_ma1947.csv")
    write.csv(res, csv_path, row.names = FALSE)

    meta <- list(n_dest = gate$n_dest, n_est = nrow(d), dmax = gate$dmax,
                 n_cov = length(covered), n_p1 = nrow(dd), theta = th,
                 caba_id = caba_id, drop_med = drop$med,
                 drop_max = drop$max, caba_med = drop$caba_med,
                 caba_max = drop$caba_max, cor_years = cor_years,
                 pcor_years = pcor_years)
    write_report(res, meta, variants, outcomes2,
                 file.path(dir_tables, "diagnostic_placebo_ma1947.txt"))
    message(sprintf("[b] Saved: %s and .txt", csv_path))
}

main()

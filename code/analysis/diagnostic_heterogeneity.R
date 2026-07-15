# ===========================================================================
# diagnostic_heterogeneity.R
#
# PURPOSE: Task C7 (heterogeneity). Does the population elasticity of
#          market access vary with baseline district characteristics?
#          Interacts d ln(MA) with each characteristic, one at a time,
#          instrumenting the interaction with instrument x characteristic
#          (both LP and hypo), per tasks.md C7.
#
#          Runs as a DIAGNOSTIC, not a numbered paper table: Block 2
#          framing is pending the coauthor meeting (memo Decisions A-E),
#          and elasticity LEVELS here inherit Decision A (theta / tau
#          object). The object of interest is the interaction PATTERN,
#          which — like the sectoral theta sweep (PR #71) — is expected
#          to be more robust to Decision A than the levels.
#
# CHARACTERISTICS (z-scored within each regression sample — complete
# cases of all model variables — so interaction coefficients read as
# "effect of +1 SD" among the districts the model is fit on):
#   1. log_pop_1960   — initial population (main effect already in
#                       geo_controls_main; only interaction added).
#   2. rurshr_1960    — rural share 1960 = 1 - urbshr_1960, proxy for
#                       agricultural orientation (no direct agricultural
#                       employment share exists in the sample; Table 10's
#                       sectoral outcomes live at different aggregations).
#                       Main effect added (not in controls).
#   3. dist_to_BA     — distance to Buenos Aires (std version already in
#                       geo_controls_main; only interaction added).
#   Distance to port (tasks.md C7) is NOT run: no port-distance variable
#   exists in the estimation sample. Flagged, not silently proxied.
#
# SPEC (one characteristic X at a time; X_z = z-score of X):
#   d ln pop = b1 * dlnMA + b2 * (dlnMA x X_z) + [X_z if not in ctrls]
#              + geo_controls_main + e
#   Endogenous: dlnMA, dlnMA x X_z.
#   Instruments (IV-Both): dlnMA_LP, dlnMA_hypo, and their X_z
#   interactions (4 instruments, 2 endogenous regressors).
#   Characteristics are NOT interacted jointly: 4+ endogenous regressors
#   on N=309 would be hopelessly weak; one-at-a-time is the honest cut.
#
# READING: b2 > 0 means MA matters more in districts with higher X.
#          Theory prior (tasks.md A5): MA should matter more in
#          agricultural, less-developed, BA-distant districts, i.e.
#          b2 < 0 on initial population, b2 > 0 on rural share and
#          distance to BA. Watch BOTH per-regressor first-stage Fs:
#          with two endogenous regressors, a weak first stage on either
#          one contaminates both IV coefficients. In the committed run
#          it is the LEVEL F that is weak (~7, down from 13.6 in the
#          single-endogenous Table 9 spec), while the interaction Fs
#          are 11-15.
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/tables/diagnostic_heterogeneity.txt
#   results/tables/diagnostic_heterogeneity.csv
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
})

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)

    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)

    report_path <- file.path(dir_tables, "diagnostic_heterogeneity.txt")
    con <- file(report_path, open = "wt")
    rep <- function(...) { line <- sprintf(...); cat(line, "\n")
                           cat(line, "\n", file = con) }

    rep("%s", strrep("=", 74))
    rep("HETEROGENEITY DIAGNOSTIC (task C7)")
    rep("d ln(MA) interacted with baseline characteristics, one at a time.")
    rep("Interactions instrumented by instrument x characteristic.")
    rep("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
    rep("%s", strrep("=", 74))

    d <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))
    d <- ensure_geolev2_char(d)
    d$rurshr_1960 <- 1 - d$urbshr_1960

    endog <- "chg_logMA_86_60_s0_elow"
    lp    <- "chg_logMA_stu_s0_elow"
    hypo  <- main_hypo_instrument

    # in_ctrls: main effect (up to affine transform) already in
    # geo_controls_main -> adding the z-scored level would be collinear.
    chars <- list(
        list(var = "log_pop_1960", label = "Initial log population (1960)",
             in_ctrls = TRUE),
        list(var = "rurshr_1960",  label = "Rural share (1960, = 1 - urban)",
             in_ctrls = FALSE),
        list(var = "dist_to_BA",   label = "Distance to Buenos Aires",
             in_ctrls = TRUE)
    )

    rep("\nSpec: outcome chg_log_pop_91_60; endog %s;", endog)
    rep("instruments %s + %s (+ their X interactions);", lp, hypo)
    rep("controls: geo_controls_main. HC1 SEs. Characteristics z-scored.")
    rep("NOT run: distance to port (no such variable in the sample).")

    csv_rows <- list()
    for (ch in chars) {
        rep("\n%s", strrep("-", 74))
        rep("[X = %s]", ch$label)
        rep("%s", strrep("-", 74))

        # z-score within the REGRESSION sample: complete cases of the
        # characteristic, the outcome, the endogenous regressor, the
        # instruments, and all controls — so "+1 SD" and "at the sample
        # mean" refer to the same 309 districts the model is fit on.
        model_vars <- c(ch$var, "chg_log_pop_91_60", endog, lp, hypo,
                        geo_controls_main)
        dd <- d[stats::complete.cases(d[, model_vars]), ]
        dd$X_z    <- as.numeric(scale(dd[[ch$var]]))
        dd$ma_X   <- dd[[endog]] * dd$X_z
        dd$lp_X   <- dd[[lp]]    * dd$X_z
        dd$hypo_X <- dd[[hypo]]  * dd$X_z

        ctrls <- geo_controls_main
        if (!ch$in_ctrls) ctrls <- c(ctrls, "X_z")
        ctrls_expr <- paste(ctrls, collapse = " + ")

        # ---- OLS ----------------------------------------------------------
        f_ols <- as.formula(sprintf(
            "chg_log_pop_91_60 ~ %s + ma_X + %s", endog, ctrls_expr))
        m_ols <- feols(f_ols, data = dd, vcov = "hetero")
        b1 <- safe_coef(m_ols, endog)
        b2 <- safe_coef(m_ols, "ma_X")
        rep("  OLS      beta_MA   = %+.4f (SE %.4f, p %.3f)",
            b1$est, b1$se, b1$p)
        rep("           beta_MAxX = %+.4f (SE %.4f, p %.3f)   N=%d",
            b2$est, b2$se, b2$p, nobs(m_ols))
        csv_rows[[length(csv_rows) + 1L]] <- data.frame(
            characteristic = ch$var, spec = "OLS",
            beta_ma = b1$est, se_ma = b1$se, p_ma = b1$p,
            beta_int = b2$est, se_int = b2$se, p_int = b2$p,
            F_ma = NA_real_, F_int = NA_real_, n_obs = nobs(m_ols))

        # ---- IV-Both (2 endogenous, 4 instruments) --------------------------
        f_iv <- as.formula(sprintf(
            "chg_log_pop_91_60 ~ %s | %s + ma_X ~ %s + %s + lp_X + hypo_X",
            ctrls_expr, endog, lp, hypo))
        m_iv <- feols(f_iv, data = dd, vcov = "hetero")
        b1 <- safe_coef(m_iv, paste0("fit_", endog))
        b2 <- safe_coef(m_iv, "fit_ma_X")
        Fs <- fitstat_F_all(m_iv)
        F_ma  <- Fs[[paste0("ivf1::", endog)]]
        F_int <- Fs[["ivf1::ma_X"]]
        rep("  IV-Both  beta_MA   = %+.4f (SE %.4f, p %.3f)   F = %.1f",
            b1$est, b1$se, b1$p, F_ma)
        rep("           beta_MAxX = %+.4f (SE %.4f, p %.3f)   F = %.1f   N=%d",
            b2$est, b2$se, b2$p, F_int, nobs(m_iv))
        csv_rows[[length(csv_rows) + 1L]] <- data.frame(
            characteristic = ch$var, spec = "IV-Both",
            beta_ma = b1$est, se_ma = b1$se, p_ma = b1$p,
            beta_int = b2$est, se_int = b2$se, p_int = b2$p,
            F_ma = F_ma, F_int = F_int, n_obs = nobs(m_iv))
    }

    csv_df <- do.call(rbind, csv_rows)
    out_csv <- file.path(dir_tables, "diagnostic_heterogeneity.csv")
    write.csv(csv_df, out_csv, row.names = FALSE)
    rep("\nSaved: %s", out_csv)

    rep("\n%s", strrep("=", 74))
    rep("READING: beta_MAxX is the change in the MA elasticity per +1 SD of")
    rep("the characteristic; beta_MA is the elasticity at the sample mean.")
    rep("Theory prior (tasks.md A5): negative on initial population,")
    rep("positive on rural share and distance to BA.")
    rep("")
    rep("CAVEAT (identification strength): with two endogenous regressors,")
    rep("a weak first stage on EITHER contaminates BOTH IV coefficients.")
    rep("Here the LEVEL F is ~7 in every spec (vs 13.6 in the single-")
    rep("endogenous Table 9 baseline) while the interaction Fs are 11-15:")
    rep("adding the interaction instrument dilutes the level first stage.")
    rep("Read the IV cells as sign checks, not magnitudes. Also note that")
    rep("per-equation Fs can overstate joint strength when the two first")
    rep("stages are correlated; the referee-grade statistic would be the")
    rep("Sanderson-Windmeijer conditional F (not computed here).")
    rep("Levels inherit memo Decision A; patterns are the object here.")
    rep("%s", strrep("=", 74))

    close(con)
    message("\nSaved report: ", report_path)
}

main()

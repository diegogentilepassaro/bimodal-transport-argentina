# ===========================================================================
# diagnostic_ma_iceberg_sectoral.R
#
# PURPOSE: Decision A de-risking on the ICEBERG object (companion to
#          diagnostic_ma_iceberg.R, following the theta_sweep /
#          theta_sweep_sectoral pair precedent). The paper's headline is
#          the sectoral contrast — manufacturing value/wage respond,
#          agriculture does not. The theta sweep established that the
#          contrast survives any theta on the RAW-cost object (memo
#          Decision A section); this script asks the same question on
#          the object actually under discussion Wednesday:
#
#              does the sectoral pattern survive tau' = 1 + cost/V
#              across the whole V grid?
#
#          If yes, the meeting can choose the tau object on measurement
#          grounds without fearing the headline flips; if the pattern
#          breaks somewhere on the curve, that is essential input
#          BEFORE choosing.
#
# DESIGN: identical MA machinery to diagnostic_ma_iceberg.R (loaders
#   and transform copied from there with this provenance note;
#   diagnostics stay self-contained by repo convention). Per (theta, V):
#   recompute MA for the four cases with 1960 pop weights, rebuild the
#   treatment + both instrument deltas, then run the Table 10 spec
#   (fit_iv_quad; IV-Both reported, as the decay experiment did) for
#   SIX outcomes: total population (reference row, must reproduce
#   diagnostic_ma_iceberg.csv) plus Table 10's five sectoral outcomes.
#   Controls: six standardized geo + V-specific iceberg 1960 logMA +
#   log pop 1960. HC1.
#
# GRID: same as diagnostic_ma_iceberg.R — raw (V->0) anchor + V in
#   {100, ..., 100000} pesos/ton; theta 8.22 (D&H headline) and 4.55
#   (continuity).
#
# VERIFICATION (asserted in code):
#   - Raw anchor at theta 4.55 must reproduce Table 10's committed
#     IV-B estimates exactly (results/tables/table_10_sectoral_iv.csv),
#     and the population row must reproduce Table 9's IV-B.
#   - The population rows must match diagnostic_ma_iceberg.csv's IV-B
#     columns at every (theta, V).
#
# READS:
#   data/derived/03_taus/tau_{actual_1960,actual_1986,
#                              instrument_stu,instrument_lcp_mst}_s0.parquet
#   data/derived/base/census_1960/census_1960_ipums.parquet
#   data/derived/06_analysis/estimation_sample.parquet
#   results/tables/table_10_sectoral_iv.csv       (anchor check)
#   results/tables/table_9_population_iv.csv      (anchor check)
#   results/tables/diagnostic_ma_iceberg.csv      (population check)
#
# PRODUCES (diagnostic only; no paper exhibit, no main.R wiring):
#   results/tables/diagnostic_ma_iceberg_sectoral.txt
#   results/tables/diagnostic_ma_iceberg_sectoral.csv
# ===========================================================================
suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
})

CASES <- c("actual_1960", "actual_1986", "instrument_stu", "instrument_lcp_mst")
V_GRID_PESOS <- c(100, 500, 1000, 2000, 4400, 10000, 20000, 50000, 100000)
THETAS <- c(4.55, 8.22)

OUTCOMES <- list(
    list(var = "chg_log_pop_91_60",        lab = "population"),
    list(var = "chg_log_valprod_85_54",    lab = "mfg value"),
    list(var = "chg_log_massal_85_54",     lab = "mfg wage mass"),
    list(var = "chg_log_nestab_85_54",     lab = "mfg establish."),
    list(var = "chg_log_nexp_88_60",       lab = "ag farms"),
    list(var = "chg_log_areatot_ha_88_60", lab = "ag farmed area")
)

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)

    pop <- load_pop_1960()
    sym <- lapply(CASES, load_tau_sym, pop = pop)
    names(sym) <- CASES
    est <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))
    est <- ensure_geolev2_char(est)

    geo_ctrls <- setdiff(geo_controls_main,
                         c("logMA_actual_1960_s0_elow", "log_pop_1960"))
    ctrls <- c(geo_ctrls, "logMA_iceberg_1960", "log_pop_1960")

    rows <- list()
    for (th in THETAS) {
        rows[[length(rows) + 1L]] <-
            run_one(th, V_pesos = 0, sym, est, ctrls, raw_anchor = TRUE)
        for (v in V_GRID_PESOS) {
            rows[[length(rows) + 1L]] <-
                run_one(th, V_pesos = v, sym, est, ctrls, raw_anchor = FALSE)
        }
    }
    df <- do.call(rbind, rows)

    verify(df)
    write_outputs(df)
}

# ---------------------------------------------------------------------------
# One (theta, V) cell: recompute MA, run the Table 10 quad per outcome
# ---------------------------------------------------------------------------
run_one <- function(th, V_pesos, sym, est, ctrls, raw_anchor) {
    V_raster <- V_pesos * tau_units_to_pesos
    transform <- function(tau) {
        if (raw_anchor) tau else 1 + tau / V_raster
    }
    lma <- lapply(sym, function(s) {
        w <- ifelse(s$ok, 1 / transform(s$tau)^th, 0)
        ma <- rowsum(w * s$pop_dest, group = s$origin_geolev2)
        data.frame(geolev2 = rownames(ma), logMA = log(ma[, 1]),
                   stringsAsFactors = FALSE)
    })
    d <- Reduce(function(a, b) merge(a, b, by = "geolev2"), list(
        setNames(lma[["actual_1960"]],        c("geolev2", "logMA_iceberg_1960")),
        setNames(lma[["actual_1986"]],        c("geolev2", "lma86")),
        setNames(lma[["instrument_stu"]],     c("geolev2", "lma_stu")),
        setNames(lma[["instrument_lcp_mst"]], c("geolev2", "lma_lcp"))
    ))
    d$chg_iceberg <- d$lma86   - d$logMA_iceberg_1960
    d$z_stu       <- d$lma_stu - d$logMA_iceberg_1960
    d$z_lcp       <- d$lma_lcp - d$logMA_iceberg_1960

    m <- merge(est,
               d[, c("geolev2", "logMA_iceberg_1960",
                     "chg_iceberg", "z_stu", "z_lcp")],
               by = "geolev2", all.x = FALSE)
    stopifnot(nrow(m) == nrow(est))

    out <- list()
    for (o in OUTCOMES) {
        fits <- fit_iv_quad(
            y          = o$var,
            data       = m,
            endog      = "chg_iceberg",
            lp_instr   = "z_stu",
            hypo_instr = "z_lcp",
            ctrls_vec  = ctrls
        )
        co  <- safe_coef(fits[["IV-B"]],  "fit_chg_iceberg")
        # IV-LP too (cr-review PR #135 consider 1): the IV-LP-only
        # main-spec question is on the meeting agenda, and PR #130
        # showed normalization reverses the instrument ranking — the
        # de-risking must cover the contrast under IV-LP as well.
        clp <- safe_coef(fits[["IV-LP"]], "fit_chg_iceberg")
        out[[length(out) + 1L]] <- data.frame(
            theta     = th,
            V_pesos   = V_pesos,
            V_raster  = V_raster,
            anchor    = ifelse(raw_anchor, "raw (V->0)", "affine"),
            outcome   = o$var,
            label     = o$lab,
            ivb_beta  = co$est,
            ivb_se    = co$se,
            ivb_p     = co$p,
            ivb_F     = fitstat_F(fits[["IV-B"]]),
            ivlp_beta = clp$est,
            ivlp_se   = clp$se,
            ivlp_p    = clp$p,
            ivlp_F    = fitstat_F(fits[["IV-LP"]]),
            n_obs     = nobs(fits[["OLS"]]),
            stringsAsFactors = FALSE
        )
    }
    res <- do.call(rbind, out)
    message(sprintf(
        "[iceberg-sect] th=%.2f V=%6.0f | mfg val p=%.3f wage p=%.3f | ag farms p=%.3f",
        th, V_pesos,
        res$ivb_p[res$outcome == "chg_log_valprod_85_54"],
        res$ivb_p[res$outcome == "chg_log_massal_85_54"],
        res$ivb_p[res$outcome == "chg_log_nexp_88_60"]))
    res
}

# ---------------------------------------------------------------------------
# Verification anchors (asserted, not just reported)
# ---------------------------------------------------------------------------
verify <- function(df) {
    tol <- 1e-9

    is_raw_low <- df$anchor != "affine" & abs(df$theta - theta[["low"]]) < 1e-9

    # (1) Raw theta-low anchor reproduces Table 10's IV-B and IV-LP exactly
    t10 <- read.csv(file.path(dir_tables, "table_10_sectoral_iv.csv"))
    for (o in OUTCOMES) {
        if (o$var == "chg_log_pop_91_60") next
        ours <- df[is_raw_low & df$outcome == o$var, ]
        ref_b  <- t10[t10$outcome == o$var & t10$spec == "IV-B", ]
        ref_lp <- t10[t10$outcome == o$var & t10$spec == "IV-LP", ]
        stopifnot(nrow(ours) == 1L, nrow(ref_b) == 1L, nrow(ref_lp) == 1L)
        stopifnot(abs(ours$ivb_beta  - ref_b$estimate)  < tol,
                  abs(ours$ivb_se    - ref_b$std_err)   < tol,
                  abs(ours$ivlp_beta - ref_lp$estimate) < tol,
                  abs(ours$ivlp_se   - ref_lp$std_err)  < tol)
    }
    message("[verify] raw theta-low anchor == Table 10 IV-B and IV-LP (all 5)")

    # (2) ... and Table 9's IV-B/IV-LP for the population reference row
    t9 <- read.csv(file.path(dir_tables, "table_9_population_iv.csv"))
    ref9b  <- t9[t9$outcome == "chg_log_pop_91_60" & t9$spec == "IV-B", ]
    ref9lp <- t9[t9$outcome == "chg_log_pop_91_60" & t9$spec == "IV-LP", ]
    ours9 <- df[is_raw_low & df$outcome == "chg_log_pop_91_60", ]
    # Row-count guards BEFORE the comparisons (cr-review PR #135 SF1: a
    # zero-row filter would make the stopifnot pass vacuously).
    stopifnot(nrow(ours9) == 1L, nrow(ref9b) == 1L, nrow(ref9lp) == 1L)
    stopifnot(abs(ours9$ivb_beta  - ref9b$estimate)  < tol,
              abs(ours9$ivb_se    - ref9b$std_err)   < tol,
              abs(ours9$ivlp_beta - ref9lp$estimate) < tol,
              abs(ours9$ivlp_se   - ref9lp$std_err)  < tol)
    message("[verify] raw theta-low anchor == Table 9 IV-B and IV-LP (population)")

    # (3) Population rows match diagnostic_ma_iceberg.csv at every (theta, V)
    ib <- read.csv(file.path(dir_tables, "diagnostic_ma_iceberg.csv"))
    pop <- df[df$outcome == "chg_log_pop_91_60", ]
    n_cells <- length(THETAS) * (length(V_GRID_PESOS) + 1L)
    stopifnot(nrow(pop) == n_cells)
    chk <- merge(pop,
                 ib[, c("theta", "V_pesos", "ivb_beta", "ivb_se",
                        "ivlp_beta", "ivlp_se")],
                 by = c("theta", "V_pesos"), suffixes = c("", "_ref"))
    stopifnot(nrow(chk) == n_cells,
              max(abs(chk$ivb_beta  - chk$ivb_beta_ref))  < tol,
              max(abs(chk$ivb_se    - chk$ivb_se_ref))    < tol,
              max(abs(chk$ivlp_beta - chk$ivlp_beta_ref)) < tol,
              max(abs(chk$ivlp_se   - chk$ivlp_se_ref))   < tol)
    message("[verify] population rows == diagnostic_ma_iceberg.csv (all cells)")
}

# ---------------------------------------------------------------------------
# Loaders — copied from diagnostic_ma_iceberg.R (provenance note in header)
# ---------------------------------------------------------------------------
load_pop_1960 <- function() {
    d <- arrow::read_parquet(
        file.path(dir_derived_census1960, "census_1960_ipums.parquet"))
    d <- ensure_geolev2_char(d)
    data.frame(geolev2 = d$geolev2, pop = as.numeric(d$pop),
               stringsAsFactors = FALSE)
}

load_tau_sym <- function(case, pop) {
    tau_df <- arrow::read_parquet(
        file.path(dir_derived_taus, sprintf("tau_%s_s0.parquet", case)))
    tau_df <- ensure_geolev2_char(tau_df, "origin_geolev2")
    tau_df <- ensure_geolev2_char(tau_df, "destination_geolev2")
    s <- rbind(
        data.frame(origin_geolev2      = tau_df$origin_geolev2,
                   destination_geolev2 = tau_df$destination_geolev2,
                   tau = tau_df$tau, stringsAsFactors = FALSE),
        data.frame(origin_geolev2      = tau_df$destination_geolev2,
                   destination_geolev2 = tau_df$origin_geolev2,
                   tau = tau_df$tau, stringsAsFactors = FALSE)
    )
    s <- merge(s,
               data.frame(destination_geolev2 = pop$geolev2,
                          pop_dest = pop$pop, stringsAsFactors = FALSE),
               by = "destination_geolev2", all.x = TRUE)
    s$pop_dest[is.na(s$pop_dest)] <- 0
    s$ok <- is.finite(s$tau) & s$tau > 0
    s[order(s$origin_geolev2, s$destination_geolev2), ]
}

# ---------------------------------------------------------------------------
# Report + CSV
# ---------------------------------------------------------------------------
write_outputs <- function(df) {
    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)
    csv_path <- file.path(dir_tables, "diagnostic_ma_iceberg_sectoral.csv")
    write.csv(df, csv_path, row.names = FALSE)

    txt_path <- file.path(dir_tables, "diagnostic_ma_iceberg_sectoral.txt")
    con <- file(txt_path, open = "wt")
    wline <- function(...) { line <- sprintf(...); cat(line, "\n")
                             cat(line, "\n", file = con) }
    star <- function(p) ifelse(p < 0.01, "***",
                       ifelse(p < 0.05, "**",
                       ifelse(p < 0.10, "*", "")))
    wline("%s", strrep("=", 90))
    wline("ICEBERG NORMALIZATION x SECTORAL OUTCOMES (Decision A de-risking)")
    wline("Does 'manufacturing responds, agriculture does not' survive")
    wline("tau' = 1 + cost/V across the V grid? Table 10 spec, V-specific")
    wline("iceberg baseline logMA control. IV-Both grid first (the")
    wline("published spec), IV-LP grid second (the IV-LP-only main-spec")
    wline("question is on the meeting agenda and normalization reverses")
    wline("the instrument-strength ranking, PR #130). Companion to")
    wline("diagnostic_ma_iceberg.{txt,csv} (population curve, F columns).")
    wline("Stars: * p<.10  ** p<.05  *** p<.01 ; HC1 SE")
    wline("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
    wline("%s", strrep("=", 90))
    grid_block <- function(spec_lab, bcol, pcol) {
        for (th in unique(df$theta)) {
            wline("")
            wline("---- theta = %.2f (%s)", th, spec_lab)
            wline("%-9s %-14s %-15s %-15s %-13s %-13s %-13s",
                  "V (p/t)", "population", "mfg value", "mfg wage",
                  "mfg estab.", "ag farms", "ag area")
            sub <- df[df$theta == th, ]
            for (v in unique(sub$V_pesos)) {
                r <- sub[sub$V_pesos == v, ]
                cell <- function(var) {
                    x <- r[r$outcome == var, ]
                    sprintf("%+.3f%s", x[[bcol]], star(x[[pcol]]))
                }
                wline("%-9s %-14s %-15s %-15s %-13s %-13s %-13s",
                      ifelse(r$anchor[1] != "affine", "raw",
                             format(v, trim = TRUE, scientific = FALSE)),
                      cell("chg_log_pop_91_60"),
                      cell("chg_log_valprod_85_54"),
                      cell("chg_log_massal_85_54"),
                      cell("chg_log_nestab_85_54"),
                      cell("chg_log_nexp_88_60"),
                      cell("chg_log_areatot_ha_88_60"))
            }
        }
    }
    grid_block("IV-Both", "ivb_beta",  "ivb_p")
    grid_block("IV-LP",   "ivlp_beta", "ivlp_p")
    # Computed verdict block
    wline("")
    wline("Verdict (computed from the grids above):")
    for (spec in list(list(lab = "IV-Both", p = "ivb_p"),
                      list(lab = "IV-LP",   p = "ivlp_p"))) {
        for (th in unique(df$theta)) {
            sub <- df[df$theta == th, ]
            p <- sub[[spec$p]]
            maxp_val  <- max(p[sub$outcome == "chg_log_valprod_85_54"])
            maxp_wage <- max(p[sub$outcome == "chg_log_massal_85_54"])
            minp_null <- min(p[sub$outcome %in%
                c("chg_log_nestab_85_54", "chg_log_nexp_88_60",
                  "chg_log_areatot_ha_88_60")])
            wline("- %s, theta %.2f: mfg value max p = %.3f, wage max p = %.3f",
                  spec$lab, th, maxp_val, maxp_wage)
            wline("  across ALL %d grid points (incl. raw anchor);",
                  length(unique(sub$V_pesos)))
            wline("  establishments + both ag outcomes: min p = %.3f.",
                  minp_null)
        }
    }
    wline("")
    wline("Note on levels: beta grows by an order of magnitude along the")
    wline("grid as the transform compresses Delta log MA; the growth")
    wline("factor is OUTCOME-DEPENDENT because the transform is nonlinear")
    wline("(theta 8.22, raw -> V=100k: wage x41.7, population x13.1). The")
    wline("levels are object-dependent; the CONTRAST is not.")
    wline("")
    wline("Full columns (SE, p, F, N): diagnostic_ma_iceberg_sectoral.csv")
    close(con)
    message("\nSaved: ", txt_path)
    message("Saved: ", csv_path)
}

main()

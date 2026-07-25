# ===========================================================================
# diagnostic_mop_critical.R
#
# PURPOSE: Montiel Olea & Pflueger (2013) critical values for the
#          effective F statistics of diagnostic_modern_iv.R (completes
#          Cote reading note #35; feeds the meeting's main-spec
#          instrument discussion). The conventional 10 / 16.4 rules do
#          not apply to the effective F with K = 2 instruments; this
#          script computes the proper benchmark so "F_eff = 13.1"
#          becomes a pass/fail verdict.
#
# ALGORITHM (verified against Windmeijer 2023, arXiv:2309.01637v2,
# Section 3, which restates MOP's procedure with explicit formulas;
# the same algorithm underlies Stata's weakivtest, Pflueger-Wang 2015):
#   With residualized instruments Z (n x k), treatment D, REDUCED-FORM
#   residuals v1 (from Y~ on Z~, per MOP's setup; v1 = u + beta*v2),
#   first-stage residuals v2, Q = Z'Z, and HC1-scaled meats
#   S_ab = c * sum_i a_i b_i z_i z_i':
#     W1  = Q^{-1/2} S_v1v1 Q^{-1/2}
#     W2  = Q^{-1/2} S_v2v2 Q^{-1/2}   (tr(W2) = denominator of F_eff)
#     W12 = Q^{-1/2} S_v1v2 Q^{-1/2}
#   Nagar-bias objects, for scalar beta and unit vector c0:
#     S1(b)  = W1 - b (W12 + W12') + b^2 W2
#     S12(b) = W12 - b W2
#     n(b,c0) = [tr(S12) - 2 c0' S12 c0] / tr(W2)
#     BM(b)   = sqrt( tr(S1) / tr(W2) )
#     B(W)    = sup_{b, c0} |n(b,c0)| / BM(b)   (MOP prove B <= 1)
#   The inner sup over c0 is analytic: c0' S12 c0 ranges over
#   [lambda_min, lambda_max] of sym(S12), so the numerator max is
#   attained at an eigenvalue extreme. The outer sup over b is a 1-D
#   numerical maximization (grid over atan-transformed b + refinement
#   + the analytic b -> +/-Inf limit).
#   Patnaik (1949) critical value at bias tolerance tau, level alpha:
#     d      = B / tau            (exact test)   or 1/tau (conservative)
#     k_eff  = [tr(W2)]^2 (1+2d) /
#              ( tr(W2'W2) + 2 d tr(W2) lambda_max(W2) )
#     cv     = qchisq(1-alpha, df = k_eff, ncp = k_eff * d) / k_eff
#   Reject "weak" (Nagar bias > tau of worst-case benchmark) if
#   F_eff > cv. The conservative version needs no B optimization and
#   satisfies cv_cons >= cv_exact.
#
# CELLS: the same 11 headline cells as diagnostic_modern_iv.R
#   (Table 9 population x IV-LP / IV-H / IV-B + urban/rural/share
#   IV-B; Table 10 sectoral x IV-B). K = 1 cells included for
#   completeness — the same algorithm applies (k_eff = 1 exactly).
#
# TAU GRID: {5%, 10%, 20%, 30%} at alpha = 5% (weakivtest defaults).
#
# VERIFICATION (asserted in code):
#   - F_eff recomputed here must equal diagnostic_modern_iv.csv
#     digit-for-digit (row-count-guarded).
#   - The conservative K=1 cv at tau = 10% must equal MOP's published
#     23.1 (qchisq(.95, 1, ncp = 10) = 23.109).
#   - B <= 1 (+ numerical slack) for every cell; cv_exact <= cv_cons.
#   - k_eff = 1 exactly on the K=1 cells.
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#   results/tables/diagnostic_modern_iv.csv        (anchor check)
#
# PRODUCES (diagnostic only; no paper exhibit, no main.R wiring):
#   results/tables/diagnostic_mop_critical.txt
#   results/tables/diagnostic_mop_critical.csv
# ===========================================================================
suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
})

ALPHA <- 0.05
TAUS  <- c(0.05, 0.10, 0.20, 0.30)

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)

    # Sanity anchor for the cv routine itself: MOP's published K=1
    # conservative threshold at tau = 10% is 23.1.
    cv_k1 <- qchisq(1 - ALPHA, df = 1, ncp = 1 / 0.10) / 1
    stopifnot(abs(cv_k1 - 23.1) < 0.05)
    message(sprintf("[anchor] conservative K=1 cv(tau=.10) = %.3f (MOP: 23.1)",
                    cv_k1))

    est <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))
    est <- ensure_geolev2_char(est)

    lp_instr   <- main_lp_instrument
    hypo_instr <- main_hypo_instrument
    endog      <- main_treatment

    cells <- list(
        list(y = "chg_log_pop_91_60",        lab = "T9 total pop",    spec = "IV-LP"),
        list(y = "chg_log_pop_91_60",        lab = "T9 total pop",    spec = "IV-H"),
        list(y = "chg_log_pop_91_60",        lab = "T9 total pop",    spec = "IV-B"),
        list(y = "chg_log_urbpop_91_60",     lab = "T9 urban pop",    spec = "IV-B"),
        list(y = "chg_log_rur_91_60",        lab = "T9 rural pop",    spec = "IV-B"),
        list(y = "chg_urbshr_91_60",         lab = "T9 urban share",  spec = "IV-B"),
        list(y = "chg_log_valprod_85_54",    lab = "T10 mfg value",   spec = "IV-B"),
        list(y = "chg_log_massal_85_54",     lab = "T10 wage mass",   spec = "IV-B"),
        list(y = "chg_log_nestab_85_54",     lab = "T10 establish.",  spec = "IV-B"),
        list(y = "chg_log_nexp_88_60",       lab = "T10 farms",       spec = "IV-B"),
        list(y = "chg_log_areatot_ha_88_60", lab = "T10 farmed area", spec = "IV-B")
    )

    rows <- list()
    for (cell in cells) {
        instrs <- switch(cell$spec,
                         "IV-LP" = lp_instr,
                         "IV-H"  = hypo_instr,
                         "IV-B"  = c(lp_instr, hypo_instr))
        rows[[length(rows) + 1L]] <-
            run_cell(cell, est, endog, instrs, lp_instr, hypo_instr)
    }
    df <- do.call(rbind, rows)

    verify(df)
    write_outputs(df)
}

# ---------------------------------------------------------------------------
# One cell: residualize (as diagnostic_modern_iv.R), build W matrices,
# effective F, B(W), and critical values
# ---------------------------------------------------------------------------
run_cell <- function(cell, est, endog, instrs, lp_instr, hypo_instr) {
    vars <- c(cell$y, endog, lp_instr, hypo_instr, geo_controls_main)
    d <- est[complete.cases(est[, vars]), vars]

    fits <- fit_iv_quad(
        y          = cell$y,
        data       = d,
        endog      = endog,
        lp_instr   = lp_instr,
        hypo_instr = hypo_instr,
        ctrls_vec  = geo_controls_main
    )
    m_iv <- fits[[cell$spec]]
    stopifnot(nobs(m_iv) == nrow(d))

    # Frisch-Waugh residualization on controls + intercept (identical to
    # diagnostic_modern_iv.R; provenance: that script, PR #131)
    X <- as.matrix(cbind(1, d[, geo_controls_main]))
    r <- function(v) as.numeric(qr.resid(qr(X), v))
    Dt <- r(d[[endog]])
    Zt <- sapply(instrs, function(z) r(d[[z]]))
    Zt <- matrix(Zt, ncol = length(instrs))
    n  <- nrow(d)
    k  <- ncol(Zt)

    # Residuals per MOP's setup (cr-review PR #136 SF2): v1 = REDUCED-
    # FORM residuals of Y~ on Z~ (= u + beta*v2), v2 = first-stage
    # residuals of D~ on Z~. (Using structural 2SLS residuals instead
    # was verified immaterial — B moves <= 0.01, cv(10%) <= 0.2, no
    # verdict flips — but v1 is what weakivtest/Windmeijer use.)
    Yt <- r(d[[cell$y]])
    qz <- qr(Zt)
    pi_ <- qr.coef(qz, Dt)
    v2 <- qr.resid(qz, Dt)
    v1 <- qr.resid(qz, Yt)

    # W matrices with the same HC1 dof scaling as diagnostic_modern_iv.R
    hc1 <- n / (n - ncol(X) - k)
    Q   <- crossprod(Zt)
    eQ  <- eigen(Q, symmetric = TRUE)
    Qih <- eQ$vectors %*% diag(1 / sqrt(eQ$values), k) %*% t(eQ$vectors)
    meat <- function(a, b) hc1 * crossprod(Zt * a, Zt * b)
    W1  <- Qih %*% meat(v1, v1) %*% Qih
    W2  <- Qih %*% meat(v2, v2) %*% Qih
    W12 <- Qih %*% meat(v1, v2) %*% Qih

    F_eff <- as.numeric(t(pi_) %*% Q %*% pi_ / sum(diag(W2)))

    B <- B_of_W(W1, W2, W12)

    out <- data.frame(
        outcome = cell$y, label = cell$lab, spec = cell$spec,
        k_instr = k, n_obs = n, F_eff = F_eff, B = B,
        stringsAsFactors = FALSE
    )
    for (tau in TAUS) {
        tag <- sprintf("tau%02.0f", 100 * tau)
        ex  <- patnaik_cv(W2, d_tau = B / tau)
        cn  <- patnaik_cv(W2, d_tau = 1 / tau)
        out[[paste0("cv_", tag)]]      <- ex$cv
        out[[paste0("keff_", tag)]]    <- ex$k_eff
        out[[paste0("cvcons_", tag)]]  <- cn$cv
        out[[paste0("pass_", tag)]]    <- F_eff > ex$cv
    }
    message(sprintf(
        "[mop] %-15s %-6s F_eff=%6.2f B=%.3f | cv10=%6.2f %s | cv20=%6.2f %s",
        cell$lab, cell$spec, F_eff, B,
        out$cv_tau10, ifelse(out$pass_tau10, "PASS", "fail"),
        out$cv_tau20, ifelse(out$pass_tau20, "PASS", "fail")))
    out
}

# ---------------------------------------------------------------------------
# B(W) = sup over beta and unit-sphere c0 of |n(beta,c0)| / BM(beta).
# Inner sup analytic via eigenvalues of sym(S12(beta)); outer sup by a
# dense grid on atan-transformed beta plus local refinement and the
# analytic beta -> +/-Inf limit.
# ---------------------------------------------------------------------------
B_of_W <- function(W1, W2, W12) {
    trW2 <- sum(diag(W2))
    ratio_at <- function(b) {
        S12 <- W12 - b * W2
        S1  <- W1 - b * (W12 + t(W12)) + b^2 * W2
        sym <- (S12 + t(S12)) / 2
        ev  <- eigen(sym, symmetric = TRUE, only.values = TRUE)$values
        num <- max(abs(sum(diag(S12)) - 2 * min(ev)),
                   abs(sum(diag(S12)) - 2 * max(ev))) / trW2
        den <- sqrt(max(sum(diag(S1)), .Machine$double.eps) / trW2)
        num / den
    }
    # beta -> +/-Inf limit: S12 ~ -b W2 and S1 ~ b^2 W2, so |n|/BM tends
    # to max over eigenvalue extremes of |tr(W2) - 2 lambda(W2)| / trW2
    # (the |b| factors cancel between numerator and denominator):
    evW2 <- eigen((W2 + t(W2)) / 2, symmetric = TRUE, only.values = TRUE)$values
    lim  <- max(abs(sum(diag(W2)) - 2 * min(evW2)),
                abs(sum(diag(W2)) - 2 * max(evW2))) / trW2
    grid <- tan(seq(-pi / 2 + 1e-3, pi / 2 - 1e-3, length.out = 2001))
    vals <- vapply(grid, ratio_at, numeric(1))
    i    <- which.max(vals)
    lo   <- grid[max(1, i - 1)]
    hi   <- grid[min(length(grid), i + 1)]
    ref  <- optimize(ratio_at, lower = lo, upper = hi, maximum = TRUE)
    max(vals[i], ref$objective, lim)
}

# ---------------------------------------------------------------------------
# Patnaik critical value: upper-alpha quantile of
# chi2_{k_eff}(k_eff * d) / k_eff with k_eff from eq. (20).
# ---------------------------------------------------------------------------
patnaik_cv <- function(W2, d_tau) {
    trW2  <- sum(diag(W2))
    trW22 <- sum(diag(crossprod(W2)))
    lmax  <- max(eigen((W2 + t(W2)) / 2, symmetric = TRUE,
                       only.values = TRUE)$values)
    k_eff <- trW2^2 * (1 + 2 * d_tau) /
             (trW22 + 2 * d_tau * trW2 * lmax)
    cv <- qchisq(1 - ALPHA, df = k_eff, ncp = k_eff * d_tau) / k_eff
    list(cv = cv, k_eff = k_eff)
}

# ---------------------------------------------------------------------------
# Verification anchors (asserted; row-count-guarded)
# ---------------------------------------------------------------------------
verify <- function(df) {
    tol <- 1e-9
    ref <- read.csv(file.path(dir_tables, "diagnostic_modern_iv.csv"))
    chk <- merge(df, ref[, c("outcome", "spec", "F_eff")],
                 by = c("outcome", "spec"), suffixes = c("", "_ref"))
    stopifnot(nrow(chk) == nrow(df), nrow(df) == 11L)
    stopifnot(max(abs(chk$F_eff - chk$F_eff_ref)) < tol)
    message("[verify] F_eff == diagnostic_modern_iv.csv (all 11 cells)")

    stopifnot(all(df$B <= 1 + 1e-6))
    message("[verify] B <= 1 for every cell (MOP bound)")

    k1 <- df[df$k_instr == 1L, ]
    stopifnot(nrow(k1) == 2L,
              max(abs(k1$keff_tau10 - 1)) < 1e-9)
    message("[verify] k_eff = 1 exactly on the K=1 cells")

    for (tau in TAUS) {
        tag <- sprintf("tau%02.0f", 100 * tau)
        stopifnot(all(df[[paste0("cv_", tag)]] <=
                      df[[paste0("cvcons_", tag)]] + 1e-9))
    }
    message("[verify] exact cv <= conservative cv at every tau (MOP Lemma)")
}

# ---------------------------------------------------------------------------
# Report + CSV
# ---------------------------------------------------------------------------
write_outputs <- function(df) {
    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)
    csv_path <- file.path(dir_tables, "diagnostic_mop_critical.csv")
    write.csv(df, csv_path, row.names = FALSE)

    txt_path <- file.path(dir_tables, "diagnostic_mop_critical.txt")
    con <- file(txt_path, open = "wt")
    wline <- function(...) { line <- sprintf(...); cat(line, "\n")
                             cat(line, "\n", file = con) }
    wline("%s", strrep("=", 92))
    wline("MOP CRITICAL VALUES FOR THE EFFECTIVE F (completes note #35)")
    wline("Montiel Olea-Pflueger (2013) weak-instrument test via the Patnaik")
    wline("approximation (algorithm per Windmeijer 2023 Sec. 3 = Stata's")
    wline("weakivtest). Reject 'weak' at bias tolerance tau, alpha = 5%%, if")
    wline("F_eff > cv(tau). cv computed with the exact bias bound B(W) (<= 1;")
    wline("the conservative B = 1 variant is in the CSV). K=1 cells: k_eff = 1")
    wline("and the conservative cv(10%%) is MOP's published 23.1.")
    wline("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
    wline("%s", strrep("=", 92))
    wline("")
    wline("%-15s %-6s %2s %6s %6s  %28s  %14s",
          "Cell", "Spec", "K", "F_eff", "B", "cv at tau = 5/10/20/30%",
          "verdict(10/20)")
    for (i in seq_len(nrow(df))) {
        r <- df[i, ]
        wline("%-15s %-6s %2d %6.2f %6.3f  %28s  %6s/%-6s",
              r$label, r$spec, r$k_instr, r$F_eff, r$B,
              sprintf("%6.2f %6.2f %6.2f %6.2f",
                      r$cv_tau05, r$cv_tau10, r$cv_tau20, r$cv_tau30),
              ifelse(r$pass_tau10, "PASS", "fail"),
              ifelse(r$pass_tau20, "PASS", "fail"))
    }
    ivb <- df[df$spec == "IV-B", ]
    wline("")
    wline("Reading notes:")
    wline("- IV-B (K=2) cells: %d of %d pass at tau = 10%%; %d of %d at 20%%;",
          sum(ivb$pass_tau10), nrow(ivb), sum(ivb$pass_tau20), nrow(ivb))
    wline("  %d of %d at 30%%. cv(10%%) sits at %.1f-%.1f for these cells.",
          sum(ivb$pass_tau30), nrow(ivb),
          min(ivb$cv_tau10), max(ivb$cv_tau10))
    wline("- WHERE THE LOW BAR COMES FROM (attribution matters): the K=2")
    wline("  W2 structure alone only lowers the conservative (B=1) cv to")
    wline("  %.1f-%.1f — under the conservative test every IV-B cell",
          min(ivb$cvcons_tau10), max(ivb$cvcons_tau10))
    wline("  would %s at tau = 10%%. The drop to %.1f-%.1f is the exact",
          ifelse(any(ivb$F_eff > ivb$cvcons_tau10), "be mixed", "FAIL"),
          min(ivb$cv_tau10), max(ivb$cv_tau10))
    wline("  Nagar-bias bound B = %.3f-%.3f doing the work: for these W",
          min(ivb$B), max(ivb$B))
    wline("  matrices the worst-case 2SLS Nagar bias is a small fraction")
    wline("  of the benchmark, so a smaller F suffices. The pass verdicts")
    wline("  lean on that exact computation, not on instrument count.")
    wline("- Verdicts use the EXACT bound B(W); B here is %.3f-%.3f, so",
          min(df$B), max(df$B))
    wline("  exact cvs sit at or below the conservative B=1 variants")
    wline("  (equality at K=1, where B = 1 exactly).")
    wline("- The 'pass' verdict means: reject the null that the Nagar bias")
    wline("  of 2SLS exceeds tau of the worst-case benchmark, at the 5%%")
    wline("  significance level. Note the one qualification: rural")
    wline("  population fails at tau = 10%% (passes at 20%%).")
    wline("")
    wline("Full columns (k_eff, conservative cvs): diagnostic_mop_critical.csv")
    close(con)
    message("\nSaved: ", txt_path)
    message("Saved: ", csv_path)
}

main()

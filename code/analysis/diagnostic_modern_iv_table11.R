# ===========================================================================
# diagnostic_modern_iv_table11.R
#
# PURPOSE: extend the modern weak-IV inference check (PR #131) and the
#          MOP critical values (PR #136) to TABLE 11's four outcomes,
#          which the original pass skipped (it covered Table 9's
#          population outcomes and Table 10's sectoral ones).
#
#          This is the evidence for meeting agenda item D (migration
#          sign / demote Section 5.4). Table 11's committed estimates
#          show an instrument-dependent pattern that matters for that
#          decision:
#            recent migration  IV-B  -0.0217 (p = 0.007) but
#                              IV-LP -0.0066 (p = 0.52)
#            employment rate   IV-B  -0.0109 (p = 0.032) and
#                              IV-LP -0.0121 (p = 0.042)
#          i.e. migration's significance may rest on the hypothetical
#          instrument (the weak one: robust F 4.3, MOP-fails at every
#          tolerance) while employment survives the Larkin instrument.
#          Identification-robust inference settles which reading holds:
#          an AR set is valid regardless of instrument strength.
#
# CELLS: four outcomes x three IV specs = 12. All three specs are
#   reported (not just the published IV-B column) because the
#   instrument-dependence IS the object of interest here.
#
# PER CELL: beta/SE/p (2SLS, HC1), classical first-stage F (the
#   paper's current statistic), robust Wald F, MOP effective F with
#   Patnaik critical values at tau in {10%, 20%} and pass/fail, and
#   the 95% Anderson-Rubin confidence set by robust test inversion.
#
# MACHINERY: residualization + AR inversion copied from
#   diagnostic_modern_iv.R; effective F + B(W) + Patnaik cv copied
#   from diagnostic_mop_critical.R (reduced-form residuals per MOP's
#   setup). Diagnostics are self-contained by repo convention; NOTE
#   this is the fourth consumer of these helpers, which per the
#   PR #137 review triggers promotion to a shared _diagnostic_helpers
#   file — deliberately deferred to a post-meeting refactor rather
#   than mixed into a pre-meeting evidence task.
#
# VERIFICATION (asserted in code, row-count-guarded):
#   - beta and SE reproduce table_11_other_outcomes_iv.csv
#     digit-for-digit for all 12 cells.
#   - K=1 effective F == fixest's robust ivwald (the PR #131 identity
#     anchor); K=1 MOP cv == the fixed 23.11 (10%) / 15.06 (20%).
#   - B <= 1 everywhere (MOP bound); exact cv <= conservative cv.
#
# READS:
#   data/derived/06_analysis/estimation_sample.parquet
#   results/tables/table_11_other_outcomes_iv.csv        (anchor)
#
# PRODUCES (diagnostic only; no paper change in this PR):
#   results/tables/diagnostic_modern_iv_table11.txt
#   results/tables/diagnostic_modern_iv_table11.csv
# ===========================================================================
suppressPackageStartupMessages({
    library(arrow)
    library(fixest)
})

ALPHA    <- 0.05
AR_ALPHA <- 0.05
TAUS     <- c(0.10, 0.20)

OUTCOMES <- list(
    list(var = "chg_college_91_70",     lab = "college share"),
    list(var = "chg_secondary_91_70",   lab = "secondary share"),
    list(var = "chg_mig5_91_70",        lab = "recent migration"),
    list(var = "chg_empstat_emp_91_70", lab = "employment rate")
)

SPECS <- c("IV-LP", "IV-H", "IV-B")

main <- function() {
    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)
    source(file.path(dir_code, "analysis", "_iv_helpers.R"), echo = FALSE)

    est <- ensure_geolev2_char(as.data.frame(arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))))

    rows <- list()
    for (o in OUTCOMES) {
        for (sp in SPECS) {
            rows[[length(rows) + 1L]] <- run_cell(o, sp, est)
        }
    }
    df <- do.call(rbind, rows)

    verify(df)
    write_outputs(df)
}

# ---------------------------------------------------------------------------
# One (outcome, spec) cell
# ---------------------------------------------------------------------------
run_cell <- function(o, sp, est) {
    instrs <- switch(sp,
                     "IV-LP" = main_lp_instrument,
                     "IV-H"  = main_hypo_instrument,
                     "IV-B"  = c(main_lp_instrument, main_hypo_instrument))
    vars <- c(o$var, main_treatment, main_lp_instrument,
              main_hypo_instrument, geo_controls_main)
    d <- est[complete.cases(est[, vars]), vars]

    fits <- fit_iv_quad(
        y          = o$var,
        data       = d,
        endog      = main_treatment,
        lp_instr   = main_lp_instrument,
        hypo_instr = main_hypo_instrument,
        ctrls_vec  = geo_controls_main
    )
    m <- fits[[sp]]
    stopifnot(nobs(m) == nrow(d))
    co <- safe_coef(m, paste0("fit_", main_treatment))

    # Residualize on controls + intercept (Frisch-Waugh)
    X <- as.matrix(cbind(1, d[, geo_controls_main]))
    r <- function(v) as.numeric(qr.resid(qr(X), v))
    Dt <- r(d[[main_treatment]])
    Yt <- r(d[[o$var]])
    Zt <- matrix(sapply(instrs, function(z) r(d[[z]])),
                 ncol = length(instrs))
    n  <- nrow(d)

    mop <- mop_check(Yt, Dt, Zt, n_ctrl = ncol(X))
    ar  <- ar_invert(Yt, Dt, Zt, n_ctrl = ncol(X),
                     beta_hat = co$est, se_hat = co$se)

    out <- data.frame(
        outcome = o$var, label = o$lab, spec = sp,
        k_instr = ncol(Zt), n_obs = n,
        beta = co$est, se = co$se, p = co$p,
        F_classical = fitstat_F(m),
        F_robust    = fitstat_F_robust(m),
        F_eff       = mop$F_eff, B = mop$B,
        ar_set = ar$print, ar_bounded = ar$bounded,
        ar_status = ar$status, ar_maxp = ar$ar_maxp,
        sargan_p = sargan_p(m, ncol(Zt)),
        stringsAsFactors = FALSE
    )
    for (i in seq_along(TAUS)) {
        tag <- sprintf("tau%02.0f", 100 * TAUS[i])
        out[[paste0("cv_", tag)]]     <- mop$cv[i]
        out[[paste0("cvcons_", tag)]] <- mop$cvcons[i]
        out[[paste0("pass_", tag)]]   <- mop$F_eff > mop$cv[i]
    }
    message(sprintf(
        "[t11] %-17s %-6s b=%+9.5f p=%.3f | F_eff %5.1f cv10 %5.1f %s | AR %s",
        o$lab, sp, out$beta, out$p, out$F_eff, out$cv_tau10,
        ifelse(out$pass_tau10, "PASS", "fail"), out$ar_set))
    out
}

# ---------------------------------------------------------------------------
# MOP: effective F, exact bias bound, Patnaik critical values
# (copied from diagnostic_mop_critical.R, PR #136)
# ---------------------------------------------------------------------------
mop_check <- function(Yt, Dt, Zt, n_ctrl) {
    n <- length(Dt); k <- ncol(Zt)
    qz  <- qr(Zt)
    pi_ <- qr.coef(qz, Dt)
    v2  <- qr.resid(qz, Dt)
    v1  <- qr.resid(qz, Yt)
    hc1 <- n / (n - n_ctrl - k)
    Q   <- crossprod(Zt)
    eQ  <- eigen(Q, symmetric = TRUE)
    Qih <- eQ$vectors %*% diag(1 / sqrt(eQ$values), k) %*% t(eQ$vectors)
    meat <- function(a, b) hc1 * crossprod(Zt * a, Zt * b)
    W1  <- Qih %*% meat(v1, v1) %*% Qih
    W2  <- Qih %*% meat(v2, v2) %*% Qih
    W12 <- Qih %*% meat(v1, v2) %*% Qih
    F_eff <- as.numeric(t(pi_) %*% Q %*% pi_ / sum(diag(W2)))
    B <- B_of_W(W1, W2, W12)
    list(F_eff = F_eff, B = B,
         cv     = vapply(TAUS, function(t) patnaik_cv(W2, B / t), numeric(1)),
         cvcons = vapply(TAUS, function(t) patnaik_cv(W2, 1 / t), numeric(1)))
}

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
    evW2 <- eigen((W2 + t(W2)) / 2, symmetric = TRUE,
                  only.values = TRUE)$values
    lim  <- max(abs(sum(diag(W2)) - 2 * min(evW2)),
                abs(sum(diag(W2)) - 2 * max(evW2))) / trW2
    grid <- tan(seq(-pi / 2 + 1e-3, pi / 2 - 1e-3, length.out = 2001))
    vals <- vapply(grid, ratio_at, numeric(1))
    i    <- which.max(vals)
    ref  <- optimize(ratio_at, lower = grid[max(1, i - 1)],
                     upper = grid[min(length(grid), i + 1)],
                     maximum = TRUE)
    max(vals[i], ref$objective, lim)
}

patnaik_cv <- function(W2, d_tau) {
    trW2  <- sum(diag(W2))
    trW22 <- sum(diag(crossprod(W2)))
    lmax  <- max(eigen((W2 + t(W2)) / 2, symmetric = TRUE,
                       only.values = TRUE)$values)
    k_eff <- trW2^2 * (1 + 2 * d_tau) /
             (trW22 + 2 * d_tau * trW2 * lmax)
    qchisq(1 - ALPHA, df = k_eff, ncp = k_eff * d_tau) / k_eff
}

# Classical (homoskedastic) Sargan overid p-value; NA when k = 1
# (not overidentified). Reported alongside the robust AR-based
# evidence because the two can differ under heteroskedasticity —
# the college cell is exactly that case (see the report's notes).
sargan_p <- function(iv_model, k_instr) {
    if (k_instr < 2L) return(NA_real_)   # just-identified: no overid test
    s <- tryCatch(fitstat(iv_model, type = "sargan"),
                  error = function(e) NULL)
    if (is.list(s) && is.list(s$sargan) && !is.null(s$sargan$p)) {
        return(as.numeric(s$sargan$p))
    }
    NA_real_
}

fitstat_F_robust <- function(iv_model) {
    fs <- tryCatch(fitstat(iv_model, type = "ivwald"),
                   error = function(e) NULL)
    if (is.list(fs) && !is.null(fs[[1]]$stat)) return(as.numeric(fs[[1]]$stat))
    fs2 <- tryCatch(fitstat(iv_model, type = "ivwald", simplify = TRUE),
                    error = function(e) NULL)
    if (is.list(fs2) && !is.null(fs2$stat)) return(as.numeric(fs2$stat))
    NA_real_
}

# ---------------------------------------------------------------------------
# AR test inversion (copied from diagnostic_modern_iv.R, PR #131)
# ---------------------------------------------------------------------------
ar_p <- function(beta0, Yt, Dt, Zt, n_ctrl) {
    u  <- Yt - beta0 * Dt
    n  <- length(u); k <- ncol(Zt)
    qz <- qr(Zt)
    g  <- qr.coef(qz, u)
    e  <- qr.resid(qz, u)
    ZZinv <- solve(crossprod(Zt))
    meat  <- crossprod(Zt * e, Zt * e)
    df2   <- n - n_ctrl - k
    vcv   <- (n / df2) * ZZinv %*% meat %*% ZZinv
    Fst   <- as.numeric(t(g) %*% solve(vcv) %*% g) / k
    pf(Fst, k, df2, lower.tail = FALSE)
}

ar_invert <- function(Yt, Dt, Zt, n_ctrl, beta_hat, se_hat) {
    grid <- sort(unique(c(
        seq(beta_hat - 25 * se_hat, beta_hat - 3.1 * se_hat,
            by = 0.05 * se_hat),
        seq(beta_hat - 3 * se_hat, beta_hat + 3 * se_hat,
            by = 0.02 * se_hat),
        seq(beta_hat + 3.1 * se_hat, beta_hat + 25 * se_hat,
            by = 0.05 * se_hat)
    )))
    acc <- vapply(grid, function(b) {
        ar_p(b, Yt, Dt, Zt, n_ctrl) >= AR_ALPHA
    }, logical(1))
    ng <- length(grid)
    fmt <- function(x) sprintf("%.4f", x)
    # The largest AR p over the grid determines emptiness: the set is
    # empty iff every beta0 is rejected. An EMPTY set is NOT "precisely
    # estimated away from zero" — it means no beta is compatible with
    # both instruments, i.e. the robust overidentification restriction
    # fails. Classified separately from excludes/covers zero.
    maxp <- max(vapply(grid, function(b) ar_p(b, Yt, Dt, Zt, n_ctrl),
                       numeric(1)))
    out <- if (all(acc)) {
        list(print = "(-Inf, Inf)", bounded = FALSE, status = "covers 0")
    } else if (!any(acc)) {
        list(print = "empty", bounded = FALSE, status = "EMPTY (overid)")
    } else if (!acc[1] && !acc[ng]) {
        b <- range(grid[acc])
        list(print = sprintf("[%s, %s]", fmt(b[1]), fmt(b[2])),
             bounded = TRUE,
             status = if (b[1] <= 0 && 0 <= b[2]) "covers 0" else "excludes 0")
    } else if (acc[1] && acc[ng]) {
        b <- range(grid[!acc])
        list(print = sprintf("(-Inf, %s] U [%s, Inf)", fmt(b[1]), fmt(b[2])),
             bounded = FALSE,
             status = if (b[1] < 0 && 0 < b[2]) "excludes 0" else "covers 0")
    } else if (acc[1]) {
        b <- max(grid[acc])
        list(print = sprintf("(-Inf, %s]", fmt(b)), bounded = FALSE,
             status = if (0 > b) "excludes 0" else "covers 0")
    } else {
        b <- min(grid[acc])
        list(print = sprintf("[%s, Inf)", fmt(b)), bounded = FALSE,
             status = if (0 < b) "excludes 0" else "covers 0")
    }
    out$ar_maxp <- maxp
    out
}

# ---------------------------------------------------------------------------
# Verification anchors
# ---------------------------------------------------------------------------
verify <- function(df) {
    tol <- 1e-9
    ref <- read.csv(file.path(dir_tables, "table_11_other_outcomes_iv.csv"))
    chk <- merge(df, ref[, c("outcome", "spec", "estimate", "std_err",
                             "n_obs")],
                 by = c("outcome", "spec"), suffixes = c("", "_ref"))
    stopifnot(nrow(chk) == nrow(df), nrow(df) == 12L)
    stopifnot(max(abs(chk$beta - chk$estimate)) < tol,
              max(abs(chk$se   - chk$std_err))  < tol,
              all(chk$n_obs == chk$n_obs_ref))
    message("[verify] beta/SE/N == table_11_other_outcomes_iv.csv (12 cells)")

    # K=1 identity anchors: F_eff == robust ivwald; cv fixed at MOP's
    # published K=1 thresholds (B = 1 exactly when k = 1).
    k1 <- df[df$k_instr == 1L, ]
    stopifnot(nrow(k1) == 8L)
    stopifnot(max(abs(k1$F_eff - k1$F_robust) / k1$F_robust) < 1e-6,
              max(abs(k1$B - 1)) < 1e-6,
              max(abs(k1$cv_tau10 - 23.109)) < 0.01,
              max(abs(k1$cv_tau20 - 15.062)) < 0.01)
    message("[verify] K=1: F_eff == robust ivwald; B = 1; cv = 23.11/15.06")

    stopifnot(all(df$B <= 1 + 1e-6),
              all(df$cv_tau10 <= df$cvcons_tau10 + 1e-9),
              all(df$cv_tau20 <= df$cvcons_tau20 + 1e-9))
    message("[verify] B <= 1 and exact cv <= conservative cv everywhere")
}

# ---------------------------------------------------------------------------
# Report + CSV
# ---------------------------------------------------------------------------
write_outputs <- function(df) {
    if (!dir.exists(dir_tables)) dir.create(dir_tables, recursive = TRUE)
    csv_path <- file.path(dir_tables, "diagnostic_modern_iv_table11.csv")
    write.csv(df, csv_path, row.names = FALSE)

    txt_path <- file.path(dir_tables, "diagnostic_modern_iv_table11.txt")
    con <- file(txt_path, open = "wt")
    wline <- function(...) { line <- sprintf(...); cat(line, "\n")
                             cat(line, "\n", file = con) }
    wline("%s", strrep("=", 100))
    wline("MODERN WEAK-IV INFERENCE FOR TABLE 11 (agenda item D evidence)")
    wline("Extends PR #131 (robust/effective F, AR sets) and PR #136 (MOP")
    wline("critical values) to the four Table 11 outcomes, which the")
    wline("original pass skipped. All three IV specs shown: the")
    wline("instrument-dependence is the object of interest. AR sets are")
    wline("valid regardless of instrument strength; MOP pass/fail is at")
    wline("the stated Nagar-bias tolerance, alpha = 5%%.")
    wline("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
    wline("%s", strrep("=", 100))
    wline("")
    wline("%-17s %-6s %2s %11s %7s %7s %7s %-6s %-14s %-22s",
          "Outcome", "Spec", "K", "beta", "p", "F_eff", "cv10",
          "MOP10", "AR status", "AR 95% set")
    for (i in seq_len(nrow(df))) {
        r <- df[i, ]
        wline("%-17s %-6s %2d %+11.5f %7.3f %7.1f %7.1f %-6s %-14s %-22s",
              r$label, r$spec, r$k_instr, r$beta, r$p,
              r$F_eff, r$cv_tau10,
              ifelse(r$pass_tau10, "PASS", "fail"),
              r$ar_status, r$ar_set)
    }
    wline("")
    wline("An EMPTY AR set means no beta is compatible with BOTH")
    wline("instruments at 5%% — the robust overidentification restriction")
    wline("fails. It is NOT evidence of a precisely estimated effect.")
    wline("Classical (homoskedastic) Sargan p per outcome, IV-B: %s",
          paste(sprintf("%s %.4f", df$label[df$spec == "IV-B"],
                        df$sargan_p[df$spec == "IV-B"]), collapse = "; "))
    wline("(Robust AR and classical Sargan can disagree under")
    wline("heteroskedasticity; the college cell is that case — Sargan")
    wline("rejects at 5%% while the robust AR set is non-empty.)")
    # Computed reading notes, organized around the item-D question
    wline("")
    wline("Reading notes (per outcome, the item-D relevant facts):")
    for (o in OUTCOMES) {
        sub <- df[df$outcome == o$var, ]
        lp  <- sub[sub$spec == "IV-LP", ]
        h   <- sub[sub$spec == "IV-H", ]
        ivb <- sub[sub$spec == "IV-B", ]
        wline("- %-16s IV-LP %+.5f (p=%.3f) | IV-H %+.5f (p=%.3f) | IV-B %+.5f (p=%.3f)",
              o$lab, lp$beta, lp$p, h$beta, h$p, ivb$beta, ivb$p)
        wline("  %-16s AR: LP %s, H %s, B %s; Sargan p = %.4f",
              "", lp$ar_status, h$ar_status, ivb$ar_status, ivb$sargan_p)
    }
    wline("")
    wline("How to read this for item D:")
    wline("- A result is defensible when the well-identified instrument")
    wline("  (Larkin, F_eff %.1f) carries it AND its AR set excludes zero",
          df$F_eff[df$spec == "IV-LP"][1])
    wline("  AND the overid test does not reject.")
    wline("- A result is NOT defensible when it appears only in a spec")
    wline("  whose AR set is empty (instruments disagree) or only under")
    wline("  the hypothetical instrument (F_eff %.1f, MOP-fails at every",
          df$F_eff[df$spec == "IV-H"][1])
    wline("  tolerance).")
    wline("")
    wline("Full columns (SE, robust F, conservative cvs, tau = 20%%):")
    wline("diagnostic_modern_iv_table11.csv")
    close(con)
    message("\nSaved: ", txt_path)
    message("Saved: ", csv_path)
}

main()

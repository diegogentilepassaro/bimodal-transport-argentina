# ===========================================================================
# diagnostic_ma_gains.R
#
# PURPOSE: Diagnose why 91% of districts GAIN market access between 1960
#          and 1986, despite the rail network contracting ~23%. This is
#          Cote's review point P2 / C28 / Bloque 1: the near-universal
#          MA gain may be an artefact of how MA is constructed (fluvial
#          channel, centroid + HMI off-network cost), and the artefact
#          could explain the implausibly small main population elasticity
#          (0.046 vs Donaldson-Hornbeck ~0.5-0.7).
#
#   This is a DESCRIPTIVE diagnostic on existing pipeline outputs. It does
#   NOT re-run the pipeline. Questions that require alternate cost surfaces
#   (no-fluvial tau, alternate district reference point) are flagged at the
#   end as "needs a pipeline re-run" — they are the natural Bloque 1 follow
#   ups but out of scope for a same-day diagnostic.
#
# WHAT IT REPORTS:
#   1. The 91% gain, decomposed: how much of the gain is broad (most
#      destination pairs get cheaper) vs. concentrated (a few pairs).
#   2. Distribution of pairwise tau changes (1960 -> 1986): do most O-D
#      pairs get cheaper, and by how much?
#   3. Whether MA gain correlates with local road densification
#      (chg_pav_and_grav_86_54) — the C16 centroid+HMI channel: more
#      local road => lower off-network cost => higher MA.
#   4. Whether MA gain correlates with distance to Buenos Aires — a proxy
#      for the C28 fluvial / BA-corridor channel.
#   5. The handful of districts that LOSE MA: where are they, what do they
#      have in common?
#
# READS:
#   data/derived/03_taus/tau_actual_1960_s0.parquet
#   data/derived/03_taus/tau_actual_1986_s0.parquet
#   data/derived/06_analysis/estimation_sample.parquet
#
# PRODUCES:
#   results/figures/diagnostic_ma_gain_vs_road.png
#   results/figures/diagnostic_ma_gain_vs_distBA.png
#   results/figures/diagnostic_tau_change_hist.png
#   results/tables/diagnostic_ma_gains.txt   (text report)
# ===========================================================================

suppressPackageStartupMessages({
    library(arrow)
})

main <- function() {

    source(file.path(here::here(), "code", "config.R"), echo = FALSE)
    source(file.path(dir_code, "base", "utils.R"), echo = FALSE)

    if (!dir.exists(dir_figures)) dir.create(dir_figures, recursive = TRUE)
    if (!dir.exists(dir_tables))  dir.create(dir_tables,  recursive = TRUE)

    report_path <- file.path(dir_tables, "diagnostic_ma_gains.txt")
    con <- file(report_path, open = "wt")
    rep <- function(...) {
        line <- sprintf(...)
        cat(line, "\n")
        cat(line, "\n", file = con)
    }

    rep("%s", strrep("=", 70))
    rep("MA GAINS DIAGNOSTIC  (Cote review P2 / C28 / Bloque 1)")
    rep("Generated: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
    rep("%s", strrep("=", 70))

    d <- arrow::read_parquet(
        file.path(dir_derived_analysis, "estimation_sample.parquet"))
    d <- ensure_geolev2_char(d)

    # ---- 1. Confirm the 91% gain ------------------------------------------
    chg <- d$chg_logMA_86_60_s0_elow
    rep("\n[1] MA CHANGE 1960->1986 (sector 0, theta=4.55)")
    rep("    N districts:        %d", sum(!is.na(chg)))
    rep("    Share gaining (>0):  %.1f%%", 100 * mean(chg > 0, na.rm = TRUE))
    rep("    Share losing (<0):   %.1f%%", 100 * mean(chg < 0, na.rm = TRUE))
    rep("    mean=%.3f  median=%.3f  sd=%.3f  min=%.3f  max=%.3f",
        mean(chg, na.rm = TRUE), median(chg, na.rm = TRUE),
        sd(chg, na.rm = TRUE), min(chg, na.rm = TRUE), max(chg, na.rm = TRUE))

    # ---- 2. Pairwise tau changes ------------------------------------------
    rep("\n[2] PAIRWISE TRANSPORT-COST (tau) CHANGES, 1960 -> 1986")
    t60 <- arrow::read_parquet(
        file.path(dir_derived_taus, "tau_actual_1960_s0.parquet"))
    t86 <- arrow::read_parquet(
        file.path(dir_derived_taus, "tau_actual_1986_s0.parquet"))

    # Join on (origin, destination)
    key60 <- paste(t60$origin_geolev2, t60$destination_geolev2, sep = "_")
    key86 <- paste(t86$origin_geolev2, t86$destination_geolev2, sep = "_")
    m <- match(key60, key86)
    tau60 <- t60$tau
    tau86 <- t86$tau[m]
    ok <- is.finite(tau60) & is.finite(tau86) & tau60 > 0 & tau86 > 0
    dtau <- tau86[ok] / tau60[ok]   # ratio < 1 means cheaper in 1986

    rep("    O-D pairs with finite tau both years: %d", sum(ok))
    rep("    Share of pairs CHEAPER in 1986 (ratio<1): %.1f%%",
        100 * mean(dtau < 1))
    rep("    Median tau ratio (1986/1960): %.3f", median(dtau))
    rep("    (ratio 0.50 = pair got twice as cheap; 1.0 = unchanged)")
    q <- quantile(dtau, c(.05, .25, .5, .75, .95))
    rep("    tau-ratio quantiles: p5=%.2f p25=%.2f p50=%.2f p75=%.2f p95=%.2f",
        q[1], q[2], q[3], q[4], q[5])

    # ---- 3. MA gain vs local road densification (C16 channel) -------------
    rep("\n[3] MA GAIN vs LOCAL ROAD DENSIFICATION (C16: centroid+HMI)")
    sub <- d[!is.na(d$chg_logMA_86_60_s0_elow) &
             !is.na(d$chg_pav_and_grav_86_54), ]
    rho <- cor(sub$chg_logMA_86_60_s0_elow, sub$chg_pav_and_grav_86_54)
    rep("    cor(dlogMA, d road km) = %.3f   (N=%d)", rho, nrow(sub))
    rep("    If high & positive: MA gain tracks LOCAL road building,")
    rep("    consistent with the centroid+HMI off-network channel inflating")
    rep("    MA where roads densify near the district reference point.")

    p3_path <- file.path(dir_figures, "diagnostic_ma_gain_vs_road.png")
    grDevices::png(p3_path, width = 700, height = 500, res = 100)
    plot(sub$chg_pav_and_grav_86_54, sub$chg_logMA_86_60_s0_elow,
         pch = 19, col = grDevices::adjustcolor("black", 0.4),
         xlab = "Change in paved+gravel road km (1954-1986)",
         ylab = "Change in log MA (1960-1986)",
         main = sprintf("MA gain vs local road densification (cor=%.3f, N=%d)",
                        rho, nrow(sub)))
    fit3 <- lm(chg_logMA_86_60_s0_elow ~ chg_pav_and_grav_86_54, data = sub)
    graphics::abline(fit3, col = "#c00000", lwd = 2)
    graphics::abline(h = 0, col = "grey70", lty = 2)
    grDevices::dev.off()

    # ---- 4. MA gain vs distance to Buenos Aires (C28 fluvial/corridor) ----
    rep("\n[4] MA GAIN vs DISTANCE TO BUENOS AIRES (C28: fluvial/BA corridor)")
    sub2 <- d[!is.na(d$chg_logMA_86_60_s0_elow) & !is.na(d$dist_to_BA), ]
    rho2 <- cor(sub2$chg_logMA_86_60_s0_elow, sub2$dist_to_BA)
    rep("    cor(dlogMA, dist_to_BA) = %.3f   (N=%d)", rho2, nrow(sub2))
    rep("    Negative => districts FAR from BA gain LESS; near gain more.")

    p4_path <- file.path(dir_figures, "diagnostic_ma_gain_vs_distBA.png")
    grDevices::png(p4_path, width = 700, height = 500, res = 100)
    plot(sub2$dist_to_BA, sub2$chg_logMA_86_60_s0_elow,
         pch = 19, col = grDevices::adjustcolor("black", 0.4),
         xlab = "Distance to Buenos Aires",
         ylab = "Change in log MA (1960-1986)",
         main = sprintf("MA gain vs distance to BA (cor=%.3f, N=%d)",
                        rho2, nrow(sub2)))
    fit4 <- lm(chg_logMA_86_60_s0_elow ~ dist_to_BA, data = sub2)
    graphics::abline(fit4, col = "#c00000", lwd = 2)
    graphics::abline(h = 0, col = "grey70", lty = 2)
    grDevices::dev.off()

    # ---- 5. Who loses MA? -------------------------------------------------
    rep("\n[5] THE %d DISTRICTS THAT LOSE MA",
        sum(d$chg_logMA_86_60_s0_elow < 0, na.rm = TRUE))
    losers <- d[!is.na(d$chg_logMA_86_60_s0_elow) &
                d$chg_logMA_86_60_s0_elow < 0, ]
    rep("    mean dist_to_BA, losers:  %.3f  (vs %.3f all)",
        mean(losers$dist_to_BA, na.rm = TRUE),
        mean(d$dist_to_BA, na.rm = TRUE))
    rep("    mean d road km, losers:   %.1f  (vs %.1f all)",
        mean(losers$chg_pav_and_grav_86_54, na.rm = TRUE),
        mean(d$chg_pav_and_grav_86_54, na.rm = TRUE))
    rep("    mean d rail km, losers:   %.1f  (vs %.1f all)",
        mean(losers$chg_tot_rails_86_60, na.rm = TRUE),
        mean(d$chg_tot_rails_86_60, na.rm = TRUE))

    # ---- tau-change histogram ---------------------------------------------
    ph_path <- file.path(dir_figures, "diagnostic_tau_change_hist.png")
    grDevices::png(ph_path, width = 700, height = 500, res = 100)
    ratio_trim <- dtau[dtau > 0 & dtau < 3]
    graphics::hist(ratio_trim, breaks = 60, col = "steelblue",
         border = "white",
         xlab = "tau ratio (1986 / 1960)  --  <1 means cheaper in 1986",
         ylab = "Number of O-D pairs",
         main = sprintf("Pairwise transport-cost changes (%.1f%% cheaper)",
                        100 * mean(dtau < 1)))
    graphics::abline(v = 1, col = "#c00000", lty = 2, lwd = 2)
    grDevices::dev.off()

    # ---- Interpretation guide ---------------------------------------------
    rep("\n%s", strrep("=", 70))
    rep("INTERPRETATION GUIDE (for the meeting)")
    rep("%s", strrep("=", 70))
    rep("- If [2] shows nearly all pairs cheaper and the median ratio is")
    rep("  well below 1, road expansion is lowering costs broadly. That is")
    rep("  mechanically why ~everyone gains. Question for the team: is a")
    rep("  ~%.0f%%-cheaper median O-D cost plausible for 1960-1986?",
        100 * (1 - median(dtau)))
    rep("- If [3] cor is high & positive, MA gain tracks LOCAL road km,")
    rep("  pointing at the centroid+HMI off-network channel (C16).")
    rep("- If [4] cor is weak, the gain is NOT mainly a BA-distance story;")
    rep("  if strongly negative, the BA-corridor/fluvial channel (C28) is")
    rep("  a live suspect for the spatial pattern.")
    rep("")
    rep("NEEDS A PIPELINE RE-RUN (Bloque 1, not done here):")
    rep("  (a) No-fluvial tau: set navigation cost to Inf, recompute MA,")
    rep("      see if the 91%% gain and the riverine pattern shrink. (C28)")
    rep("  (b) Alternate district reference point: pole-of-inaccessibility")
    rep("      or urban-area centroid instead of geographic centroid, to")
    rep("      test the centroid+HMI artefact. (C16/C18 Level 1)")
    rep("  (c) Transshipment cost > 0 at mode-switch points. (C18 Level 2)")
    rep("%s", strrep("=", 70))

    close(con)
    message("\nSaved report: ", report_path)
    message("Saved 3 figures to ", dir_figures)
}

main()

# ==============================================================================
# main.R
#
# PURPOSE: Master controller for the full replication pipeline. Sources every
#          script in sequence, from environment bootstrap through final tables
#          and figures. Running this file end-to-end is the single required
#          step to reproduce all results from raw data.
#
# USAGE:
#   From a terminal at the repo root:
#     R CMD BATCH --no-save --no-restore code/main.R logs/main.Rout
#   Or interactively:
#     source("code/main.R")
#
# STAGES:
#   Stage A. Bootstrap    — load config, restore renv
#   Stage B. Base cleaning — one script per raw data source
#   Stage C. Pipeline      — cost rasters → taus → MA → panel
#   Stage D. Analysis      — estimation sample → tables and figures
#
# PRODUCES:
#   logs/main.Rout           — console log (via R CMD BATCH)
#   logs/makelog.log         — timestamped per-step build log
#   data/derived/**          — all intermediate datasets
#   results/tables/*.{tex,csv} — LaTeX table fragments + CSV summaries
#   results/figures/*.{pdf,png} — figures
#
# DESIGN:
#   Each step is wrapped in `run_step()` which logs START/END timestamps,
#   sources the script, and logs elapsed wall time. If a script errors,
#   the error propagates and main.R halts with the stack trace in the log.
#
#   verify_outputs() checks expected files exist after each step and stops
#   with a clear error if anything is missing.
# ==============================================================================

# ---- Stage A. Bootstrap -----------------------------------------------------
if (!requireNamespace("here", quietly = TRUE)) {
    stop("[main.R] The 'here' package is not installed.\n",
         "         Install it once with: install.packages('here')")
}

source(file.path(here::here(), "code", "config.R"), echo = FALSE)
source(file.path(dir_code, "00_setup.R"), echo = TRUE)

# ==============================================================================
# LOGGING HELPERS
# ==============================================================================

log_step <- function(status, step_id, description, logfile) {
    ts  <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    sep <- strrep("-", 72)
    msg <- sprintf("[%s]  %s  |  %s  |  %s", ts, status, step_id, description)
    cat(sep, "\n", msg, "\n", file = logfile, append = TRUE)
    message(sep); message(msg)
}

run_step <- function(step_id, script, description, makelog) {
    log_step("START", step_id, description, makelog)
    t_start <- proc.time()
    source(script, echo = TRUE)
    elapsed <- round((proc.time() - t_start)[["elapsed"]])
    log_step(sprintf("END [%ds]", elapsed), step_id, description, makelog)
    invisible(elapsed)
}

verify_outputs <- function(step_id, files, makelog) {
    missing <- files[!file.exists(files)]
    if (length(missing) > 0L) {
        msg <- sprintf(
            "[main.R] Step %s: %d expected output(s) missing:\n%s",
            step_id, length(missing),
            paste(" -", missing, collapse = "\n"))
        cat(msg, "\n", file = makelog, append = TRUE)
        stop(msg)
    }
    cat(sprintf("  [verify] All %d expected outputs present.\n", length(files)),
        file = makelog, append = TRUE)
}

log_output_inventory <- function(makelog) {
    cat(strrep("-", 72), "\nOUTPUT INVENTORY (",
        format(Sys.time(), "%Y-%m-%d %H:%M:%S"), ")\n",
        strrep("-", 72), "\n",
        file = makelog, append = TRUE, sep = "")
    result_files <- list.files(dir_results, recursive = TRUE, full.names = TRUE)
    if (length(result_files) == 0L) {
        cat("  (no files in results/)\n", file = makelog, append = TRUE)
        return(invisible(NULL))
    }
    for (f in sort(result_files)) {
        info <- file.info(f)
        cat(sprintf("  %-65s  %8.1f KB  %s\n",
                    sub(rootdir, "", f, fixed = TRUE),
                    info$size / 1024,
                    format(info$mtime, "%Y-%m-%d %H:%M")),
            file = makelog, append = TRUE)
    }
}

# ==============================================================================
# STAGE B. BASE CLEANING
# One script per raw data source. Each produces data/derived/base/<source>/*.
# ==============================================================================

stage_b_base <- function(makelog) {
    b <- function(subdir, fname) file.path(dir_code, "base", subdir, fname)

    run_step("B.1  geo_controls",
             b("geo_controls", "clean_geo_controls.R"),
             "Geographic controls (elevation, ruggedness, wheat, caloric, etc.)",
             makelog)
    verify_outputs("B.1",
        file.path(dir_derived_geo, "geo_controls.parquet"), makelog)

    run_step("B.2  census_1947",
             b("census_1947", "clean_census_1947.R"),
             "Digitized 1947 census (placebo population)", makelog)
    verify_outputs("B.2",
        file.path(dir_derived_census1947, "census_1947.parquet"), makelog)

    run_step("B.3  census_1960",
             b("census_1960", "clean_census_1960.R"),
             "Digitized 1960 census (baseline population, urban share)",
             makelog)
    verify_outputs("B.3",
        file.path(dir_derived_census1960, "census_1960.parquet"), makelog)

    run_step("B.4  ipums",
             b("ipums", "clean_ipums.R"),
             "IPUMS 1970-2010 microdata aggregated to district-year", makelog)
    verify_outputs("B.4",
        file.path(dir_derived_ipums, "ipums_panel.parquet"), makelog)

    run_step("B.5  industrial",
             b("industrial", "clean_industrial.R"),
             "Industrial census 1954 and 1985", makelog)
    verify_outputs("B.5",
        file.path(dir_derived_ind, "industrial_panel.parquet"), makelog)

    run_step("B.6  agricultural",
             b("agricultural", "clean_agricultural.R"),
             "Agricultural census 1960 and 1988", makelog)
    verify_outputs("B.6",
        file.path(dir_derived_agr, "agricultural_panel.parquet"), makelog)

    run_step("B.7  railroads",
             b("networks", "clean_railroads.R"),
             "Railroad network: Larkin Plan, 1979 status, studied flags",
             makelog)
    verify_outputs("B.7",
        file.path(dir_derived_networks, "rails_district_panel.parquet"),
        makelog)

    run_step("B.8  roads",
             b("networks", "clean_roads.R"),
             "Road network: paved and gravel, 1954/1970/1986", makelog)
    verify_outputs("B.8",
        file.path(dir_derived_networks, "roads_district_panel.parquet"),
        makelog)

    run_step("B.9  hypo_networks",
             b("networks", "clean_hypo_networks.R"),
             "Hypothetical networks (LCP-MST, Euc-MST, LCP, Euc)", makelog)
    verify_outputs("B.9",
        file.path(dir_derived_networks, "hypo_networks.parquet"), makelog)
}

# ==============================================================================
# STAGE C. PIPELINE
# Cost rasters → transition grids → tau matrices → MA indices → wide panel.
# ==============================================================================

stage_c_pipeline <- function(makelog) {
    p <- function(fname) file.path(dir_code, "pipeline", fname)

    # Cost rasters for the main actual network configurations
    run_step("C.1  cost_raster (actual)",
             p("01_cost_raster.R"),
             "Build cost rasters from actual rail+road networks",
             makelog)

    # Cost rasters for hypothetical networks and counterfactual configs
    run_step("C.2  cost_raster (hypothetical)",
             p("02_hypothetical_networks.R"),
             "Build cost rasters for hypothetical-road instruments",
             makelog)

    # Combined cost raster builder (full grid expansion)
    run_step("C.3a build_cost_raster",
             p("03a_build_cost_raster.R"),
             "Assemble all cost rasters (period × sector × elasticity)",
             makelog)

    # Transition grids for Dijkstra
    run_step("C.3b transition_grids",
             p("03b_transition_grids.R"),
             "Convert cost rasters to gdistance transition grids",
             makelog)

    # Tau matrices (parallel Dijkstra)
    run_step("C.3c compute_taus",
             p("03c_compute_taus_parallel.R"),
             "Pairwise transport costs via Dijkstra (parallel)",
             makelog)

    # Market access indices
    run_step("C.4  market_access",
             p("04_market_access.R"),
             "MA = Σ Pop_j / tau_ij^θ, all cases × sectors × elasticities",
             makelog)

    # Wide panel assembly
    run_step("C.5  build_panel",
             p("05_build_panel.R"),
             "Merge all cleaned sources into 312-district wide panel",
             makelog)
    verify_outputs("C.5",
        file.path(dir_derived_panel, "departments_wide_panel.parquet"),
        makelog)

    # Merge MA columns into the panel (overwrites departments_wide_panel)
    run_step("C.6  merge_ma_into_panel",
             p("06_merge_ma_into_panel.R"),
             "Merge all MA columns into the wide panel", makelog)
    verify_outputs("C.6",
        file.path(dir_derived_panel, "departments_wide_panel.parquet"),
        makelog)
}

# ==============================================================================
# STAGE D. ANALYSIS
# Estimation sample → figures → tables.
# ==============================================================================

stage_d_analysis <- function(makelog) {
    a <- function(fname) file.path(dir_code, "analysis", fname)
    bn <- function(fname) file.path(dir_code, "base", "networks", fname)

    # Estimation sample
    run_step("D.1  build_estimation_sample",
             a("build_estimation_sample.R"),
             "Derive outcomes and controls → 312-district estimation sample",
             makelog)
    verify_outputs("D.1",
        file.path(dir_derived_analysis, "estimation_sample.parquet"),
        makelog)

    # Figures (from networks and from the estimation sample)
    run_step("D.2  figure_1_networks",
             bn("plot_figure_1.R"),
             "Figure 1: rail and road network changes, 1960-1986",
             makelog)
    verify_outputs("D.2",
        file.path(dir_figures, "figure_1_network_changes.pdf"), makelog)

    run_step("D.3  figure_2_ma",
             a("plot_figure_2.R"),
             "Figure 2: ΔlogMA choropleth",
             makelog)
    verify_outputs("D.3",
        file.path(dir_figures, "figure_2_ma_change_choropleth.pdf"), makelog)

    run_step("D.4  figure_3_rail_road",
             bn("plot_figure_3.R"),
             "Figure 3: Δrail-km vs Δroad-km scatter", makelog)
    verify_outputs("D.4",
        file.path(dir_figures, "figure_3_rail_vs_road_change.pdf"), makelog)

    run_step("D.5  figure_4_infra_ma",
             a("plot_figure_4.R"),
             "Figure 4: infrastructure change vs ΔlogMA", makelog)
    verify_outputs("D.5",
        file.path(dir_figures, "figure_4_infra_vs_ma_scatter.pdf"), makelog)

    run_step("D.6  table_1_networks",
             bn("make_table_network_changes.R"),
             "Table 1: summary of network changes by period", makelog)
    verify_outputs("D.6",
        file.path(dir_tables, "table_1_network_changes.tex"), makelog)

    # Tables 6-12 (validation, first stage, main results, sectoral,
    # other outcomes, robustness)
    run_step("D.7  table_6_pre_balance",
             a("table_6_pre_balance.R"),
             "Table 6: pre-period balance on instruments", makelog)
    verify_outputs("D.7",
        file.path(dir_tables, "table_6_pre_balance.tex"), makelog)

    run_step("D.8  table_7_pre_trends",
             a("table_7_pre_trends.R"),
             "Table 7: pre-trends placebo (1947-1960 population)", makelog)
    verify_outputs("D.8",
        file.path(dir_tables, "table_7_pre_trends.tex"), makelog)

    run_step("D.9  table_8_first_stage",
             a("table_8_first_stage.R"),
             "Table 8: first-stage regressions", makelog)
    verify_outputs("D.9",
        file.path(dir_tables, "table_8_first_stage.tex"), makelog)

    run_step("D.10 table_9_population",
             a("table_9_population.R"),
             "Table 9: main IV regressions on population outcomes",
             makelog)
    verify_outputs("D.10",
        file.path(dir_tables, "table_9_population_iv.tex"), makelog)

    run_step("D.11 table_10_sectoral",
             a("table_10_sectoral.R"),
             "Table 10: sectoral activity (manufacturing + agriculture)",
             makelog)
    verify_outputs("D.11",
        file.path(dir_tables, "table_10_sectoral_iv.tex"), makelog)

    run_step("D.12 table_11_other_outcomes",
             a("table_11_other_outcomes.R"),
             "Table 11: education, migration, employment rate", makelog)
    verify_outputs("D.12",
        file.path(dir_tables, "table_11_other_outcomes_iv.tex"), makelog)

    run_step("D.13 table_12_robustness",
             a("table_12_robustness.R"),
             "Table 12: robustness (alt theta, alt hypo, subsample)",
             makelog)
    verify_outputs("D.13",
        file.path(dir_tables, "table_12_robustness.tex"), makelog)

    run_step("D.13b table_13_counterfactual",
             a("table_13_counterfactual.R"),
             "Table 13: counterfactual decomposition (full/only-rail/only-road)",
             makelog)
    verify_outputs("D.13b",
        file.path(dir_tables,
                  paste0("table_13_counterfactual.", c("tex", "csv"))),
        makelog)

    run_step("D.13c table_14_mechanisms",
             a("table_14_mechanisms.R"),
             "Table 14: local-infrastructure mechanism (progressive Z_i)",
             makelog)
    verify_outputs("D.13c",
        file.path(dir_tables,
                  paste0("table_14_mechanisms.", c("tex", "csv"))),
        makelog)

    run_step("D.13d diagnostic_heterogeneity",
             a("diagnostic_heterogeneity.R"),
             "Heterogeneity diagnostic (MA x baseline characteristics)",
             makelog)
    verify_outputs("D.13d",
        file.path(dir_tables,
                  paste0("diagnostic_heterogeneity.", c("txt", "csv"))),
        makelog)
    # AutoFill scalars — must run after all tables so it has every CSV
    run_step("D.14 generate_scalars",
             a("generate_scalars.R"),
             "AutoFill macros for in-text numbers (results/scalars.tex)",
             makelog)
    verify_outputs("D.14",
        file.path(dir_results, "scalars.tex"), makelog)

    # Appendix figures A1-A3
    run_step("D.15 figure_a1_cost_schedule",
             a("plot_figure_a1_cost_schedule.R"),
             "Figure A1: B&P transport cost schedule by mode and density",
             makelog)
    verify_outputs("D.15",
        file.path(dir_figures,
                  paste0("figure_a1_cost_schedule.", c("pdf", "png"))),
        makelog)

    run_step("D.16 figure_a2_hypothetical_networks",
             a("plot_figure_a2_hypothetical_networks.R"),
             "Figure A2: hypothetical road networks vs actual 1986 roads",
             makelog)
    verify_outputs("D.16",
        file.path(dir_figures,
                  paste0("figure_a2_hypothetical_networks.", c("pdf", "png"))),
        makelog)

    run_step("D.17 figure_a3_larkin_studied",
             bn("plot_figure_a3_larkin_studied.R"),
             "Figure A3: Larkin Plan studied vs non-studied rail segments",
             makelog)
    verify_outputs("D.17",
        file.path(dir_figures,
                  paste0("figure_a3_larkin_studied.", c("pdf", "png"))),
        makelog)

    run_step("D.18 figure_c13_ma_counterfactual_trio",
             a("plot_figure_c13.R"),
             "Figure C13: dlogMA total vs rail-only vs road-only choropleths",
             makelog)
    verify_outputs("D.18",
        file.path(dir_figures,
                  paste0("figure_c13_ma_counterfactual_trio.",
                         c("pdf", "png"))),
        makelog)
}

# ==============================================================================
# MAIN
# ==============================================================================

main <- function() {
    makelog <- file.path(dir_logs, "makelog.log")
    dir.create(dir_logs, recursive = TRUE, showWarnings = FALSE)

    cat(strrep("=", 72), "\n",
        "REPLICATION PACKAGE - BUILD LOG\n",
        sprintf("Started:   %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
        sprintf("rootdir:   %s\n", rootdir),
        sprintf("R version: %s\n", R.version.string),
        strrep("=", 72), "\n",
        file = makelog, append = FALSE, sep = "")

    build_start <- proc.time()

    stage_b_base(makelog)
    stage_c_pipeline(makelog)
    stage_d_analysis(makelog)

    total_elapsed <- round((proc.time() - build_start)[["elapsed"]])
    summary_msg <- sprintf(
        "\nBuild complete: %s\nTotal wall-clock time: %d min %d sec\n",
        format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
        total_elapsed %/% 60L, total_elapsed %% 60L)

    cat(strrep("=", 72), "\n", summary_msg, strrep("=", 72), "\n",
        file = makelog, append = TRUE, sep = "")
    message(strrep("=", 72)); message(summary_msg); message(strrep("=", 72))

    log_output_inventory(makelog)
    invisible(TRUE)
}

main()

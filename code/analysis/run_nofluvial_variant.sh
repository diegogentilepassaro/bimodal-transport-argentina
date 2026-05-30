#!/usr/bin/env bash
# ===========================================================================
# run_nofluvial_variant.sh
#
# Bloque-1 test (a): rebuild the cost surface with the fluvial/navigation
# channel DISABLED, then recompute transitions, taus, and MA for the four
# main cases. Tests Cote's C28 concern that cheap uniform navigation cost
# inflates riverine MA and drives the implausible near-universal MA gain.
#
# The DISABLE_NAVIGATION=1 env var makes combine_cost() treat navigable
# water as non-navigable, and save_cost_raster() append a _nofluvial
# suffix so baseline outputs are never clobbered.
#
# Cases: actual_1960, actual_1986 (main treatment) + instrument_stu,
#        instrument_lcp_mst (instruments), all sector 0.
#
# Runtime: ~4 raster builds + 4 transitions + 4 taus (~6 min each) ≈ 40-60 min.
#
# Run from repo root:  bash code/analysis/run_nofluvial_variant.sh
# ===========================================================================
set -euo pipefail

CASES_BASE="actual_1960_s0 actual_1986_s0 instrument_stu_s0 instrument_lcp_mst_s0"
CASES_NF="actual_1960_s0_nofluvial actual_1986_s0_nofluvial instrument_stu_s0_nofluvial instrument_lcp_mst_s0_nofluvial"

echo "=== Step 1: build no-fluvial cost rasters ==="
DISABLE_NAVIGATION=1 Rscript code/pipeline/03a_build_cost_raster.R $CASES_BASE

echo "=== Step 2: build transitions from no-fluvial rasters ==="
Rscript code/pipeline/03b_transition_grids.R $CASES_NF

echo "=== Step 3: compute taus (parallel, 4 cores) ==="
Rscript code/pipeline/03c_compute_taus_parallel.R 4 $CASES_NF

echo "=== Step 4: compute MA for no-fluvial taus ==="
Rscript code/pipeline/04_market_access.R $CASES_NF

echo "=== Done. No-fluvial outputs carry the _nofluvial suffix. ==="

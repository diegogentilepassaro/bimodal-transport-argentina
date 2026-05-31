#!/usr/bin/env bash
# ===========================================================================
# run_unimodal_variant.sh
#
# Bloque-1 test (c) SCREEN: bound the transshipment assumption.
#
# The baseline allows free mode-switching (zero transshipment cost): a
# least-cost path can hop road->rail->water at any pixel for free. The
# opposite extreme is INFINITE transshipment: each trip uses a single
# primary mode plus off-network walking, no switching. The truth is in
# between. If the 91% MA-gain and the broad cost collapse survive the
# infinite-transshipment (unimodal) bound, transshipment is not the
# driver; if they collapse, the zero-transshipment assumption is
# load-bearing and the full mode-expanded-graph model is warranted.
#
# Unimodal tau_ij = min( tau_road_ij, tau_rail_ij, tau_water_ij ),
# where each single-mode tau uses that mode + off-network land only.
#
# This script builds the three single-mode cost surfaces for the two
# treatment periods (1960, 1986), their transitions and taus. The
# element-wise min and the MA/elasticity comparison are done by
# diagnostic_ma_unimodal.R afterwards.
#
# Runtime: 6 rasters + 6 transitions + 6 taus (~5 min each) ~= 45-55 min.
#
# Run from repo root:  bash code/analysis/run_unimodal_variant.sh
# ===========================================================================
set -euo pipefail

build_mode () {
    local variant="$1"   # roadonly | railonly | wateronly
    local base="$2"      # actual_1960_s0 | actual_1986_s0
    local case_v="${base}_${variant}"
    echo "=== ${case_v}: raster ==="
    MODE_VARIANT="$variant" Rscript code/pipeline/03a_build_cost_raster.R "$base"
    echo "=== ${case_v}: transition ==="
    Rscript code/pipeline/03b_transition_grids.R "$case_v"
    echo "=== ${case_v}: tau ==="
    Rscript code/pipeline/03c_compute_taus.R "$case_v"
}

for base in actual_1960_s0 actual_1986_s0; do
    for variant in roadonly railonly wateronly; do
        build_mode "$variant" "$base"
    done
done

echo "=== All six single-mode taus built. Run diagnostic_ma_unimodal.R next. ==="

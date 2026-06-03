# Connector pre-check: the centroid→network land leg dominates τ

Research note prompted by Cote's "connector re-cost" experiment proposal
(2026-06-03). Question before committing to a multi-hour four-case τ
rebuild: how big is the centroid→nearest-on-network land leg as a share
of τ, and does road densification (1960→1986) shrink it enough to move
Δlog MA materially?

Script: `code/analysis/diagnostic_connector_share.R`
Outputs: `results/tables/diagnostic_connector_share.{csv,txt}`

## Method (cheap proxy, not the full Dijkstra)

τ accumulates as Σ(cost_cell × metres) on the ESRI:54034 equal-area grid.
On-network cells carry cost ∈ {0.621, 1.777, 1.874}; off-network land
cells carry cost_land × HMI (~95–773, median ~146). The two-order-of-
magnitude gap lets a simple threshold (cost ≤ 50) flag on-network cells.
For each district centroid, in 1960 and 1986:

- `d_i` = straight-line distance to nearest on-network cell (`terra::distance`)
- `leg_i` = d_i(m) × c_repr, where c_repr = median land cost (~146)

This is Cote's toy model τ_ij = c·d_i + r_ij + c·d_j with c = c_repr.
Caveat: the real pipeline uses the Dijkstra least-cost path (can be longer
than straight-line, uses the local HMI not the median), so these numbers
are indicative magnitudes, not exact. The signal is large enough that the
conclusion is robust to the approximation.

## Result — the land leg is NOT a minor part of τ

Distance centroid → nearest on-network cell (km):

| | p10 | p25 | p50 | p75 | p90 | max |
|---|---|---|---|---|---|---|
| 1960 | 0.0 | 1.2 | 6.3 | 15.0 | 28.2 | 245 |
| 1986 | 0.0 | 1.2 | 3.7 | 9.0 | 16.1 | 90 |

Land share of a **typical pair** through district i,
(leg_i + median_leg)/median_τ_i, 1960:

| p10 | p25 | p50 | p75 | p90 |
|---|---|---|---|---|
| 0.38 | 0.42 | **0.51** | 0.60 | 0.67 |

One-ended share (leg_i/median_τ_i), 1960: median **0.26**, p75 0.42.

So **half of a typical pair's τ is the two off-network land legs**, not the
on-network route. 59% of districts have a one-ended land share above 20%.

## Result — densification shrinks the leg, and it moves τ

- **123 of 312 districts** saw their centroid→network distance shrink
  1960→1986 (median shrink 7.65 km).
- Among those, the change in one land leg as a share of τ (|Δleg|/median_τ):
  median **0.22**, p90 **0.48**.
- 41% of all districts have |Δleg|/τ > 0.05.

The connector contributes materially to the *period-over-period change* in
τ that builds Δlog MA — exactly the inflation channel Cote described.

## Verdict

Cote's intuition holds quantitatively. The first-kilometer land leg both
(a) dominates the *level* of τ and (b) shrinks substantially where roads
densified, so it drives a large part of Δlog MA in road-building districts.
Because Δlog MA is the regressor, inflation concentrated in treated
districts attenuates β — so re-costing the connector to road cost is
predicted to **raise β** (Cote's sign).

→ The full four-case rebuild (actual_1960, actual_1986, instrument_stu,
instrument_lcp_mst → re-cost connector → 03b→03c→04→regression) is
**worth doing**. This is a multi-hour build-and-validate job, not a
same-day run.

θ-interaction: the rebuild produces new τ matrices; θ is the exponent on
them, so this is θ-agnostic and can run before the θ framing decision.
Same root cause as the θ note, though: raw accumulated τ dominated by a
few expensive (here, land) legs.

## Cross-refs
- `code/analysis/diagnostic_connector_share.R`
- `.kiro/theta_benchmark_note.md` (raw-τ-dominated-by-cheap/expensive-legs)
- `code/pipeline/03a_build_cost_raster.R` (the four cases to rebuild)
- `config.R` section 7 (cost_land = 17.9 × cost_road[mfg] / hmi_argentina ≈ 733)

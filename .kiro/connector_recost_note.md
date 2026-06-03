# Connector re-cost experiment: result and interpretation

Cote's "connector re-cost" experiment, full Dijkstra version. Prompted by
the pre-check (`.kiro/connector_share_precheck_note.md`) which showed the
centroid→network land leg is ~half of τ and shrinks where roads densified.

Script: `code/analysis/diagnostic_ma_connector.R`
Outputs: `results/tables/diagnostic_ma_connector.txt`,
         `data/derived/06_analysis/connector_variant/`

## What was done

For each district centroid and each of the four IV-Both cases
(actual_1960, actual_1986, instrument_stu, instrument_lcp_mst), found the
least-cost path from the centroid to the nearest on-network cell on that
case's own cost raster, and overwrote those connector cells' cost from
cost_land×HMI (~146) down to cost_road[overall] = 1.777. Then rebuilt
transition → τ → MA → population IV, isolated in `connector_variant/`.
Baseline pipeline outputs untouched. Runtime ~25 min (4 × costDistance
~270 s plus 312×4 connector LCP extractions).

Connector land-cells re-cost: 1960 = 2,819; 1986 = 1,540; stu = 4,094;
lcp_mst = 3,355. (1986 < 1960 is the expected sign: denser 1986 network →
shorter on-ramps.)

## Result

MA gain (share of districts with ΔlogMA > 0): **91.0% → 76.9%**.
Mean ΔlogMA 1.559 → 0.506. So re-costing compresses the near-universal
gain, as expected — the inflated winners deflate.

Population elasticity, sector 0, θ_low, four-column grid. F is the
project-canonical first-stage statistic (`fitstat_F`, type `ivf`) — the
same one the baseline tables report (baseline reproduces exactly):

| spec | baseline β (F) | re-cost β (F) |
|------|----------------|---------------|
| OLS | +0.022 | +0.032 |
| IV-LP (Larkin) | +0.042 (F=19.32) | **−39.5 (F=0.00)** |
| IV-Hypo (road) | +0.059 (F=4.95) | **+0.064 (F=191.0)** |
| IV-Both | +0.046 (F=13.59) | **+0.063 (F=95.4)** |

First-stage instrument coefs under re-cost (IV-Both first stage):
Larkin (stu) −0.0245 (t=−0.37), Hypo (lcp_mst) +0.566 (t=5.07). In the
just-identified IV-LP the Larkin coef is −0.0006 (t=−0.01), hence F≈0.

> Note on the statistic: an earlier version of this note reported F via
> `ivwald` (IV-Hypo 25.5, IV-Both 13.5). That was the WRONG statistic —
> the baseline tables use `ivf` through `fitstat_F()`. Corrected here and
> in the script; the qualitative story is unchanged but sharper.

## Interpretation — two findings, the second is the important one

1. **β moves up, Cote's predicted sign, but modestly.** IV-Both
   +0.046 → +0.063, toward Gibbons (~0.3) but nowhere near it. Capillarity
   inflation was attenuating β somewhat, consistent with the pre-check.
   This is NOT the order-of-magnitude fix; θ remains the dominant lever
   (see theta_benchmark_note: β hits 0.3 only at θ≈1).

2. **The identification FLIPS — this is the headline.** Re-costing the
   connector kills the Larkin first stage (F 19.32 → 0.00; coef
   insignificant) and turns the hypothetical-road instrument from WEAK
   (F 4.95, below Stock-Yogo 10) into very strong (F → 191). Mechanism:
   once cheap road connectors blanket the landscape, removing *studied
   rail* segments barely changes MA, so the Larkin discontinuity loses
   its bite. The +0.063 IV-Both is now driven almost entirely by the road
   instrument, not the rail one.

## Why this matters for the paper

The Larkin Plan discontinuity is the paper's flagship instrument (the one
with the clean historical narrative and the strong baseline first stage).
The connector re-cost is therefore NOT a free de-biasing: it trades the
rail instrument for the weaker road instrument. Any decision to adopt it
as a headline spec has to confront that trade. It also interacts with the
open "hypo instrument is weak / IV-LP-only headline" decision
(tasks.md PENDING DECISIONS #8) — the re-cost pushes in the OPPOSITE
direction (away from Larkin, toward the hypo road instrument).

This is a coauthor decision, not a unilateral spec change. Flag for the
meeting alongside θ.

## Caveats

- Damping test, not structural isolation: re-cost fixes the on-ramp unit
  cost, but connector LENGTH still varies by period, and it assumes
  road-quality first km even for districts with no road.
- Connector = euclidean-nearest-on-network seed routed by least-cost path
  (literal reading of Cote's proposal). A "snap centroid to network node"
  variant (Gibbons-style, addressing the HMI concern more directly) is a
  different experiment, not run here.
- θ-agnostic: operates on τ, not the exponent. Can run before θ framing.

## Cross-refs
- `.kiro/connector_share_precheck_note.md` (the pre-check that justified this)
- `.kiro/theta_benchmark_note.md` (θ is the dominant lever on the level)
- `code/analysis/diagnostic_ma_connector.R`
- tasks.md PENDING DECISIONS #8 (hypo-instrument weakness — interacts)

# Theta: what the benchmarks use, and what ours means

Research note prompted by Cote's review and the theta sweep (PR #67).
Question: our main-spec elasticity (0.046) is far below Gibbons et al.
2024 (~0.3). The sweep shows the elasticity is highly sensitive to theta
(0.37 at theta=1, 0.046 at theta=4.55). So: what theta is defensible,
what do the benchmark papers use, and is our theta even the same object?

## The theta sweep result (PR #67)

Main population IV-Both elasticity vs theta (controls = Table 9 spec):

| theta | IV-Both beta | first-stage F |
|-------|--------------|---------------|
| 1.00  | +0.372       | 10.1 |
| 2.00  | +0.152       | 12.3 |
| 3.00  | +0.084       | 13.7 |
| 4.55  | +0.046       | 13.6  (main) |
| 6.00  | +0.031       | 12.8 |
| 8.11  | +0.020       | 12.0  (alt) |
| 10.00 | +0.015       | 11.6 |
| 12.00 | +0.012       | 11.5 |

At theta ~ 1, our elasticity lands on the Gibbons 0.3 benchmark.

## What Gibbons et al. (2024) actually use

From the paper (Related Papers/beeching_axe_paper.pdf, Section 3.3, Eq. 3):

    cent_it = sum_j [ sum_k ( m_k * railtime_jk^(-0.5) ) * roadtime_ij^(-0.5) ]

- The decay exponent is **0.5**, applied to **travel time** (rail time
  between stations, road time from zone to local station).
- m_k are node weights (1 for unweighted centrality, or 1951 population
  for the "market access" variant).
- Their headline population-centrality elasticity ~0.3 is estimated with
  this decay = 0.5.
- KEY: "A doubling of road and rail speeds implies a doubling of
  accessibility" — this fixes the scale interpretation of their index.
- They call it interchangeably a "centrality index" and a "market access
  index" and cite Donaldson & Hornbeck (2016) as the market-access
  lineage, but their own exponent is 0.5, NOT a trade elasticity in the
  4-12 range.

So Gibbons' ~0.3 is produced with a MILD decay (0.5 on time). That is the
single biggest reason their elasticity is an order of magnitude above
ours: we raise transport COST to 4.55, they raise TIME to 0.5.

## What Donaldson & Hornbeck (2016) use

D&H define MA_i = sum_j Pop_j * tau_ij^(-theta), where tau_ij is an
iceberg trade-cost factor (>= 1, dimensionless) and theta is the TRADE
ELASTICITY from the underlying Eaton-Kortum/Armington trade model. They
take theta from the trade literature (the trade elasticity is the
structural object that maps trade costs to trade flows), and run
robustness across a range. The high exponent is coherent there BECAUSE
tau is a normalized iceberg multiplier, not a raw distance or time.

[VERIFY before citing a specific D&H theta value in the paper. The
literature trade-elasticity range is roughly 4-12 (Eaton-Kortum 2002;
Simonovska-Waugh 2014). Do not state a single D&H number from memory.]

## The conceptual problem for OUR measure

Our pipeline computes:

    MA_i = sum_j Pop_j / tau_ij^theta,  theta = 4.55

where tau_ij is the **accumulated generalized transport cost** along the
least-cost path (pesos per ton-km summed over the route, from the cost
raster). This is NEITHER of the two benchmark objects:

- It is not Gibbons' raw travel time with a 0.5 decay.
- It is not D&H's normalized iceberg multiplier with a trade-elasticity
  theta.

Our tau is a cost in currency units accumulated over distance. Raising it
to 4.55 is a very aggressive decay: a pair twice as expensive contributes
2^(-4.55) ~ 1/23 as much. That is far steeper than Gibbons' 2^(-0.5) ~
0.71. This steepness is most of why our elasticity is small AND why 91%
of districts gain (the measure is dominated by a few cheap near
neighbours; broad road cost reductions move it a lot).

The 4.55 / 8.11 values were inherited from the old pipeline without
documented justification (config.R flags this PENDING). The literature
citations currently in config.R (Eaton-Kortum, Simonovska-Waugh) are for
the TRADE elasticity, which is the right object ONLY if our tau is a
normalized iceberg trade cost. It is not, as currently built.

## Implication / options for the team

This is a framing-blocking decision (Cote's point: identification and
measurement before narrative). Options:

1. **Adopt the Gibbons centrality parameterization** (decay ~0.5 on a
   time or cost metric). Most defensible if we frame as a centrality /
   accessibility paper rather than a structural trade-MA paper. Gives
   elasticities comparable to Gibbons by construction.

2. **Make tau a proper normalized trade cost** (iceberg multiplier
   relative to own-district cost) and keep a trade-elasticity theta from
   the literature. Most defensible if we want the structural MA
   interpretation. Requires re-deriving tau as a ratio, not a raw cost.

3. **Report the elasticity as a function of theta** (the sweep itself),
   and argue the qualitative pattern (manufacturing responds, agriculture
   does not; rail dominates) is robust across theta even if the level is
   not. Weakest as a headline, useful as robustness.

Option 1 or 2 is a real decision about what the paper's MA object IS, not
a parameter tweak. Recommend resolving this with Cote before any further
results work — it determines the headline elasticity and the comparability
to the literature.

## Cross-refs
- PR #67 (theta sweep): results/tables/diagnostic_theta_sweep.{txt,csv}
- config.R section 8 (theta PENDING justification)
- Section 3.3.2 of the paper (theta literature review, flagged in tasks.md)
- Gibbons et al. 2024, JUE 143:103691 (Related Papers/beeching_axe_paper.pdf)
- Donaldson & Hornbeck 2016, QJE 131(2):799-858

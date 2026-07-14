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

## What Donaldson & Hornbeck (2016) use  [verified from primary source]

Source: Related Papers/Donaldson_Hornbeck_Railroads_paper.pdf (QJE 2016).

Their market access (their Eq. 12, first-order approximation):

    MA_o = sum_d  tau_od^(-theta) * N_d

where:
- tau_od is an ICEBERG trade cost: a proportional cost factor tau_od >= 1,
  "a proportional trade cost applied to each unit of the variety shipped"
  (their Section IV, iceberg formulation). CRUCIALLY (their footnote 32):
  "While we measure the absolute cost of trade between counties, we
  express this cost in PROPORTIONAL terms using Fogel's average value of
  transported agricultural goods." So tau is the absolute transport cost
  NORMALIZED by the value of the goods shipped -> a dimensionless
  multiplier, not a raw cost.
- theta is the TRADE ELASTICITY (their footnote 54): "trade costs affect
  trade flows with this elasticity." It is the structural Eaton-Kortum
  object, NOT a gravity decay on distance.

Their theta choice (verified, their Section IV.D + footnotes 54-57):
- Preferred value: theta = 8.22, estimated by NLS (grid search, 1,000
  points) to best fit their own data via the model structure. 95% CI
  [3.73, 26.83], highly right-skewed.
- Literature range they cite (footnote 55): Eaton-Kortum (2002) extremes
  3.60 and 12.86 (EK preferred 8.28); Caliendo-Parro avg 8.64 (8.11 for
  agriculture); Costinot-Donaldson-Komunjer 6.53; Donaldson (2015 Raj)
  avg 3.80; Simonovska-Waugh 4.10; Head-Mayer (2014) meta-survey mean
  6.74, median 5.03.
- They run robustness across this range and also report theta = 1.

Note (their final paragraph, p.833): their MA "recalls an older concept
of market potential (Harris 1954)... Harris used distance as a proxy for
trade costs... The remaining practical difference is that we allow trade
costs to affect the importance of distant market sizes with a power of
-theta rather than -1." So the EK trade elasticity (~8) is the power on a
NORMALIZED iceberg trade cost. The high exponent is coherent there
precisely because tau is a dimensionless multiplier near 1, not a raw
accumulated cost.

Our 8.11 "alt" value evidently traces to Caliendo-Parro's agricultural
trade elasticity (8.11), and 4.55 is near Simonovska-Waugh / D&H-Raj
territory — i.e. they ARE trade-elasticity numbers. The problem is not
the numbers; it is that they are the right exponent only for a NORMALIZED
iceberg tau, which ours is not (see below).

## The conceptual problem for OUR measure

Our pipeline computes:

    MA_i = sum_j Pop_j / tau_ij^theta,  theta = 4.55

where tau_ij is the **accumulated generalized transport cost** along the
least-cost path (pesos per ton-km summed over the route, from the cost
raster). This is NEITHER of the two benchmark objects:

- It is not Gibbons' raw travel time with a 0.5 decay.
- It is not D&H's NORMALIZED ICEBERG multiplier with a trade-elasticity
  theta.

The decisive issue, now that the D&H source is verified: D&H's theta ~8
is the power on a DIMENSIONLESS iceberg cost tau >= 1 (absolute transport
cost DIVIDED BY the value of the goods shipped, their footnote 32). Our
tau is the raw accumulated peso-cost, which is a large number (median
~4.4 million cost-units in the 1960 s0 matrix), not a multiplier near 1.

Raising a raw cost to 4.55 vs raising a normalized multiplier to 8.22 are
not comparable operations:
- D&H: a route with iceberg cost 1.5 (50% value lost in transit) vs 1.2
  contributes (1.5/1.2)^-8.22 ~ 0.16 as much. The exponent acts on a
  ratio near 1.
- Ours: because tau is a raw magnitude, only the RATIO of tau across
  pairs matters for relative MA (the overall scale cancels in logs), but
  the EXPONENT still controls how sharply nearer/cheaper pairs dominate.
  At 4.55 the measure is dominated by a handful of cheapest neighbours,
  which is why broad road-cost reductions move it so much and 91% gain.

VERIFIED EMPIRICALLY (two checks on the actual tau matrices):
- Constant rescale cancels: multiplying every tau by 1000 leaves
  Delta log MA identical (mean 1.5589, sd 2.4829, 91.0% gain, to 4 d.p.).
  So a Fogel-style DIVISION OF TAU BY A CONSTANT value-of-goods would do
  NOTHING to our results -- the scale drops out of the first difference.
  This is the critical caveat for option 1 below.
- Spread + convexity: raw tau (1960 s0) has p90/p10 ~ 6.7 and max/min
  ~ 6000. Raised to 4.55, a p10-vs-p90 pair contributes ~6.7^4.55 ~ 7000x
  more. MA is therefore dominated by the few cheapest pairs. D&H's tau,
  by contrast, lives in a narrow band near 1 where x^(-8) is locally
  gentle; the SAME exponent behaves completely differently on a variable
  that spans orders of magnitude (ours) vs one bounded near 1 (theirs).

So the real difference from D&H is NOT the scale of tau (that cancels);
it is that D&H's tau is a dimensionless ratio in a narrow band near 1,
while ours spans orders of magnitude. The exponent's curvature does
different things on the two. Our theta is mislabeled as a "trade
elasticity": the trade elasticity is the right exponent ONLY for a
normalized iceberg tau confined near 1. As built, our tau is closer to
Gibbons' raw cost/time object, for which the defensible decay is ~0.5-1,
not ~4.5-8.

## Implication / options for the team

This is a framing-blocking decision (Cote's point: identification and
measurement before narrative). Options, now sharpened by the D&H source:

1. **Make tau a proper normalized iceberg trade cost and keep a trade-
   elasticity theta (~4-8).** The D&H-consistent fix. CAUTION (verified
   above): dividing tau by a CONSTANT does nothing -- it cancels in the
   first difference. To actually change behaviour, tau must become a
   dimensionless ratio confined near 1, i.e. the normalization must vary
   across pairs. [SUPERSEDED in part -- see decision_a_option1_scoping.md:
   the actual D&H form is AFFINE, tau = 1 + cost/V, and the "+1" breaks
   the multiplicative cancellation, so a single scalar V DOES change
   Delta log MA. The cancellation result above stands; the "must vary
   across pairs" inference drawn from it was too strong.] Candidate
   constructions:
     - per-unit-value iceberg: (accumulated cost / value of goods), where
       value is high enough that tau sits in, say, [1, 2] rather than
       spanning orders of magnitude; this compresses the spread so the
       high exponent behaves like D&H's.
     - cost relative to a free-trade / minimum-cost benchmark per pair.
   This is real modeling work, not a rescale. Requires deciding the
   value-of-goods normalization (which D&H take from Fogel) and checking
   it varies meaningfully over 1960->1986. Does NOT require re-running
   Dijkstra (transform of existing tau matrices), but DOES require getting
   the normalization conceptually right. Most defensible for a structural
   market-access framing comparable to D&H.

2. **Adopt the Gibbons centrality parameterization** (decay ~0.5-1 on the
   raw cost or a travel-time metric, no normalization). Most defensible if
   we frame as a centrality / accessibility paper. Gives elasticities
   comparable to Gibbons (~0.3) by construction. CHEAPEST: the sweep
   already shows theta~0.5-1 on our existing raw tau lands near 0.3, so
   this is essentially just adopting a low theta and reframing what the
   index represents. The honest description would then be "network
   centrality / accessibility," not "structural market access."

3. **Report the elasticity as a function of theta** (the sweep itself)
   and defend the qualitative pattern (manufacturing responds, agriculture
   does not, rail dominates) as robust across theta even if the level is
   not. Useful as robustness regardless of (1)/(2).

KEY INSIGHT for the meeting: options 1 and 2 are not just different theta
values -- they are different claims about what MA IS. Under D&H, theta~8
is correct but ONLY on a normalized iceberg tau (which we'd have to
build). Under Gibbons, theta~0.5 is correct on a raw cost/time (which is
closer to what we already have). Our current spec mixes the two: a D&H-
sized exponent on a Gibbons-style raw cost. That mismatch is the most
likely reason the elasticity is an order of magnitude off the benchmark.

## Cross-refs
- PR #67 (theta sweep): results/tables/diagnostic_theta_sweep.{txt,csv}
- config.R section 8 (theta PENDING justification)
- Section 3.3.2 of the paper (theta literature review, flagged in tasks.md)
- Gibbons et al. 2024, JUE 143:103691 (Related Papers/beeching_axe_paper.pdf), Eq. 3, decay 0.5
- Donaldson & Hornbeck 2016, QJE 131(2):799-858 (Related Papers/Donaldson_Hornbeck_Railroads_paper.pdf), Eq. 12, theta=8.22, footnotes 32/54/55
- Donaldson 2018 "Railroads of the Raj", AER (Related Papers/Donaldson_RRRaj_AER.pdf) — estimates trade costs from price gaps (different method)
- Donaldson HRUE (Related Papers/Donaldson_HRUE.pdf)

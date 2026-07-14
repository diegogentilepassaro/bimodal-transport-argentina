# Scoping note: what Option 1 (normalized-iceberg tau) would actually take

Companion to `theta_benchmark_note.md`, which established the conceptual
problem (our theta = 4.55 is a trade-elasticity-sized exponent applied to a
raw accumulated cost, valid only on a dimensionless iceberg multiplier near
1) and left Option 1 scoped only as "real modeling work." This note prices
that work so the Decision A meeting can compare options with known costs.
It does not advocate; it scopes.

## The one fact that changes the price

`theta_benchmark_note.md` verified that a MULTIPLICATIVE constant rescale of
tau cancels exactly in Delta log MA (tau -> 1000*tau leaves every result
identical to 4 d.p.). From this it concluded "dividing tau by a CONSTANT
value-of-goods does nothing" and therefore that the normalization "must vary
across pairs."

That conclusion is right for a pure rescale but does NOT apply to the actual
D&H iceberg form, which is AFFINE, not multiplicative:

    tau_iceberg_ij = 1 + cost_ij / V        (V = value per ton of the good)

The "+1" breaks the cancellation: log(1 + c/V) is nonlinear in c, so a
single scalar V changes Delta log MA, changes the spread of tau (compresses
it toward 1 as V grows), and changes how the exponent's curvature bites.
No pair-varying data is required for a first implementation. This makes
Option 1 substantially cheaper than the theta note implies: the modeling
work is choosing and defending V, not building a pair-level normalization.

Limiting behavior (why this nests what we already know):
- V -> 0: tau_iceberg ~ cost/V, a pure rescale of raw cost; theta bites on
  the full raw spread. Approaches current behavior (beta ~ 0.046 at 4.55).
- V -> Inf: tau_iceberg -> 1 for every pair; MA -> sum of Pop (no distance
  decay); beta -> the no-gradient limit.
- In between, a V-sweep traces a curve of beta against V at FIXED
  theta = 8.22 (D&H's preferred value). Somewhere on that curve tau spans
  a D&H-like band (their tau lives roughly in [1, 2.5]); that segment is
  the economically disciplined region, and V is pinned by data, not by
  fitting beta.

**VERIFIED on the cached tau matrices (actual_1960/1986 s0)** before
writing this note (scratch check, 2026-07-14):

| transform | Delta log MA: gain share / mean / median | tau' p10-p90 (1960) |
|---|---|---|
| raw tau, theta 4.55 (baseline) | 91.0% / 1.559 / 0.639 | 1.6e6 - 1.1e7 (raw units) |
| tau x 1000, theta 4.55 | identical to baseline (max abs diff 1.4e-14) | scaled |
| 1 + tau/1e6, theta 8.22 | 92.0% / 1.841 / 0.589 | 2.57 - 11.58 |
| 1 + tau/4.4e6, theta 8.22 | 94.9% / 0.999 / 0.299 | 1.36 - 3.40 |
| 1 + tau/2e7, theta 8.22 | 97.1% / 0.406 / 0.132 | 1.08 - 1.53 |

The multiplicative row reproduces the theta note's cancellation exactly;
the affine rows do not cancel and move the MA distribution monotonically
with V. At V near the median raw tau (4.4e6 raster units), tau' spans
[1.36, 3.40] p10-p90 — the D&H-like band. The mechanism works; what V
should BE is the data question in work item 2. (Regression betas were not
run in the scratch check — that is the diagnostic in work item 3.)

## Construction 1a: Fogel-style scalar V (the D&H-faithful version)

    tau_ij = 1 + (pesos-per-ton cost of route ij) / (pesos-per-ton value of goods)

**Work items:**

1. **Unit audit of the tau matrices (prerequisite, ~half a day).** Our
   cached tau values are in cost-raster units (B&P pesos per ton-km times
   cell traversal at ~1.24 km/cell, accumulated along the least-cost path;
   1960 s0 median ~4.4e6 units). Before any V is applied, the conversion
   from raster units to actual 1960 pesos per ton must be derived and
   sanity-checked against a hand-computed route (e.g., BA-Rosario ~300 km
   on road at 1.777 pesos/ton-km ~ 533 pesos/ton). Deliverable: a scalar
   `tau_units_to_pesos` with a derivation comment in config.R.

2. **Source V, the 1960 value per ton (the real bottleneck, external
   data).** D&H take theirs from Fogel's average value of transported
   agricultural goods. Argentine candidates, in rough order of preference:
   - Ferrocarriles del Estado / Ferrocarriles Argentinos annual statistics:
     freight revenue and ton-km by commodity class -> implied revenue per
     ton; anuarios exist for the late 1950s-1960s.
   - The Larkin Plan itself (the 3-volume *Transportes Argentinos*): its
     traffic studies tabulate tonnage and cargo composition; possibly value
     density by category. This piggybacks on the archive check already
     assigned for issue #68 (studied-share denominator) — same physical
     volumes, one library visit.
   - CEPAL / INDEC historical series: value and tonnage of domestic cargo.
   - Fallback: construct V from our own census data (agricultural + industrial
     production values) divided by output tonnage proxies — weakest, since
     tonnage is not directly in the censuses.
   Deliverable: one number (or a low/mid/high band) with a citable source.

3. **Transform + re-run (fast, ~a day).** No Dijkstra re-run: read cached
   tau parquets, apply the affine transform, recompute MA for the 4 IV-Both
   cases x theta = 8.22, re-run the Table 9 spec. Same pattern as
   diagnostic_ma_refpoint.R. Deliverable: `diagnostic_ma_iceberg.R`
   reporting beta as a function of V over the sourced band (plus the V -> 0
   and V -> Inf limits as anchors), with the tau distribution (p10/p50/p90)
   at each V so we can see when it enters the D&H-like [1, 2.5] band.

4. **Sensitivity/honesty check (half a day, part of the same script).**
   Because beta varies with V, the write-up must show the whole curve and
   justify V from the source, not from where beta lands. If the sourced V
   band happens to put tau in the D&H range AND beta near Gibbons, that is
   a finding; if it puts beta elsewhere, that is also a finding.

**Total: ~2-3 working days once V is sourced; V sourcing is the open-ended
part and can ride on the issue-#68 archive visit.**

## Construction 1b: pair-varying route-inefficiency ratio (no external data)

    tau_ij = cost_ij / (cheapest-mode unit cost x geodesic distance_ij)

Dimensionless, >= 1 by construction, pair-varying, computable today: the
denominator is a straight-line frictionless benchmark per pair (geodesic
distances between the 312 reference points are trivial to compute). This is
the "cost relative to a free-trade / minimum-cost benchmark" candidate from
the theta note.

- Pro: zero external data; genuinely pair-varying; interpretable as a route
  inefficiency multiplier; changes both cross-section and (because networks
  change routes) the 1960->1986 difference.
- Con: it is NOT the D&H object (their tau is cost relative to goods VALUE,
  not to a frictionless transport benchmark), so it buys internal coherence
  but not direct comparability to theta = 8.22's meaning. A referee can ask
  why the trade elasticity is the right exponent for an inefficiency ratio.
- Effort: ~1 day total (distance matrix + transform + re-run, same
  diagnostic pattern). No unit audit needed (units cancel in the ratio)
  though doing the audit anyway is cheap insurance.

## Construction 1c: sector-specific V (extension, not first pass)

Value per ton differs sharply between bulk grain and manufactures, which is
the same scale-economies logic as the B&P cost sectors already in config.R.
A sector-specific V would make the iceberg tau sector-specific and ties
directly into the pending "sector-specific MA" decision (tasks.md item 2).
Scope only after 1a works and the meeting settles how sector-specific MA
relates to the counterfactual exercise.

## What this means for the meeting

- Option 1 is NOT a monolithic "big modeling project." Construction 1b is
  ~1 day with no new data; construction 1a is ~2-3 days plus one number
  that the issue-#68 archive visit can likely supply.
- The decision the meeting actually has to make is therefore NOT "can we
  afford option 1" but "which object do we want to defend": D&H-comparable
  iceberg (1a), internally-coherent inefficiency ratio (1b), or Gibbons
  centrality (option 2, zero work). The sweep (option 3) remains free
  robustness under any of them.
- Recommended sequencing if the meeting picks 1a: unit audit first (it is
  a prerequisite for anything peso-denominated and also hardens the paper's
  cost-parameter section), V sourcing in parallel with the archive visit,
  then the diagnostic.

## Cross-refs

- `theta_benchmark_note.md` — the conceptual case; verified D&H footnote 32
  (Fogel normalization) and the multiplicative-cancellation result.
- `results/tables/diagnostic_theta_sweep.{txt,csv}` (PR #67) — beta vs theta
  on raw tau; the V-sweep in 1a is the complementary experiment at fixed
  theta.
- Issue #68 — the Biblioteca Nacional visit that V sourcing can piggyback on.
- config.R section 7 — B&P cost parameters and the unit conventions the
  audit must reconcile.

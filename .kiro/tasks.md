# Task List

Derived from paper.tex skeleton. Color key in paper.tex: RED = placeholder numbers/text, ORANGE = coding tasks, PURPLE = writing tasks, TEAL = analysis/interpretation tasks.

---

## CURRENT STATUS (updated after PR #80)

Block 1 is drafted and results are in. Block 2 has its first two substantive
results (Tables 13 and 14). Cote's review of Block 1 surfaced a deeper
question than the original framing decisions below: the headline population
elasticity (β=0.046) is an order of magnitude below the closest benchmark
(Gibbons et al. 2024, ≈0.3), and a round of diagnostics (PRs #65–71) traced
this to four entangled measurement/identification decisions — consolidated in
**`Plan/memo_identification_measurement_decisions.md`** (2026-06-11), which is
now the authoritative source for Block 1's open decisions. Read that memo's
Section 6 before touching any Pending Decision below that it supersedes.

Diagnostics since PR #62 (descriptive/exploratory, no pipeline re-run of the
committed results unless noted):
- MA-gains diagnostic (PR #65): 91% of districts gain MA 1960–86 despite the
  rail network contracting ~23%, driven by broad road-cost reduction (median
  O-D pair ~20% cheaper), not a fluvial or BA-corridor artifact.
- Bloque-1 mechanical-artifact tests (PR #66): alternate reference point
  (interior point vs. centroid) and no-fluvial variants both leave the 91%
  gain share and small elasticity essentially unchanged — centroid/HMI
  coupling and the fluvial channel are exonerated as the driver. A
  zero-vs-infinite-transshipment bracket shows transshipment reshapes the MA
  distribution but does not explain the small elasticity either.
- Connector-share pre-check and Larkin/MA_rail first stage (PRs #69, #70):
  the Larkin instrument is strong for the rail component of MA in every
  specification (F ≈ 20–240 depending on connector re-costing); its apparent
  weakness on total MA is a composition effect once road connectors are
  re-costed, not instrument invalidity.
- θ sweep, overall and sectoral (PRs #67, #71–72): the population elasticity
  *level* is highly sensitive to θ (0.37 at θ=1 to 0.012 at θ=12), but the
  *sectoral contrast* (manufacturing value/wages respond, agriculture does
  not) holds at every θ in {1,2,3,4.55,6,8.11,10,12} — partially de-risking
  Decision A below.
- §4.2/4.3 instrument-construction prose drafted (PR #73), including the
  open placeholder for the studied-share discrepancy (issue #68).
- Urban-center reference-point diagnostic (PR #75, memo Decision D): re-
  anchoring MA at each district's largest settlement (by area, IGN polygons;
  276/312 anchored) gives β = +0.005 (F = 11.0) vs +0.046 baseline; Larkin
  does NOT revive on total MA (IV-LP F = 5.2). Third reference-point variant,
  same conclusion: the anchor is not the lever that closes the Gibbons gap.
- Conley spatial-SE sensitivity (PR #76, C37 / memo Decision E): pre-trends
  placebo p-value *sharpens* under Conley (0.061 HC1 → 0.001 at 100 km, no
  cutoff softens it); headline unchanged (0.168 → 0.182). "Spatially
  correlated noise" is dead as an escape for the pre-trend — what remains for
  the meeting is how to own the limitation in the text.
- Decision A scoping note for the normalized-iceberg τ option (PR #77) and
  tau unit audit with exact conversion + corridor checks (PR #79, Decision
  A 1a groundwork).
- Copy-pasted diagnostic helpers consolidated into
  `code/analysis/_diagnostic_helpers.R` (PR #78); appendix figure scripts
  also source it.
- Meeting-prep email sent to Cote (2026-07-14,
  `Plan/email_cote_meeting_prep.md`): summarizes Decisions A-E status
  including the urban-anchor and Conley results. Does NOT cover Block 2
  Tables 13-14 — separate follow-up email pending (see below).

New raw data landed 2026-06-04 (untracked until this PR): the city/locality
universe at `data/raw/networks_hypo/city_universe/` (517-point IGN
settlements, 50/68-city curated sets, provincial capitals, 9 legacy zone
sets) — the source for building a per-district urban-center reference point
(Decision D below), not yet wired into the pipeline. See its `readme.md`.

Block 2 prose (§§6, 7) is held pending Cote's response on Block 1 framing.
The analysis side of Block 2 is otherwise underway (see below).

### Done (Block 1 core)

**Code infrastructure:**
- [x] Full R pipeline end-to-end (base → pipeline → analysis). See `code/main.R`.
- [x] Shared IV helpers: `code/analysis/_iv_helpers.R` (`fit_iv_quad`, `safe_coef`, `fitstat_F`). Issue #50 closed in PR #53.
- [x] `config.R` centralizes `main_hypo_instrument` and `geo_controls_main`.
- [x] `renv.lock` populated with actual versions (PR #60). `requirements.txt` declared empty — no Python.
- [x] Self-repairing `00_setup.R` (install.packages fallback from lockfile).

**Figures:**
- [x] Figure 1 (network changes) — `code/base/networks/plot_figure_1.R` — C10 done
- [x] Figure 2 (ΔlogMA choropleth) — `code/analysis/plot_figure_2.R` — C11 done
- [x] Figure 3 (rail vs road scatter) — `code/base/networks/plot_figure_3.R` — C12 done
- [x] Figure 4 (infra vs MA scatter) — `code/analysis/plot_figure_4.R` — C12b done
- [x] Figure A1 (B&P cost schedule) — `code/analysis/plot_figure_a1_cost_schedule.R` — C15 done (PR #80)
- [x] Figure A2 (hypothetical networks) — `code/analysis/plot_figure_a2_hypothetical_networks.R` — C16 done (PR #80)
- [x] Figure A3 (Larkin studied segments) — `code/base/networks/plot_figure_a3_larkin_studied.R` — C17 done (PR #80)

**Tables (all in `code/analysis/`):**
- [x] Table 1 (network changes) — C18 partial (Table 1 done)
- [x] Table 6 (pre-balance) — A7 partial (pre-balance done)
- [x] Table 7 (pre-trends placebo) — A7 partial (pre-trends done)
- [x] Table 8 (first stage) — C19 done
- [x] Table 9 (main population IV) — C20 done
- [x] Table 10 (sectoral activity IV) — C21 done (as industrial+agricultural census, not IPUMS employment; see PR #49 header note)
- [x] Table 11 (other outcomes IV: education, migration, employment rate) — C22 done
- [x] Table 12 (robustness: alt θ, alt hypo, subsample) — C24 done

**Writing:**
- [x] §1 Introduction drafted including main-findings paragraph (PR #57; W11 partial)
- [x] §2 Historical Context drafted — W1-W4 done
- [x] §3 Data drafted — W5-W7 done
- [x] §4 Empirical Strategy drafted with 4.4 pre-balance + 4.5 pre-trends prose (PR #52, #56) — W8-W10 done
- [x] §5 Main Results §§5.1-5.5 drafted — W11 done for population, sectoral, other, robustness
- [x] scalars.tex AutoFill pipeline (PR #59). Demonstrated on §1 headline; paper-wide substitution still pending.

**AEA housekeeping:**
- [x] 1/4 main.R orchestrates full pipeline (PR #58)
- [x] 2/4 scalars.tex AutoFill (PR #59)
- [x] 4/4 renv.lock + requirements.txt freshness (PR #60)

### Done (Block 2 first cut)

**Counterfactual rasters and MA**: `cf_only_rail` and `cf_only_road` cases
were already wired into the existing pipeline (Phase 2c of
`03a_build_cost_raster.R`). No additional code needed for C1.

**Local infrastructure variables Z_i** (PR #62, partial C4):
- [x] `lost_all_rails_86` (binary; 14 districts) — built in `build_estimation_sample.R`
- [x] `gained_first_road_86` (binary; 82 districts) — built in `build_estimation_sample.R`
- [x] `chg_tot_rails_86_60`, `chg_pav_and_grav_86_54`, `share_studied_larkin` — already in panel
- [ ] gained/lost national highway (needs raw road-class data)
- [ ] gained/lost railway station (needs additional raw data; lp_1979.shp is lines-only)
- [ ] lost railway depot (needs Damus or similar source)

**Tables (Block 2):**
- [x] Table 13 (counterfactual decomposition) — PR #61, C5 done. Headline: rail-only IV +0.032 (F=105), road-only IV +0.039 (F=13). Population effect runs primarily through rail.
- [x] Table 14 (local-infrastructure mechanisms) — PR #62, C6 done. Headline elasticity drops ~50% when Δrail-km and Δroad-km are added (0.046 → 0.021 in spec 4). About half the population effect runs through within-district infrastructure changes.

### Pending (order: blocked first, then easiest-value)

**Block 1 loose ends:**
- [ ] **Coauthor meeting on the identification memo** — resolve the four decisions in `Plan/memo_identification_measurement_decisions.md` §6 (θ/τ object, estimand, connector re-cost, reference point). This is now the blocking item for Block 1 — everything else in this section is downstream of it. Meeting-prep email sent 2026-07-14; waiting on Cote to schedule.
- [x] ~~Urban-center reference point diagnostic (Decision D)~~ — done in PR #75 (`diagnostic_ma_urbancenter.R`). Anchor is not the lever; θ still dominates. Cote's geocoded-census version remains the referee-proof answer but the expectation is now confirmation, not rescue.
- [ ] Sector-specific indgen shares (Table 10 rebuild) — deferred, see Pending Decision 9.
- [ ] Paper-wide scalar AutoFill substitution (replace remaining inline numbers in §§4.5, 5.1-5.5 with macros from `scalars.tex`). Deliberately deferred until after Cote locks framing.
- [ ] A1 (OLS vs IV bias direction) — 1-2 paragraph in §5.2. Deferred until §5.5 robustness is fully decided.
- [ ] A2 (sectoral patterns + scale economies) — 2 paragraphs. Deferred so the argument lives in one place, after §7 mechanisms is drafted.

**AEA housekeeping 3/4:**
- [x] ~~W18 References bibliography~~ — done in PR #89 (all entries verified; data citations added and cited in §3).
- [x] ~~W18 full AEA README~~ — drafted in PR (docs/aea-readme): dataset list, replicator instructions, table-program mapping, runtimes from logs. Rights certifications and ACA redistribution rights left as `[AUTHORS: confirm]` checkboxes; revisit mapping table when final exhibit numbering locks.

**Block 2 next steps:**
- [ ] Block 2 follow-up email summarizing Tables 13 and 14 to Cote (the original Block-1-complete email predates these results).
- [ ] §6 counterfactuals writeup (anchored by Table 13). Hold until Cote weighs in on framing for the rail-dominant story.
- [ ] §7 mechanisms writeup (anchored by Table 14). Hold until Cote weighs in.
- [ ] Two remaining findings paragraphs in §1 (counterfactuals, mechanisms) — close once §§6 and 7 prose are drafted.
- [ ] C3 Sector-specific MA regressions — pending coauthor discussion on framing.
- [ ] C7 Heterogeneity regressions — interact ΔlnMA with baseline characteristics (initial pop, ag share, distance to port, dist to BA).

**Polish / final (order after Block 2):**
- [ ] W13-16 §6.3 caveats, §7 interpretation, §8 Discussion, Conclusion.
- [x] ~~W17 Abstract~~ — drafted in PR #89 (148 words, scalars macros, theta hedge); flagged in red for Cote's sign-off.
- [x] ~~Appendix figures A1-A3~~ — done in PR #80 (see Figures above).
- [ ] C34 θ=8.11 appendix table (currently exists as Panel A of Table 12; may lift into its own table).
- [ ] C35 C36 Industrial + agricultural census appendix tables (currently folded into Table 10; may lift into appendices).
- [x] ~~C37 Spatial autocorrelation / Conley SE robustness~~ — done in PR #76 (`diagnostic_pretrends_conley.R`). Sensitivity does not soften the pre-trend; it sharpens it. Remaining question (how to own the limitation in prose) folded into memo Decision E for the coauthor meeting.

---

## DEFERRED LEDGER (as of 2026-07-15, post PR #90)

Blocked on Diego (decisions):
- [x] ~~LOG-AREA CONTROL~~ — DECIDED (Diego, 2026-07-16): log(area) is
      NOT a control, NOT reported as sensitivity, and NOT discussed in
      the paper. Rationale: not a good control — conditional on
      baseline log population (already a control), adding log area is
      algebraically equivalent to controlling baseline population
      density (over-control of the initial condition the instruments
      exploit), and area is mechanically entangled with the MA
      construction (centroid-to-centroid tau; first-stage F drops
      16.2 -> 12.3 when added). The diagnostic script, main.R step
      D.13e, areaCtl macros, and results files were removed. Table 6
      keeps the log-area balance row and §4.5 keeps the factual
      statement that it is not among the controls — the paper reports
      the balance fact and does not chase it. For the record, the
      removed diagnostic showed: pop IV-B +0.052 -> -0.013 (p=.73,
      CI contains baseline); mfg valprod 0.317 -> 0.203 (p=.13); wage
      mass 0.378 -> 0.264 (p=.05); placebo point unchanged.
- [x] ~~Issue #22 (CF + TdF)~~ — done in PR #93 (option (a), merged).

Blocked on Cote:
- [ ] Log-area awareness (decision made: excluded entirely, see above):
      Cote should know the balance-table correlation exists and that a
      referee may ask; the agreed answer is the density/over-control +
      mechanical-entanglement rationale, not a sensitivity table.
- [ ] Abstract wording sign-off (red flag in PDF). Post-#93 framing pass
      (PR pending) recharacterizes population as "small, marginally
      significant" — needs explicit sign-off alongside the abstract.
- [ ] [optional, decided — no action required] Vicente López 1960
      digitization discrepancy (Part 2: 241,656 vs Part 3: 247,656;
      one digit). Decision (Diego, 2026-07-16): document and leave as
      is — pipeline uses Part 3, both values pinned in
      clean_census_1960.R. Check the published volume only if
      convenient.
- [ ] Issue #68 studied-share basis: two testable reconciliation hypotheses posted on the issue (exact-match 39.60% arithmetic; ~43,856 km denominator); needs the physical Larkin volumes. Then align §2/§4 + document in clean_railroads.R.
- [ ] Issue #91: 1954 industrial census issuing agency (title page of scanned volumes) → one-line references.bib fix.
- [ ] Baumgartner & Palazzo author initials (JSTOR: Jean-Pierre / Pascual Santiago) vs repo docs.
- [ ] Migration sign interpretation, theta justification, title (long-standing flags).

Pre-deposit (see README's author checklist):
- [ ] Rights certifications + ACA digitized-geometry redistribution rights.
- [ ] If deposit slips past 2026: move IGN access-year fields + README dates together.
- [ ] \doi macro not verbatim-safe for DOIs containing % or # (caveat documented in paper.tex preamble).
- [ ] Clean-machine rerun: delete results/ + data/derived/, R CMD BATCH code/main.R, verify exhibits match manuscript.
- [ ] Lock final exhibit numbering; update README mapping table.

Block 2 (gated on Cote's framing decisions — see Block 2 next steps above).

---

## PENDING DECISIONS

**Superseded for Block 1 framing:** items 3, 4, 6, 7, 8 below (θ justification,
tau/transshipment, pre-trends, migration sign, hypo-instrument weakness) are
now consolidated with supporting evidence in
`Plan/memo_identification_measurement_decisions.md` Section 6, as four
decisions for the upcoming coauthor meeting:
1. θ / τ object — normalized-iceberg cost vs. Gibbons-style centrality
   (sets the elasticity level; sectoral contrast is robust to θ per PR #71).
2. Estimand — report rail-MA effect (Larkin-clean, strong instrument) or
   total-MA effect (weak once road connectors are re-costed).
3. Connector re-cost — adopt/report/drop, conditional on 1–2.
4. Reference point — wait for Cote's geocoded census, or use the
   largest-settlement-by-area proxy now that raw data has landed (see
   Decision D in the memo; diagnostic in progress, see below).
Read the memo before making any of these decisions from the items below —
they're kept here for traceability but the memo has the current numbers.

Remaining items not covered by the memo (most flagged in
`Plan/email_cote_block1_complete.md`; items 10-11 are new from Block 2):

1. **Title**: Working title is "Transport Restructuring and Regional Development." Alternatives: "From Rail to Road," "Reshaping the Economic Map." Decide later.
2. **Sector-specific MA**: We want to do something sector-specific but need to agree on what exactly and how it connects with the paper. The counterfactual exercise (freeze one mode) and sector-specific MA (different cost calibrations) are conceptually distinct — their interaction is unresolved. Defer until Block 1 framing is locked.
3. ~~**Elasticity justification**~~ — superseded by memo Decision A (θ / τ object).
4. ~~**Tau calculation**~~ — superseded by memo Decision B/A (connector re-cost, transshipment already screened in PR #66).
5. **Sector interpretation**: Confirmed sectors 0/1/2 = overall/agriculture/manufacturing in config.R. Block 1 uses sector 0 + θ_low (4.55) throughout.
6. ~~**Pre-trends not clean null**~~ — superseded by memo Decision E (numbers updated: OLS 0.035**, IV-Both 0.078* on the 235-district placebo subset).
7. ~~**Migration sign wrong-way**~~ — superseded by memo Decision E (carried unchanged; no new evidence).
8. ~~**Hypo instrument is weak**~~ — superseded by memo Decision C (estimand) — the rail-vs-total-MA question replaces the two-instrument-vs-LP-only framing.
9. **Tabla 10 sectoral outcomes**: currently uses industrial + agricultural census activity outcomes (not IPUMS employment). Option to rebuild IPUMS `indgen` shares if coauthor prefers the original framing.
10. **Counterfactual framing (Table 13)**: rail-only point estimate (+0.032, F=105) is similar magnitude to full-MA headline (+0.046). Decide §6 framing: "rail dominates" vs. "rail and road both contribute, with rail more precisely identified." Currently neither is in prose.
11. **Mechanism framing (Table 14)**: headline elasticity drops ~50% when Δrail-km + Δroad-km are added as controls. Decide §7 framing: "half the effect is local infrastructure" vs. "MA captures regional connectivity beyond local km." Currently neither is in prose.
12. **Z_i completeness**: stations and depots Z_i variables not built (lp_1979.shp is lines-only; would need additional raw data, possibly from the Damus source flagged in tasks.md Q1). Decide whether to source the additional data or live with the partial Z_i set.
13. **Studied-share discrepancy (39.6% vs 48.8%)**: tracked as GitHub issue #68 — do not fix until Larkin's own denominator is confirmed (requires archive access) and a reporting basis is chosen. The §4 placeholder explicitly says not to touch it until then.

---

## CODE TASKS

### Pipeline / Data Construction

#### C1. Create 2 new counterfactual cost rasters (Block 2)

Add 2 new network configurations to the pipeline (extension of
`code/pipeline/02_hypothetical_networks.R`): (a) 1986 rails + 1954 roads
(only rail changes), (b) 1960 rails + 1986 roads (only road changes).
These produce the counterfactual MA measures ΔMA^only_rail and ΔMA^only_road.
All in R via `terra` + `sf` — no QGIS/Python required. See
`Plan/tau_calculation_review.md` "Decision 3."

Blocks: C5, counterfactual regressions.

---

#### C2. Restructure regressions from km-based to MA-based specification

Rewrite regressions so the regressor is Δln(MA^full) instead of Δ(km of rail) and Δ(km of roads), and instruments are Δln(MA^LP) and Δln(MA^hypo). The MA approach (Donaldson & Hornbeck 2016, Gibbons et al. 2024) integrates both modes into a single index.

Check which MA variables already exist in `MA_centroids.dta`. Create new regression files rather than modifying old ones.

Blocks: all main results tables.

---

#### C3. Sector-specific MA regressions (Block 2, pending discussion)

Run the main specification using agricultural MA and manufacturing MA separately. Only pursue after: (a) Block 1 establishes that overall MA affects outcomes, (b) sectoral employment results show differential effects, (c) coauthors agree on approach.

---

#### C4. Construct local infrastructure variables Z_i (Block 2)

District-level variables: Δ(km of roads), Δ(km of rail), gained/lost national highway, gained/lost railway station, lost railway depot. Will build from scratch in new repo.

---

#### C5. Run counterfactual regressions (Block 2)

Three separate IV regressions per outcome using ΔMA^full, ΔMA^only_rail, ΔMA^only_road. Do NOT put them in the same regression (multicollinearity). Compare magnitudes across specifications.

Instrument question: LP instrument is natural for ΔMA^only_rail, hypothetical road for ΔMA^only_road, both for ΔMA^full. Exclusion restriction for counterfactuals needs discussion in paper.

Depends on: C1, C2.

---

#### C6. Mechanism regressions with Z_i controls (Block 2)

Add Z_i progressively to the main specification. Report how β on ΔlnMA changes and the signs/magnitudes of θ on Z_i.

Depends on: C2, C4.

---

#### C7. Heterogeneity regressions (Block 2)

Interact ΔlnMA with baseline characteristics: initial population, agricultural share, distance to port, distance to Buenos Aires. Instrument the interaction by interacting the instruments.

Depends on: C2.

---

#### C8. Verify raster resolution

Check pixel size of cost rasters for documentation in Section 3. Grid is 2399 × 3090 in ESRI:54034.

---

#### C9. Verify tau units

Check one tau value (e.g., Buenos Aires–Córdoba) against known distance to confirm units after the /1000 division.

---

### Figures

#### C10. Network change maps (Figure 1)
Two-panel map: (A) railways 1960 vs 1986, closed segments highlighted; (B) roads 1954 vs 1986, new segments highlighted. From georeferenced shapefiles. **Done in R via `sf` + `ggplot2`: `code/base/networks/plot_figure_1.R`.**

#### C11. ΔMA^full choropleth (Figure 2)
Choropleth of districts by change in log market access. Diverging color scale.

#### C12. Rail-vs-road correlation scatter (Figure 3)
Δ(rail km) vs Δ(road km) by district. Shows whether the two shocks are spatially independent.

#### C12b. Infrastructure-vs-MA scatter (Figure 4)
Two panels: (A) ΔlnMA vs Δ(road km), (B) ΔlnMA vs Δ(rail km). Shows what drives MA variation.

#### C13. Counterfactual MA maps (Block 2)
Three choropleth maps: ΔMA^full, ΔMA^only_rail, ΔMA^only_road. Depends on C1.

#### C14. Local infrastructure scatter (Block 2)
Δln(pop) vs ΔlnMA, colored by station loss. Depends on C4.

#### C15. Transport cost schedule (Figure A1)
Road and rail costs vs cargo density from Baumgartner & Palazzo (1969). Shows crossover interval (500-1,000 t/day; the three tabulated points do not identify a crossover point). **Done: `code/analysis/plot_figure_a1_cost_schedule.R` (PR #80). Densities read from `cost_density` in config.R.**

#### C16. Hypothetical network maps (Figure A2)
Four panels: Euclidean bilateral, LCP bilateral, Euclidean MST, LCP MST. Overlaid on actual 1986 roads. **Done: `code/analysis/plot_figure_a2_hypothetical_networks.R` (PR #80). 1986 selector read from `roads_type2_1986` in config.R; CRS asserted per layer.**

#### C17. Larkin Plan studied segments map (Figure A3)
Studied vs non-studied rail segments. **Done: `code/base/networks/plot_figure_a3_larkin_studied.R` (PR #80). Legend reports km, not share, pending issue #68.**

---

### Tables

#### C18. Summary statistics (Tables 1–4)
Table 1: Network changes by period and region. Table 2: Outcome variables N/Mean/SD/Min/Max. Table 3: MA changes summary. Table 4: Balance by ΔlnMA quartile.

#### C19. First stage (Table 8)
ΔlnMA^full on instruments + controls. Columns: LP only, Hypo only, Both. Report F-stat, Hansen J.

#### C20–C24. Main IV regressions (Tables 9–13)
Column structure: (1) OLS, (2) IV-LP, (3) IV-Hypo, (4) IV-Both.
- Table 9: Population (total, urban, rural, urban share)
- Table 10: Sectoral employment (agriculture, manufacturing, services)
- Table 11: Other outcomes (education, migration, employment status)
- Table 12: Robustness (alternative θ, controls, samples, periods)

#### C25–C28. Counterfactual tables (Block 2)
Depends on C1, C2.

#### C29–C32. Mechanism and heterogeneity tables (Block 2)
Depends on C4, C7.

#### C34. Alternative θ (Appendix)
Main results with θ = 4.55 instead of 8.11.

#### C35. Industrial census outcomes (Appendix)
Production value, wages, employment, firms from 1954 and 1985 censuses.

#### C36. Agricultural census outcomes (Appendix)
Total area, number of farms from 1960 and 1988 censuses.

#### C37. Spatial autocorrelation (Appendix)
Moran's I on residuals or Conley standard errors with various distance cutoffs.

---

## ANALYSIS TASKS

#### A1. OLS vs IV comparison
Interpret direction of bias. If IV > OLS → negative selection (rail closed in declining areas). Write 1–2 paragraphs in Section 5.2.

#### A2. Sectoral patterns and scale economies
Connect empirical patterns to the cost data from Baumgartner & Palazzo. Write 2 paragraphs in Section 5.3.

#### A3. Counterfactual interpretation (Block 2)
Careful language: "suggestive evidence," "characterize relative importance," NOT "the effect of rail closures is X." Write 2–3 paragraphs in Section 6.3.

#### A4. Mechanism decomposition (Block 2)
How much does β drop when adding Z_i? What do θ coefficients on Z_i tell us? Write 2 paragraphs in Section 7.1.

#### A5. Heterogeneity patterns (Block 2)
Connect to theory: MA matters more in agricultural, less-developed, port-distant districts. Write 1–2 paragraphs in Section 7.2.

#### A6. Literature comparison
Benchmark per the verified memo `Plan/2026-05-31_bimodal-transport-market-access.md` (NOT the older seeds that used to live here — they caused a drafting error caught in PR #86): Gibbons, Heblich & Pinchbeck (2024, JUE 143, Beeching Axe) ≈ 0.3 is the ONLY like-for-like population benchmark; Donaldson & Hornbeck (2016) is method provenance only (land values ≈ 1.1, no population elasticity); Ahlfeldt & Feddersen (2018) headline is county GDP (output-side — benchmark the manufacturing numbers there, never the population one). Drafted in §8.2 (PR #86).

#### A7. Spatial placebo tests
Pre-period placebo (1947–1960 population change as outcome, instruments should not predict). Permutation test as additional robustness.

---

## WRITING TASKS

#### W1. Section 2.1: Railway history and fiscal crisis
Already written in paper.tex. May need trimming for some journals.

#### W2. Section 2.2: The Larkin Plan
Already written in paper.tex.

#### W3. Section 2.3: Implementation timeline
Already written in paper.tex.

#### W4. Section 2.4: Scale economies
Already written in paper.tex. Reframed as hypothesis, not established fact.

#### W5. Section 3.1: Transport networks
Partially written. Needs completion with specific details about each data source.

#### W6. Section 3.2: Census data
Describe all outcome variables, geographic unit (312 districts), time periods, sources.

#### W7. Section 3.3: MA construction
Partially written. Needs: fuller cost surface explanation, limitations paragraph (no transshipment, single path), elasticity justification (pending).

#### W8. Section 4.2: Endogeneity concerns
1–2 paragraphs on why OLS is biased and in which direction.

#### W9. Section 4.3.1: LP instrument formalization
How the LP discontinuity maps to ΔlnMA. Exclusion restriction.

#### W10. Section 4.3.2: Hypothetical road instrument formalization
How hypothetical networks map to ΔlnMA. Exclusion restriction.

#### W11. Section 5: Results interpretation
2–3 paragraphs per subsection. Can only be done after tables exist.

#### W13. Section 6.3: Counterfactual caveats (Block 2)
Critical to get language right. See A3.

#### W14. Section 7: Mechanisms interpretation (Block 2)
See A4 and A5.

#### W15. Section 8: Discussion
Four subsections: fiscal consolidation as regional policy, MA elasticities in comparative perspective, sector-mode specialization, comparison to Gibbons et al.

#### W16. Conclusion
3–4 paragraphs. Write after everything else.

#### W17. Abstract
Write last. Under 150 words.

#### W18. References
Set up BibTeX file. Collect all citations.

---

## QUESTIONS TO RESOLVE

1. **Damus as data source**: The code references `Train/base/damus/` and `Train/derived/damus_stations/`. What does it provide? Station locations? Freight data? Need to cite properly.
2. **Ferrocarriles del Estado (1961)**: Original source or reported through Larkin/Damus?
3. **Tau units**: What is the unit of tau after /1000? See C9.

---

## PRIORITY ORDER

### Block 1 core (do first):
1. C2 — restructure regressions to MA-based
2. C19–C22 — first stage + main results tables
3. W5–W7 — data section writing
4. W9–W10 — instrument formalization
5. C18 — summary statistics
6. C12b — infrastructure-vs-MA scatter

### Block 1 validation + robustness:
7. C25 — robustness (alternative θ, controls, samples)
8. A7 — spatial placebo tests
9. W8, W11 — endogeneity discussion + results interpretation

### Block 2 (sequencing matters):
10. Check: do sectoral employment results show differential effects? If not, reconsider Block 2 scope.
11. C1 — counterfactual cost rasters (R, extension of 02_hypothetical_networks.R)
12. C5, C25–C28 — counterfactual regressions
13. C3 — sector-specific MA (pending coauthor discussion)
14. C4, C6, C29–C31 — local infrastructure mechanisms
15. C7, C32 — heterogeneity

### Polish:
16. W1–W4 — historical context (good as is, may trim)
17. Revisit intro contributions (organize around 4 themes)
18. W13–W16 — discussion + conclusion
19. C10–C17 — all figures
20. W17 — abstract
21. W18 — references

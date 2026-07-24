# Task List

Derived from paper.tex skeleton. Color key in paper.tex: RED = placeholder numbers/text, ORANGE = coding tasks, PURPLE = writing tasks, TEAL = analysis/interpretation tasks.

---

## OPEN ITEMS (single source of truth, grouped by state)

Every open task or flag lives here, exactly once. The sections below
this one (CURRENT STATUS, Done, DEFERRED LEDGER, PENDING DECISIONS,
the original C/A/W task specs, QUESTIONS TO RESOLVE, and PRIORITY
ORDER) are historical record: completed items, dated decisions, and
superseded plans. When an item here closes, mark it [x] with the
PR/date; if it records a decision, add the record to the DEFERRED
LEDGER.

### 0. In flight right now (2026-07-24)

- [x] Corridor-timing design instrument — DONE, PR #117 squash-merged
      2026-07-24 (1e66852) with published review + fix pass. Verdict:
      real dose (recentered F 3-4.5, order of magnitude over the
      settlement design) but loaded dice (early corridors traverse
      districts with faster 1947-60 placebo growth, b=+0.074 p=0.039;
      unadjusted placebo IV p=0.025). Four-design map complete and
      posted on issue #114 (left open for the Cote meeting): Larkin
      collapses / hypo backbone / settlement clean-but-small /
      corridor dosed-but-loaded. Known limitation recorded: snap-
      tolerance sensitivity unexplored (needs recompute).
- [x] PR #115 (settlement road-timing design, balanced but weak dose:
      recentered F ~ 1) — squash-merged 2026-07-24 (eede3e7) after
      published review + fixes. Issue #114 stays open for the
      corridor verdict.
- [x] Branch-sync step — done 2026-07-24 (clean merge, no conflicts)
      before PR #117; branch deleted after squash-merge, local main
      synced to 1e66852.
- [ ] Fused-instrument (BH-2026 Stage 3) S=100 run — LAUNCHED
      2026-07-24 ~14:18 (6 workers, log /tmp/fused_full.log, ETA
      ~16:30-17:00); branch `analysis/fused-instrument` synced with
      main (03a default-exclusion conflict resolved as union).
      Comparison script diagnostic_fused_results.R written + committed
      (paired fused-vs-stu: backbone, recentered F, estimates, RI p).
      On completion: results -> commit -> PR -> review -> handoff.
- [ ] Growth-stratified corridor repair (approved + queued
      2026-07-24): machinery on `analysis/roadseg-growth` (pushed;
      variant arg on the three roadseg scripts, rg### tags, seed
      offset +300000). Density gate PASSED: 23 cells all >= 4/4,
      zero growth-level mixing. Prep outputs staged in
      roadseg_growth/. QUEUED: smoke S=1 then full S=100 after the
      fused run frees the machine (both runs pre-authorized). Reading:
      recentered F holds => first balanced AND dosed design-based
      road instrument; F collapses => timing was demand, door closed
      with a number.
- [ ] Geocoding 1960 intake — instructions email sent (2026-07-24,
      `Plan/email_cote_geocoding_instrucciones.md`): Cote pushes
      branch `data/geocoding-1960` (data to
      data/raw/census/geocoding_1960/, scripts to
      code/base/census_1960/geocoding/, .gitignore exception, pages/
      via Drive + sha256). When his branch lands: we open the PR,
      review, merge. Downstream (bigger, separate): integration into
      MA (load_centroids replacement or multi-point), gated on the
      θ/τ conversation.
- [ ] Recentering brief for the Wednesday 2026-07-29 meeting —
      DRAFTED 2026-07-24 (`Plan/brief_cote_recentering_2026-07-29.md`,
      Spanish, 8 sections incl. the six-design map, the
      placebo-does-not-clean finding, five recommendations, five
      decision items). TWO [PENDIENTE] slots remain: fused results
      (run in flight) and growth-repair results (queued). Fill when
      the runs land, then prose pass (streamline + humanizer) before
      sending.

### 1. Blocked on Cote

- [ ] **Coauthor meeting Wednesday 2026-07-29** — agenda now set by
      Cote's 2026-07-24 email: (a) recentering (he asked for our
      status; brief in flight above); (b) θ/τ object — he agrees with
      the memo framing, wants the two cheap experiments first
      (Gibbons-style decay ~0.5, τ normalization) before closing
      interpretation; (c) identification package below. Decision A
      remains the deepest open question. Density-schedule gradient
      (PR #99) still interacts with it.
- [x] Theta 4.55 provenance — CLOSED by Cote's email 2026-07-24: he
      does not recall the source. Adopt a cited value (Simonovska-
      Waugh ~4.1) or declare midpoint, Diego's choice. NOTE: adopting
      4.1 as the *computed* θ would force a full MA/tau recompute —
      the cheap version is re-justifying 4.55 as a midpoint with SW
      cited. Decision + implementation now in "Paper fixes" group
      below (item f).
- [x] Abstract wording sign-off — CLOSED for now (Cote 2026-07-24:
      "me sirve como está por ahora"); he rewrites it himself at
      publication time, together with title/narrative.
- [ ] Log-area awareness (decision made: excluded entirely, see
      DEFERRED LEDGER): Cote should know the balance-table correlation
      exists and that a referee may ask; the agreed answer is the
      density/over-control + mechanical-entanglement rationale, not a
      sensitivity table.
- [x] Larkin Plan canonical year — CLOSED by Cote 2026-07-24: report
      cover + elevation letter dated 1962 (Ministerio de Obras y
      Servicios Públicos); 1961 = Frondizi's *announcement*.
      Implementation in "Paper fixes" below (item a).
- [x] Issue #91: 1954 industrial census issuing agency — CLOSED by
      Cote 2026-07-24 against the physical title page: Dirección
      Nacional de Estadística y Censos (Secretaría de Estado de
      Hacienda), Buenos Aires 1960; "INDEC" in catalogs is
      anachronistic (INDEC created 1968). Source:
      bibliotecadigital.estadistica.ec.gba.gov.ar cn1958i post.
      Implementation in "Paper fixes" below (item b).
- [ ] Issue #68 studied-share basis — SUBSTANTIALLY RESOLVED
      2026-07-24, awaiting Cote's 10-minute confirmation. Audit
      findings (posted on the issue): CABA hypothesis tested — metro
      rail IS digitized (99 km CABA / 685 km belt) and mostly
      non-studied as he guessed, but magnitude cannot carry the gap
      (would need ~10,600 missing km). Real basis = numerator:
      recom_code semantics decoded (1 maintain 2,310 km / 2 close
      14,377 km / 3 new-study 5,197 km); excluding new-study gives
      38.4% on §2's 43,500 km. Reconciliation footnote IN THE PAPER
      (PR #119, squash-merged 2026-07-24, with review + fix pass;
      also renamed the missed 'Discontinuity' heading). Cote lookup =
      two numbers in Train/Docs: the report's studied definition
      (excludes new-study?) and its network denominator (§2's 15,000
      km ~ 32% implies ~46,900, which would shift the arithmetic to
      35.6%). generate_scalars wiring deferred until confirmed.
- [x] Issue #103 (35,000 vs 79,820 km) — CLOSED by Cote 2026-07-24:
      source difference. §2's ~35,000 = DNV national-network series
      (`Train/raw_data/kms_road_arg/kmVia_DNV`: 27,276 paved + 7,153
      gravel in 1986, excludes dirt); §3.1's 79,820 = digitized ACA
      network (broader). Implementation in "Paper fixes" below
      (item c).
- [x] B&P source volume — CLOSED by Cote 2026-07-24: El Trimestre
      Económico article ("Estructura económica del transporte de
      carga automotor y ferroviario en la Argentina"), NOT a CONADE
      report. URL provided (eltrimestreeconomico.com.mx article
      3317). Implementation in "Paper fixes" below (item d).
- [ ] Migration sign interpretation — Cote 2026-07-24 leans REMOVE
      from the paper ("la sacaría si hace ruido, que creo que es el
      caso"), at most a paragraph in Other Outcomes; also demote §5.4
      Other Outcomes to an annex (his note #42). Needs Diego's
      concurrence, then a writing task. Title: fine for now, closed
      jointly with narrative at publication time.
- [ ] [optional, decided — no action required] Vicente López 1960
      digitization discrepancy (Part 2: 241,656 vs Part 3: 247,656;
      one digit). Decision (Diego, 2026-07-16): document and leave as
      is — pipeline uses Part 3, both values pinned in
      clean_census_1960.R. Check the published volume only if
      convenient.

### 1b. Paper fixes settled by Cote's email (2026-07-24)

- [x] ALL TEN DONE — PR #116 squash-merged 2026-07-24 (closed issues
      #91 and #103 automatically). Items: (a) Larkin year 1962 +
      announcement footnote; (b) 1954 census agency (DNEC, Secretaría
      de Estado de Hacienda) in bib; (c) DNV-vs-ACA road-km footnote
      + new dnvseries data citation; (d) B&P El Trimestre URL, CONADE
      resolved; (e) "discontinuity" removed §1/§2.2, "Argentine
      restructuring" referent; (f) θ = 4.55 declared midpoint, SW 4.1
      cited, provenance placeholder retired (Decision A flag kept);
      (g) sectoral θ-robustness sentence in §5.5; (h) Table 15 rows
      low→high (CSV order-only); (i) Table 6 self-partialling note +
      explicit setdiff in code + §4.5.1 full control list; (j) §8.2
      Gibbons gap = granularity (\meanDistrictArea macro added to
      generate_scalars.R, censo1960pop cited). Review published on
      the PR; blocking finding (Table 6 doc/spec mismatch) fixed in
      the same PR; regenerated CSVs byte-identical.
- [ ] Follow-up from the PR #116 review, small: confirm the exact
      DNV publication volume with Cote before deposit (dnvseries bib
      entry is year = n.d. until then).

### 1c. New experiments from Cote's email (each needs a plan gate)

- [ ] Placebo spec with 1947 baselines on the RHS (his 1.1) — cheap,
      concrete; revisit placebo after each MA-definition change.
- [ ] Gibbons-style decay ~0.5 on the existing sweep machinery (his
      1.5 / memo point i).
- [ ] τ normalization experiment (his 1.5 / memo point ii; connects
      to the τ crude-vs-iceberg PLACEHOLDER in §3.3, note #21).
- [ ] Controls rationalization — CO-OWNED: Cote took it as homework
      (collinearity, region FE from census regions, threats-based
      selection, what the literature uses; notes #26-#31). Our side
      already has the outcome-blind grid (PR #112) to feed in.
- [ ] Manufacturing robustness exhibits in the paper (note #44:
      sweep exists, not shown) and sectoral counterfactual (notes
      #40/#45: §6 decomposition is population-only) — the second is
      real compute (only-rail/only-road for sectoral outcomes).
- [ ] Navigation/ports map (note #19, Cote: "necesario, no diferir"):
      figure showing the navigation layer + which ports connect.
- [ ] §2.4 conceptual paragraph: build-vs-close + radial-vs-capillary
      channels (notes #5/#15); defer empirics.
- [ ] Modern IV inference check (note #35): Montiel Olea-Pflueger
      effective F / Anderson-Rubin CIs alongside current F stats.
- [ ] Agri intensive-margin outcome + urbanization measurement
      doubts (notes #36/#39) — data-limited; Cote may fold into his
      digitization track.

### 2. Held until Cote's decisions (unblocked, deliberately parked)

Parked because memo Decision A (θ/τ) could reshuffle exhibits and
force a rerun anyway (Diego, 2026-07-20).

- [ ] Rights certifications + ACA digitized-geometry redistribution
      rights (README `[AUTHORS: confirm]` checkboxes from PR #92).
- [ ] Lock final exhibit numbering; update README mapping table. NOTE
      (PR #99): compiled numbers already diverge from filenames
      (in-text placement + multi-panel tables), and one paper exhibit
      now has a diagnostic filename (diagnostic_theta_sweep.tex). Key
      the README mapping on labels/captions, not filename numbers.
- [ ] Final pre-deposit clean-machine rerun: delete results/ +
      data/derived/, run `R CMD BATCH code/main.R`, verify
      byte-identical CSVs and a zero-diff pdftotext against the
      committed PDF. Now includes the unimodal step (D.13f, ~15 min
      extra vs the PR #97 run). Run after the draft stabilizes
      post-Cote.

### 3. Data-limited (need new raw sources; flagged to Cote)

- [ ] gained/lost national highway (needs raw road-class data)
- [ ] gained/lost railway station (needs additional raw data;
      lp_1979.shp is lines-only)
- [ ] lost railway depot (needs Damus or similar source)

### 4. Bookkeeping (small, no urgency)

- [ ] \doi macro not verbatim-safe for DOIs containing % or # (caveat
      documented in paper.tex preamble; matters only if such a DOI
      enters the bib).
- [ ] If deposit slips past 2026: move IGN access-year fields + README
      dates together.
- [ ] Decide gitignore treatment of logs/makelog.log and
      logs/session_info.txt (untracked and unignored since the PR #97
      rerun; flagged by PR #106's review).

### 5. Deferred by explicit decision

- [ ] C7 Heterogeneity regressions — diagnostic + §7.2 prose exist
      (sign patterns only; weak interaction first stages). Remaining:
      optional lift into a numbered table once structure is final.
- [ ] Sector-specific indgen shares (Table 10 rebuild) — deferred, see
      PENDING DECISIONS item 9.
- [ ] Demand-side sectoral MA (sectoral destination weights) — stated
      as future work in the Conclusion (PRs #101, #102); revisit only
      if the coauthors want it in this paper.
- [ ] BACK-POCKET (Diego, 2026-07-23): DH-style own-district MA
      robustness. Baseline MA excludes j = i (eq:ma; matches
      Donaldson-Hornbeck's baseline; avoids the reflection problem and
      the undefined tau_ii). The standard referee answer if asked is
      the DH robustness variant: include own-district access with an
      internal trade cost built from district area (the
      (2/3)-radius-type convention on area_km2). Cheap: touches only
      the MA step (04), no new Dijkstra. Not needed unless asked;
      Table 14's Z_i decomposition already speaks to the own-district
      margin more informatively.
- [ ] OPTIONAL: Borusyak-Hull recentering + GPHK contamination-bias +
      Fuchs-Wong positioning additions — staged plan prepared
      2026-07-20 in `Plan/borusyak_hull_recentering_plan.md`
      (workspace root, untracked), after reading the four papers Diego
      added to Related Papers/. Nice-to-have, not blocking; execute in
      whole or part on Diego's call. Stage 0 floor = prose/citations
      only (BH 2023 formula-instrument caveat in §4, GPHK caveat in §7,
      separate-regressions justification in §6, Fuchs-Wong NBER 35065
      multimodal-frontier positioning in §1/§8.2 + §6 vocabulary +
      §5.5 bracket sentence + §3 eta footnote, three bib entries).
      Fuchs-Wong is MINIMAL-ONLY by decision 2026-07-20: no middle
      option (their designs need traffic data that does not exist for
      1960s Argentina), and the ideal is a structural companion paper
      (future work), not an addition.
      STAGE 1 EXECUTED (PRs #111, #112; 2026-07-21/22) with the STOP
      outcome: 39% of the Larkin instrument is expected given
      geography; controls span 17% of mu; estimates collapse under
      recentering (imprecisely, recentered F ~ 10); placebo does NOT
      clean. Outcome-blind control exploration (protocol: seven
      predetermined sets fixed ex ante, ranked by recentered
      first-stage F only, all outcome cells reported, LOO-mu RI)
      returned a clean negative: best set is the existing
      geo_controls_main + mu. Stage 2 NOT recommended without the
      Cote conversation (Decision A + issue #68 strata; BH-2026
      efficient instrument is the remaining principled power lever).
      Optional stages: S=100 recentering diagnostic for the Larkin
      instrument (line-level permutation within region×branch strata;
      includes the pre-trends-cleaning check), promotion to recentered
      spec + randomization inference (gated on diagnostic + Cote), and
      the BH-2026 optimal-IV fusion instrument (Cote required), plus a
      referee-contingent Table 14 interacted-controls robustness
      variant (GPHK ideal). The
      stratification choice is flagged for Cote (interacts with
      Decision A and issue #68); known implementation facts verified:
      lp_1979 id_main is segment-level (line grouping must be built),
      recom_code populated for non-studied segments (meaning unclear —
      clarify before stratifying on it).

---

## CURRENT STATUS (updated after PR #104, 2026-07-17)

The paper is drafted END TO END: Blocks 1 and 2, 44 pages, zero
placeholders outside the intentional coauthor flags (abstract sign-off,
theta provenance, studied-share). Working mode since 2026-07-17 is
DECIDE-AND-DOCUMENT: we make the calls, record them in the DEFERRED
LEDGER below, and Cote inspects/reverses. Highlights of PRs #93–#104:
- Sample: CF+TdF resolution (#93, N=311); framing pass (#94).
- Infrastructure: paper-wide AutoFill (#95, 169 macros then; 218 now
  after #99–#104); theta-justification paragraph (#96; references +
  abstract were #89, just before this run); clean-machine rerun found
  and fixed
  six cold-start bugs incl. a memory crash (#97); exhibits embedded
  in-text, plain-English data section (#98).
- New results with prominence: density-schedule table — IV-B rises
  monotonically 0.026 → 0.052 → 0.087 rail-favouring → road-favouring,
  with an instrument-role reversal at low density (#99); transshipment
  bound in-paper, hands-off (#100); sector-matched MA — the sectoral
  contrast STRENGTHENS under matched schedules, mfg valprod 0.367,
  wage mass 0.444, F≈26 (#101).
- Prose completion: A1 (OLS-vs-IV, sign error caught by review and
  fixed), A2 (scale economies written once), §8 + Conclusion fixes
  (#102); Appendix Table A1 descriptives, C34 mooted (#104).
The identification memo (`Plan/memo_identification_measurement_decisions.md`)
remains the authoritative source for the deep open decisions (A–E),
headlined by Decision A (θ/τ object). The historical status notes below
are kept for the record.

### Historical status (as of PR #80; superseded above)

Block 1 is drafted and results are in. Block 2 has its first two substantive
results (Tables 13 and 14). Cote's review of Block 1 surfaced a deeper
question than the original framing decisions below: the headline population
elasticity (β=0.046, pre-#22) is an order of magnitude below the closest benchmark
(Gibbons et al. 2024, ≈0.3), and a round of diagnostics (PRs #65–71) traced
this to four entangled measurement/identification decisions — consolidated in
**`Plan/memo_identification_measurement_decisions.md`** (2026-06-11), which is
the authoritative source for those open decisions. Read that memo's
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

Block 2 prose (§§6, 7) was drafted under the provisional framings F1/F2
(decisions log) and completed in PR #102 under decide-and-document; the
"hold for Cote" gate was retired 2026-07-17 by Diego's working-mode
correction. Framing reversals remain one-subsection rewrites.

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
- [x] Table 15 (density schedules, main-text) — PR #99
- [x] Table 16 (sector-matched MA) — PR #101 (C3)
- [x] Theta sweep table (tab:theta_sweep, §5.5) — PR #99
- [x] Appendix Table A1 (descriptives) — PR #104 (C35/C36)

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
- Unbuilt Z_i variables (national highway, station, depot) are
  data-limited — tracked in OPEN ITEMS §3.

**Tables (Block 2):**
- [x] Table 13 (counterfactual decomposition) — PR #61, C5 done. (Numbers quoted at merge time are pre-#22 and the "runs primarily through rail" framing was later revised to "similar point estimates, rail better identified" — current values live in the table CSV and §6.)
- [x] Table 14 (local-infrastructure mechanisms) — PR #62, C6 done. (Pre-#22 numbers at merge time; the ~half-through-local-infrastructure reading held after #22 — current values in the CSV and §7.)

### Pending (order: blocked first, then easiest-value)

**Block 1 loose ends:**
- Coauthor meeting on the identification memo — tracked in OPEN ITEMS §1.
- [x] ~~Urban-center reference point diagnostic (Decision D)~~ — done in PR #75 (`diagnostic_ma_urbancenter.R`). Anchor is not the lever; θ still dominates. Cote's geocoded-census version remains the referee-proof answer but the expectation is now confirmation, not rescue.
- Sector-specific indgen shares — deferred, tracked in OPEN ITEMS §5.
- [x] ~~Paper-wide scalar AutoFill substitution~~ — done in PR #95 (218+ macros; render-identical first pass verified by pdftotext diff).
- [x] ~~A1 (OLS vs IV bias direction)~~ — done in PR #102 (5.2 paragraph; the review caught a sign error in the selection mechanism, fixed against §4.2's taxonomy).
- [x] ~~A2 (sectoral patterns + scale economies)~~ — done in PR #102 (facts in §5.3, weighing in §8.3, written once).

**AEA housekeeping 3/4:**
- [x] ~~W18 References bibliography~~ — done in PR #89 (all entries verified; data citations added and cited in §3).
- [x] ~~W18 full AEA README~~ — done in PR #92: dataset list, replicator instructions, table-program mapping, runtimes from logs. Rights certifications and ACA redistribution rights left as `[AUTHORS: confirm]` checkboxes; revisit mapping table when final exhibit numbering locks.

**Block 2 next steps:**
- [x] ~~Consolidated Cote email~~ — SENT 2026-07-20
      (`Plan/email_cote_borrador_completo.md`, Spanish): sample change
      + moved numbers, density gradient + instrument reversal,
      sector-matched MA, transshipment bound, decide-and-document
      decisions, and the full blocked-on-Cote list. Supersedes the
      planned Tables-13/14 follow-up.
- [x] ~~§6 counterfactuals writeup~~ — drafted in PR #84 under
      provisional framing F1; verified/gap-filled in PR #102.
- [x] ~~§7 mechanisms writeup~~ — drafted in PR #85 under provisional
      framing F2; verified/gap-filled in PR #102.
- [x] ~~Two remaining findings paragraphs in §1~~ — filled in PR #87
      (contractual pass); contract updated through PR #102.
- [x] ~~C3 Sector-specific MA regressions~~ — done in PR #101
      (sector-matched schedules; Table 16). The demand-side variant
      (sectoral destination weights) remains genuinely future work,
      stated in the Conclusion.
- C7 heterogeneity table lift — optional, tracked in OPEN ITEMS §5.

**Polish / final (order after Block 2):**
- [x] ~~W13-16 §6.3 caveats, §7 interpretation, §8 Discussion,
      Conclusion~~ — drafted in PRs #84–#86; gaps and stale claims
      closed in PR #102.
- [x] ~~W17 Abstract~~ — drafted in PR #89 (148 words; 149 after the
      #94 framing pass; Larkin clause added within budget in #98;
      scalars macros, theta hedge); flagged in red for Cote's
      sign-off.
- [x] ~~Appendix figures A1-A3~~ — done in PR #80 (see Figures above).
- [x] ~~C34 θ=8.11 appendix table~~ — MOOTED in PR #104 (Table 12
      Panel A + the §5.5 sweep table cover it; documented decision).
- [x] ~~C35 C36 Industrial + agricultural census appendix tables~~ —
      served by Appendix Table A1 descriptives (PR #104).
- [x] ~~C37 Spatial autocorrelation / Conley SE robustness~~ — done in PR #76 (`diagnostic_pretrends_conley.R`). Sensitivity does not soften the pre-trend; it sharpens it. Remaining question (how to own the limitation in prose) folded into memo Decision E for the coauthor meeting.

---

## DEFERRED LEDGER (as of 2026-07-17, post PR #104)

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

Decisions made by us, documented for Cote's inspection (may be revised):
- [x] DENSITY SCHEDULES (Diego, 2026-07-17): all three B&P cargo-density
      schedules are now reported prominently — new main-text table
      (table_15_density_schedules, Section 5.2) re-estimates the
      population spec under s1 (high, rail-favouring) and s2 (low,
      road-favouring) with every MA object switched per row. Medium
      stays the baseline: general-purpose schedule, mode-neutral costs
      (rail 1.874 vs road 1.777 pesos/ton-km), does not pre-judge the
      mode question that Block 2 answers. RESULT IS A FINDING: IV-B
      rises monotonically from 0.026 (high) through 0.052 (baseline)
      to 0.087 (low, p=.051); F strongest at low density (26.0). The
      deeper sector-MATCHED design (agricultural outcomes with
      high-density MA etc.) remains a separate open question below.
      SUB-FLAG for Cote (cr-review PR #99): under the low-density
      schedule the instrument roles REVERSE — the hypo instrument
      carries identification (IV-H F = 42.6) while Larkin weakens
      (LP F = 6.2), the mirror image of the baseline (LP 22.1 /
      hypo 6.9). Bears directly on the standing "should main spec be
      IV-LP-only?" question.
- [x] TRANSSHIPMENT BOUND IN-PAPER (Diego, 2026-07-17): the unimodal
      (infinite-transshipment) bound is now presented in Section 5.5
      and regenerated hands-off (new pipeline driver
      07_unimodal_taus.R = main.R D.13f builds the six single-mode
      taus, ~15 min cold; diagnostic_ma_unimodal.R = D.13g emits the
      CSV the macros read; run_unimodal_variant.sh kept for
      reference). Post-#22 numbers: gain share 90.7% -> 77.2%; median
      pair ratio 0.80 -> 0.61; OLS elasticity 0.016 -> 0.018 (same
      spec both sides, no baseline-MA control, N=311). The bound is
      OLS-only (no unimodal instruments), stated in a footnote.
      Old stale pre-#22 constants in the diagnostic report were
      replaced with live-computed baselines. STILL OPEN for Cote:
      whether to rebuild tau with explicit finite transshipment costs
      at stations/ports (mode-expanded graph) — the bound suggests it
      is not load-bearing, which de-prioritises that rebuild.
- [x] FOLLOW-UP (cr-review PR #104; DONE PR #106): the four IPUMS
      outcome changes (college/secondary/mig5/empstat, 1991-1970) were
      constructed identically in two scripts (table_11_other_outcomes.R,
      table_a1_descriptives.R). Construction moved into
      build_estimation_sample.R (D.1); both table scripts consume
      stored columns with stopifnot guards; manifest and validation
      logging extended. Verified byte-identical table CSVs before and
      after (pure refactor); sample gains 4 columns (228->232), N=311
      each.
- [x] APPENDIX DECISIONS C34-C36 (2026-07-18, decide-and-document):
      C34 (dedicated theta=8.11 appendix table) MOOTED — Table 12
      Panel A has the full estimator grid at 8.11 and the sweep table
      has the whole theta grid; a third exhibit would duplicate both.
      C35/C36 served by ONE appendix descriptives table
      (table_a1_descriptives.R, main.R D.19, tab:descriptives): the
      paper had NO summary-statistics exhibit at all (a submission
      gap; Gibbons et al.'s Appendix Table A1 is the model). Covers
      all outcomes incl. the sectoral censuses, treatment,
      instruments, controls; N column makes the coverage differences
      (sectoral censuses, 1947 placebo subsample) visible. Appendix
      now has a Tables part (A-numbered) before the Figures part.
- [x] BLOCK 2 PROSE COMPLETED (2026-07-17, decide-and-document,
      PR #102 merged): A1 (OLS-vs-IV gap, Section 5.2) drafted stating both
      mechanisms (closure selection into declining districts;
      measurement error in the constructed regressor) WITHOUT
      apportioning, and noting the placebo's positive correlation
      weighs against selection carrying the whole gap. A2 drafted as
      facts-in-5.3 + interpretation-in-8.3 (written once): the three
      aligned exhibits (Table 10 contrast, Table 16 matched-schedule
      strengthening, Table 15 density gradient) stated in 5.3;
      8.3 rewritten to replace the now-false "sector-specific MA
      left for future work" claim and to state the identification
      caveat (matched schedules strengthen first stages too, so part
      of the sharpening may be measurement). Conclusion extensions
      updated to demand-side weights + freight flows. Section 1
      contract extended with one matched-schedule sentence. Stale
      hardcoded 91% in 8.1 -> \maSharePos macro. The wage-mass
      composition story was NOT drafted (old placeholder said discuss
      with Cote first) — left as a tex comment flag in 5.3. Section
      8.2's every-theta sectoral claim now carries an archive
      footnote (the paper's sweep exhibit is population-only).
- [x] SECTOR-MATCHED MA (C3) BUILT (2026-07-17, decide-and-document):
      Table 10's five sectoral outcomes re-estimated under the
      cargo-matched schedules per config.R's B&P mapping —
      manufacturing <- s2 (low density, road-favouring), agriculture
      <- s1 (high density, rail-favouring); all MA objects switch
      together (table_16_sector_matched.R, main.R D.11b, Section 5.3
      paragraph). DESIGN DECISION: Table 10 (s0) stays the headline —
      one common treatment keeps magnitudes comparable across sectors;
      the matched table answers the scale-economies question. RESULT
      STRENGTHENS THE STORY: mfg valprod 0.317 -> 0.367 (p=.020), wage
      mass 0.378 -> 0.444 (p=.016), F ~ 26 (vs 16.5 at s0); agriculture
      stays null under its matched rail-favouring measure (F = 18.6).
      The sectoral contrast is not an artifact of the common schedule.
      For Cote: consistent with the low-density instrument-reversal
      flag above — the road-favouring measure is simply better
      identified.
- [x] THETA SWEEP PLACEMENT (Diego, 2026-07-17): the full sweep is now
      presented as a table in Section 5.5 (robustness, next to the
      baseline results) rather than only quoted in the Discussion;
      Section 3 introduces theta with an immediate pointer to it
      (D&H presentation pattern). Section 8.2 keeps the calibration
      interpretation and references the table.

Blocked on Cote — open items moved to OPEN ITEMS §1 at the top of this
file (log-area awareness, abstract sign-off, Vicente López note, issues
#68/#91, theta provenance, Larkin year, B&P source volume, migration
sign, title; issue #103 is newly tracked there, previously GitHub-only).
Completed record kept here:
- [x] ~~Baumgartner & Palazzo author initials~~ — VERIFIED 2026-07-16
      against the publisher's archive (eltrimestreeconomico.com.mx):
      Jean-Pierre Baumgartner, Pascual Santiago Palazzo. bib was already
      correct; data/raw/costs/readme.md citation completed. Sub-item
      (journal article vs CONADE report as the digitization source) is
      open and tracked in OPEN ITEMS §1.

Pre-deposit (see README's author checklist) — open items moved to
OPEN ITEMS §2 (rights certifications, exhibit-numbering lock) and §4
(\doi caveat, IGN dates); the final clean rerun is newly tracked in
§2. Completed record:
- [x] ~~Clean-machine rerun~~ — DONE (2026-07-16, PR #97 merged): deleted
      results/ + data/derived/, ran `R CMD BATCH code/main.R` end to
      end (~68 min after fixes; the first two attempts crashed the
      machine / hard-stopped — see below). All 11 regenerated table
      CSVs byte-identical to main; recompiled PDF's pdftotext output
      is a zero-line diff against main's committed PDF; zero undefined
      refs, zero bibtex warnings. Six bugs found and fixed, none
      reproducible from an incremental (non-empty data/derived/) state:
      Stage B/C ordering, stale verify_outputs names, a memory crash
      capped via n_cores_heavy, a CLI-args hard-stop, and an unwired
      diagnostic script — full narrative + rationale for each lives in
      the code comments (code/main.R, code/config.R,
      code/pipeline/03c_compute_taus_parallel.R) and README.md's
      pipeline-order and memory sections; not restated here to avoid a
      third copy going stale. Added fail-fast crosswalk-existence
      guards to the four Stage B cleaners that depend on ipums's
      output, so a future ordering regression dies immediately instead
      of after wasted work.

Block 2 (gated on Cote's framing decisions — see Block 2 next steps above).

---

## PENDING DECISIONS

**Live items are tracked in OPEN ITEMS at the top of this file.** This
section is kept as a historical/traceability record of the original
decision list and its supersessions.

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
2. **Sector-specific MA**: PARTIALLY RESOLVED by PR #101 (cost-side
   sector-matched MA, Table 16; decide-and-document — see ledger). The
   demand-side variant (sectoral destination weights) and its
   interaction with the counterfactual exercise remain open for the
   coauthors; the Conclusion states the remaining steps.
3. ~~**Elasticity justification**~~ — superseded by memo Decision A (θ / τ object).
4. ~~**Tau calculation**~~ — superseded by memo Decision B/A (connector re-cost, transshipment already screened in PR #66).
5. **Sector interpretation**: Confirmed sectors 0/1/2 = overall/agriculture/manufacturing in config.R. Block 1 uses sector 0 + θ_low (4.55) throughout.
6. ~~**Pre-trends not clean null**~~ — superseded by memo Decision E (numbers updated: OLS 0.035**, IV-Both 0.078* on the 235-district placebo subset).
7. ~~**Migration sign wrong-way**~~ — superseded by memo Decision E (carried unchanged; no new evidence).
8. ~~**Hypo instrument is weak**~~ — superseded by memo Decision C (estimand) — the rail-vs-total-MA question replaces the two-instrument-vs-LP-only framing.
9. **Tabla 10 sectoral outcomes**: currently uses industrial + agricultural census activity outcomes (not IPUMS employment). Option to rebuild IPUMS `indgen` shares if coauthor prefers the original framing.
10. ~~**Counterfactual framing (Table 13)**~~ — DRAFTED (PR #84, F1
    provisional): §6 states "both contribute, similar point estimates,
    rail far better identified." Reversal to "rail dominates" is a
    one-subsection rewrite if the coauthors prefer it. (The +0.032 /
    F=105 numbers quoted here were pre-#22.)
11. ~~**Mechanism framing (Table 14)**~~ — DRAFTED (PR #85, F2
    provisional): §7 leads with the decomposition (half through local
    infrastructure) and gives the complementary regional-connectivity
    reading in the following paragraph. Reversal = reorder two
    paragraphs.
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

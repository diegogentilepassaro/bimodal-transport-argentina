# Task List

Derived from paper.tex skeleton. Color key in paper.tex: RED = placeholder numbers/text, ORANGE = coding tasks, PURPLE = writing tasks, TEAL = analysis/interpretation tasks.

---

## OPEN ITEMS (single source of truth, grouped by state)

Every open task or flag lives here, exactly once. The sections below
this one (CURRENT STATUS, Done, DEFERRED LEDGER, PENDING DECISIONS,
the original C/A/W task specs, QUESTIONS TO RESOLVE, and PRIORITY
ORDER) are historical record: completed items, dated decisions, and
superseded plans. When an item here closes, mark it [x] with the
PR/date and move it to section 7 (completed this cycle); if it
records a decision, add the record to the DEFERRED LEDGER.
Reorganized 2026-07-25 around the Wednesday meeting; the pre-meeting
work program (PRs #115-#133) is section 7.

### 0. WEDNESDAY MEETING 2026-07-29 — prepared agenda

Agenda set by Cote's 2026-07-24 email. Each item below is a decision
the meeting has to make, with the evidence now in hand. Where an
item's follow-up work is tracked elsewhere, the pointer is noted.

- [ ] Pre-meeting evidence brief for Cote — DRAFTED 2026-07-25
      (`Plan/email_cote_evidencia_theta_tau.md`, workspace root,
      untracked): the θ/τ three-object comparison (items A i-v),
      the four instrument pieces (item C a-d incl. the A×C
      interaction), the V-sourcing spec for his #68 archive visit,
      and the closed reading-note items. All numbers audited
      against the committed diagnostic outputs. SENT 2026-07-27 after
      four factual fixes (merged-branch pointer with page numbers, the
      "nada toca el paper" exception, AR upper bound −0.0003 not
      −0.000, and a conventions paragraph on planned-vs-compiled table
      numbering and the two meanings of θ), plus the secondary-share
      AR nuance that cuts against a no-schooling reading.
      ⚠ ONE THING THE SENT EMAIL PREDATES: it says the
      1947-consistent spec "queda limpia en TODOS los objetos
      candidatos". True of full47, but PR #143 showed that cleanliness
      is not defensible. Lead the meeting with the correction (item B).

- [ ] A. DECISION A — the θ/τ object (deepest open question).
      Both cheap experiments Cote asked for are done:
      (i) Gibbons-style decay (PR #127): θ ∈ {0.25, 0.5, 0.75};
          β·θ ≈ 0.37 scale-invariant; at θ=0.5 the level is +0.745
          (SE 0.42), CI covers Gibbons 0.3; sectoral pattern
          survives at every decay.
      (ii) Iceberg normalization τ' = 1 + cost/V (PR #130): β(V)
          rises monotonically toward the D&H band, F strengthens
          13→37, but nothing clears 5% anywhere — including the
          published main spec itself (p=0.096). V unsourced (rides
          the #68 archive visit; honesty rule: source picks V).
      (iii) Sectoral de-risking ON the iceberg object (2026-07-25,
          diagnostic_ma_iceberg_sectoral): under IV-BOTH (the
          published spec) the headline contrast SURVIVES at every V
          grid point and both θ — mfg value/wage max p =
          0.013/0.004, establishments + both ag outcomes min p =
          0.127. Under IV-LP the contrast holds only in the
          low-to-mid V region (mfg ** at V=100-1000; already just *
          at raw, per Table 10) and fades to null at high V (max p
          0.18/0.33) as normalization weakens the Larkin instrument
          — so Decision A and the instrument choice INTERACT:
          IV-LP-only + deep normalization together would cost the
          sectoral significance.
      (v) Option 1b QUANTIFIED (2026-07-25,
          diagnostic_tau_inefficiency): the route-inefficiency
          object τ' = cost/(c_min × geodesic) — pair-varying, no
          external data, τ' band 4.5-25.5, 0% below 1. The
          sectoral contrast HOLDS under IV-Both (mfg value/wage
          p ≤ 0.004 at both θ; establishments/ag/population null)
          and Δlog MA correlates 0.88 with the raw treatment. THE
          COSTS, quantified: classical IV-B F falls from 13-16
          (raw) to 8-12 — and by the MOP test the two AGRICULTURAL
          cells fail even at 20% bias tolerance (F_eff ≈ 7.0 vs
          cv ≈ 10.2-11.5) while population + all three mfg cells
          pass at 10% (F_eff 8.2-8.7 vs cv 6.5-7.4); the ag null
          also thins (farms p 0.22 → 0.12). Under IV-LP the 1b
          object is MOP-weak everywhere (F_eff 3.4-5.7 vs 23.1;
          mfg value still significant at 5% at θ=4.55, p=0.047,
          10% at θ=8.22 — but the real cost vs raw is the F
          collapse from 22-23 to 4-6.5). CAVEAT the meeting should
          weigh: τ' spans 4.5-25.5 — internally coherent as an
          inefficiency multiple, but NOT the D&H narrow-band-
          above-1 object, so "why is a trade elasticity the right
          exponent for an inefficiency ratio" (the scoping note's
          own con) stands. All three τ-object options now have
          quantitative evidence.
      Options priced in .kiro/decision_a_option1_scoping.md (1a
      Fogel-scalar ~2-3 days once V sourced; 1b route-inefficiency
      DONE above; option 2 Gibbons-centrality ≈ the decay
      experiment). Interacts with the density-schedule gradient (PR #99,
      Table 15). Mechanics: the main-spec swap is now a config.R
      edit (PR #133) plus the label/binning sweep listed in
      config.R's comment.
- [ ] B. TABLE 7 PLACEBO SPEC — which 1947-consistent baseline set
      becomes the paper's Table 7? DIEGO'S POSITION (2026-07-27,
      adopted): **pop47, NOT full47.** Swap the indefensible control,
      keep the baseline-MA control, and report the result as a
      marginal rejection rather than a clean null. Cote's visto bueno
      is what remains.
      ⚠ READ ITEM H FIRST (found 2026-07-29, after PR #145 shipped).
      The placebo OUTCOME mixes universes: chg_log_placebo_pop_60_47 =
      log(pop_1960) - log(pop_1947), which is locality-universe 1960
      against full-universe 1947, and 143 of the 237 districts
      "shrink" as a result. Everything below is about which BASELINE
      CONTROLS to use; item H is about whether the outcome measures
      what the table says it measures. The control question does not
      go away, but it is the second-order one. TESTED in PR #149 with a
      universe-comparable placebo (urban population on both ends): the
      point estimates fall hard but the movement is NOT statistically
      distinguishable from zero, so the rejection is fragile to how the
      outcome is measured without being shown to be caused by the
      coverage gap. Detail and the numbers in item H.
      WHY THE POSITION CHANGED (PR #143): the clean null in full47
      comes from DROPPING baseline log MA, not from swapping the
      population baseline to 1947 — pop47 alone moves IV-B p only
      0.034 → 0.085 with the coefficient nearly unchanged (+0.087 →
      +0.084). The justification offered for dropping the MA control
      was post-outcome conditioning. PR #143 probed that by
      re-weighting the SAME 1960 τ matrix with 1947 population:
      β = +0.078 (p = 0.122), close to the t7 point estimate though
      its CI overlaps full47's, and the two levels correlate 0.9994
      raw / 0.9990 after controls.
      WHAT THAT DOES AND DOES NOT ESTABLISH (stated as the artifact
      states it, diagnostic_placebo_ma1947.txt): the with-vs-without
      comparison is SUGGESTIVE, NOT A TEST — the CIs overlap and the
      first stages differ (F 11.1 with vs 23.2 without, the absorption
      mechanism in baseline_ma_control_note.md). The post-outcome
      justification is UNSUPPORTED, not refuted, and it is unsupported
      on the CORRELATION: at 0.9990 partial correlation the ma47-vs-
      ma60 contrast never had power to separate the two controls, so
      "remove the post-outcome content and see if the null returns"
      cannot be answered by that comparison — there was almost nothing
      to remove. Read literally on p-values, neither constructed
      control rejects at 10% (0.122 / 0.104), which taken alone would
      favour full47. The position rests on the collinearity, on the
      coefficient pattern (+0.078 to +0.087 with a baseline-MA level,
      −0.004 without), and on the judgment that a validation test
      should not drop the control the headline spec needs.
      WHAT IS STILL AIRTIGHT: log pop 1960 must go regardless of any
      p-value. The placebo DV is log pop 1960 − log pop 1947, and log
      pop 1960 is its TERMINAL level — conditioning on it conditions on
      the endpoint of the outcome being tested. (log pop 1947, which
      pop47 keeps, is the INITIAL level: a standard convergence
      control, not the same object.) That is the part of the swap that
      is not a judgment call; whether the MA baseline stays is.
      SECOND ARGUMENT FOR THE SWAP, independent of p-values: in the
      current Table 7 the IV-Hypo column has first-stage F = 1.7 and
      cannot reject anything. Under pop47 it is still 1.7; under
      full47 it rises to 16.0. Worth stating whichever spec wins.
      A×B CHECKED for full47 (2026-07-26, PR #139): the clean null
      survived on every candidate τ object — decay θ=0.5, iceberg
      V=4,400/20,000, route-inefficiency at both θ — smallest any-IV
      p = 0.34, IV-B first stages F 21.5-61.1.
      ⚠ NOW RUN FOR pop47 TOO (2026-07-29, PR #154), AND THE ANSWER
      INVERTS. On the ADOPTED spec the placebo rejection is NOT
      dissolved by the τ object: IV-B rejects at 10% on FIVE of six
      objects and at 5% on one (decay θ=0.5, p 0.042); max p 0.151
      (iceberg V=20,000). OLS is starker still — p 0.001-0.008 on five
      objects, the exception being the RAW anchor at p 0.149, i.e. the
      object the paper actually uses. IV-LP rejects nowhere either way
      (0.29-0.94). So "changing the τ object" is not an escape route
      from the placebo, and the full47 conclusion above belongs to
      full47 only.
      TWO CAVEATS ON THE pop47 CROSS-OBJECT RESULT: IV-B first stages
      fall to 6.2-20.2 (from 21.5-61.1), and three of the five 10%
      rejections sit on F < 10 — suggestive, not firm. The weakest
      IV-LP first stages are now the route-inefficiency objects
      (F 2.2 / 3.3), not the iceberg ones.
      WHY full47 LOOKED CLEAN — MECHANISM ESTABLISHED (PR #154 review).
      Because chg ≡ l86 − l60 identically, dropping the baseline MA
      does not remove a control, it IMPOSES THE RESTRICTION
      b_l86 = −b_l60. Verified on the raw object: unrestricted
      y ~ l86 + l60 gives b_l86 = +0.0275, which equals the pop47 `chg`
      coefficient to machine precision, and the pop47 coefficient ON
      l60 (+0.0460, p < 0.0001) IS the test of that restriction — it
      rejects. Same on the decay object (OLS chg −0.110 → +0.588,
      l60 +0.860). Sample and df are ruled out (N = 237 both ways, one
      regressor). So full47's clean null is attenuation from a REJECTED
      linear restriction, not merely an indefensibly dropped control.
      That is a stronger argument for pop47 than the one on record.
      §8.2 LIMITATION 1 STAYS, BOTH HALVES. Under pop47 the placebo
      still rejects at 10% (p = 0.085), so the pre-trend half is
      softened, not fixed. The selection half (237 of 311 districts) is
      untouched by any placebo spec. The limitation was NOT deleted.
      IMPLEMENTED 2026-07-27, PR #145 (Diego's call: do the half that
      is not a judgment call now, leave the MA-baseline question to
      Cote). Table 7 is now pop47: OLS +0.027 (p 0.149), IV-LP +0.051
      (0.374), IV-Hypo +0.333 (0.379), IV-Both +0.084 (0.085), N 237,
      F 18.7 / 1.7 / 11.2. New appendix Table B2 publishes the full
      ladder (1960 baselines → 1947 pop → no MA baseline → no
      baselines), so the sensitivity is on the page rather than left
      for a referee. Prose updated in four places: §4.6's placebo
      paragraph (control-set rationale + p-values instead of
      significance words), §5.2's selection-logic argument (weakened
      to "suggestive rather than decisive" — the positive correlation
      survives, its significance does not), the intro validation
      sentence, and §8.2 limitation 1.
      CONLEY RERUN DONE on the new spec (diagnostic_pretrends_conley.R
      now carries a per-spec control set): spatially robust SEs still
      SHARPEN the rejection — IV-Both HC1 p 0.085 → 0.017 / 0.016 /
      0.062 / 0.030 at 100 / 250 / 500 / 1000 km, and OLS 0.149 →
      0.093 / 0.064 / 0.055 / 0.003. So the paper's "spatial SEs do not
      soften it" claim holds and is now stated as "sharpen rather than
      soften". Uncomfortable but the honest direction: under HC1 the
      placebo is a marginal rejection, under spatial SEs a clear one.
      STILL COTE'S CALL: (a) full47 instead of pop47 — a one-line
      config.R change plus the §4/§8.2 wording, with Table B2 already
      showing what it does; (b) whether Table B2 stays in the appendix
      or moves to the main text; (c) whether the item-B2 direct test
      enters the paper.
      SCOPE LIMIT — RESOLVED 2026-07-29 (PR #153). The seven diagnostics
      that also estimate the placebo outcome now report it under BOTH
      baselines, as separate rows `placebo_pretrend` (log_pop_1960,
      values unchanged) and `placebo_pretrend_pop47`. Reporting both
      rather than repointing keeps the numbers already sent to Cote
      reproducible from the repo — including the recentering
      "+0.074 → +0.088" quoted in the brief and in this ledger — while
      showing the adopted spec. Verified: 0 pre-existing values moved,
      0 lost, 321 rows added across ten outputs.
      WHAT THE ADOPTED BASELINE DOES TO THE RECENTERING PLACEBO — the
      concern largely dissolves. Recentered: +0.0879 (p 0.244) →
      +0.0215 (p 0.812), reduced-form RI p 0.337 → 0.881; across the
      whole C0–C6 ladder +0.022 to −0.016, every p between 0.77 and
      0.97; rail-only +0.0617 → +0.0142; road-only +0.5944 → +0.2295;
      fused +0.0873 → +0.0557. Consistency check: the `unadjusted`
      pop47 rows reproduce Table 7 exactly (IV-LP +0.0512 p 0.374,
      IV-Hypo +0.3331 p 0.379 F 1.72).
      ⚠ ONE CHARACTERIZATION CHANGES, and the brief needs the edit
      before it is quoted again. In the CORRIDORS design
      (diagnostic_roadseg) the unadjusted placebo IV was significant at
      5% — total +0.0797 (p 0.0250), road-only +0.0783 (p 0.0227) — and
      is not under the adopted baseline: +0.0551 (p 0.206) and +0.0538
      (p 0.202), with reduced-form RI p 0.099 → 0.495. roadtiming moves
      the same way (total +0.0756 p 0.083 → +0.0401 p 0.229; RI p
      0.812 → 0.495). THE VERDICT DOES NOT CHANGE: the other strike
      against corridors is a corridor-level BALANCE fact (early
      corridors cross faster-growing districts, b +0.074, p 0.039)
      which does not depend on the placebo control set, and PR #124's
      stratification result (recentered F collapses to 0.5–1.4) is the
      decisive one. But "el IV placebo sin ajustar da significativo"
      in the brief is now true only of the pre-adoption spec.
- [ ] B2. PRE-1960 GROWTH AS A CONFOUND (new, PR #143) — the direct
      test the placebo only addresses indirectly: condition the main
      1960-91 regressions on 1947-60 growth. Manufacturing survives.
      Wage mass 0.378 (N=309) → 0.350 (237 subsample) → 0.319 with the
      control, p = 0.011; production value 0.317 (N=310) → 0.280 →
      0.240, p = 0.069; population 0.052 (N=311) → 0.077 → 0.065.
      The control is itself insignificant in all three (p = 0.17-0.23),
      which is a limit on how strong a test this is: little pre-period
      signal to absorb. NOTE (cr-review): row 3 is algebraically a log
      pop 1947 CONVERGENCE control, since the growth term equals log
      pop 1960 − log pop 1947 and log pop 1960 is already a control;
      verified identical to 1e-10. Also visible: the placebo subsample
      is where population is STRONGEST (0.077, p=0.032, vs 0.052,
      p=0.096 on the full sample) and conditioning pulls it about
      halfway back — a reason for care with that subsample. That
      subsample row is NOT new: it is already Table 12 Panel C in the
      paper (\subIVBCoef 0.077, \subIVBP 0.032). What IS new is the
      third row, which conditions on the pre-period as well.
      DECISION FOR COTE: does this go in the paper (robustness row or
      appendix), and if so under which label?
- [ ] C. MAIN-SPEC INSTRUMENT — IV-LP-only vs IV-Both. Three new
      evidence pieces this week, all pointing the same way:
      (i) under iceberg normalization the instrument-strength
          ranking REVERSES (IV-H F 7.9→65.5 while Larkin falls
          15.6→9.7; PR #130);
      (ii) under robust inference the hypo instrument weakens
          (IV-H F 6.9 classical → 4.3 robust) while IV-LP slightly
          strengthens (22.1 → 22.6; PR #131);
      (iii) recentering: the Larkin instrument is the only one with
          exploitable quasi-random variation, and it collapses on
          total MA after correction as a composition effect, not
          invalidity (six-design map, issue #114 / brief).
      (iii-b) The A×C matrix is now COMPLETE (2026-07-26,
          diagnostic_crossobject_checks, PR #139): on the DECAY
          object IV-LP keeps the mfg significance at 5% across the
          whole θ grid (max p = 0.030, classical F(LP) 9.2-15.0) —
          the only candidate that does; the iceberg holds it only
          at low V (≲1,000-2,000) and 1b nowhere. BUT by the same
          MOP standard that judged 1b, decay IV-LP is also weak:
          F_eff 10.7-16.3 vs the fixed K=1 cvs (23.11 at 10%,
          15.06 at 20%) → 0/18 cells pass at 10%, 1/18 at 20%.
          Honest summary for the meeting: decay is the LEAST-WEAK
          IV-LP candidate; no object makes IV-LP-only MOP-strong.
      (iv) MOP critical values (2026-07-25,
          diagnostic_mop_critical): 8 of 9 IV-B cells PASS at 10%
          Nagar-bias tolerance (rural pop fails 10%, passes 20%);
          IV-LP alone sits at the boundary (F_eff 22.64 vs cv
          23.11); IV-H fails at every tolerance. ATTRIBUTION
          (matters for the meeting): the low K=2 cvs (6.5-9.7) are
          NOT mainly the instrument count — the conservative B=1
          test would still demand ~19.7-20.5 and every IV-B cell
          would FAIL it at 10%. The bar drops because the exact
          Nagar-bias bound B ≈ 0.15-0.30 for our W matrices: the
          worst-case 2SLS bias is a small fraction of the
          benchmark. So "the combined spec passes MOP at 10%
          (except rural pop)" leans on the exact bias-bound
          computation; dropping to IV-LP-only would move the main
          spec from "passes" to "borderline".
      Modern-IV wiring into the paper is a follow-up decision
      (section 2).
- [ ] D. MIGRATION SIGN + §5.4 — Cote leans REMOVE ("la sacaría si
      hace ruido"), at most a paragraph in Other Outcomes, and
      demote §5.4 to an annex (his note #42). Needs Diego's
      concurrence; then it's a writing task (section 2). Title
      stays as-is, closed jointly with narrative at publication.
      EVIDENCE NOW IN (2026-07-27, PR #140,
      diagnostic_modern_iv_table11, corrected after cr-review): the
      decision turns on IDENTIFICATION, not noise. THE HEADLINE
      FACT: the joint (IV-B) overidentification test rejects at 5%
      for THREE of the four outcomes — only employment survives.
      Robust J (min-AR quadratic form, χ²_{k-1}) agrees with the
      classical Sargan in all four cells, so heteroskedasticity is
      doing no work:
      (i) recent migration — Sargan p = 0.0056, robust J p = 0.012,
          IV-B AR set EMPTY (the joint K=2 test rejects every β).
          IV-LP says -0.0066 (p = 0.52, AR covers zero); IV-H says
          -0.064 (p = 0.029) on the instrument that MOP-fails at
          every tolerance. The published -0.0217 sits between two
          jointly incompatible moments — an unidentified
          coefficient, not a noisy one. (The individual AR sets do
          overlap on [-0.0303, -0.0297]; state the failure in
          joint-moment terms. The interval was first recorded as
          [-0.0303, -0.0294] from a hardcoded string in the
          diagnostic; it is computed from the AR bounds now, PR #141
          fix pass.)
      (ii) secondary share — same joint failure (Sargan p = 0.0078,
          robust J p = 0.0068, AR empty) with the instruments
          pointing OPPOSITE ways (+0.0059 LP vs -0.0101 H). NOTE:
          an overid rejection does not say which moment fails —
          under the natural reading (hypo instrument is the weak,
          MOP-failing one) the IV-LP result STANDS on its own
          (+0.0059, AR [0.0019, 0.0114], p at zero = 0.005).
      (iii) employment rate — the clean one: Sargan p = 0.74,
          robust J p = 0.72, AR excludes zero under both IV-LP
          (-0.0121, p at zero = 0.017) and IV-B (-0.0109,
          marginally: p at zero = 0.044). The only Table 11 outcome
          whose JOINT spec survives identification-robust
          inference.
      (iv) college share — both instruments give individual nulls
          (all AR sets cover zero) but they differ significantly
          from each other (+0.00103 LP vs -0.00205 H; Sargan
          p = 0.034, robust J p = 0.031). So the joint spec is not
          identified here either; "clean null" would be wrong.
          Its AR set is non-empty only because the emptiness
          criterion uses k df while the overid test uses k-1 —
          strictly more conservative (not a heteroskedasticity
          artifact, as the first pass wrongly claimed).
      ALSO RELEVANT TO ITEM C: three overid rejections concentrated
      in the 1970-91 outcome window is itself evidence about the
      hypo instrument, beyond the first-stage findings.
      POSITION CONFIRMED (Diego, 2026-07-27) and IMPLEMENTED
      (PR #141, branch paper/section-5-4-restructure): remove
      migration from the narrative on the overidentification ground
      rather than "it makes noise"; do NOT demote §5.4 wholesale.
      §5.4 stays in the main text reorganized by what is
      identified — employment as the result (AR [-0.0280, -0.0003]
      excludes zero), education reported as instrument
      disagreement, migration in one paragraph pointing to the new
      Appendix A where the estimates and the two readings of the
      sign are recorded unadjudicated. Table 11 gains an
      overidentification row; §8.2 limitation #2 and §1's outcome
      list updated to match; 26 new AutoFill macros (one removed:
      \migrationCoefAbs), scalars.tex now 261. Compile 54 pp clean.
      FIX PASS (same PR) added the §4 paragraph that defines the
      overidentification test and the AR sets (sargan1958,
      andersonrubin1949, andrewsstocksun2019 added to
      references.bib, all three verified), led the §5.4 prose with
      the robust J rather than the classical Sargan, corrected a
      "rejects most strongly" claim that reversed under the robust J
      (secondary 0.0068 < migration 0.0120), added the secondary
      IV-LP AR set that cuts against a no-schooling reading, gave
      Table 11 a reader-visible note, and reordered its panels to
      match the prose. STILL OPEN for Wednesday: Cote's visto bueno,
      and his option to demote §5.4 entirely if he prefers (one-line
      change). Email §4 and brief item 6 carry the write-up.
      ASYMMETRY ON RECORD: Table 11 is the only table with an
      overidentification row; extending it to Tables 9/10 is a
      two-line change left as a joint decision (comment in §4).
- [ ] E. RECENTERING READ-OUT — walk the six-design map
      (Plan/brief_cote_recentering_2026-07-29.md, final): Larkin
      collapses / hypo backbone / settlement clean-but-small /
      corridor dosed-but-loaded / stratification closes the door /
      fused strengthens F but stays null. One characterization:
      variation big enough to matter was chosen for
      growth-correlated reasons; plausibly-random variation is too
      small at 312-district aggregation. Close issue #114 after
      the conversation. Known limitation on record:
      snap-tolerance sensitivity unexplored.
- [ ] F. MENTION-ONLY — log-area awareness: the balance-table
      correlation exists and a referee may ask; agreed answer is
      the density/over-control + mechanical-entanglement rationale
      (decision recorded in DEFERRED LEDGER; no sensitivity table).
- [ ] G. COLLECT FROM COTE. Original four: the #68 lookup (report's
      studied definition + network denominator), the #113 CABA-node
      sign-off, the DNV publication volume, and geocoding branch
      status — the last of which is now answered, his branch landed
      2026-07-28 and merged 2026-07-29 (PR #146 + the intake-fix
      PR #150, squashed to main as 00e0334).
      LIST REVISED 2026-07-29 after the intake fix pass. Eight items in
      three blocks. (2) and (4) are new; (5) and (7) are the same
      questions with a sharper basis; the rest are unchanged. Only
      (1) has its own section-1 entry; the rest live here, so when this
      agenda item closes the survivors must be moved to section 1
      rather than closed with it.

      BLOCK 1 — WHAT THE VOLUMES CONTAIN. One archive/scan check
      disposes of all three, so present them together.
      (1) DO THE 1960 VOLUMES PUBLISH DEPARTAMENTO TOTALS (including
          dispersed rural population)? Still the highest-value ask on
          the list — see item H. Rides the same archive visit as the
          #68 lookup and the V parameter for Decision A option 1a.
      (2) NEW — THE 18 GRAN BUENOS AIRES PARTIDOS. `footnote == "1"`
          rows are the whole-partido total for a conurbano partido
          rather than a locality, and they hold 3,772,411 people =
          27.85% of the file (La Matanza 401,738, Lanús 375,428,
          Morón 341,920, …). So in the densest part of the country the
          geocoding gives one point per partido, not per locality.
          CAREFUL HOW THIS IS PUT: the property is documented in his
          own material and the handling was AGREED WITH HIM — PROTOCOLO
          .md:186 flags `tipo=total_partido`, and ROADMAP.md:105
          records the 2026-07-15 decision "footnote(1) conurbano →
          1 punto = cabecera del partido". What is new is only the
          MAGNITUDE. So the ask is not "did you know", it is: given
          that this is 27.85% of the file, does the cabecera decision
          still stand, and do the volumes list localities inside those
          partidos or is one row the ceiling the source imposes? This
          governs how much the multi-point MA upgrade actually buys —
          if one row is the ceiling, it buys least where density is
          highest.
      (3) Was volume 2 (Capital Federal) skipped from the geocoding by
          design? Near-certain yes (his v3-v9 = our Parts 3-9, and our
          own 1c1960_2.xlsx IS Part 2 = Capital Federal). Now
          documented in the folder readme as a by-construction
          omission; confirmation is so it stops being an inference.

      BLOCK 2 — ONE-LINE ANSWERS THAT UNBLOCK US.
      (4) NEW — CRS and what the point represents. Neither is recorded
          anywhere in the material. EPSG:4326 is a safe inference from
          Georef and the readme states it as an assumption, but he can
          settle it in one line, and it gates any spatial use of the
          file. Whether a point is a locality centroid, a town centre
          or a station varies with `fuente` and matters for
          short-distance tau. Partially answered already for the 18
          GBA rows: the 2026-07-15 decision puts those at the
          cabecera del partido.
      (5) What was the plan behind the `propuesto` / `en_muestra` /
          `pendiente_muestra` labels? SHARPER BASIS: the question is
          not "confirm 1,003 rows". Reading `criterio_aceptacion`,
          21 of 3,063 rows (0.7%) carry an individual human decision —
          auto_match 1,993, auto_muestreo 829 (never inspected),
          humano_lote 172 (queued, not done), documentado_3.3b 31,
          humano_individual 21, sin_coordenada 17. `estado` is not a
          confirmation status and reading it as one overstates the QC
          by ~50x. So the sampling design is the whole question, not a
          detail. Still ask before proposing a triage.

      BLOCK 3 — WORK OR DECISIONS HE HAS TO MAKE.
      (6) The 23 `rojo` rows (point outside its expected departamento,
          all 23 with coordinates) are worth his eyes regardless of any
          sampling design: a cross-district location error is the one
          kind MA cares about.
      (7) LICENSING, WIDER THAN FIRST FRAMED. Not only the provenance
          and licence of ref/deptos_argentina.geojson (55 MB,
          third-party, no source recorded): 128 rows mention Wikipedia
          (CC-BY-SA) and 54 mention OSM/Nominatim (ODbL), 4 of them
          both, so the two sets overlap and must not be added. Both
          licences are share-alike, so they may attach conditions to
          redistributing the coordinate column itself. `fuente` is free
          text with 88 distinct values and a tail naming further sites
          (Mapcarta, Wikimapia, GeoNames, Mindat, dices.net, db-city,
          citypopulation, getamap, SIPAR, provincial and municipal
          pages, among others), so this needs a decision, not just a
          source list.
      (8) The 19 geocoding scripts with hardcoded Windows paths: make
          them runnable, or keep the archived-non-runnable label the
          readme now carries? Plus the Python version used in
          production, if recoverable — requirements.txt currently pins
          a known-working set, explicitly not the production one.

- [ ] H. THE 1960 POPULATION UNIVERSE (new, 2026-07-29, PR #147).
      FINDING: data/raw/census/censo1960/1c1960_*.xlsx has three
      columns — provincia, distrito, pop — ONE ROW PER LOCALITY, no
      locality name, no rural-dispersed line. So pop_1960 is
      "population living in named localities", NOT district
      population; dispersed rural population is absent from the source
      and cannot be in it. pop_1970 and later, from IPUMS, do include
      it. This is a property of the source, not a coding bug.
      DECISIVE INTERNAL EVIDENCE (needs no external benchmark):
      pop_1947 is Cuadro 1 = DISTRICT TOTALS, a full universe. On the
      237 districts where both exist, pop_1960 is BELOW pop_1947 in
      143 of them (60%); aggregate ratio 1.146 over 1947-60 against
      1.460 over 1960-70. Population does not fall in most of a
      country over thirteen years.
      WHERE IT PROPAGATES: (1) chg_log_pop_91_60, the headline
      outcome; (2) urbshr_1960 and chg_urbshr_91_60; (3) log_pop_1960
      as a baseline control; (4) the MA population weights, which
      under-weight rural destinations relative to CABA (CABA has
      almost no dispersed rural population, rural districts have a
      lot); (5) THE PLACEBO — see below.
      ⚠ THE PLACEBO OUTCOME IS THE SAME MISMATCH.
      chg_log_placebo_pop_60_47 = log(pop_1960) - log(pop_1947)
      exactly (verified), so it is full-universe 1947 against
      locality-universe 1960, and the 143 districts that "shrink" are
      exactly the 143 above. Table 7 is therefore measuring
      1947-60 growth PLUS cross-district variation in 1960 locality
      coverage. The coverage shortfall's relation to the treatment is
      a partial correlation of -0.119 with p = 0.065, and PR #147's
      pre-committed verdict on treatment-correlation is INCONCLUSIVE
      (its two proxies disagree in sign once conditioned). Both
      qualifiers matter and an earlier draft of this entry dropped
      them, quoting p = 0.039, a number that appears nowhere in the
      diagnostic.
      TESTED (PR #149): a universe-comparable placebo measuring both
      endpoints on an agglomerated concept — urbpop_1947 against
      urbpop_1960. The 1947 urban threshold was checked rather than
      assumed: across the 24 Cuadro 14 sheets the smallest positive
      population is 2,002 and none is below 2,000, so both sides use
      the same 2,000 rule.
      RESULT: apparent DECLINE affects 143 of 237 districts on the
      published outcome and 27 of 234 on the comparable one, and the
      published outcome correlates +0.304 with the coverage proxy
      against +0.100 for the comparable one at similar SDs. Point
      estimates fall hard (OLS +0.0253 -> +0.0039; IV-B +0.0829 ->
      +0.0484, both against the same-sample row).
      BUT THE MOVEMENT IS NOT DISTINGUISHABLE FROM ZERO. Rows share a
      sample and 2SLS is linear in the outcome, so the slope
      difference is exactly estimable: OLS +0.0214 (p 0.114), IV-LP
      +0.0434 (p 0.229), IV-B +0.0345 (p 0.299). And the ten-percent
      crossing is NOT the outcome's doing — IV-B's p runs 0.085 to
      0.105 on the same outcome with three fewer districts, then to
      0.293.
      SO THE DEFENSIBLE CLAIM IS: the placebo rejection is not robust
      to how the outcome is measured, and the published outcome is
      measurably more contaminated — NOT that the coverage gap caused
      the rejection.
      ⚠ AND MY EARLIER SIGN REASONING WAS WRONG. This entry first
      guessed the artifact pushed the slope DOWN, so the true
      pre-trend might exceed +0.084. It was labelled a hypothesis
      because it ran through the contaminated proxy, and the test
      reversed it: removing the artifact lowers the slope, so the
      artifact was pushing it UP. Recorded because the guess reached
      the coauthor brief before the test did.
      This still reframes agenda item B: the pop47-vs-full47 control
      debate sits on top of an outcome whose measurement is contested,
      and the control question should not close the placebo discussion
      on its own.
      WHAT IS NOT SETTLED: whether the mismatch biases the IV
      estimates. PR #147's Part 1 is INCONCLUSIVE by its own
      pre-committed rule (the two coverage proxies disagree in sign
      against the treatment once conditioned on controls; the Larkin
      instrument does predict urbshr_1960 at p 0.007 but predicts
      neither cov60 nor the growth gap). Part 2's shorter windows have
      a rival explanation: a 1970 or 1980 baseline is already partly
      treated by the 1960-86 change, so a first-decade response is
      differenced away, and no aligned treatment exists.
      WHAT IS CLEAR: the population OLS estimate is sensitive
      (+0.0247 → +0.0089 with a coverage control, +0.0047 on the
      1970-91 window); the IV estimates move less (IV-B +0.0518 →
      +0.0471 / +0.0373; IV-LP +0.0456 → +0.0487 / +0.0451). Note the
      direction is inconvenient for §5.2: correcting the mismatch
      pushes OLS DOWN and WIDENS the OLS/IV gap that §5.2 attributes
      to attenuation and selection.
      THE FIX IS A DATA REQUEST: the published departamento totals in
      the 1960 volumes, which neither digitization captured (item G
      (1)).

### 1. Waiting on Cote (independent of the meeting)

- [ ] 1960 DEPARTAMENTO TOTALS from the published volumes — the fix
      for item H, and the highest-value item on the archive list.
      Neither our digitization (locality lists, Parts 2-9) nor Cote's
      geocoding (v3-v9) captured a departamento-total column, so we
      have no full-universe 1960 population at district level. Rides
      the same archive visit as the #68 lookup and the V parameter for
      Decision A option 1a. Until it lands, pop_1960 stays a
      locality-universe variable and everything in item H stands.
- [x] Geocoding 1960 intake — MERGED 2026-07-29 as 00e0334.
      His branch landed 2026-07-28; PR #146 reviewed 2026-07-29 (review
      published on the thread, plus a correction: the first review
      claimed a ~6M coverage gap against a half-remembered national
      total, when against our own non-CABA 1960 figure the file
      reconciles to 99.90%). The four intake fixes went in as PR #150
      onto his branch first, so the recursive .gitignore never reached
      main; #150 was itself reviewed and got a fix pass. 80 data files
      and 36 script files now tracked. main.R untouched — nothing in
      the R pipeline reads the folder yet, and downstream MA
      integration stays gated on the θ/τ conversation and on item H.
      WHAT THE INTAKE ESTABLISHED (all now in the folder readme, and
      all of it either an input to item G or a constraint on the MA
      upgrade):
      - Capital Federal absent (volume 2 not transcribed). Clean
        non-CABA universe: 13,544,686 against 13,558,587 for the 311
        non-CABA districts, 99.90%. Two things that headline hides.
        First, WHAT IT COMPARES: two independent transcriptions of the
        SAME volumes (clean_census_1960.R reads the manually
        transcribed 1c1960_*.xlsx; his is a vision transcription of the
        v3-v9 scans), so it validates transcription and not coverage,
        and both inherit item H's gap. Second, IT IS A NETTED TOTAL,
        not a row-level check: the -13,901 gap is 91% Santa Fe alone
        (-12,720), gross |delta| across provinces is 15,689, and only
        13 of 23 provinces agree exactly. And the independence holds
        only because a gated step did not run — the intake ships
        validar_depto_xlsx.py (step 2.4), whose job is to check his
        departamento sums against OUR 1c1960_3_*.xlsx for Buenos Aires
        and La Pampa, and its output intermedios/validacion_depto.csv
        is 65 bytes, header only.
      - 27.85% of the file's population sits on 18 whole-partido GBA
        totals, not localities. See item G (2). NOTE what our own
        cleaner does and does not do here: clean_census_1960.R:24-31
        and check_gba_duplicates() drop the PART 2 copies of those
        partidos as duplicates of Part 3 and then consume the Part 3
        whole-partido rows normally. So we handle a double count
        between volumes; the loss of locality resolution is invisible
        to us because we collapse to district anyway. It is not
        machinery for the problem item G (2) asks about.
      - 0.7% of rows human-confirmed. See item G (5).
      - (page, n_orden) is NOT unique. Three of the four colliding
        pairs are cross-PROVINCE; the fourth (v3_p08/25, Ensenada vs
        Florencio Varela) is cross-departamento inside Buenos Aires.
        Either way a naive merge moves population across districts. The
        mechanism is the conurbano rows: `page` is a reused page label
        and the footnote(1) partidos sit in their own numbering section
        (his ROADMAP.md:162), which is why Florencio Varela — one of
        the 18 — collides with an ordinary locality row.
      - coordenadas_1960.csv, not poblados_1960.csv, is authoritative
        for population: they differ on 6 rows, net +292, all logged in
        decisiones.csv as step 2.3 correccion_lectura.
      - .gitignore exception is now extension-scoped with *.png/*.jpg
        denied explicitly, verified with `git check-ignore --no-index`
        (the plain form skips indexed paths and cannot catch this class
        of regression — that is how the first pass missed a tracked
        .txt).
      - The 35 scripts are an archived record, not a runnable pipeline:
        19 hardcode Windows absolute paths, and the folder
        reorganisation means the pinned caches are not where they look,
        so a re-run would hit the live Georef API. Open decision in
        item G (8).
- [x] Geocoding 1960 intake — instructions email sent (2026-07-24,
      `Plan/email_cote_geocoding_instrucciones.md`). Closed by the
      entry above: branch pushed, PR opened, reviewed and merged. The
      only deviation from the instructions is that pages/ went via
      Dropbox rather than Drive, and that a name collision forced his
      51 KB protocol to PROTOCOLO.md so the convention readme.md could
      exist (internal links in INFORME/ROADMAP still point at the old
      name — cosmetic, unfixed).
- [ ] Geocoding 1960 — DOWNSTREAM MA INTEGRATION (the bigger, separate
      piece; memo Decision D). Replace load_centroids or move to
      multi-point weights per district. Gated on the θ/τ conversation
      (section 2), on item H, and now also on item G (2): if the 18 GBA
      partidos cannot be broken into localities, the upgrade buys least
      where density is highest, and that changes how much the whole
      exercise is worth. This is the single open home for the
      integration — the section-2 copy was removed 2026-07-29 to keep
      the "exactly once" rule.
      ACCEPTANCE CRITERIA, from what the intake established (these are
      live work, not record, and would otherwise be archived with the
      closed intake entry above):
      - Join on (page, n_orden, localidad_canon) or (provincia_canon,
        departamento_canon, localidad_canon), NEVER on (page, n_orden).
      - Build the geolev2 crosswalk: there is no geolev2 column, and
        georef_depto has 417 distinct non-blank values against our 312
        districts and is blank on 236 rows (7.7%). The blanks are the
        hard part, not the cardinality.
      - Strip U+00AD before any join on nombre_oficial (27 values carry
        an invisible soft hyphen; exact joins fail silently).
      - Add CABA (2,966,634, geolev2 32002001) from
        census_1960_ipums.parquet rather than inheriting the gap.
      - Take population from coordenadas_1960.csv, not
        poblados_1960.csv.
      - Decide the usable-tier rule explicitly, and do not lean on
        `estado` for it (item G (5)).
- [ ] Issue #68 studied-share — SUBSTANTIALLY RESOLVED (PR #119
      reconciliation footnote in the paper; recom_code semantics
      decoded: 1 maintain 2,310 km / 2 close 14,377 km / 3
      new-study 5,197 km; excluding new-study gives 38.4% on §2's
      43,500 km). Awaiting Cote's 10-minute lookup: the report's
      studied definition (excludes new-study?) and its network
      denominator (§2's 15,000 km ~ 32% implies ~46,900 → 35.6%).
      generate_scalars wiring deferred until confirmed (section 2).
      The V-sourcing for Decision A option 1a rides the same
      archive visit.
- [ ] Issue #113 — hypo-instrument node set omits CABA (curation
      artifact); awaiting Cote's sign-off.
- [ ] DNV publication volume (PR #116 review follow-up): dnvseries
      bib entry is year = n.d. until Cote confirms the exact
      volume. Needed before deposit.
- [ ] Controls rationalization — CO-OWNED: Cote took it as homework
      (collinearity, region FE from census regions, threats-based
      selection, what the literature uses; notes #26-#31). Our side
      already has the outcome-blind grid (PR #112) to feed in.

### 2. Unblocked by Wednesday's decisions (work that flows)

Nothing here starts before the meeting; each item lists its trigger.

- [x] ~~[if B confirms] Swap Table 7 to **pop47**~~ — DONE 2026-07-27,
      PR #145, ahead of the meeting rather than after it: the log-pop
      1960 removal is not a judgment call, so waiting on it would have
      left an indefensible control in a published table. All six parts
      below were executed; details and what remains for Cote are in
      agenda item B. Kept here as the record of what the swap touched:
      (a) table_7_pre_trends.R spec edit + regenerate. All 13 placebo
          scalars move, not one (placeboOLSCoef/SE, IVLP, IVH, IVBoth,
          FLP/FHypo/FBoth, coverage); N stays 237.
      (b) THE SIGNIFICANCE WORDS BREAK. Under pop47 no column reaches
          5%: OLS +0.028 (p=0.149), IV-LP +0.051 (0.374), IV-H +0.333
          (0.379), IV-B +0.084 (0.085). So the hardcoded
          "significant at the five-percent level" sentences at
          section_4_empirical_strategy.tex:276 and :286, the intro
          sentence at section_1_intro.tex:105-109 ("rejects a clean
          null at the five-percent level"), and §8.2's "at the
          five-percent level" (section_8_discussion.tex:167) all
          become false. Significance WORDS are prose, not AutoFill —
          they need a manual pass (the standing warning at the top of
          section_5_results.tex).
      (c) §5.2's SELECTION-LOGIC ARGUMENT depends on the placebo's
          SIGN and significance (section_5_results.tex:118-129): it
          uses the positive placebo correlation as evidence that the
          efficiency-selection pattern dominated, which weighs against
          selection explaining the OLS/IV gap. Under pop47 the sign is
          still positive but no longer significant, so that argument
          weakens and the paragraph needs rework.
      (d) CONLEY RERUN REQUIRED. Two claims — intro:105-109 and
          section_8_discussion.tex:170-171 — assert that spatially
          robust SEs "do not soften that rejection". That was measured
          on the t7 spec (p 0.061 HC1 → 0.001 at 100 km). Rerun the
          spatial-SE diagnostic on pop47 before restating either.
      (e) §8.2 limitation 1: reword the pre-trend half from a
          five-percent to a marginal rejection; KEEP the selection
          half; do not delete the limitation.
      (f) Publish the t7 → pop47 → full47 ladder so the reader sees
          which control moves the result, rather than leaving a
          referee to find it.
      Numbers ready in diagnostic_placebo_1947.csv (pop47 variant) and
      diagnostic_placebo_ma1947.csv.
      NOTE: the "pre-trend failure was an artifact" claim does NOT
      appear in the paper — the paper never adopted it. It lives in
      this ledger (corrected above), in
      Plan/memo_identification_measurement_decisions.md, and in the
      brief already sent to Cote. Correcting the brief is a
      conversation on Wednesday, not an edit.
- [x] Cross-object check for pop47 — DONE 2026-07-29 (PR #154).
      diagnostic_crossobject_checks.R Part 1 now runs the adopted pop47
      spec, with an object-consistent baseline MA (`l60` recomputed per
      object, matching Part 2's convention). The answer inverts the
      full47 conclusion; detail and the mechanism in agenda item B.
- [x] ~~[if D concurs] Migration paragraph trim + demote §5.4 Other
      Outcomes to an annex~~ — SUPERSEDED by the confirmed item-D
      position, done in PR #141: §5.4 stays in the main text
      reorganized by what is identified, migration moved to
      Appendix A. Wholesale demotion remains Cote's option (one
      line) and stays on the agenda under item D, not here.
- [ ] [if A settles] Main-spec swap mechanics: config.R edit (PR
      #133) + LaTeX labels / figure axis labels / figure_2 binning
      sweep (list in config.R comment) + full Stage D rerun +
      recompile. If option 1a: V sourcing first (rides #68 visit),
      then productionize diagnostic_ma_iceberg.R's transform into
      the MA step. τ-rebuild design (transshipment costs etc.,
      Plan/tau_rebuild_plan.md) also lands here.
- [ ] [if C or on demand] Wire modern-IV inference into the paper:
      report robust/effective F alongside (or instead of) classical
      F in Tables 8-10, AR sets for the headline cells
      (diagnostic_modern_iv.R computes everything; AR tail grid
      already densified; MOP K=2 critical values COMPUTED, PR #136
      — diagnostic_mop_critical.R has the verdicts ready to quote
      or wire).
- [ ] [after #68 confirms] generate_scalars wiring for the
      studied-share footnote numbers.
      (MA integration of the geocoded 1960 localities used to sit here
      as a second open item. It now lives once, in section 1, with its
      acceptance criteria; the old "[after geocoding lands]" gate is
      spent — the geocoding landed in 00e0334.)
- [ ] [standing, post-A] Sector-specific MA — how it relates to the
      counterfactual exercise (PENDING DECISIONS item; Cote
      discussion).

### 3. Parked until pre-deposit (unblocked, deliberately held)

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
- [ ] Pre-submission figure format decision: the six heavy map
      figures compile from PNG since PR #132 (repo-size fix);
      journal production typically wants vector. Decide whether to
      flip the includes back to .pdf for the final submission /
      AEA deposit (vector PDFs are still produced by every plot
      script, so the flip is six one-line edits + recompile).

### 4. Data-limited (need new raw sources; flagged to Cote)

- [ ] gained/lost national highway (needs raw road-class data)
- [ ] gained/lost railway station (needs additional raw data;
      lp_1979.shp is lines-only)
- [ ] lost railway depot (needs Damus or similar source)
- [ ] Agri intensive-margin outcome + urbanization measurement
      doubts (notes #36/#39) — Cote may fold into his digitization
      track.

### 5. Bookkeeping (small, no urgency)

- [ ] If deposit slips past 2026: move IGN access-year fields + README
      dates together.
- [ ] Post-meeting refactor: promote the diagnostic helper trio
      (tau/pop loaders, geodesic pairs) AND the modern-IV machinery
      (AR inversion, MOP effective F / B(W) / Patnaik cv) into the
      EXISTING code/analysis/_diagnostic_helpers.R. Four copies now
      (PRs #130/#135/#137/#139/#140) and two drifts to reconcile
      when it happens: patnaik_cv returns a bare cv in some copies
      vs list(cv, k_eff) in diagnostic_mop_critical.R (so the k_eff
      columns are silently absent downstream), and one copy dropped
      the beta->Inf derivation comment in B_of_W. Flagged by the
      PR #137 and #140 reviews; deferred deliberately so pre-meeting
      evidence work stayed surgical.
- [ ] Standing gap (structure.md "results are regenerable"): the
      diagnostic_*.{txt,csv} outputs are committed but not produced
      by main.R (same status as every diagnostic since PR #67).
      Decide before deposit whether main.R gains a diagnostics stage
      or the README states they are exploratory artifacts.

### 6. Deferred by explicit decision

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
      meeting's input.
- [ ] [optional, decided — no action required] Vicente López 1960
      digitization discrepancy (Part 2: 241,656 vs Part 3: 247,656;
      one digit). Decision (Diego, 2026-07-16): document and leave as
      is — pipeline uses Part 3, both values pinned in
      clean_census_1960.R. Check the published volume only if
      convenient.

### 7. Completed this cycle (2026-07-24 → 2026-07-25), record

The pre-meeting work program: fourteen merged PRs (#115-#128 wave)
plus the five of 2026-07-25 (#129-#133). Entries kept verbatim as
the verification record.

- [x] PR #115 (settlement road-timing design, balanced but weak dose:
      recentered F ~ 1) — squash-merged 2026-07-24 (eede3e7) after
      published review + fixes.
- [x] Corridor-timing design instrument — DONE, PR #117 squash-merged
      2026-07-24 (1e66852) with published review + fix pass. Verdict:
      real dose (recentered F 3-4.5, order of magnitude over the
      settlement design) but loaded dice (early corridors traverse
      districts with faster 1947-60 placebo growth, b=+0.074 p=0.039;
      unadjusted placebo IV p=0.025). Known limitation recorded:
      snap-tolerance sensitivity unexplored (needs recompute).
- [x] Branch-sync step — done 2026-07-24 (clean merge, no conflicts)
      before PR #117; branch deleted after squash-merge.
- [x] Paper fixes settled by Cote's email — ALL TEN DONE, PR #116
      squash-merged 2026-07-24 (closed issues #91 and #103
      automatically). Items: (a) Larkin year 1962 + announcement
      footnote; (b) 1954 census agency (DNEC, Secretaría de Estado
      de Hacienda) in bib; (c) DNV-vs-ACA road-km footnote + new
      dnvseries data citation; (d) B&P El Trimestre URL, CONADE
      resolved; (e) "discontinuity" removed §1/§2.2, "Argentine
      restructuring" referent; (f) θ = 4.55 declared midpoint, SW
      4.1 cited, provenance placeholder retired (Decision A flag
      kept); (g) sectoral θ-robustness sentence in §5.5; (h) Table
      15 rows low→high (CSV order-only); (i) Table 6
      self-partialling note + explicit setdiff in code + §4.5.1
      full control list; (j) §8.2 Gibbons gap = granularity
      (\meanDistrictArea macro, censo1960pop cited). Review
      published; blocking finding fixed in the same PR.
- [x] Studied-share reconciliation footnote — PR #119, squash-merged
      2026-07-24 (see the open #68 item in section 1 for what
      remains with Cote).
- [x] Placebo spec with 1947 baselines (Cote 1.1) — DONE, PR #120
      merged 2026-07-24. HEADLINE AS RECORDED THEN: the placebo
      failure is a post-outcome-conditioning artifact (1960 baselines
      postdate the 1947-60 window; log pop 1960 is a DV component).
      With the 1947-consistent set the placebo is a clean null in all
      four estimators (IV-B -0.004, p=0.89) and first stages
      STRENGTHEN (F 16-24). Scope: placebo-specific; 1960 baselines
      stay legitimate for the main 1960-91 regressions.
      ⚠ SUPERSEDED IN PART by PR #143 (2026-07-27): the "artifact"
      reading does not hold. The clean null comes from dropping the
      baseline-MA control, not from the 1947 population swap, and the
      post-outcome justification for dropping it is unsupported. The
      adopted spec is pop47, which still rejects at 10% (p = 0.085).
      Read this entry as the dated record of what #120 found, and
      agenda item B for the current position.
- [x] Manufacturing robustness exhibit (note #44) — DONE, PR #121
      merged 2026-07-24: sectoral theta-sweep is now a paper exhibit
      (tab:theta_sweep_sectoral; mfg value/wage significant at every
      theta, max p = \sweepSectoralMaxP = 0.014; establishments + ag
      null throughout). BONUS: the archive CSV was stale (PR #71
      vintage); refreshed to current pipeline, matches Table 10 to
      float precision. Sweep wired into main.R as D.13h.
- [x] Navigation/ports map (note #19) — DONE, PR #122 merged
      2026-07-24: Appendix Figure A4 + §3.3 prose. Answer: inland
      Parana-Plata system (8,062 km geodesic) + a few Patagonian
      rivers + Magellan crossing; NO open-ocean coastal shipping;
      Atlantic ports connect by land/river legs only. Also wired the
      never-included A2/A3 into the appendix (paper 47 -> 51 pp).
      (Its +8.8 MB PDF-bloat note: RESOLVED by PR #132 below.)
- [x] Fused-instrument (BH-2026 Stage 3) S=100 run — DONE, PR #123
      squash-merged 2026-07-24 (with published review + fix pass).
      VERDICT: efficiency gain is real (recentered F 9.6 -> 15.9 vs
      the studied-only backbone) but estimates are nulls everywhere;
      pairing integrity asserted via the studied_km fingerprint
      (deviation 0.0e+00). Feeds the recentering characterization.
- [x] Growth-stratified corridor repair — DONE, PR #124
      squash-merged 2026-07-24 (with review + fix pass). VERDICT:
      door closed with a number. F recentrado 0.5-1.4 (base 3-4.5)
      with recentered variance intact (sd 0.964 vs 0.956): the
      timing entropy WAS demand, not mechanical over-stratification.
      Residual continuous placebo imbalance within terciles (+0.042,
      p=0.045) corroborates. RECENTERING PROGRAM COMPLETE: six
      designs, one characterization → agenda item E.
- [x] §2.4 conceptual channels — DONE, PR #125 merged 2026-07-24
      (margin-of-change + network geometry paragraphs, hypotheses
      flagged, anchored to Figure 1; empirics deferred per Cote's own
      note). Review considers left for Cote: §6.3/§8.3 loop-closing
      sentence; durability premise used differently in §2.4 vs §8.2.
- [x] Gibbons-style decay — DONE 2026-07-24, PR #127 (variant
      "gibbons" on the sectoral sweep script; theta in {0.25, 0.5,
      0.75}; diagnostic only, paper exhibits untouched). HEADLINE:
      population turns marginally significant at low decay (+0.49*
      at 0.75 rising to +1.50* at 0.25 — through and past the
      Gibbons ~0.3 benchmark) and the sectoral pattern SURVIVES
      (mfg value/wage ***, ag and establishments null), F 10-15.
      → agenda item A.
- [x] Recentering brief — COMPLETE 2026-07-24
      (`Plan/brief_cote_recentering_2026-07-29.md`): all six design
      verdicts in, placebo-1.1 breakthrough integrated, prose pass
      done. Converted to a sendable email
      (Plan/email_cote_recentering_estado.md); SENT by Diego.
- [x] Sectoral counterfactual — DONE, PR #128 squash-merged
      2026-07-25 (Table 17, tab:counterfactual_sectoral, mirrors
      Table 13's panels for the five Table 10 outcomes;
      regression-only, MA columns existed). FINDING (answers Cote
      note #40): manufacturing responds through BOTH modal channels
      (only-rail +0.215*/+0.222* val/wage; only-road
      +0.323*/+0.471**), estimates not statistically
      distinguishable across channels; establishments and
      agriculture null in every panel. The rail-loss channel is
      real (consistent with a de-industrialization component) but
      the road channel is at least as large. Wired: §6 prose + 12
      AutoFill macros + main.R D.13i; §8.3's false frequency claim
      retired.
- [x] Shared table formatters extracted to
      code/analysis/_table_helpers.R (fmt + tex_cell + star_str;
      tables 12-17) — PR #129, squash-merged 2026-07-25 (cr-review
      PR #128 consider C1). Byte-identical outputs verified for all
      six tables. The two theta-sweep diagnostics keep their own
      cells (different signatures; not duplicates). table_14 keeps
      its single-use tex_cell_or_blank.
- [x] Stale gitignored .tex after worktree-built PRs — RESOLVED in
      PR #129 (recompile folded in). Full staleness audit of all 23
      paper inputs (16 table .tex + 7 figure .pdf + scalars) found
      two real casualties: table_15_density_schedules.tex was
      pre-#116 (Medium/High/Low row order; Cote note #37 reordering
      never reached the PDF) and table_6_pre_balance.tex had
      pre-#119 notes (missing the geographic-controls +
      self-partialling sentences). Both regenerated; paper.pdf
      recompiled and committed (52 pp, zero undefined, Table 15
      order and Table 6 notes verified in the PDF text). All other
      flags were mtime false positives (byte-identical on re-run).
      Process rule going forward: after merging a worktree-built PR
      that touches table/figure scripts, re-run those scripts in the
      main tree before the next paper.pdf commit.
- [x] τ normalization experiment (Cote 1.5 / memo point ii; the
      second cheap experiment for the θ/τ conversation, after the
      Gibbons decay) — DONE 2026-07-25, PR #130 squash-merged,
      diagnostic_ma_iceberg.R (diagnostic only, paper untouched;
      §3.3 placeholder stays until Decision A). Affine iceberg
      τ' = 1 + cost/V, V-sweep at θ=8.22 and 4.55. HEADLINE: β(V)
      rises monotonically as τ' compresses toward the D&H band —
      IV-B at θ=8.22 goes 0.024 (raw) → 0.067 (V=4,400 p/t, τ'
      p10-p90 1.36-3.40) → 0.12-0.32 at high V — but no point on
      either curve clears the 5% LEVEL, including the raw anchor
      itself (p=0.096, the published main spec); the t-stat peaks
      at V≈100-500 (p=0.055 at θ=4.55 V=100, no weaker than the
      main spec) before precision decays. The combined-F
      strengthening (13→37) is ENTIRELY the hypo instrument (IV-H F
      7.9→65.5 at θ=8.22 while Larkin F falls 15.6→9.7): the
      normalization REVERSES the instrument-strength ranking, which
      feeds the IV-LP-only main-spec question. The Gibbons ≈0.3
      crossing lives only in the degenerate tail (V=100k p/t ≈ 23×
      the median raw cost). θ·β rescaling from the decay experiment
      roughly holds (~20% loose at raw/low V, within ~3% by
      V=4,400). V not sourced yet (rides the #68 archive visit);
      honesty rule: V gets picked by the source, not by where β
      lands. Raw anchor reproduces the pipeline with zero diff,
      asserted in code. → agenda items A and C.
- [x] Modern IV inference check (note #35) — DONE 2026-07-25, PR
      #131 squash-merged, diagnostic_modern_iv.R (diagnostic only,
      paper untouched). 11 headline IV cells (Table 9 pop ×
      IV-LP/IV-H/IV-B, Table 10 sectoral × IV-B): classical F vs
      robust Wald F vs MOP effective F + 95% AR sets by robust
      inversion. HEADLINES: (a) the mfg results SURVIVE weak-IV
      robust inference — AR sets exclude zero (valprod [0.035,
      0.678], wage [0.136, 0.732]); (b) every AR set is bounded,
      population sets include zero (consistent with published
      nulls); (c) the IV-B (K=2) effective Fs run 1.5-3.3 points
      below the classical Fs the paper quotes (total pop 13.1 vs
      16.2); the K=1 IV-LP cell is essentially unchanged and
      slightly STRONGER under robust inference (22.6 vs 22.1),
      while the hypo instrument weakens (IV-H F 6.9 classical →
      4.3 robust); (d) hand-rolled F_eff validated exactly against
      fixest ivwald on the K=1 cells and cross-checked against the
      ivDiag reference implementation. MOP K=2 critical values not
      computed at the time (since computed: PR #136 below). Wiring
      into the paper = post-Wednesday decision (section 2).
      → agenda item C.
- [x] \doi macro verbatim-safety — FIXED 2026-07-25, PR #132
      squash-merged: catcode-based doi.sty-pattern definition;
      hostile DOIs (% and #) need no escaping. Verified via
      standalone test compile against 10.1000/weird%20case#frag;
      all 15 in-paper DOIs render (whitespace-insensitive match
      against the .bbl call sites), including the two structurally
      unusual note-field DOIs (IPUMS 10.18128/D020.V7.3, USGS
      10.5066/F7DF6PQS).
- [x] Gitignore treatment of logs/makelog.log and
      logs/session_info.txt — DECIDED 2026-07-25 (Diego): ignore
      both, consistent with main.Rout (per-run artifacts; the AEA
      deposit produces them fresh at the final clean-machine run).
      Two lines added to .gitignore, PR #132.
- [x] paper.pdf bloat from embedded vector maps (ledger note from PR
      #122: +8.8 MB per recompile) — FIXED 2026-07-25, PR #132
      squash-merged: the six heavy map figures (1, 2, A2, A3, A4,
      C13) compile from their existing PNGs (1400-3600 px;
      effective ~253 dpi at the figure_2 minimum to ~554 at the c13
      maximum, given textwidth 6.5in / 0.85x = 5.525in); figure_a1
      stays vector (5 KB). Vector PDFs still produced to
      results/figures/ by every plot script (unchanged). Committed
      paper.pdf: 17.95 MB -> 3.18 MB (-82%), 52 pp, zero undefined,
      all 15 DOIs render. Pre-deposit flip-back decision tracked in
      section 3.
- [x] Cross-decision checks (A×B and A×C completion) — DONE
      2026-07-26, PR #139, diagnostic_crossobject_checks.R
      (diagnostic only). PART 1 (A×B): the full47 placebo spec run
      on every candidate τ object (raw anchor, decay θ=0.5,
      iceberg V=4,400/20,000, 1b both θ). VERDICT: no placebo
      rejection on any candidate object — smallest any-IV p = 0.34,
      IV-B p range 0.34-0.95, IV-B placebo first stages F
      21.5-61.1. Caveat: the IV-LP first stage collapses on the
      iceberg objects (F 3.7 / 1.6), so those IV-LP nulls are
      low-power; the IV-B/IV-H cells carry the verdict there. No
      A×B interaction detected on the candidate set — ⚠ TRUE OF full47
      ONLY; PR #154 reran Part 1 on the adopted pop47 spec and the
      placebo rejects at 10% on five of six objects. See agenda item B.
      PART 2 (A×C
      decay cell + MOP, the latter added in the fix pass after the
      review caught an inconsistent evidentiary standard): mfg
      value/wage stay significant at 5% under IV-LP across the
      whole decay grid (max p = 0.030, classical F(LP) 9.2-15.0) —
      the only candidate grid where that holds (iceberg: low V
      only; 1b: nowhere) — but by the MOP standard that judged 1b,
      decay IV-LP is also weak (F_eff 10.7-16.3 vs K=1 cvs
      23.11/15.06 → 0/18 pass at 10%, 1/18 at 20%). Honest
      summary: decay = least-weak IV-LP candidate; no object makes
      IV-LP-only MOP-strong. Anchors asserted: raw placebo row ==
      diagnostic_placebo_1947 full47 (all four estimators + N=237,
      constant across objects); decay IV-B ==
      diagnostic_theta_gibbons (18 cells incl. n_obs); BA-Rosario
      geodesic; CSV union row-count guard.
      → agenda items B and C(iii-b).
- [x] Route-inefficiency tau (Decision A option 1b) — DONE
      2026-07-25, PR #137, diagnostic_tau_inefficiency.R
      (diagnostic only; completes the three-option evidence set
      for Decision A). τ' = cost/(c_min × geodesic distance
      between the 03c centroids), pair-varying, zero external
      data. FINDINGS: τ' p10/p50/p90 = 4.5/9.4/25.5 with 0% of
      pairs below 1; Δlog MA correlates 0.88 with the raw-object
      treatment; IV-Both sectoral contrast HOLDS (mfg value
      p=0.003-0.004, wage p=0.001, at both θ; establishments/ag
      min p=0.12; population null p=0.15-0.17). Quantified costs:
      classical IV-B F 8-12 (vs 13-16 raw); MOP test (machinery
      from PR #136, added in the fix pass): population + mfg cells
      PASS at 10% (F_eff 8.2-8.7 vs cv 6.5-7.4) but both ag cells
      FAIL even at 20% (F_eff ≈ 7.0 vs cv 10.2-11.5); ag farms p
      thins 0.22 → 0.12. IV-LP on the 1b object is MOP-weak
      everywhere (F_eff 3.4-5.7 vs 23.1; mfg value p=0.047 at
      θ=4.55, 0.069 at 8.22). Referee caveat carried: τ' is not
      the D&H narrow-band object. Anchors asserted: BA-Rosario
      geodesic = 265.9 km (scoping-note value); distance matrix
      symmetric/positive/zero-diagonal; per-outcome Ns match
      Tables 9/10. → agenda item A(v).
- [x] MOP critical values for the effective F — DONE 2026-07-25,
      PR #136, diagnostic_mop_critical.R (diagnostic only;
      completes note #35's remaining gap). Algorithm verified against Windmeijer
      2023 (arXiv:2309.01637) Sec. 3 = Stata weakivtest: exact
      Nagar-bias bound B(W) (eigenvalue-analytic inner sup + 1-D
      numeric outer sup), Patnaik k_eff, noncentral-chi2 cv;
      tau ∈ {5,10,20,30}%, alpha 5%. VERDICTS: 8/9 IV-B cells PASS
      at tau=10% (rural pop fails 10%, passes 20%); IV-LP alone
      borderline (22.64 vs 23.11); IV-H fails all. Attribution
      (per the PR #136 review, which independently reimplemented
      the machinery): the low exact cvs (6.5-9.7) come from the
      Nagar-bias bound B ≈ 0.15-0.30, not from K=2 per se — the
      conservative B=1 cvs are 19.7-20.5 and every IV-B cell would
      fail those at 10%. Residuals per MOP's reduced-form setup
      (v1 = u + βv2; the structural-residual variant was verified
      immaterial, no verdict flips). Anchors asserted in code:
      F_eff == diagnostic_modern_iv.csv digit-for-digit (11
      cells); conservative K=1 cv(10%) == MOP's published 23.1;
      B <= 1 everywhere; k_eff = 1 exactly at K=1; exact cv <=
      conservative cv. → agenda item C(iv).
- [x] Sectoral outcomes on the iceberg V-sweep — DONE 2026-07-25,
      PR #135, diagnostic_ma_iceberg_sectoral.R (diagnostic only;
      companion to PR #130, theta_sweep-pair precedent). VERDICT,
      two parts: (a) under IV-BOTH the paper's headline contrast
      survives the iceberg object at EVERY grid point and both θ —
      mfg value max p = 0.013 (θ=8.22) / 0.009 (θ=4.55), wage max
      p = 0.004/0.002; establishments + both ag outcomes never
      significant (min p = 0.127); (b) under IV-LP (added per the
      PR #135 review — the IV-LP-only question is on the agenda)
      the contrast holds only at low-to-mid V (** at V=100-1000;
      already just * at raw, per Table 10) and fades to null at
      high V (max p 0.18/0.33), consistent with the Larkin F
      falling with V (PR #130): Decision A and the instrument
      choice interact. β levels grow by an order of magnitude
      along the grid as Δlog MA compresses; the growth factor is
      OUTCOME-DEPENDENT because the transform is nonlinear (θ=8.22
      raw → V=100k: wage ×41.7, population ×13.1) — levels are
      object-dependent, the contrast (under IV-Both) is not.
      Verified in code with row-count-guarded assertions: raw θ-low
      anchor reproduces Table 10 IV-B AND IV-LP (all 5) and Table 9
      IV-B and IV-LP (population) exactly; population rows match
      diagnostic_ma_iceberg.csv at every cell, both specs.
      → agenda item A(iii), feeds C too.
- [x] Main-spec column names centralized — DONE 2026-07-25, PR #133
      squash-merged (PR #131 review consider C4): main_treatment +
      main_lp_instrument now live in config.R next to
      main_hypo_instrument; the 17 paper-surface scripts that
      carried the literals (tables 6-15, 17, a1 — table_16 had
      none — figures 2/4/c13, generate_scalars,
      build_estimation_sample) use the constants. Changing the main
      spec after Decision A is now a one-line config edit (plus the
      label/binning sweep listed in config.R's comment).
      Diagnostics keep their own literals (archived experiments).
      Verified: all 17 touched scripts rerun; 24 table files + 3
      PNGs byte-identical, scalars identical net of timestamp,
      estimation-sample parquet content-identical.

Closed by Cote's 2026-07-24 email (implementation = PR #116 above):

- [x] Theta 4.55 provenance — CLOSED: he does not recall the source;
      4.55 declared a midpoint with Simonovska-Waugh ~4.1 cited
      (adopting 4.1 as the computed θ would force a full MA/tau
      recompute; not done). Decision A flag kept.
- [x] Abstract wording sign-off — CLOSED for now ("me sirve como
      está por ahora"); Cote rewrites it himself at publication
      time, together with title/narrative.
- [x] Larkin Plan canonical year — CLOSED: report cover + elevation
      letter dated 1962 (Ministerio de Obras y Servicios Públicos);
      1961 = Frondizi's announcement.
- [x] Issue #91: 1954 industrial census issuing agency — CLOSED
      against the physical title page: Dirección Nacional de
      Estadística y Censos (Secretaría de Estado de Hacienda),
      Buenos Aires 1960; "INDEC" in catalogs is anachronistic
      (INDEC created 1968). Source:
      bibliotecadigital.estadistica.ec.gba.gov.ar cn1958i post.
- [x] Issue #103 (35,000 vs 79,820 km) — CLOSED: source difference.
      §2's ~35,000 = DNV national-network series
      (`Train/raw_data/kms_road_arg/kmVia_DNV`: 27,276 paved +
      7,153 gravel in 1986, excludes dirt); §3.1's 79,820 =
      digitized ACA network (broader).
- [x] B&P source volume — CLOSED: El Trimestre Económico article
      ("Estructura económica del transporte de carga automotor y
      ferroviario en la Argentina"), NOT a CONADE report. URL:
      eltrimestreeconomico.com.mx article 3317. (Exact DNV volume
      still open — section 1.)

### 8. Completed 2026-07-26 → 2026-07-27, record

The second pre-meeting wave. Section 7 above stops at PR #133; these
are the twelve that followed (#134-#145). One line each, keyed to the
agenda item they serve — the substantive findings live in the agenda
items themselves (section 0), not here.

- [x] PR #134 — ledger reorganized around the Wednesday agenda
      (OPEN ITEMS restructured into sections 0-7).
- [x] PR #135 — sectoral outcomes on the iceberg V-sweep: the
      manufacturing/agriculture contrast survives at every V and both
      θ under IV-Both (item A de-risking).
- [x] PR #136 — Montiel Olea-Pflueger critical values for the
      effective F; 8 of 9 IV-B cells pass at 10% bias tolerance
      (item C evidence, closes reading note #35).
- [x] PR #137 — route-inefficiency τ (Decision A option 1b)
      quantified: contrast holds under IV-Both, both agricultural
      cells go MOP-weak (item A).
- [x] PR #138 — ledger records the pre-meeting brief + pendings
      refresh.
- [x] PR #139 — cross-decision checks: A×B (placebo clean on every
      candidate τ object, UNDER full47 — superseded for the adopted
      spec by PR #154) and the decay IV-LP cell, completing the
      A×C matrix.
- [x] PR #140 — modern weak-IV inference for the four Table 11
      outcomes: the overidentification test rejects in three of four
      (the item-D headline fact).
- [x] PR #141 — §5.4 restructured on the confirmed item-D position;
      Table 11 gains an overidentification row; migration moved to
      Appendix A. Detail in item D, section 0.
- [x] PR #142 — appendix exhibit prefixes match their appendix
      (Table B1, Figures B1-B4).
- [x] PR #143 — item-B evidence, two parts. Part 1 probed whether
      full47 is defensible: a baseline-MA level re-weighted with 1947
      population (correlating 0.9994 raw, 0.9990 partial, with the
      1960-weighted one) leaves β at +0.078, so MA(1960)'s post-1947
      population content is too small for "it is post-outcome" to
      justify dropping the control. Part 2 is the direct
      pre-1960-growth test; manufacturing survives. Detail in agenda
      items B and B2.
- [x] PR #144 — ledger records the adopted item-B position, adds item
      B2, and rewrites the downstream work item. Its own review caught
      four overstatements in that text, including a pre-registration
      claim that was not true; corrected in the same PR (see the note
      below).
- [x] PR #143 note — HONEST NOTE ON HOW THE REVERSAL HAPPENED
      (cr-review PR #144):
      the reading guide WAS fixed before the numbers were seen, but
      read literally the outcome mapped to its second scenario
      (neither constructed control rejects at 10%), which favours
      full47. The reversal rests on the collinearity argument and the
      coefficient pattern, assembled after seeing the numbers. The
      pre-committed note did anticipate that a correlation near 1
      would weaken the post-outcome case, but the scenario mapping did
      not, so this is not a case of pre-registration vindicating a
      call. Recorded because the temptation to claim otherwise is
      exactly what the pre-commitment was for.

- [x] PR #145 — Table 7 swapped to pop47 (placebo_controls in
      config.R), appendix Table B2 publishes the four-row baseline
      ladder, log_pop_1947 becomes a stored column in
      build_estimation_sample.R, the Conley diagnostic carries a
      per-spec control set and was rerun, and the placebo prose was
      updated in §1, §4.6, §5.2 and §8.2. Two new scalars
      (\placeboOLSP, \placeboIVBP) so the significance claims are
      p-values rather than words — the words were what broke. 55 pp,
      zero undefined, zero warnings. Detail in agenda item B.

- [x] PR #146 + #150 — Cote's 1960 geocoding intake, 3,063 localities,
      merged as 00e0334. Archive checksum verified against the readme,
      the "sin fuente no hay coordenada" rule verified to hold, and the
      file reconciles to 99.90% of our own non-CABA 1960 total. Blockers
      were ours, not his, and went in as #150 on his branch: the
      recursive .gitignore, and readme statements on coverage, QC, the
      join key and the citation. #150's own review then caught four
      overstatements in my write-up — the QC claim wrong by ~50x, the
      licence counts undercounting the two share-alike sources, an
      undocumented 27.85% of population on 18 GBA aggregates, and a
      .gitignore check that could not detect its own failure mode — all
      fixed before merge. Detail in section 1 and agenda item G. NOTE
      FOR FUTURE PASSES: reviews have now caught claims in the
      WRITE-UP rather than errors in the numbers on #144, #145, #147,
      #149, #150 and #151 — an unbroken run, not the "four" an earlier
      version of this entry counted. Treat summaries as claims to
      verify, not as prose.
- [x] PR #149 — universe-comparable placebo. Established that Table 7's
      rejection is fragile to how the outcome is measured (apparent
      declines 143/237 vs 27/234; published outcome correlates +0.304
      with the coverage proxy vs +0.100) while the slope movement is
      not distinguishable from zero (difference test p 0.11-0.30) and
      the ten-percent crossing comes from a three-district sample
      change rather than the outcome swap. Also settled in-repo that
      the 1947 and 1960 urban definitions share the 2,000 rule
      (smallest Cuadro 14 centre is 2,002, none below), recorded in
      censo1947/readme.md. Its review caught a wrong p-value (0.039
      for 0.065) that this ledger had inherited, and a headline
      asserted without the test that was one line away.
- [x] PR #147 — the 1960 population universe mismatch, found while
      reviewing #146. See agenda item H for the finding and what it
      does and does not settle. Its own review found five blocking
      problems, four of them overstatements in the write-up rather
      than errors in the numbers: the orthogonality claim rested on
      raw instead of control-conditional correlations, and the script
      had written a stopping rule it then made impossible to trigger.
      Rebuilt; the rule now fires and Part 1 reports INCONCLUSIVE. The
      review also supplied the better argument the fix pass adopted
      (the 1947 district-totals comparison).

APPENDIX EXHIBIT RENAME (PR #142, 2026-07-27). The appendix now has
two lettered sections — A. Recent Migration (prose) and B. Additional
Tables and Figures — and exhibit prefixes match the letter of the
appendix holding them. Before, the floats were prefixed A while
sitting under two UNNUMBERED headings, so "Table A1" appeared to
belong to Appendix A, which has no exhibits. The mapping:

      table_a1_descriptives           -> table_b1_descriptives
      figure_a1_cost_schedule         -> figure_b1_cost_schedule
      figure_a2_hypothetical_networks -> figure_b2_hypothetical_networks
      figure_a3_larkin_studied        -> figure_b3_larkin_studied
      figure_a4_navigation            -> figure_b4_navigation

Scripts, main.R steps D.15-D.19, results/ files, the paper.tex
includes, README's exhibit table, and the in-code exhibit labels all
moved with them; compiled numbering is Table B1 and Figures B1-B4.
Cross-references go through labels, so the in-text numbers updated
themselves. In paper.tex the prefix is derived from \thesection with
\counterwithin*, so a future Appendix C needs no further edit.

DATED RECORDS KEEP THE OLD NAMES deliberately — rewriting them would
falsify what PRs #80/#104 actually produced. Survivors sit in section 7
above, CURRENT STATUS, Done (Block 1 core), and the DEFERRED LEDGER;
they are dated statements about what those PRs shipped, so read them
through the mapping. The forward-looking exhibit specs under CODE TASKS
> Figures (C15, C16, C17) DID move, since a reader consults those for
which script builds an exhibit. And "Appendix Table A1" in the ledger's
Gibbons comparisons refers to THEIR table, not ours: it stays.

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
6. **Pre-trends not clean null** — REOPENED 2026-07-27, see agenda item B. Was struck through as "superseded by memo Decision E" when the 1947-consistent spec looked like a clean null; PR #143 showed that reading does not hold. Published Table 7 numbers are OLS +0.0386** and IV-Both +0.0870** on the 237-district placebo subset (the old entry's 0.035/0.078 and "235 districts" were stale). Under the adopted pop47 spec: OLS +0.0275 (p=0.149), IV-Both +0.0839 (p=0.085) — a marginal rejection, not a clean null.
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

#### C15. Transport cost schedule (Figure B1)
Road and rail costs vs cargo density from Baumgartner & Palazzo (1969). Shows crossover interval (500-1,000 t/day; the three tabulated points do not identify a crossover point). **Done: `code/analysis/plot_figure_b1_cost_schedule.R` (PR #80). Densities read from `cost_density` in config.R.**

#### C16. Hypothetical network maps (Figure B2)
Four panels: Euclidean bilateral, LCP bilateral, Euclidean MST, LCP MST. Overlaid on actual 1986 roads. **Done: `code/analysis/plot_figure_b2_hypothetical_networks.R` (PR #80). 1986 selector read from `roads_type2_1986` in config.R; CRS asserted per layer.**

#### C17. Larkin Plan studied segments map (Figure B3)
Studied vs non-studied rail segments. **Done: `code/base/networks/plot_figure_b3_larkin_studied.R` (PR #80). Legend reports km, not share, pending issue #68.**

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

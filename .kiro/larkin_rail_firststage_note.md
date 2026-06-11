# Larkin first stage on MA_rail (Cote point 1): result

Question (Cote): does the Larkin instrument predict the **rail-only**
component of market access? If yes in both the baseline (HMI) and the
connector-recost surfaces, then the F≈0 for Larkin on *total* MA under
the re-cost is mechanical (total MA dominated by road MA), not a loss of
instrument validity for the rail channel.

Script: `code/analysis/diagnostic_ma_rail_firststage.R`
Report: `results/tables/diagnostic_larkin_rail_first_stage.txt`
Outputs: `data/derived/06_analysis/rail_firststage_variant/` (gitignored)

## Method

MA_rail = MA on the rail-only cost surface (rail + off-network land, no
road, no nav), built via `MODE_VARIANT=railonly`. First stage (LP only):

- endogenous: ΔlogMA_rail_86_60 = logMA_rail(1986) − logMA_rail(1960)
- instrument: ΔlogMA_rail_stu  = logMA_rail(instrument_stu) − logMA_rail(1960)
- controls: `geo_controls_main`; F via `fitstat_F()` (`ivf`), same statistic
  as the baseline tables. Second stage on population reported as a bonus
  (the rail-MA estimand).

τ built: baseline `instrument_stu_s0_railonly` (actual_1960/1986 railonly
already existed); re-cost railonly for all three (connector to nearest rail
re-cost to `cost_road[overall]`), isolated in `rail_firststage_variant/`.

## Result — confirms Cote's hypothesis

F is reported two ways: `ivf` (IID, the statistic the baseline tables use
via `fitstat_F` — comparable to the total-MA F) and robust HC1 (= t²).

| spec | Larkin→MA_rail F (ivf / robust) | (ref) Larkin→total-MA F (ivf) |
|------|---------------------------------|-------------------------------|
| Baseline (HMI) | **75.6 / 22.3** (coef +0.277, t=4.7) | 19.3 |
| Connector re-cost | **237.9 / 38.3** (coef +0.538, t=6.2) | ~0 |

Larkin predicts MA_rail strongly in BOTH specs (every F ≫ 10) — stronger
after the re-cost. So the total-MA F~0 under re-cost is a **composition
effect**: cheap road connectors make total MA road-dominated, so a rail
instrument can't move total MA, but Larkin's grip on the rail channel is
intact (sharper, even). **The instrument is valid for the rail channel.**

Bonus (rail-MA IV beta on population): +0.043 (baseline) / +0.026 (re-cost)
— small, consistent with the total-MA headline (+0.046 / +0.063).

Regression N = 309 (CF + 2 TdF drop on missing pop/controls). The rail-only
surface leaves a few districts disconnected (e.g. Tierra del Fuego; 620 Inf
pairs per τ, which contribute 0 to MA).

## Implication / caveat (decision, not result)

Using Larkin therefore identifies the **rail-MA** effect cleanly. But that
shifts the estimand from "effect of total (multimodal) MA" to "effect of
rail MA" — a framing decision for the coauthors, flagged for the meeting.
Connects to PENDING DECISIONS #8 (IV-LP-only vs two-instrument headline):
this strengthens the case that Larkin is a clean rail-channel instrument.

## Cross-refs
- `.kiro/connector_recost_note.md` (the total-MA identification flip this explains)
- `code/analysis/diagnostic_ma_rail_firststage.R`
- tasks.md PENDING DECISIONS #8

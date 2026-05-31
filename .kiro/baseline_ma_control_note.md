# Should baseline log MA be a control? (Cote review Q2)

Short answer: yes, it is the standard convergence / lagged-level control,
and it is economically motivated. But it is also the single most
influential control in the spec — it doubles the headline coefficient —
so it should be stated as a deliberate decision, not a default.

## The spec

    Delta Y_i = beta * Delta log MA_i + X_i' gamma + eps_i

X_i currently includes baseline log MA (1960), i.e. the LEVEL of the
treatment variable at the start of the period, alongside log pop 1960 and
the six standardized geographic controls (geo_controls_main in config.R).

## Why include it (the economic case)

This is a growth-style first-difference regression. Controlling for the
initial level of the regressor is standard:

- It separates "the effect of CHANGING market access" from "the effect of
  WHERE market access started." Without the initial level, beta conflates
  the two.
- It absorbs mean reversion / convergence dynamics in the network:
  isolated (low-MA) districts had the most room to gain access when roads
  expanded, and may have had systematically different population
  trajectories for reasons unrelated to the MA change itself.
- D&H and the spatial-MA literature standardly condition on initial
  conditions for this reason.

## Why it matters empirically (our data)

    cor(baseline log MA_1960, Delta log MA_86_60) = -0.468

Strong NEGATIVE correlation: districts that started with low MA gained the
most. So baseline MA and the treatment are mechanically entangled.

Effect on the headline IV-Both population elasticity:

    WITH    baseline log MA control:  beta = +0.046 (0.033)  F = 13.6
    WITHOUT baseline log MA control:  beta = +0.020 (0.022)  F = 29.9

Including the control MORE THAN DOUBLES the coefficient (0.020 -> 0.046).
The mechanism: because low-baseline districts both gained the most MA and
had distinct growth dynamics, omitting the baseline level biases beta
toward zero (the change variable partly proxies for "started isolated").
Conditioning on the baseline removes that, raising beta.

(Note the first-stage F also moves: 29.9 without -> 13.6 with. Adding the
baseline level, which is correlated with the instruments, absorbs some
instrument variation. Still well above the weak-instrument threshold.)

## Takeaway for the team

- Keep the control. It is the right specification on economic grounds.
- BUT flag it as a load-bearing decision: the headline 0.046 vs 0.020
  depends on it, just as it depends on theta (PR #67 sweep). The two
  sensitivities compound.
- For the paper: report the spec with the baseline control as the main
  result, and show the without-control version in robustness so the
  dependence is transparent. This is the same "state the decision, do not
  bury it" logic Cote applied to theta and the MA construction.

## Cross-refs
- .kiro/theta_benchmark_note.md (the other load-bearing parameter)
- Table 9 (main spec uses the control); config.R geo_controls_main
- PR #67 (theta sweep)

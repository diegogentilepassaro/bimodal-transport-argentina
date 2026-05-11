# Paper

Self-contained LaTeX source for the paper. Each section lives in its
own `.tex` file and is `\input{}`ed from `paper.tex` when that master
file is eventually written.

## Authoritative source

`paper_planned.pdf` is the coauthor-maintained snapshot of the
complete planned paper (copied from `Plan/paper.pdf`). When porting
a new section into LaTeX source, this PDF is the source of truth.
Keep it in sync with `Plan/paper.pdf` when the planning doc is
updated.

## Current contents

| File | Section | Status |
|---|---|---|
| `paper_planned.pdf` | Full planned paper (coauthor draft) | Reference |
| `section_1_intro.tex` | Section 1 (Introduction) | Ported from PDF |
| `section_2_history.tex` | Section 2 (Historical Context) | Ported from PDF |
| `section_3_data.tex` | Section 3 (Data) | First draft (cost table now in) |
| `section_4_empirical_strategy.tex` | Section 4 (Empirical Strategy) | Ported (scaffolded) |

## Writing conventions (Shapiro Robot mode, see `Plan/foursteps.pdf`)

- State facts, do not argue. Interpretation and discussion go in later
  sections.
- Define every concept before using it.
- No forward references to constructs that haven't been introduced yet.
- Plain precise language, no fancy talk.

## Numbers and tables

All quantitative claims in the section files should eventually be
pulled from `results/scalars.tex` via AutoFill macros so no number is
ever hand-typed. For the current first draft, numbers are inlined
with source comments at the top of each file pointing to the script
and file that generated them.

TODOs flagged as `[PLACEHOLDER: ...]` are bracketed decisions or open
tasks in `Plan/tasks.md`.

## Cross-references between sections

Section 2 references `Section~\ref{sec:data}` (defined in
`section_3_data.tex`) and `Table~\ref{tab:cost_params}` (not yet
created). These resolve when a master `paper.tex` inputs all
sections together.

## Missing references

`references.bib` will be populated when all sections are drafted.
Citation keys used so far:

- `argentina1981politica`
- `baumgartnerpalazzo1969`
- `donaldsonhornbeck2016`
- `eatonkortum2002`
- `fajgelbaumredding2018`
- `faoiiasa2012gaez`
- `galorozak2016`
- `keeling1993`
- `larkin1962`
- `lopez_waddell2007`
- `nunn2012ruggedness`
- `ozak2018`
- `pahowka2005`
- `potash1969`
- `rock1985`
- `usgs1996gtopo30`
- `whitaker2013`

## Compilation

Not yet set up. The master `paper.tex` that wraps these into a full
document is a pending task. Current sections compile individually
(they use `\ref{}` for cross-references to sections not yet written,
so standalone compile produces reference warnings but not errors).

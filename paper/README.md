# Paper

Self-contained LaTeX source for the paper. Each section lives in its
own `.tex` file and is `\input{}`ed from `paper.tex` when that master
file is eventually written.

## Current contents

| File | Section | Status |
|---|---|---|
| `section_3_data.tex` | Section 3 (Data) | First draft, 2026-05-10 |

## Writing conventions (Shapiro Robot mode, see `Plan/foursteps.pdf`)

- State facts, do not argue. Interpretation and discussion go in later
  sections.
- Define every concept before using it.
- No forward references to constructs that haven't been introduced yet.
- Plain precise language, no fancy talk.

## Numbers and tables

All quantitative claims in the section files should eventually be
pulled from `results/scalars.tex` via AutoFill macros so no number is
ever hand-typed. For the current first draft, numbers are inlined with
source comments at the top of each file pointing to the script and
file that generated them.

TODOs flagged as `[PLACEHOLDER: ...]` are bracketed decisions or open
tasks in `Plan/tasks.md`.

## Missing references

`references.bib` will be populated when all sections are drafted. For
Section 3, the citation keys used are:

- `argentina1981politica`
- `baumgartnerpalazzo1969`
- `donaldsonhornbeck2016`
- `eatonkortum2002`
- `faoiiasa2012gaez`
- `galorozak2016`
- `nunn2012ruggedness`
- `ozak2018`
- `usgs1996gtopo30`

## Compilation

Not yet set up. The first compilable target will be when the intro,
Section 3, and Section 4 are all drafted plus `references.bib` exists.

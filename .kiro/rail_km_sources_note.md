# Rail kilometres: two sources, and which one the paper uses

Research note. Opened 2026-09-17 from Cote's kms-via block (his email of
the same date, working folder `kms_via/` sent as an attachment because it
was gitignored on his side). His decision ledger is reproduced beside this
file as `rail_km_sources_decisiones.csv`.

Reading that ledger: it is in Spanish, as his working notes are, and it
refers to the same person by two names. "Cote" and "José"/"Jose" are both
José Manuel Belmar, the coauthor; the `quien` column records which of them
did a step in his own workflow, and the `1_deterministica` values record
that a step was a deterministic computation rather than a judgement.
Paths prefixed `EXTERNAL:` are files in his working folder that were not
imported here; his three Python scripts are among them, deliberately (see
Open, below). Two edits were made on import: Gmail message and thread
identifiers were stripped, and the shapefile md5 was annotated with the
fact that it matches our copy.

## The question

The paper quoted the size of the rail network twice with different
numbers. Section 2 said roughly 43,500 km in 1960 falling to roughly
33,500 by 1986; Section 3 said 42,780 falling to 33,303. Cote asked
where the gap came from, and whether the digitised network was missing
lines from some period.

## Answer: two sources measuring the same thing, one uniformly shorter

There are two rail sources in the project, only one of which is in this
package.

| source | what it is | in the package? |
|---|---|---|
| `lp_1979.shp` | 565 segments hand-traced from the 1979 *Política Ferroviaria* map. One snapshot; the only temporal field is `status1979` in {1,2,3}. No date field. | yes, `data/raw/networks/`, md5 `89c28feab3535f83a1e49933b43f8404` |
| national track-kilometre series | 60 observations 1857--1989, columns `year` and `kms_rail` | **no** |

The series as used here came to us as `out/serie_km_via_1857_1989.csv` inside
Cote's `kms_via` attachment, which his `scripts/b12_serie.py` produced by
reading sheet `Sheet1` of `data_trains.xlsx` under `Train/raw_data/
kms_train_arg/` in the old project tree. We hold no hash for that
spreadsheet and no primary source for the figures in it, which is why
nothing in the paper rests on it. Anyone re-deriving the year-by-year
reading below needs that file, not this package.

Cote fixed a decision rule before looking at the numbers: if the shortfall
differs across dates, lines are missing from some period; if it is the
same, the digitisation is uniformly short. It is the same.

| cut | status included | map | series | gap |
|---|---|---|---|---|
| 1960 | 1+2+3 | 42,785 | 43,923 | −2.59% |
| pre-1976 | 1+2 | 38,863 | 39,779 | −2.30% |
| 1986 | 1 | 33,300 | 34,140 | −2.46% |

Range 0.29pp against a 0.75pp threshold. Verdict: a proportional
digitisation shortfall, not a composition problem. Consistent with
hand-tracing generalising the route at map scale, and equally consistent
with the series counting track kilometres (double track, sidings) against
our line kilometres.

An earlier version of this note said the two explanations "imply the same
thing for us, since everything the paper uses is a ratio, a share or a
change". That was too strong on both counts, and the PR #161 review was
right to flag it. Sections 2 and 3 print absolute levels, which a
proportional shortfall does move. And the two explanations are not
equivalent for the cost rasters: if hand-tracing generalised the route,
the geometry feeding rail travel costs really is shorter, which does not
cancel in a change in log market access; if the difference is track
kilometres against line kilometres, the geometry is untouched. What is
true and sufficient is narrower: the treatment variable is built from this
map, the map is what the paper now quotes, and neither is affected by how
a second source counts.

Reproduced independently in R before acting on it. `sf::st_length()` on
EPSG:4326 gives 33,304.1 / 5,564.1 / 3,921.6 km by status against his
33,299.8 / 5,563.1 / 3,922.7, and a total of 42,789.9 against his
42,785.5. The difference is s2 great-circle versus WGS-84 ellipsoid, which
he had already measured at 0.01% and predicted. Field list and segment
counts match exactly: `id_main, status1979, id_1979, studied_co,
recom_code`, 565 segments, 237 studied, 328 not.

## What the paper does

Section 2 and Section 3 now both quote the map, via
`diagnostic_rail_km.R` and AutoFill macros. One source, so the gap needs
no explaining in the text.

The series is **not cited and not read by any script**. Its provenance is
unresolved: `data_trains.xlsx` is a spreadsheet from the old project tree
with no recorded primary source. Asked of Cote 2026-09-17; he applied the
same standard to his own provincial totals earlier, so the request is
consistent with how he is already working. Until a primary source exists
the series cannot back a sentence in the paper.

## What the series would buy us, once citable

More than the reconciliation. Read year by year it contradicts the
narrative Section 2 carried, and supports a better one:

| period | change | pace |
|---|---|---|
| 1955--1960 | −7 km | flat |
| 1960--1970 | −4,018 km | 402 km/yr |
| 1970--1976 | −126 km | 21 km/yr, observed annually |
| 1976--1977 | −2,849 km | the single largest annual drop |
| 1977--1978 | −2,603 km | |
| 1978--1979 | −43 km | |
| 1979--1986 | −144 km | 21 km/yr |

The rows tile 1955--1986 with no gaps and reconcile to the endpoints
exactly: 43,930 km in 1955 less the 9,790 km of changes is 34,140, the
1986 value. (An earlier version of this table omitted 1976--1977 and
1978--1979 and mislabelled a two-year drop as one year, so it did not
reconcile; the PR #161 review caught that.)

One caveat on resolution rather than arithmetic. The series has no
observations for 1961--1962, 1964--1965 or 1967--1969, so the
1960--1970 row is a decade change measured across missing years and
cannot say where inside the decade the closures fell. The 1970--1979
stretch has every year present. So the halt is better observed than the
wave that precedes it.

Two things follow, the first with a hedge that belongs in it. Section 2's
"approximately 4,000 kilometers" is close to the 1960--1970 decline
(4,018 km) and far from the 1960--1966 decline (1,982 km) that the
sentence claimed, which is suggestive of where the figure came from but
does not establish it; Diego does not recall the source and it is still
open below. Second, there really was a halt, and unlike the wave it is
well observed: the 1970--1976 stretch has every year present and moves
126 km in total.

Section 2 currently makes neither claim: it reports the map's pre-1976
total and explicitly declines to apportion it within the window. If the
series becomes citable, the paragraph can date the wave and the halt.

Section 2's other rail number, "over 6,000 additional kilometers between
1976 and 1979", is supported by neither source: the map gives 5,563 for
the dictatorship bucket and the series 5,495 for 1976--1979, both about
8% short. It has been replaced by the map figure.

## A third discrepancy, found while fixing the first two

Section 4's studied-share footnote quoted a `recom_code` breakdown of
2,310 / 14,377 / 5,197 km (maintain / close / new study), giving
16,687 km and a 38.4% studied share excluding new-study segments. Those
figures do not reproduce from the shapefile. Measured geodesically the
same groups are 2,202 / 13,743 / 4,922 km, about 5% shorter, giving a
48.8% studied share and 37.3% excluding new study.

The file is not the issue: our copy's md5 matches the ledger's, so the
figures were computed from these exact bytes. The likely cause is
measurement method. Lengths taken in a single planar CRS inflate by
roughly this much over Argentina, which spans seven Gauss-Krüger zones,
and that is the mistake Cote's own protocol lists as prohibited. The
`clean_railroads.R` header records the old figures as "raw-shapefile
based", so they were not clipped-versus-unclipped either.

Section 4 now AutoFills all three from `diagnostic_rail_km.R`. The
external figure the footnote compares against, the plan's own cited
39.6% studied share, is unaffected: it comes from the report, not from us.

## Also settled here

The assumption that the 1960 network stands in for 1947, which Section 2
asserts without a source, is confirmed by the series: 1947 = 43,555 km and
1960 = 43,923, a rise of 368 km or 0.84%, with the historical peak in 1957
at 43,938. It stays an unsourced assumption in the paper for now, for the
same provenance reason.

## Open

- Primary source for the national series. With Cote.
- Where Section 2's original "approximately 4,000" and "over 6,000" came
  from. Diego does not recall; asked of Cote.
- Cote offered to branch his working folder. Declined for the package:
  his three scripts are Python and the package is R-only with an
  intentionally empty `requirements.txt`. The measurement is ported to
  `code/analysis/diagnostic_rail_km.R`; his protocol and ledger are kept
  here as the provenance record.

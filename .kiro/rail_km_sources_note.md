# Rail kilometres: two sources, and which one the paper uses

Research note. Opened 2026-09-17 from Cote's kms-via block (his email of
the same date, working folder `kms_via/` sent as an attachment because it
was gitignored on his side). His protocol and decision ledger are
reproduced beside this file as `rail_km_sources_decisiones.csv`.

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
| `lp_1979.shp` | 565 segments hand-traced from the 1979 *Política Ferroviaria* map. One snapshot; the only temporal field is `status1979` in {1,2,3}. No date field. | yes, `data/raw/networks/` |
| national track-kilometre series | 60 annual-ish observations 1857--1989, from `data_trains.xlsx` in the old project tree | **no** |

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
our line kilometres. The two explanations imply the same thing for us,
since everything the paper uses is a ratio, a share or a change.

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
| 1977--1978 | −5,452 km | 2,726 km/yr |
| 1979--1986 | −144 km | 21 km/yr |

Two things follow. Section 2's "approximately 4,000 kilometers" almost
certainly came from this series and is the 1960--1970 decline (4,018 km),
not the 1960--1966 decline (1,982 km) the sentence claimed. And there
really was a halt, but it ran 1970--1976, not from 1966; the years in that
stretch are all present, so the flatness is observed rather than
interpolated across gaps.

Section 2 currently makes neither claim: it reports the map's pre-1976
total and explicitly declines to apportion it within the window. If the
series becomes citable, the paragraph can date the wave and the halt.

Section 2's other rail number, "over 6,000 additional kilometers between
1976 and 1979", is supported by neither source: the map gives 5,563 for
the dictatorship bucket and the series 5,495 for 1976--1979, both about
8% short. It has been replaced by the map figure.

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

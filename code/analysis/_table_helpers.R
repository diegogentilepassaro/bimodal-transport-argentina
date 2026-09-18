# ===========================================================================
# _table_helpers.R
#
# PURPOSE: Shared formatting helpers for the analysis table scripts,
#          extracted from tables 12-17 where they were duplicated
#          (cr-review PR #128, consider C1). Estimation helpers live in
#          _iv_helpers.R; this file is formatting only.
#
# USED BY: table_12_robustness.R, table_13_counterfactual.R,
#          table_14_mechanisms.R, table_15_density_schedules.R,
#          table_16_sector_matched.R, table_17_counterfactual_sectoral.R
#          (the theta-sweep diagnostics keep their own cell formatters;
#          different signatures).
#
# PROVIDES:
#   star_str(p, tex)      Significance stars for a p-value; tex = TRUE
#                         wraps them as a LaTeX superscript. The single
#                         home of the 0.01 / 0.05 / 0.10 thresholds.
#   fmt(est, se, p)       Console-print cell: signed estimate with
#                         significance stars and SE, fixed width, used
#                         in the diagnostic message() blocks.
#   tex_cell(est, se, p)  LaTeX table cell: two-row tabular with the
#                         estimate plus stars over the SE in parentheses.
#   tex_cell_F(est, se, p, F_stat, stacked, size)
#                         tex_cell() plus that cell's first-stage F in
#                         brackets. Used by the controls ladders (Table 12
#                         Panel D and appendix Table B3), where the rungs
#                         change the first stage as well as the control
#                         set, so the F belongs beside each estimate
#                         rather than in a single footer row.
#
# Stars: * p<0.10, ** p<0.05, *** p<0.01. NA estimates render blank
# (tex) or a fixed-width NA marker (console).
# ===========================================================================

star_str <- function(p, tex = FALSE) {
    stars <- ifelse(p < 0.01, "***",
             ifelse(p < 0.05, "**",
             ifelse(p < 0.10, "*", "")))
    if (!tex) return(stars)
    # Preserve legacy edge cases exactly: "" stays "" (no empty
    # superscript) and NA p propagates as literal "NA".
    ifelse(is.na(stars) | stars == "", stars, sprintf("$^{%s}$", stars))
}

fmt <- function(est, se, p) {
    if (is.na(est)) return("     NA       ")
    sprintf("%+6.3f%-3s(%.3f)", est, star_str(p), se)
}

tex_cell <- function(est, se, p) {
    if (is.na(est)) return(" ")
    sprintf(
        "\\begin{tabular}{@{}c@{}} %.3f%s \\\\ (%.3f) \\end{tabular}",
        est, star_str(p, tex = TRUE), se
    )
}

# tex_cell_F(): tex_cell() with the cell's first-stage F in brackets.
#
# stacked = TRUE puts the F on its own third line (Table 12 Panel D, four
# rows, vertical room to spare). stacked = FALSE shares a line with the SE
# (Table B3, twenty rows, where three lines per cell pushed the float past
# the bottom of the page and LaTeX dropped the tail of the notes silently
# -- no Overfull \vbox, no error, just a missing caveat sentence in the
# compiled PDF).
#
# size is the LaTeX size command for the bracketed figure, without the
# backslash. It stays subordinate to the estimate rather than competing
# with it, and the two callers sit at different table sizes.
#
# An NA F prints an em dash rather than the literal "[NA]" that sprintf
# would produce: fitstat_F_robust() has a documented NA path, and a
# published table should not carry "[NA]" (cr-review PR #163). NA
# estimates render blank, as in tex_cell().
tex_cell_F <- function(est, se, p, F_stat, stacked = TRUE,
                       size = "scriptsize") {
    if (is.na(est)) return(" ")
    f_txt <- if (is.na(F_stat)) "[---]" else sprintf("[%.1f]", F_stat)
    sep <- if (stacked) " \\\\ " else "~"
    sprintf(
        "\\begin{tabular}{@{}c@{}} %.3f%s \\\\ (%.3f)%s{\\%s %s} \\end{tabular}",
        est, star_str(p, tex = TRUE), se, sep, size, f_txt
    )
}

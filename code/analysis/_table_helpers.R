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

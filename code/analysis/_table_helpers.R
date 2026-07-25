# ===========================================================================
# _table_helpers.R
#
# PURPOSE: Shared formatting helpers for the analysis table scripts,
#          extracted from tables 12-17 where they were duplicated
#          (cr-review PR #128, consider C1). Estimation helpers live in
#          _iv_helpers.R; this file is formatting only.
#
# PROVIDES:
#   fmt(est, se, p)       Console-print cell: signed estimate with
#                         significance stars and SE, fixed width, used
#                         in the diagnostic message() blocks.
#   tex_cell(est, se, p)  LaTeX table cell: two-row tabular with the
#                         estimate plus stars over the SE in parentheses.
#
# Stars: * p<0.10, ** p<0.05, *** p<0.01. NA estimates render blank
# (tex) or a fixed-width NA marker (console).
# ===========================================================================

fmt <- function(est, se, p) {
    if (is.na(est)) return("     NA       ")
    stars <- ifelse(p < 0.01, "***",
            ifelse(p < 0.05, "**",
            ifelse(p < 0.10, "*", "")))
    sprintf("%+6.3f%-3s(%.3f)", est, stars, se)
}

tex_cell <- function(est, se, p) {
    if (is.na(est)) return(" ")
    stars <- ifelse(p < 0.01, "$^{***}$",
            ifelse(p < 0.05, "$^{**}$",
            ifelse(p < 0.10, "$^{*}$", "")))
    sprintf(
        "\\begin{tabular}{@{}c@{}} %.3f%s \\\\ (%.3f) \\end{tabular}",
        est, stars, se
    )
}

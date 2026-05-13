#!/usr/bin/env python3
"""Build paper/paper_planned.md from paper/paper_planned.pdf.

This is a reviewer-convenience generator. The planned paper is
maintained as a PDF (Cote's editing format). This script produces a
readable Markdown mirror so coauthors who prefer text diffs can see
the same content.

Usage:
    python3 paper/build_planned_md.py

Requires: pdftotext (poppler). On macOS: brew install poppler.
"""
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PDF = HERE / "paper_planned.pdf"
MD = HERE / "paper_planned.md"
TXT = HERE / "paper_planned.txt"  # scratch; deleted at the end


def run_pdftotext():
    if not PDF.exists():
        sys.exit(f"Missing {PDF}")
    subprocess.run(
        ["pdftotext", "-layout", str(PDF), str(TXT)],
        check=True,
    )


def clean(src):
    # Unicode quote normalization
    src = (src.replace("\u2019", "'")
              .replace("\u201c", '"')
              .replace("\u201d", '"')
              .replace("\u2013", "-")
              .replace("\u2014", "--"))

    # Drop lone page-number lines
    keep = []
    for ln in src.splitlines():
        if re.fullmatch(r"\s*\d{1,3}\s*", ln):
            continue
        keep.append(ln)
    src = "\n".join(keep)

    # Collapse multi-space padding within lines
    src = "\n".join(re.sub(r" {2,}", " ", ln).rstrip()
                    for ln in src.splitlines())

    # Un-break hyphenated words: "consolida-\ntion" -> "consolidation"
    src = re.sub(r"(\w)-\n(\w)", r"\1\2", src)

    # Split into paragraph-blocks separated by blank lines, and join
    # soft-wrapped lines within each block. A new block starts at a
    # section heading regardless of blank lines.
    HEADING = re.compile(r"^\d+(\.\d+)?\s+[A-Z]")

    blocks = []
    current = []

    def flush():
        nonlocal current
        if current:
            blocks.append(" ".join(line.strip() for line in current))
            current = []

    for ln in src.splitlines():
        stripped = ln.strip()
        if not stripped:
            flush()
            blocks.append("")  # paragraph break marker
            continue
        if HEADING.match(stripped):
            flush()
            blocks.append(stripped)
            continue
        current.append(stripped)
    flush()

    # Collapse consecutive paragraph-break markers and rebuild text
    out = []
    prev_blank = False
    for b in blocks:
        if b == "":
            if not prev_blank:
                out.append("")
            prev_blank = True
        else:
            out.append(b)
            prev_blank = False
    text = "\n\n".join(x for x in out if x != "").strip()

    # Promote section headings to markdown
    lines = text.split("\n")
    promoted = []
    for ln in lines:
        m = re.match(r"^(\d+)\.(\d+)\s+(.+)$", ln)
        if m:
            promoted.append(f"### {m.group(1)}.{m.group(2)} {m.group(3)}")
            continue
        m = re.match(r"^(\d+)\s+(.+)$", ln)
        if m:
            promoted.append(f"\n## {m.group(1)} {m.group(2)}\n")
            continue
        promoted.append(ln)
    text = "\n".join(promoted)

    # Collapse 3+ blank lines to 2
    text = re.sub(r"\n{3,}", "\n\n", text)

    # Cut pre-intro boilerplate — jump to Section 1
    m = re.search(r"^## 1 Introduction", text, re.MULTILINE)
    if m:
        text = text[m.start():]

    return text


HEADER = """# Transport Restructuring and Regional Development: Evidence from Argentina, 1960-1991

_Working title — alternatives under discussion._

**Authors:** José Belmar (Brown University), Diego Gentile Passaro (Brown University)

**Date:** February 25, 2026

> **This file is auto-generated** from `paper/paper_planned.pdf` via
> `paper/build_planned_md.py`. It is the read-only Markdown mirror of
> the coauthor-maintained planning PDF. Do not edit it directly; edit
> the PDF (or the source Quip) and rerun the script.
>
> The paper itself is written in LaTeX under `paper/section_*.tex`
> and compiled by `paper/paper.tex`. Those `.tex` files are the
> authoritative source for the paper that will be submitted.

---

"""


def main():
    run_pdftotext()
    text = clean(TXT.read_text())
    MD.write_text(HEADER + text)
    TXT.unlink()
    print(f"Wrote {MD} ({len(text)} chars)")


if __name__ == "__main__":
    main()

# Makefile for perf/ — slabbis performance charts and report
#
# Targets:
#   make charts  — generate PDF charts from benchmark data (Python + matplotlib)
#   make report  — compile LaTeX report to PDF (requires pdflatex + charts)
#   make all     — charts then report
#   make clean   — remove generated files
#   make help    — print this message

PYTHON   := python3
LATEX    := pdflatex
REPORT   := report
CHARTDIR := charts

.PHONY: all charts report clean help

all: charts report

# ─────────────────────────────────────────────────────────────────────────────
# Charts
# ─────────────────────────────────────────────────────────────────────────────

charts:
	$(PYTHON) charts.py

# ─────────────────────────────────────────────────────────────────────────────
# Report
#
# Run pdflatex twice: first pass builds structure, second resolves references.
# ─────────────────────────────────────────────────────────────────────────────

report: charts
	$(LATEX) -interaction=nonstopmode $(REPORT).tex
	$(LATEX) -interaction=nonstopmode $(REPORT).tex

# ─────────────────────────────────────────────────────────────────────────────
# Clean
# ─────────────────────────────────────────────────────────────────────────────

clean:
	rm -f $(REPORT).pdf $(REPORT).aux $(REPORT).log $(REPORT).toc \
	      $(REPORT).out $(REPORT).fls $(REPORT).fdb_latexmk
	rm -rf $(CHARTDIR)

# ─────────────────────────────────────────────────────────────────────────────
# Help
# ─────────────────────────────────────────────────────────────────────────────

help:
	@grep -E '^#   make' Makefile | sed 's/^# //'

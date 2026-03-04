#!/usr/bin/env python3
"""Verify VERSION, version.go, and docs/CHANGELOG.md all agree."""
import re, sys

version   = open("VERSION").read().strip()
changelog = re.search(r"## \[([^\]]+)\]", open("docs/CHANGELOG.md").read()).group(1)
code      = re.search(r'Version = "([^"]+)"', open("version.go").read()).group(1)

print("VERSION file      :", version)
print("version.go        :", code)
print("docs/CHANGELOG.md :", changelog)

failures = []
if version != changelog:
    failures.append("VERSION (%s) != docs/CHANGELOG.md (%s)" % (version, changelog))
if version != code:
    failures.append("VERSION (%s) != version.go (%s)" % (version, code))

if failures:
    for f in failures:
        print("MISMATCH:", f)
    sys.exit(1)

print("OK - all three agree on v" + version)

#!/usr/bin/env bash
set -euo pipefail

# lots of assumptions here - working dir & installed dependencies among others

cd .readme
jinja2 README.md.j2 -o ../README.md

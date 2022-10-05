#!/usr/bin/env bash
set -euo pipefail

# lots of assumptions here - working dir & installed dependencies among others

cd .readme
jinja2 README.md.j2 -o ../README.md
cd ..

python3 scripts/ensure_one_trailing_newline.py README.md
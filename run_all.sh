#!/usr/bin/env bash
set -euo pipefail
cd dbt
dbt deps
dbt seed --target raw || true          # skip if you already loaded raw
dbt run  --target core
dbt test --target core
dbt docs generate
echo "✅ Done. Docs in: dbt/target/index.html"

python3 ../dq_gate.py

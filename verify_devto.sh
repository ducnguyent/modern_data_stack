#!/bin/bash
set -e

echo "🚀 Verifying DEV.to End-to-End Pipeline..."

# 1. Activate virtualenv (assuming .venv)
# source .venv/bin/activate

echo "📦 1. Fetching DEV.to Articles..."
python -m src.ingestion.fetch_devto

echo "🦆 2. LoadingDEV.to Articles to DuckDB..."
python -m src.loading.load_devto

echo "🔧 3. Running dbt models for devto..."
cd dbt
dbt run --select stg_devto devto_author_stats

echo "🧪 4. Testing dbt models for devto..."
dbt test --select stg_devto devto_author_stats
cd ..

echo "✅ 5. Testing Python ingestion logic..."
pytest tests/test_devto.py -v

echo "🎉 All verifications passed successfully!"

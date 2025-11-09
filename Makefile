.PHONY: bootstrap fetch csv release

bootstrap:
\tpip install -r requirements.txt
\tpre-commit install
\tdvc init -q || true

# pulls from OpenAlex and writes JSONL to data/raw/
fetch:
\tpython pipelines/fetch_openalex.py

# converts raw -> CSVs in data/processed/
csv:
\tpython pipelines/build_csv.py

# stamp processed CSVs into a dated release folder
release:
\tpython scripts/stamp_release.py
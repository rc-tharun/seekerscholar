# SeekerScholar — Data Collection Bootstrap

## Quick start
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pre-commit install
dvc init

make fetch   # fetch raw JSONL from OpenAlex
make csv     # build processed CSVs (papers.csv, citations.csv)

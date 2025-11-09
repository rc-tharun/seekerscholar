import os, time, json, pathlib, requests
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()
OUT = pathlib.Path("data/raw"); OUT.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT / "papers.jsonl"

QUERIES = ["transformers", "text generation", "diffusion models", "instruction tuning"]

PARAMS = {"per_page": 200, "search": None, "page": 1}
EMAIL = os.getenv("OPENALEX_EMAIL")
if EMAIL: PARAMS["mailto"] = EMAIL

def fetch_page(q, page):
    url = "https://api.openalex.org/works"
    p = PARAMS.copy(); p["search"] = q; p["page"] = page
    r = requests.get(url, params=p, timeout=60)
    r.raise_for_status()
    return r.json()

with OUT_FILE.open("w", encoding="utf-8") as out:
    for q in QUERIES:
        for page in tqdm(range(1, 6), desc=f"query:{q}"):  # ~1000 rows/query
            data = fetch_page(q, page)
            for w in data.get("results", []):
                rec = {
                    "source": "openalex",
                    "openalex_id": (w.get("id") or "").split("/")[-1],
                    "doi": (w.get("doi") or "").lower().replace("https://doi.org/",""),
                    "title": w.get("title") or "",
                    "abstract": w.get("abstract") or w.get("abstract_inverted_index") or "",
                    "year": w.get("publication_year"),
                    "venue": (w.get("host_venue") or {}).get("display_name") or "",
                    "fields_of_study": [c.get("display_name") for c in (w.get("concepts") or [])][:5],
                    "open_access": (w.get("open_access") or {}).get("is_oa") or False,
                    "pdf_url": (w.get("primary_location") or {}).get("pdf_url") or "",
                    "references": [ref.split("/")[-1] for ref in (w.get("referenced_works") or [])],
                    "authors": [a["author"]["display_name"] for a in (w.get("authorships") or [])],
                }
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            time.sleep(0.3)
print(f"Wrote {OUT_FILE}")

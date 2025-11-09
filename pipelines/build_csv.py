import csv, hashlib, json, pathlib, re
from datetime import datetime

RAW = pathlib.Path("data/raw/papers.jsonl")
OUT = pathlib.Path("data/processed"); OUT.mkdir(parents=True, exist_ok=True)

def norm(s):
    s = (s or "").strip()
    return re.sub(r"\s+", " ", s)

def pid(rec):
    if rec.get("openalex_id"): return f"oa:{rec['openalex_id']}"
    if rec.get("doi"): return f"doi:{rec['doi']}"
    seed = f"{norm(rec.get('title','')).lower()}|{(rec.get('authors') or [''])[:1]}|{rec.get('year') or ''}"
    return "hash:" + hashlib.sha1(seed.encode()).hexdigest()[:20]

papers, citations, authors = {}, [], []

with RAW.open(encoding="utf-8") as f:
    for line in f:
        r = json.loads(line)
        row = {
            "pid": pid(r),
            "doi": (r.get("doi") or ""),
            "title": norm(r.get("title","")),
            "abstract": norm(r.get("abstract",""))[:4000],
            "year": r.get("year"),
            "venue": norm(r.get("venue","")),
            "fields_of_study": "|".join(r.get("fields_of_study") or []),
            "open_access": str(bool(r.get("open_access", False))).lower(),
            "url_pdf": r.get("pdf_url") or "",
            "source": r.get("source") or "openalex",
            "ingested_at": datetime.utcnow().isoformat(timespec="seconds")+"Z",
        }
        key = (row["doi"] or row["title"].lower(), row["year"])
        prev = papers.get(key)
        def score(x): return (1 if x["abstract"] else 0) + (1 if x["doi"] else 0)
        if not prev or score(row) > score(prev): papers[key] = row

        for dst in (r.get("references") or []):
            citations.append({"src_pid": row["pid"], "dst_pid": f"oa:{dst}"})

        for i, a in enumerate(r.get("authors") or []):
            authors.append({"pid": row["pid"], "author_id": "", "author_name": a, "position": i+1, "affiliation": ""})

def write_csv(path, fields, rows):
    with open(path, "w", newline="", encoding="utf-8") as g:
        w = csv.DictWriter(g, fieldnames=fields, quoting=csv.QUOTE_MINIMAL)
        w.writeheader(); w.writerows(rows)

paper_rows = list(papers.values())
write_csv(OUT/"papers.csv",
    ["pid","doi","title","abstract","year","venue","fields_of_study","open_access","url_pdf","source","ingested_at"],
    paper_rows)
write_csv(OUT/"citations.csv", ["src_pid","dst_pid"], citations)
write_csv(OUT/"authors.csv",   ["pid","author_id","author_name","position","affiliation"], authors)
print(f"wrote {len(paper_rows)} papers, {len(citations)} citations, {len(authors)} authors")

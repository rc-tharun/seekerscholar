import csv
import hashlib
import json
import pathlib
import re
from datetime import datetime

RAW = pathlib.Path("data/raw/papers.jsonl")
OUT = pathlib.Path("data/processed")
OUT.mkdir(parents=True, exist_ok=True)


def deinvert_openalex(abs_idx):
    """
    Convert OpenAlex abstract_inverted_index (dict) → plain text.
    If abs_idx is already a string (or None), return a safe string.
    """
    if not isinstance(abs_idx, dict):
        return str(abs_idx or "")
    positions = []
    for word, idxs in abs_idx.items():
        for i in idxs:
            positions.append((i, word))
    if not positions:
        return ""
    maxpos = max(i for i, _ in positions)
    words = [""] * (maxpos + 1)
    for i, w in positions:
        words[i] = w
    return " ".join(w for w in words if w)


def norm(s: str) -> str:
    s = (s or "").strip()
    return re.sub(r"\s+", " ", s)


def pid(rec: dict) -> str:
    # Prefer stable provider IDs; fall back to deterministic hash.
    if rec.get("openalex_id"):
        return f"oa:{rec['openalex_id']}"
    if rec.get("doi"):
        return f"doi:{rec['doi']}"
    first_author = (rec.get("authors") or [""])[0]
    seed = f"{norm(rec.get('title','')).lower()}|{first_author}|{rec.get('year') or ''}"
    return "hash:" + hashlib.sha1(seed.encode()).hexdigest()[:20]


papers_map = {}  # key=(doi or title_lower, year) -> best row
citations = []
authors = []

with RAW.open(encoding="utf-8") as f:
    for line in f:
        r = json.loads(line)

        # Abstract may be a string or an inverted-index dict
        raw_abs = r.get("abstract")
        if raw_abs is None and isinstance(r.get("abstract_inverted_index"), dict):
            raw_abs = r["abstract_inverted_index"]
        abstract_text = deinvert_openalex(raw_abs)

        row = {
            "pid": pid(r),
            "doi": (r.get("doi") or ""),
            "title": norm(r.get("title", "")),
            "abstract": norm(abstract_text)[:4000],
            "year": r.get("year"),
            "venue": norm(r.get("venue", "")),
            "fields_of_study": "|".join(r.get("fields_of_study") or []),
            "open_access": str(bool(r.get("open_access", False))).lower(),
            "url_pdf": r.get("pdf_url") or "",
            "source": r.get("source") or "openalex",
            "ingested_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        }

        # Dedup: keep record with abstract/doi when conflicts on (doi or title, year)
        key = (row["doi"] or row["title"].lower(), row["year"])

        def score(x):
            return (1 if x["abstract"] else 0) + (1 if x["doi"] else 0)

        prev = papers_map.get(key)
        if prev is None or score(row) > score(prev):
            papers_map[key] = row

        # Citations: prefix OpenAlex IDs for consistency
        for dst in (r.get("references") or []):
            if not (isinstance(dst, str) and (dst.startswith("oa:") or dst.startswith("doi:") or dst.startswith("s2:"))):
                dst = f"oa:{dst}"
            citations.append({"src_pid": row["pid"], "dst_pid": dst})

        # Authors (best-effort)
        for i, a in enumerate(r.get("authors") or []):
            authors.append(
                {
                    "pid": row["pid"],
                    "author_id": "",
                    "author_name": a,
                    "position": i + 1,
                    "affiliation": "",
                }
            )


def write_csv(path: pathlib.Path, fieldnames, rows):
    with path.open("w", newline="", encoding="utf-8") as g:
        w = csv.DictWriter(g, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        w.writerows(rows)


paper_rows = list(papers_map.values())
write_csv(
    OUT / "papers.csv",
    [
        "pid",
        "doi",
        "title",
        "abstract",
        "year",
        "venue",
        "fields_of_study",
        "open_access",
        "url_pdf",
        "source",
        "ingested_at",
    ],
    paper_rows,
)
write_csv(OUT / "citations.csv", ["src_pid", "dst_pid"], citations)
write_csv(OUT / "authors.csv", ["pid", "author_id", "author_name", "position", "affiliation"], authors)

print(f"wrote {len(paper_rows)} papers, {len(citations)} citations, {len(authors)} authors")

"""
search_layer.py

The "search layer" stage of the literature pipeline. Two entry points:

  search_topic(query, ...)          for topic deep-dive mode
  search_daily(topic, days, ...)    for the daily latest-papers mode

Both return a list of paper dicts in the exact shape worker_agent.Paper
expects (title, abstract, authors, year, journal, doi, url, source_query),
ready to be dumped to JSON and fed straight into worker_agent.py.

Sources: PubMed (via NCBI E-utilities, always) and Semantic Scholar
(for its citation graph and, crucially, its openAccessPdf field, which
tells us when a legal full-text PDF exists).

Paywall handling:
  - If Semantic Scholar reports an open-access PDF, it's downloaded and
    its text replaces the abstract (richer material for the worker's
    extraction step).
  - If not, and you're running interactively, you're prompted right
    there for a local PDF path -- drop the file, paste the path, and
    it's used instead. Press Enter to just use the abstract.
  - If run with --no-prompt (for unattended/cron use), paywalled papers
    are queued in <vault>/.pending_pdfs/manifest.json instead of
    blocking. Drop PDFs into <vault>/.pending_pdfs/<slug>.pdf whenever
    you get access, then run `resume-pending` to pick them up.

Usage:
    python search_layer.py topic --query "structural variation CYP2D6" \\
        --vault /path/to/pgx-vault --out papers.json

    python search_layer.py daily --topic "Pharmacogenomics" --days 1 \\
        --vault /path/to/pgx-vault --out papers.json --no-prompt

    python search_layer.py resume-pending --vault /path/to/pgx-vault --out resumed.json
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

from worker_agent import DoiIndex, slugify  # reuse the same dedup/slug logic

PUBMED_ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
PUBMED_EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
S2_API_BASE = "https://api.semanticscholar.org/graph/v1"
USER_AGENT = "pgx-lit-agent/0.1 (personal research pipeline)"
S2_RATE_LIMIT_DELAY = 1.0  # seconds between Semantic Scholar calls, unauthenticated tier is strict
MAX_FULLTEXT_CHARS = 12000  # cap on extracted PDF text handed to the worker agent


# --------------------------------------------------------------------------
# PubMed
# --------------------------------------------------------------------------

def pubmed_esearch(
    query: str,
    retmax: int = 20,
    mindate: Optional[str] = None,
    maxdate: Optional[str] = None,
    api_key: Optional[str] = None,
    email: Optional[str] = None,
    tool: Optional[str] = None,
) -> list[str]:
    params = {"db": "pubmed", "term": query, "retmax": retmax, "retmode": "json", "sort": "most+recent"}
    if mindate and maxdate:
        params.update({"datetype": "pdat", "mindate": mindate, "maxdate": maxdate})
    for key, val in (("api_key", api_key), ("email", email), ("tool", tool)):
        if val:
            params[key] = val
    resp = requests.get(PUBMED_ESEARCH_URL, params=params, timeout=20)
    resp.raise_for_status()
    return resp.json().get("esearchresult", {}).get("idlist", [])


def parse_pubmed_xml(xml_bytes: bytes) -> list[dict]:
    root = ET.fromstring(xml_bytes)
    results = []
    for article in root.findall(".//PubmedArticle"):
        medline = article.find("MedlineCitation")
        art = medline.find("Article")
        title = (art.findtext("ArticleTitle") or "Untitled").strip()

        abstract_parts = [el.text or "" for el in art.findall("Abstract/AbstractText")]
        abstract = " ".join(p.strip() for p in abstract_parts if p).strip()

        authors = []
        for author in art.findall("AuthorList/Author"):
            last = author.findtext("LastName")
            fore = author.findtext("ForeName") or author.findtext("Initials")
            if last:
                authors.append(f"{fore} {last}".strip() if fore else last)

        journal = art.findtext("Journal/Title")

        year = None
        year_text = art.findtext("Journal/JournalIssue/PubDate/Year")
        if not year_text:
            medline_date = art.findtext("Journal/JournalIssue/PubDate/MedlineDate")
            if medline_date:
                match = re.search(r"(19|20)\d{2}", medline_date)
                year_text = match.group(0) if match else None
        if year_text:
            try:
                year = int(year_text)
            except ValueError:
                year = None

        doi = None
        pmid = medline.findtext("PMID")
        for aid in article.findall("PubmedData/ArticleIdList/ArticleId"):
            if aid.get("IdType") == "doi":
                doi = aid.text

        results.append(
            {
                "title": title,
                "abstract": abstract,
                "authors": authors,
                "year": year,
                "journal": journal,
                "doi": doi,
                "url": f"https://doi.org/{doi}" if doi else (f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else None),
                "pmid": pmid,
            }
        )
    return results


def pubmed_efetch(
    pmids: list[str],
    api_key: Optional[str] = None,
    email: Optional[str] = None,
    tool: Optional[str] = None,
) -> list[dict]:
    if not pmids:
        return []
    params = {"db": "pubmed", "id": ",".join(pmids), "rettype": "abstract", "retmode": "xml"}
    for key, val in (("api_key", api_key), ("email", email), ("tool", tool)):
        if val:
            params[key] = val
    resp = requests.get(PUBMED_EFETCH_URL, params=params, timeout=30)
    resp.raise_for_status()
    return parse_pubmed_xml(resp.content)


# --------------------------------------------------------------------------
# Semantic Scholar
# --------------------------------------------------------------------------

def s2_get(path: str, params: dict, max_retries: int = 3) -> dict:
    url = f"{S2_API_BASE}/{path}"
    for attempt in range(max_retries):
        resp = requests.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=20)
        if resp.status_code == 429:
            time.sleep(2**attempt)
            continue
        resp.raise_for_status()
        time.sleep(S2_RATE_LIMIT_DELAY)
        return resp.json()
    raise RuntimeError(f"Semantic Scholar rate-limited after {max_retries} retries")


def search_semantic_scholar(query: str, limit: int = 20) -> list[dict]:
    try:
        data = s2_get(
            "paper/search",
            {"query": query, "limit": limit, "fields": "title,abstract,year,authors,externalIds,openAccessPdf,venue"},
        )
    except Exception:
        return []  # S2 is a secondary source -- never let it take down the whole search
    results = []
    for item in data.get("data", []):
        doi = (item.get("externalIds") or {}).get("DOI")
        results.append(
            {
                "title": item.get("title", "Untitled"),
                "abstract": item.get("abstract") or "",
                "authors": [a.get("name", "") for a in item.get("authors", []) if a.get("name")],
                "year": item.get("year"),
                "journal": item.get("venue"),
                "doi": doi,
                "url": f"https://doi.org/{doi}" if doi else item.get("url"),
                "openAccessPdf": item.get("openAccessPdf"),
            }
        )
    return results


def semantic_scholar_lookup_by_doi(doi: str) -> dict:
    try:
        return s2_get(f"paper/DOI:{doi}", {"fields": "abstract,isOpenAccess,openAccessPdf,citationCount,venue"})
    except Exception:
        return {}


def semantic_scholar_related(doi: str, direction: str = "citations", limit: int = 10) -> list[dict]:
    """Follow a paper's citation graph. direction='citations' -> papers that cite
    it; direction='references' -> papers it cites."""
    field_key = "citingPaper" if direction == "citations" else "citedPaper"
    fields = ",".join(f"{field_key}.{f}" for f in ("title", "abstract", "year", "authors", "externalIds", "openAccessPdf", "venue"))
    try:
        data = s2_get(f"paper/DOI:{doi}/{direction}", {"fields": fields, "limit": limit})
    except Exception:
        return []
    results = []
    for item in data.get("data", []):
        p = item.get(field_key) or {}
        if not p:
            continue
        pdoi = (p.get("externalIds") or {}).get("DOI")
        results.append(
            {
                "title": p.get("title", "Untitled"),
                "abstract": p.get("abstract") or "",
                "authors": [a.get("name", "") for a in p.get("authors", []) if a.get("name")],
                "year": p.get("year"),
                "journal": p.get("venue"),
                "doi": pdoi,
                "url": f"https://doi.org/{pdoi}" if pdoi else None,
                "openAccessPdf": p.get("openAccessPdf"),
            }
        )
    return results


# --------------------------------------------------------------------------
# PDF text extraction
# --------------------------------------------------------------------------

def extract_pdf_text(source) -> Optional[str]:
    """source: a Path, or a file-like object (e.g. io.BytesIO)."""
    if pdfplumber is None:
        raise RuntimeError("pdfplumber is required to read PDFs. pip install pdfplumber")
    try:
        with pdfplumber.open(source) as pdf:
            pages = [page.extract_text() or "" for page in pdf.pages]
        text = "\n".join(pages).strip()
        return text or None
    except Exception:
        return None


def fetch_and_extract_pdf(url: str) -> Optional[str]:
    try:
        resp = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
        resp.raise_for_status()
    except requests.RequestException:
        return None
    content_type = resp.headers.get("Content-Type", "")
    if "pdf" not in content_type.lower() and not url.lower().endswith(".pdf"):
        return None  # likely an HTML landing page, not a direct PDF link
    return extract_pdf_text(io.BytesIO(resp.content))


# --------------------------------------------------------------------------
# Merge + dedup
# --------------------------------------------------------------------------

def merge_candidates(*lists: list[dict]) -> list[dict]:
    merged: dict[str, dict] = {}
    unkeyed: list[dict] = []
    for lst in lists:
        for cand in lst:
            doi = (cand.get("doi") or "").lower().strip()
            if not doi:
                unkeyed.append(cand)
                continue
            if doi not in merged:
                merged[doi] = cand
            else:
                existing = merged[doi]
                if not existing.get("abstract") and cand.get("abstract"):
                    existing["abstract"] = cand["abstract"]
                if not existing.get("openAccessPdf") and cand.get("openAccessPdf"):
                    existing["openAccessPdf"] = cand["openAccessPdf"]
    return list(merged.values()) + unkeyed


# --------------------------------------------------------------------------
# Paywall resolution: open-access PDF -> interactive prompt -> pending queue
# --------------------------------------------------------------------------

def queue_pending(candidate: dict, pending_path: Path) -> None:
    pending_path.parent.mkdir(parents=True, exist_ok=True)
    entries = json.loads(pending_path.read_text()) if pending_path.exists() else []
    key = candidate.get("doi") or candidate["title"]
    if any((e.get("doi") or e.get("title")) == key for e in entries):
        return
    entries.append({k: candidate.get(k) for k in ("title", "authors", "year", "journal", "doi", "url", "source_query")})
    pending_path.write_text(json.dumps(entries, indent=2))


def resolve_fulltext(candidate: dict, prompt_missing: bool, pending_path: Optional[Path]) -> dict:
    if "openAccessPdf" not in candidate and candidate.get("doi"):
        s2_info = semantic_scholar_lookup_by_doi(candidate["doi"])
        candidate["openAccessPdf"] = s2_info.get("openAccessPdf")
        if not candidate.get("abstract") and s2_info.get("abstract"):
            candidate["abstract"] = s2_info["abstract"]

    oa = candidate.get("openAccessPdf") or {}
    oa_url = oa.get("url") if isinstance(oa, dict) else None

    if oa_url:
        text = fetch_and_extract_pdf(oa_url)
        if text:
            candidate["abstract"] = text[:MAX_FULLTEXT_CHARS]
            candidate["fulltext_source"] = "open_access_pdf"
            return candidate

    if prompt_missing:
        print(f"\n  Paywalled (no open-access full text found): {candidate['title']}")
        path_str = input("  Path to a PDF to use instead (Enter to skip, abstract only): ").strip()
        if path_str:
            text = extract_pdf_text(Path(path_str))
            if text:
                candidate["abstract"] = text[:MAX_FULLTEXT_CHARS]
                candidate["fulltext_source"] = "manual_pdf"
                return candidate
            print(f"  Could not read a PDF at {path_str} -- continuing with abstract only.")
    elif pending_path is not None:
        queue_pending(candidate, pending_path)

    candidate["fulltext_source"] = "abstract_only"
    return candidate


def resume_pending(vault: Path) -> list[dict]:
    pending_path = vault / ".pending_pdfs" / "manifest.json"
    pdf_dir = vault / ".pending_pdfs"
    if not pending_path.exists():
        return []
    entries = json.loads(pending_path.read_text())
    resolved, still_pending = [], []
    for entry in entries:
        key = slugify(entry.get("doi") or entry["title"])
        pdf_path = pdf_dir / f"{key}.pdf"
        text = extract_pdf_text(pdf_path) if pdf_path.exists() else None
        if text:
            entry["abstract"] = text[:MAX_FULLTEXT_CHARS]
            entry["fulltext_source"] = "manual_pdf"
            resolved.append(entry)
        else:
            still_pending.append(entry)
    pending_path.write_text(json.dumps(still_pending, indent=2))
    return resolved


# --------------------------------------------------------------------------
# Entry points
# --------------------------------------------------------------------------

def _filter_known(vault: Optional[Path], candidates: list[dict]) -> list[dict]:
    if vault is None:
        return candidates
    doi_index = DoiIndex(vault)
    return [c for c in candidates if not doi_index.has(c.get("doi"))]


def search_topic(
    query: str,
    vault: Optional[Path] = None,
    limit: int = 20,
    no_prompt: bool = False,
    ncbi_api_key: Optional[str] = None,
    email: Optional[str] = None,
    tool: Optional[str] = None,
) -> list[dict]:
    pmids = pubmed_esearch(query, retmax=limit, api_key=ncbi_api_key, email=email, tool=tool)
    pubmed_hits = pubmed_efetch(pmids, api_key=ncbi_api_key, email=email, tool=tool)
    s2_hits = search_semantic_scholar(query, limit=limit)
    candidates = _filter_known(vault, merge_candidates(pubmed_hits, s2_hits))

    pending_path = (vault / ".pending_pdfs" / "manifest.json") if (vault and no_prompt) else None
    resolved = [resolve_fulltext(c, prompt_missing=not no_prompt, pending_path=pending_path) for c in candidates]
    for c in resolved:
        c["source_query"] = query
    return resolved


def search_daily(
    topic: str,
    days: int = 1,
    vault: Optional[Path] = None,
    limit: int = 30,
    no_prompt: bool = False,
    ncbi_api_key: Optional[str] = None,
    email: Optional[str] = None,
    tool: Optional[str] = None,
) -> list[dict]:
    today = datetime.now(timezone.utc).date()
    mindate = (today - timedelta(days=days)).strftime("%Y/%m/%d")
    maxdate = today.strftime("%Y/%m/%d")
    pmids = pubmed_esearch(
        f"{topic}[Title/Abstract]", retmax=limit, mindate=mindate, maxdate=maxdate, api_key=ncbi_api_key, email=email, tool=tool
    )
    pubmed_hits = pubmed_efetch(pmids, api_key=ncbi_api_key, email=email, tool=tool)
    # Semantic Scholar's search doesn't do day-level date filtering well, so
    # PubMed's date-restricted query is the primary source for daily mode.
    candidates = _filter_known(vault, merge_candidates(pubmed_hits))

    pending_path = (vault / ".pending_pdfs" / "manifest.json") if (vault and no_prompt) else None
    resolved = [resolve_fulltext(c, prompt_missing=not no_prompt, pending_path=pending_path) for c in candidates]
    for c in resolved:
        c["source_query"] = f"daily:{topic}"
    return resolved


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Search layer: PubMed + Semantic Scholar -> papers.json")
    sub = parser.add_subparsers(dest="mode", required=True)

    topic_p = sub.add_parser("topic", help="Deep-dive on a single topic/query")
    topic_p.add_argument("--query", required=True)
    topic_p.add_argument("--limit", type=int, default=20)

    daily_p = sub.add_parser("daily", help="Fetch the latest papers on a topic")
    daily_p.add_argument("--topic", default="Pharmacogenomics")
    daily_p.add_argument("--days", type=int, default=1)
    daily_p.add_argument("--limit", type=int, default=30)

    resume_p = sub.add_parser("resume-pending", help="Pick up PDFs dropped for previously paywalled papers")
    resume_p.add_argument("--vault", type=Path, required=True)
    resume_p.add_argument("--out", type=Path, required=True)

    for p in (topic_p, daily_p):
        p.add_argument("--vault", type=Path, default=None, help="Vault path, used for DOI dedup and pending-PDF queueing")
        p.add_argument("--out", type=Path, required=True)
        p.add_argument("--no-prompt", action="store_true", help="Never block for input; queue paywalled papers instead")
        p.add_argument("--ncbi-api-key", default=os.environ.get("NCBI_API_KEY"))
        p.add_argument("--email", default=os.environ.get("NCBI_EMAIL"), help="Recommended by NCBI for E-utilities use")
        p.add_argument("--tool", default="pgx-lit-agent")

    args = parser.parse_args()

    if args.mode == "topic":
        results = search_topic(
            args.query, vault=args.vault, limit=args.limit, no_prompt=args.no_prompt,
            ncbi_api_key=args.ncbi_api_key, email=args.email, tool=args.tool,
        )
    elif args.mode == "daily":
        results = search_daily(
            args.topic, days=args.days, vault=args.vault, limit=args.limit, no_prompt=args.no_prompt,
            ncbi_api_key=args.ncbi_api_key, email=args.email, tool=args.tool,
        )
    else:
        results = resume_pending(args.vault)

    args.out.write_text(json.dumps(results, indent=2))
    print(f"Wrote {len(results)} paper(s) to {args.out}")

    if args.mode != "resume-pending" and args.vault and args.no_prompt:
        pending = args.vault / ".pending_pdfs" / "manifest.json"
        if pending.exists():
            n = len(json.loads(pending.read_text()))
            if n:
                print(f"{n} paywalled paper(s) queued for manual PDF review at {pending}")


if __name__ == "__main__":
    main()

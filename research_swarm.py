"""
research_swarm.py

This is the actual "agents" layer -- as opposed to worker_agent.py /
search_layer.py, which are a deterministic pipeline with one fixed LLM
call per paper.

Two kinds of agent:

  Planner agent   One call. Splits a topic into a handful of focused,
                  non-overlapping sub-angles for the swarm to divide up.

  Research agent  One per sub-angle, run in parallel. Each is a full
                  Claude tool-use loop: the MODEL decides what to search,
                  whether to follow a citation chain, whether a paper is
                  worth a note, and when it has covered the sub-angle
                  well enough to stop. Nothing here hardcodes "search
                  once, extract once" -- that decision-making loop is
                  the whole point.

Per your call: agents judge their own coverage (the time/tool-call
budget is a failsafe, not a target), and are free to follow citation
chains outside the original search hits.

Because these loops run in parallel and are open-ended, paywalled
papers are handled the same way daily mode does it -- queued to
<vault>/.pending_pdfs/ -- rather than blocking on interactive input,
which doesn't work cleanly across concurrent threads. Run
`search_layer.py resume-pending` afterwards to pick up anything you've
since supplied a PDF for.

Usage:
    python research_swarm.py --topic "Structural Variation" --vault /path/to/pgx-vault
"""

from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anthropic

import search_layer as sl
from worker_agent import BibWriter, DoiIndex, EntityRegistry, Paper, write_paper_note

DEFAULT_MODEL = "claude-sonnet-5"

# --------------------------------------------------------------------------
# Tool schemas -- what the research agent is allowed to do
# --------------------------------------------------------------------------

TOOLS = [
    {
        "name": "search_pubmed",
        "description": "Search PubMed for papers matching a query.",
        "input_schema": {
            "type": "object",
            "properties": {"query": {"type": "string"}, "retmax": {"type": "integer", "default": 10}},
            "required": ["query"],
        },
    },
    {
        "name": "search_semantic_scholar",
        "description": "Search Semantic Scholar for papers matching a query.",
        "input_schema": {
            "type": "object",
            "properties": {"query": {"type": "string"}, "limit": {"type": "integer", "default": 10}},
            "required": ["query"],
        },
    },
    {
        "name": "get_related_papers",
        "description": "Follow a paper's citation graph: papers it cites (references) or papers that cite it (citations).",
        "input_schema": {
            "type": "object",
            "properties": {
                "doi": {"type": "string"},
                "direction": {"type": "string", "enum": ["citations", "references"]},
                "limit": {"type": "integer", "default": 10},
            },
            "required": ["doi", "direction"],
        },
    },
    {
        "name": "fetch_fulltext",
        "description": (
            "Attempt to fetch open-access full text for a paper by DOI. If it's paywalled, "
            "it's queued for a PDF to be supplied later and you get the abstract only for now."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"doi": {"type": "string"}, "title": {"type": "string"}},
            "required": ["doi"],
        },
    },
    {
        "name": "write_note",
        "description": (
            "Write a vault note for a paper you've judged worth including. "
            "Use your own judgment -- only call this for papers that genuinely add value; "
            "skip ones that are tangential, low-quality, or redundant with a note you already wrote."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "doi": {"type": "string"},
                "authors": {"type": "array", "items": {"type": "string"}},
                "year": {"type": "integer"},
                "journal": {"type": "string"},
                "url": {"type": "string"},
                "summary": {"type": "string", "description": "2-4 sentences: the finding and its relevance."},
                "genes": {"type": "array", "items": {"type": "string"}},
                "drugs": {"type": "array", "items": {"type": "string"}},
                "methods": {"type": "array", "items": {"type": "string"}},
                "phenotypes": {"type": "array", "items": {"type": "string"}},
                "tags": {"type": "array", "items": {"type": "string"}},
                "relevance": {"type": "string", "enum": ["high", "medium", "low"]},
            },
            "required": ["title", "summary", "genes", "drugs", "methods", "phenotypes", "tags", "relevance"],
        },
    },
    {
        "name": "mark_subtopic_complete",
        "description": "Call this when you judge the sub-angle adequately covered. Ends your research loop.",
        "input_schema": {
            "type": "object",
            "properties": {
                "coverage_summary": {"type": "string", "description": "What you covered and why it's sufficient."},
            },
            "required": ["coverage_summary"],
        },
    },
]

PLANNER_TOOL = {
    "name": "propose_subangles",
    "description": "Propose the sub-angles a research swarm should investigate for a topic.",
    "input_schema": {
        "type": "object",
        "properties": {
            "subangles": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "angle": {"type": "string", "description": "A focused, search-friendly sub-angle."},
                        "rationale": {"type": "string", "description": "One sentence: why this angle matters."},
                    },
                    "required": ["angle", "rationale"],
                },
                "minItems": 2,
                "maxItems": 6,
            }
        },
        "required": ["subangles"],
    },
}

RESEARCH_AGENT_SYSTEM = """You are an autonomous research agent building a pharmacogenomics literature vault for a PhD thesis on "{topic}".

Your assignment: thoroughly investigate this sub-angle: "{sub_angle}"

You have tools to search PubMed and Semantic Scholar, follow a paper's citation graph (papers it cites, and papers that cite it), fetch open-access full text, and write a vault note for any paper you judge worth including. Use your own judgment throughout:
- Not every search hit deserves a note. Skip papers that are tangential, low-quality, or redundant with ones you've already written up.
- Following an interesting citation outside your original search results is encouraged when it's relevant to your assignment.
- You decide when this sub-angle is adequately covered. When you believe it is, call mark_subtopic_complete with a short account of what you covered and why you consider it sufficient.

You have a soft budget of about {max_minutes} minutes and {max_tool_calls} tool calls as a safety net, not a target -- stop when the research is genuinely done, not when you hit some quota. If you approach the budget, wrap up with what you have rather than starting new threads of research."""


# --------------------------------------------------------------------------
# Tool dispatch
# --------------------------------------------------------------------------

def dispatch_tool(
    name: str,
    tool_input: dict,
    sub_angle: str,
    vault: Path,
    doi_index: DoiIndex,
    entities: EntityRegistry,
    bib: BibWriter,
) -> dict:
    if name == "search_pubmed":
        pmids = sl.pubmed_esearch(tool_input["query"], retmax=tool_input.get("retmax", 10))
        return {"results": sl.pubmed_efetch(pmids)}

    if name == "search_semantic_scholar":
        return {"results": sl.search_semantic_scholar(tool_input["query"], limit=tool_input.get("limit", 10))}

    if name == "get_related_papers":
        return {"results": sl.semantic_scholar_related(tool_input["doi"], tool_input["direction"], tool_input.get("limit", 10))}

    if name == "fetch_fulltext":
        doi = tool_input["doi"]
        s2_info = sl.semantic_scholar_lookup_by_doi(doi)
        oa = s2_info.get("openAccessPdf") or {}
        url = oa.get("url") if isinstance(oa, dict) else None
        if url:
            text = sl.fetch_and_extract_pdf(url)
            if text:
                return {"status": "ok", "fulltext": text[: sl.MAX_FULLTEXT_CHARS]}
        sl.queue_pending({"doi": doi, "title": tool_input.get("title", doi)}, vault / ".pending_pdfs" / "manifest.json")
        return {"status": "paywalled", "message": "No open-access text found; queued for a manual PDF. Use the abstract for now."}

    if name == "write_note":
        doi = tool_input.get("doi")
        if doi_index.has(doi):
            return {"status": "skipped_duplicate"}
        paper = Paper(
            title=tool_input["title"],
            abstract="",
            authors=tool_input.get("authors", []),
            year=tool_input.get("year"),
            journal=tool_input.get("journal"),
            doi=doi,
            url=tool_input.get("url"),
            source_query=sub_angle,
        )
        extraction = {k: tool_input[k] for k in ("summary", "genes", "drugs", "methods", "phenotypes", "tags", "relevance")}
        note_path = write_paper_note(paper, extraction, vault, entities, bib)
        doi_index.record(doi, str(note_path))
        return {"status": "written", "path": str(note_path)}

    if name == "mark_subtopic_complete":
        return {"status": "acknowledged"}

    return {"status": "error", "message": f"Unknown tool: {name}"}


# --------------------------------------------------------------------------
# The agent loop
# --------------------------------------------------------------------------

def run_research_agent(
    sub_angle: str,
    topic: str,
    vault: Path,
    client: "anthropic.Anthropic",
    model: str,
    max_minutes: float,
    max_tool_calls: int,
    doi_index: DoiIndex,
    entities: EntityRegistry,
    bib: BibWriter,
) -> dict[str, Any]:
    system_prompt = RESEARCH_AGENT_SYSTEM.format(
        topic=topic, sub_angle=sub_angle, max_minutes=max_minutes, max_tool_calls=max_tool_calls
    )
    messages: list[dict] = [{"role": "user", "content": f"Begin researching: {sub_angle}"}]
    start = time.monotonic()
    calls_used = 0
    notes_written = 0
    coverage_summary = None

    while True:
        elapsed_min = (time.monotonic() - start) / 60
        budget_exhausted = elapsed_min >= max_minutes or calls_used >= max_tool_calls

        kwargs = dict(model=model, max_tokens=2048, system=system_prompt, tools=TOOLS, messages=messages)
        if budget_exhausted:
            kwargs["tool_choice"] = {"type": "tool", "name": "mark_subtopic_complete"}

        response = client.messages.create(**kwargs)
        messages.append({"role": "assistant", "content": response.content})

        tool_results = []
        stop = False
        for block in response.content:
            if block.type != "tool_use":
                continue
            calls_used += 1
            if block.name == "mark_subtopic_complete":
                coverage_summary = block.input.get("coverage_summary", "")
                stop = True
                tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": "Acknowledged."})
                continue
            result = dispatch_tool(block.name, block.input, sub_angle, vault, doi_index, entities, bib)
            if block.name == "write_note" and result.get("status") == "written":
                notes_written += 1
            tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": json.dumps(result)[:6000]})

        if tool_results:
            messages.append({"role": "user", "content": tool_results})
        if stop or not any(b.type == "tool_use" for b in response.content):
            break

    return {
        "sub_angle": sub_angle,
        "coverage_summary": coverage_summary,
        "notes_written": notes_written,
        "tool_calls_used": calls_used,
        "elapsed_minutes": round((time.monotonic() - start) / 60, 1),
    }


# --------------------------------------------------------------------------
# Planner
# --------------------------------------------------------------------------

def plan_subangles(topic: str, client: "anthropic.Anthropic", model: str, n_hint: int = 4) -> list[dict]:
    prompt = (
        f'Break the pharmacogenomics research topic "{topic}" into {n_hint} focused, non-overlapping '
        "sub-angles suitable for research agents to investigate independently and in parallel. "
        "Record them via propose_subangles."
    )
    response = client.messages.create(
        model=model,
        max_tokens=1024,
        tools=[PLANNER_TOOL],
        tool_choice={"type": "tool", "name": "propose_subangles"},
        messages=[{"role": "user", "content": prompt}],
    )
    for block in response.content:
        if block.type == "tool_use" and block.name == "propose_subangles":
            return block.input["subangles"]
    raise RuntimeError("Planner did not return the expected tool call.")


# --------------------------------------------------------------------------
# Swarm orchestration
# --------------------------------------------------------------------------

def run_swarm(
    topic: str,
    vault: Path,
    model: str = DEFAULT_MODEL,
    n_subangles: int = 4,
    max_minutes: float = 15,
    max_tool_calls: int = 40,
    max_workers: int = 4,
) -> dict:
    client = anthropic.Anthropic()
    vault.mkdir(parents=True, exist_ok=True)

    subangles = plan_subangles(topic, client, model, n_hint=n_subangles)
    doi_index = DoiIndex(vault)
    entities = EntityRegistry(vault)
    bib = BibWriter(vault)

    agent_results = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(
                run_research_agent, sa["angle"], topic, vault, client, model, max_minutes, max_tool_calls, doi_index, entities, bib
            ): sa
            for sa in subangles
        }
        for future in as_completed(futures):
            agent_results.append(future.result())

    manifest = {
        "topic": topic,
        "subangles": subangles,
        "agent_results": agent_results,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    manifest_dir = vault / ".manifests"
    manifest_dir.mkdir(exist_ok=True)
    slug = sl.slugify(topic) if hasattr(sl, "slugify") else topic.lower().replace(" ", "-")
    manifest_path = manifest_dir / f"swarm-{slug}-{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H%M%S')}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))

    return manifest


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Autonomous research swarm for a topic deep-dive")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--vault", type=Path, required=True)
    parser.add_argument("--subangles", type=int, default=4, help="Number of sub-angles the planner should propose")
    parser.add_argument("--max-minutes", type=float, default=15, help="Soft per-agent time budget (failsafe, not a target)")
    parser.add_argument("--max-tool-calls", type=int, default=40, help="Soft per-agent tool-call budget (failsafe)")
    parser.add_argument("--workers", type=int, default=4, help="How many sub-angle agents run in parallel")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    args = parser.parse_args()

    manifest = run_swarm(
        args.topic, args.vault, model=args.model, n_subangles=args.subangles,
        max_minutes=args.max_minutes, max_tool_calls=args.max_tool_calls, max_workers=args.workers,
    )

    total_notes = sum(r["notes_written"] for r in manifest["agent_results"])
    print(f"Swarm complete: {len(manifest['agent_results'])} sub-angles, {total_notes} notes written.")
    for r in manifest["agent_results"]:
        print(f"  - {r['sub_angle']}: {r['notes_written']} notes, {r['tool_calls_used']} tool calls, {r['elapsed_minutes']}m")
        print(f"    {r['coverage_summary']}")


if __name__ == "__main__":
    main()

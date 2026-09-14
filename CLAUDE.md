# CLAUDE.md

Context for Claude Code working in this repo. This is a personal literature-research automation pipeline for a PhD thesis in pharmacogenomics (PGx) at Hasselt University, feeding an Obsidian vault.

## What this project does

Two workflows write linked notes into an Obsidian vault:

1. **Topic deep-dive** (`research_swarm.py`) — autonomous multi-agent research on a topic, triggered manually.
2. **Daily latest-papers scan** (`search_layer.py` + `worker_agent.py`) — deterministic, unattended-safe, run on a schedule.

Notes are cross-linked via Obsidian `[[wikilinks]]` to shared gene/drug/method/phenotype "entity" stub notes. A subset of the vault is synced out to a public GitHub Pages site (Quartz).

## Repo layout

This is the **private** `pgx-vault` repo (there is a separate **public** `lit-review-site` repo for the Quartz-built site — see "Privacy boundary" below).

```
pgx-vault/
  papers/                 one note per paper (synced to the public site)
  topics/                 topic overview notes (synced)
  entities/               gene/drug/method/phenotype stub notes (synced)
  reports/                synthesized PDF reports (synced) — generator not built yet
  pgxbgb/                 clinical cohort study notes — NEVER synced, private always
  manuscripts/            unpublished manuscript drafts — NEVER synced, private always
  references.bib          agent-maintained BibTeX, deduped by citation key
  .doi_index.json         → actually papers/.doi_index.json, DOI → note path dedup index
  .pending_pdfs/          manifest.json + dropped PDFs for paywalled papers
  .manifests/             one JSON per pipeline run (worker batches and swarm runs)
  scripts/
    worker_agent.py       per-paper extraction + note/entity/bib writer
    search_layer.py       PubMed + Semantic Scholar client, paywall/pending handling
    research_swarm.py     planner agent + parallel autonomous research agents
    run_pipeline.sh       orchestrator: search/swarm → worker → git commit+push
    sync_public_notes.sh  copies safe folders into lit-review-site, strips `private: true` notes
    requirements.txt
  .gitignore

lit-review-site/          separate public repo, not this one
  content/                 populated ONLY by scripts/sync_public_notes.sh — never edit by hand
  .github/workflows/deploy.yml
```

## The two pipelines

### Daily mode (deterministic)
`search_layer.py daily` queries PubMed (date-restricted) + Semantic Scholar, dedups by DOI against `papers/.doi_index.json`, resolves full text where openly available, queues paywalled papers to `.pending_pdfs/` (never blocks — this mode must be cron-safe). Output is a `papers.json` list. `worker_agent.py` then makes exactly one Claude tool-call per paper (forced call to `record_paper_extraction`) to get a summary/entities/tags, and writes the note + entity stubs + bib entry. Every candidate that clears dedup gets a note — no judgment call about relevance happens here.

### Topic mode (autonomous swarm)
`research_swarm.py` is different in kind, not just degree: a **planner agent** (one forced tool call) splits the topic into sub-angles; then one **research agent per sub-angle** runs in parallel, each a full Claude tool-use loop with tools `search_pubmed`, `search_semantic_scholar`, `get_related_papers` (citation graph, either direction), `fetch_fulltext`, `write_note`, `mark_subtopic_complete`. The model itself decides what to search, whether to follow a citation chain, whether a given paper is worth `write_note`, and when to call `mark_subtopic_complete`. There is a soft per-agent budget (default 15 minutes / 40 tool calls) that is a **failsafe, not a target** — at budget exhaustion the next API call is forced via `tool_choice={"type":"tool","name":"mark_subtopic_complete"}` so the agent wraps up rather than looping forever. Paywalled papers here always go to `.pending_pdfs/` (no interactive prompt — parallel threads can't share stdin cleanly).

### Resuming paywalled papers
`search_layer.py resume-pending` checks `.pending_pdfs/manifest.json` entries against `.pending_pdfs/<slug>.pdf` files you've dropped in, extracts text from any that are now present, and clears them from the manifest.

## Data contracts

**Paper dict** (the shape passed between search_layer and worker_agent): `title, abstract, authors[], year, journal, doi, url, source_query`. Extra keys (`openAccessPdf`, `fulltext_source`, `pmid`) are carried along but ignored by `Paper.from_dict`.

**Note frontmatter** (`papers/<year>-<slug>.md`): `title, authors, year, journal, doi, url, source_query, tags, relevance, cite_key, type: paper, date_added`.

**Entity stub** (`entities/<slug>.md`): `title, type: entity, category, date_added`, plus a `## Mentioned in` heading left empty. **This heading is the interface contract with the linker pass (not yet built)** — nothing currently populates it. Don't have worker_agent or research_swarm write to it directly; that's the linker's job specifically because it needs to see a whole batch of new notes at once to cross-reference them.

**Pending PDF manifest entry**: `title, authors, year, journal, doi, url, source_query`.

## Commands

```bash
pip install -r scripts/requirements.txt

export ANTHROPIC_API_KEY=...
export NCBI_EMAIL=you@example.com      # optional, NCBI E-utilities courtesy param
export NCBI_API_KEY=...                # optional, raises PubMed rate limit
export VAULT=/path/to/pgx-vault

./scripts/run_pipeline.sh swarm "Structural Variation"   # autonomous topic deep-dive
./scripts/run_pipeline.sh topic "structural variation CYP2D6"  # search_layer+worker_agent path, interactive paywall prompts
./scripts/run_pipeline.sh daily "Pharmacogenomics"        # unattended-safe, for cron
./scripts/run_pipeline.sh resume                          # pick up dropped PDFs

./scripts/sync_public_notes.sh $VAULT /path/to/lit-review-site  # publish safe folders
```

Default model across all scripts: `claude-sonnet-5` (override with `--model`).

## Built vs. not yet built

Built: vault/git migration + Quartz site + deploy workflow, `search_layer.py`, `worker_agent.py`, `research_swarm.py`, `run_pipeline.sh`, `sync_public_notes.sh`.

**Not built yet:**
- **Linker pass** — should run after a batch/swarm completes, read the run's `.manifests/*.json`, and append `- [[paper-note]]` lines under each touched entity's `## Mentioned in` heading. Also the natural place for cross-paper relations (cites/extends/contradicts) that shared entities alone don't capture.
- **Report agent** — should read a run's new notes, synthesize a cited multi-page markdown report, then `pandoc report.md -o report.pdf --citeproc --csl=vancouver.csl` against `references.bib`, saved to `reports/`.

## Conventions to follow in this repo

- Preferred languages: **Python, R, Bash**.
- When asked to modify an existing script, change only what's requested — no unsolicited refactors, renames, or reorganizing of unrelated code.
- Bioinformatics CLI tools (bcftools, samtools, etc.), if ever introduced here, should be run via Docker (e.g. staphb images), not assumed to be installed locally. Not currently used in this pipeline.
- **Privacy boundary is load-bearing, not a suggestion**: `sync_public_notes.sh` only ever copies `papers/`, `topics/`, `entities/`, `reports/` into the public repo, and strips any note with `private: true` in frontmatter as a second safety net. Never add logic that syncs `pgxbgb/`, `manuscripts/`, or any new top-level folder without being explicitly told to. GitHub Pages sites are publicly viewable by URL regardless of source-repo visibility — there is no private-by-default fallback here.
- Testing pattern used so far (worth continuing): offline fixtures for PubMed XML parsing (`sample_pubmed_response.xml`), and a small scripted fake Anthropic client (records calls, returns a fixed sequence of tool-use responses) to test agent-loop mechanics — budget cutoff, termination, tool dispatch — without live API calls.

## Known limitations / open questions

- Swarm budget defaults (15 min / 40 tool calls per sub-angle) are untuned starting points, not measured.
- The Semantic Scholar citation-graph `fields` dotted-path format in `semantic_scholar_related` is per current API docs but hasn't been exercised against a live call.
- No retry/backoff beyond basic 429 handling on Semantic Scholar; PubMed has none.
- `write_note`'s dedup check races are handled via `DoiIndex`'s lock, but two agents writing the *same* new (not-yet-indexed) paper in the same run could still both pass the check before either records it — narrow window, not yet closed.

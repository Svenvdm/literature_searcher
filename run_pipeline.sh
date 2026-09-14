#!/usr/bin/env bash
#
# run_pipeline.sh — glue script for the literature pipeline.
#
# Usage:
#   ./run_pipeline.sh swarm  "Structural Variation"   # autonomous multi-agent deep-dive
#   ./run_pipeline.sh topic  "structural variation CYP2D6"
#   ./run_pipeline.sh daily  "Pharmacogenomics"
#   ./run_pipeline.sh resume
#
# Env vars:
#   VAULT              path to the private vault repo (default: ~/pgx-vault)
#   ANTHROPIC_API_KEY  required (worker agent's extraction calls)
#   NCBI_API_KEY       optional, raises PubMed's rate limit
#   NCBI_EMAIL         optional, recommended by NCBI for E-utilities use

set -euo pipefail

MODE="${1:?Usage: run_pipeline.sh <topic|daily|resume> [query/topic]}"
shift || true

VAULT="${VAULT:-$HOME/pgx-vault}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PAPERS_JSON="$(mktemp -t papers-XXXX.json)"

cd "$SCRIPT_DIR"

if [ "$MODE" = "swarm" ]; then
  TOPIC="${1:?Usage: run_pipeline.sh swarm \"<topic>\"}"
  # Autonomous multi-agent deep-dive: research_swarm.py does its own
  # searching, judging, and note-writing -- no separate worker_agent pass.
  python3 research_swarm.py --topic "$TOPIC" --vault "$VAULT"
  rm -f "$PAPERS_JSON"
  cd "$VAULT"
  git add -A
  if ! git diff --cached --quiet; then
    git commit -m "Literature agent run: swarm $(date +%Y-%m-%d) - $TOPIC"
    git push
  else
    echo "No new notes to commit."
  fi
  exit 0
fi

case "$MODE" in
  topic)
    QUERY="${1:?Usage: run_pipeline.sh topic \"<query>\"}"
    # Interactive: will prompt you for a PDF path on any paywalled paper.
    python3 search_layer.py topic --query "$QUERY" --vault "$VAULT" --out "$PAPERS_JSON" \
      --ncbi-api-key "${NCBI_API_KEY:-}" --email "${NCBI_EMAIL:-}"
    ;;
  daily)
    TOPIC="${1:-Pharmacogenomics}"
    # --no-prompt: unattended-safe. Paywalled papers get queued instead of blocking.
    python3 search_layer.py daily --topic "$TOPIC" --days 1 --vault "$VAULT" --out "$PAPERS_JSON" \
      --no-prompt --ncbi-api-key "${NCBI_API_KEY:-}" --email "${NCBI_EMAIL:-}"
    ;;
  resume)
    # Picks up any PDFs you've dropped into $VAULT/.pending_pdfs/<slug>.pdf since the last run.
    python3 search_layer.py resume-pending --vault "$VAULT" --out "$PAPERS_JSON"
    ;;
  *)
    echo "Unknown mode: $MODE (expected swarm, topic, daily, or resume)" >&2
    exit 1
    ;;
esac

python3 worker_agent.py --vault "$VAULT" --input "$PAPERS_JSON" --workers 4
rm -f "$PAPERS_JSON"

cd "$VAULT"
git add -A
if ! git diff --cached --quiet; then
  git commit -m "Literature agent run: $MODE $(date +%Y-%m-%d)"
  git push
else
  echo "No new notes to commit."
fi

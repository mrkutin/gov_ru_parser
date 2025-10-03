#!/usr/bin/env bash
set -euo pipefail

DOC_ID="${DOC_ID:-uk_1996_2}"
START_URL="${START_URL:-http://government.ru/docs/all/96145/}"
QDRANT_URL="${QDRANT_URL:-http://qdrant:6333}"
NEXT_SELECTOR="${NEXT_SELECTOR:-.show-more}"
NEXT_TEXT="${NEXT_TEXT:-Следующая}"
CONTENT_SELECTOR="${CONTENT_SELECTOR:-.reader_article_body}"
HEADLESS="${HEADLESS:-1}"
MAX_PAGES="${MAX_PAGES:-}"
ARTICLE_REGEX="${ARTICLE_REGEX:-}"
NO_ARTICLE_GROUPING="${NO_ARTICLE_GROUPING:-}"
NO_RECREATE="${NO_RECREATE:-}"

cmd=(python main.py "$DOC_ID" "$START_URL" \
  --qdrant-url "$QDRANT_URL" \
  --next-selector "$NEXT_SELECTOR" \
  --next-text "$NEXT_TEXT" \
  --content-selector "$CONTENT_SELECTOR")

if [[ -n "$MAX_PAGES" ]]; then
  cmd+=("--max-pages" "$MAX_PAGES")
fi
if [[ -n "$ARTICLE_REGEX" ]]; then
  cmd+=("--article-regex" "$ARTICLE_REGEX")
fi
if [[ "$NO_ARTICLE_GROUPING" == "1" || "$NO_ARTICLE_GROUPING" == "true" ]]; then
  cmd+=("--no-article-grouping")
fi
if [[ "$NO_RECREATE" == "1" || "$NO_RECREATE" == "true" ]]; then
  cmd+=("--no-recreate")
fi
if [[ "$HEADLESS" == "1" || "$HEADLESS" == "true" ]]; then
  cmd+=("--headless")
fi

exec "${cmd[@]}"



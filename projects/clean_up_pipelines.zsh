#!/bin/zsh
set -euo pipefail

PREFIX='[dev bruno_campos]'

echo "Fetching pipelines..."

TMP_FILE=$(mktemp)
trap 'rm -f "$TMP_FILE"' EXIT

if ! databricks pipelines list-pipelines --output json \
  | jq -r --arg prefix "$PREFIX" '
      if type == "array" then
        .[]
      elif .statuses? then
        .statuses[]
      else
        empty
      end
      | select(.name? | startswith($prefix))
      | [.pipeline_id, .name]
      | @tsv
    ' > "$TMP_FILE"; then
  echo "Failed to fetch pipelines."
  exit 1
fi

echo ""
echo "Pipelines to delete:"
cat "$TMP_FILE" 2>/dev/null || true

echo ""
TOTAL=$(wc -l < "$TMP_FILE" | tr -d ' ')
echo "Total pipelines: $TOTAL"

if [[ "$TOTAL" -eq 0 ]]; then
  echo "No pipelines found. Exiting."
  exit 0
fi

# echo ""
# echo "Deleting pipelines..."

# while IFS=$'\t' read -r pipeline_id pipeline_name; do
#   [[ -z "${pipeline_id:-}" ]] && continue

#   echo "Deleting $pipeline_id -> $pipeline_name"

#   if databricks pipelines delete "$pipeline_id"; then
#     echo ""
#   else
#     echo "Pipeline $pipeline_id does not exist or was already deleted. Skipping."
#   fi
# done < "$TMP_FILE"

# echo ""
# echo "Done."

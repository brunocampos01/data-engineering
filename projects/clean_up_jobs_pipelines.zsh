#!/bin/zsh
set -euo pipefail

PREFIX='[dev bruno_campos]'

echo "Fetching jobs..."

TMP_FILE=$(mktemp)
trap 'rm -f "$TMP_FILE"' EXIT

if ! databricks jobs list --output json \
  | jq -r --arg prefix "$PREFIX" '
      if type == "array" then
        .[]
      elif .jobs? then
        .jobs[]
      else
        empty
      end
      | select(.settings.name? | startswith($prefix))
      | [.job_id, .settings.name]
      | @tsv
    ' > "$TMP_FILE"; then
  echo "Failed to fetch jobs."
  exit 1
fi

echo ""
echo "Jobs to delete:"
cat "$TMP_FILE" 2>/dev/null || true

echo ""
TOTAL=$(wc -l < "$TMP_FILE" | tr -d ' ')
echo "Total jobs: $TOTAL"

if [[ "$TOTAL" -eq 0 ]]; then
  echo "No jobs found. Exiting."
  exit 0
fi

echo ""
echo "Deleting jobs..."

while IFS=$'\t' read -r job_id job_name; do
  [[ -z "${job_id:-}" ]] && continue

  echo "$job_id -> $job_name"

  if databricks jobs delete "$job_id"; then
    echo ""
  else
    echo "Job $job_id does not exist or was already deleted. Skipping."
  fi
done < "$TMP_FILE"

echo ""
echo "Done."

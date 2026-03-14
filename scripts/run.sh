#!/usr/bin/env bash
# run.sh — execute or schedule the doomsday-polymarket Cloud Run job.
#
# Usage:
#   ./scripts/run.sh execute --slug=<event-slug> [--category=<cat>] [--fidelity=60] [--no-volume]
#   ./scripts/run.sh schedule --slug=<event-slug> [--category=<cat>] [--schedule="0 * * * *"]
#   ./scripts/run.sh list-schedules
#   ./scripts/run.sh delete-schedule <scheduler-job-name>
set -euo pipefail

PROJECT=fg-polylabs
REGION=us-central1
JOB_NAME=doomsday-polymarket
SA=doomsday-runner@fg-polylabs.iam.gserviceaccount.com

subcommand="${1:-}"
shift || true

slug=""
category=""
fidelity=""
no_volume=false
schedule="0 * * * *"  # default: hourly

for arg in "$@"; do
  case "$arg" in
    --slug=*)       slug="${arg#*=}" ;;
    --category=*)   category="${arg#*=}" ;;
    --fidelity=*)   fidelity="${arg#*=}" ;;
    --no-volume)    no_volume=true ;;
    --schedule=*)   schedule="${arg#*=}" ;;
    *)              echo "unknown argument: $arg" >&2; exit 1 ;;
  esac
done

build_args() {
  local args="--slug=${slug}"
  [[ -n "$category"  ]] && args+=",--category=${category}"
  [[ -n "$fidelity"  ]] && args+=",--fidelity=${fidelity}"
  [[ "$no_volume" == true ]] && args+=",--no-volume"
  echo "$args"
}

case "$subcommand" in
  execute)
    if [[ -z "$slug" ]]; then
      echo "Error: --slug is required" >&2; exit 1
    fi
    ARGS=$(build_args)
    echo "Executing Cloud Run job with args: ${ARGS}"
    gcloud run jobs execute "$JOB_NAME" \
      --region="$REGION" \
      --project="$PROJECT" \
      --args="$ARGS" \
      --wait
    echo ""
    echo "Results: https://console.cloud.google.com/bigquery?project=${PROJECT}&ws=!1m5!1m4!4m3!1s${PROJECT}!2sdoomsday!3smarket_snapshots"
    ;;

  schedule)
    if [[ -z "$slug" ]]; then
      echo "Error: --slug is required" >&2; exit 1
    fi
    ARGS=$(build_args)
    SCHEDULER_NAME="doomsday-${slug}"
    echo "Creating Cloud Scheduler job '${SCHEDULER_NAME}' (${schedule})"
    gcloud scheduler jobs create http "$SCHEDULER_NAME" \
      --project="$PROJECT" \
      --location="$REGION" \
      --schedule="$schedule" \
      --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/${JOB_NAME}:run" \
      --message-body="{\"overrides\":{\"containerOverrides\":[{\"args\":[$(echo "$ARGS" | sed 's/,/\",\"/g' | sed 's/^/\"/' | sed 's/$/\"/')]}]}}" \
      --oauth-service-account-email="$SA" \
      --time-zone="UTC"
    ;;

  list-schedules)
    gcloud scheduler jobs list \
      --project="$PROJECT" \
      --location="$REGION" \
      --filter="name~doomsday"
    ;;

  delete-schedule)
    name="${1:-}"
    if [[ -z "$name" ]]; then
      echo "Usage: $0 delete-schedule <scheduler-job-name>" >&2; exit 1
    fi
    gcloud scheduler jobs delete "$name" \
      --project="$PROJECT" \
      --location="$REGION" \
      --quiet
    ;;

  *)
    echo "Usage: $0 {execute|schedule|list-schedules|delete-schedule} [options]"
    exit 1
    ;;
esac

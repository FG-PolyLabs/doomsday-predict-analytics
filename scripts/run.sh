#!/usr/bin/env bash
# run.sh — execute or schedule the doomsday-polymarket Cloud Run job.
#
# Market configs are stored in the doomsday.markets BigQuery table.
# Use the API (doomsday-api Cloud Run Service) to add/remove/toggle markets.
#
# Usage:
#   ./scripts/run.sh daily                                        # yesterday's prices, all active markets
#   ./scripts/run.sh backfill                                     # full history, all markets, no volume
#   ./scripts/run.sh execute --slug=<event-slug> [--category=<cat>] [--fidelity=60] [--no-volume]
#   ./scripts/run.sh execute --tag=<tag-slug>   [--category=<cat>] [--fidelity=60] [--no-volume]
#   ./scripts/run.sh schedule-daily [--schedule="0 1 * * *"]
#   ./scripts/run.sh list-schedules
#   ./scripts/run.sh delete-schedule <scheduler-job-name>
#   ./scripts/run.sh export
set -euo pipefail

PROJECT=fg-polylabs
REGION=us-central1
JOB_NAME=doomsday-polymarket
EXPORTER_JOB_NAME=doomsday-exporter
SA=doomsday-runner@fg-polylabs.iam.gserviceaccount.com

subcommand="${1:-}"
shift || true

slug=""
tag=""
category=""
fidelity=""
no_volume=false
schedule="0 * * * *"  # default: hourly (for ad-hoc schedules)

for arg in "$@"; do
  case "$arg" in
    --slug=*)       slug="${arg#*=}" ;;
    --tag=*)        tag="${arg#*=}" ;;
    --category=*)   category="${arg#*=}" ;;
    --fidelity=*)   fidelity="${arg#*=}" ;;
    --no-volume)    no_volume=true ;;
    --schedule=*)   schedule="${arg#*=}" ;;
    *)              echo "unknown argument: $arg" >&2; exit 1 ;;
  esac
done

build_args() {
  local args=""
  [[ -n "$slug"     ]] && args="--slug=${slug}"
  [[ -n "$tag"      ]] && args="--tag=${tag}"
  [[ -n "$category" ]] && args+=",--category=${category}"
  [[ -n "$fidelity" ]] && args+=",--fidelity=${fidelity}"
  [[ "$no_volume" == true ]] && args+=",--no-volume"
  echo "$args"
}

run_job() {
  local args="$1"
  echo "Executing Cloud Run job with args: ${args}"
  gcloud run jobs execute "$JOB_NAME" \
    --region="$REGION" \
    --project="$PROJECT" \
    --args="$args" \
    --wait
  echo ""
  echo "Results: https://console.cloud.google.com/bigquery?project=${PROJECT}&ws=!1m5!1m4!4m3!1s${PROJECT}!2sdoomsday!3smarket_snapshots"
}

case "$subcommand" in
  execute)
    if [[ -z "$slug" && -z "$tag" ]]; then
      echo "Error: --slug or --tag is required" >&2; exit 1
    fi
    run_job "$(build_args)"
    ;;

  backfill)
    # Historical backfill: all active markets in doomsday.markets, full history, no volume fields.
    run_job "--all,--no-volume"
    ;;

  daily)
    # Incremental load: yesterday's end-of-day prices for all active markets.
    run_job "--all,--yesterday,--active-only"
    ;;

  schedule-daily)
    # Create a Cloud Scheduler job that runs daily at 1 AM UTC for all active markets.
    DAILY_SCHEDULE="${schedule:-0 1 * * *}"
    [[ "$schedule" == "0 * * * *" ]] && DAILY_SCHEDULE="0 1 * * *"
    SCHEDULER_NAME="doomsday-daily"
    echo "Creating Cloud Scheduler job '${SCHEDULER_NAME}' (${DAILY_SCHEDULE})"
    gcloud scheduler jobs create http "$SCHEDULER_NAME" \
      --project="$PROJECT" \
      --location="$REGION" \
      --schedule="$DAILY_SCHEDULE" \
      --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/${JOB_NAME}:run" \
      --message-body='{"overrides":{"containerOverrides":[{"args":["--all","--yesterday","--active-only"]}]}}' \
      --oauth-service-account-email="$SA" \
      --time-zone="UTC"
    ;;

  export)
    # Run the exporter job immediately (one-off export to GCS).
    echo "Executing exporter job..."
    gcloud run jobs execute "$EXPORTER_JOB_NAME" \
      --region="$REGION" \
      --project="$PROJECT" \
      --wait
    echo "Data exported to gs://fg-polylabs-doomsday"
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
    echo "Usage: $0 {daily|backfill|execute|schedule-daily|export|list-schedules|delete-schedule} [options]"
    exit 1
    ;;
esac

#!/usr/bin/env bash
# run.sh — execute or schedule the doomsday-polymarket Cloud Run job.
#
# Usage:
#   ./scripts/run.sh execute --slug=<event-slug> [--category=<cat>] [--fidelity=60] [--no-volume]
#   ./scripts/run.sh execute --tag=<tag-slug>   [--category=<cat>] [--fidelity=60] [--no-volume]
#   ./scripts/run.sh backfill --tag=<tag-slug>  [--category=<cat>]                  # historical, no-volume
#   ./scripts/run.sh daily    --tag=<tag-slug>  [--category=<cat>]                  # yesterday's prices, active only
#   ./scripts/run.sh schedule-daily --tag=<tag-slug> [--category=<cat>] [--schedule="0 0 * * *"]
#   ./scripts/run.sh schedule --slug=<event-slug> [--category=<cat>] [--schedule="0 * * * *"]
#   ./scripts/run.sh list-schedules
#   ./scripts/run.sh delete-schedule <scheduler-job-name>
#
# Note: the monthly GCS export schedule (doomsday-export-monthly) has been removed.
# The daily job now exports to GCS and Drive automatically after each BQ insert.
set -euo pipefail

# Use //app/ (double leading slash) so Git bash on Windows doesn't convert the path;
# Linux treats //app/ identically to /app/.
CONFIG_PATH="//app/markets.json"

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
schedule="0 * * * *"  # default: hourly

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
    # Historical backfill: all markets in config, full history, no volume fields.
    run_job "--config=${CONFIG_PATH},--no-volume"
    ;;

  daily)
    # Incremental load: yesterday's end-of-day prices for all active expirations in config.
    run_job "--config=${CONFIG_PATH},--yesterday,--active-only"
    ;;

  schedule-daily)
    # Create a Cloud Scheduler job that runs daily at midnight UTC using the bundled config.
    DAILY_SCHEDULE="${schedule:-0 0 * * *}"  # default midnight UTC, overridden only if --schedule= passed
    # If user didn't pass --schedule, reset to midnight (script default is hourly for other commands)
    [[ "$schedule" == "0 * * * *" ]] && DAILY_SCHEDULE="0 1 * * *"
    SCHEDULER_NAME="doomsday-daily"
    echo "Creating Cloud Scheduler job '${SCHEDULER_NAME}' (${DAILY_SCHEDULE})"
    gcloud scheduler jobs create http "$SCHEDULER_NAME" \
      --project="$PROJECT" \
      --location="$REGION" \
      --schedule="$DAILY_SCHEDULE" \
      --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/${JOB_NAME}:run" \
      --message-body="{\"overrides\":{\"containerOverrides\":[{\"args\":[\"--config=${CONFIG_PATH}\",\"--yesterday\",\"--active-only\"]}]}}" \
      --oauth-service-account-email="$SA" \
      --time-zone="UTC"
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

  export)
    # Run the exporter job immediately (one-off export to GCS).
    echo "Executing exporter job..."
    gcloud run jobs execute "$EXPORTER_JOB_NAME" \
      --region="$REGION" \
      --project="$PROJECT" \
      --wait
    echo "Data exported to gs://fg-polylabs-doomsday"
    ;;

  schedule-export)
    echo "Error: doomsday-export-monthly has been retired." >&2
    echo "The daily job now exports to GCS and Drive automatically after each BQ insert." >&2
    echo "To delete the old scheduler job: $0 delete-schedule doomsday-export-monthly" >&2
    exit 1
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
    echo "Usage: $0 {execute|backfill|daily|schedule-daily|export|schedule|list-schedules|delete-schedule} [options]"
    exit 1
    ;;
esac

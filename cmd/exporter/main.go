// exporter queries the doomsday.market_snapshots BigQuery table and writes
// one JSON file per event slug to GCS and Google Drive.
//
// GCS layout:
//
//	gs://fg-polylabs-doomsday/index.json
//	gs://fg-polylabs-doomsday/events/<event-slug>.json
//
// Drive layout (when DRIVE_PARENT_FOLDER_ID env var is set):
//
//	<parent-folder>/doomsday/index.json
//	<parent-folder>/doomsday/<event-slug>.json
package main

import (
	"context"
	"log"
	"os"

	"github.com/FutureGadgetLabs/doomsday-predict-analytics/internal/doomsday"
)

func main() {
	ctx := context.Background()
	cfg := doomsday.DefaultExportConfig()
	if err := doomsday.RunExport(ctx, cfg); err != nil {
		log.Printf("export failed: %v", err)
		os.Exit(1)
	}
	if err := doomsday.RunThetaExport(ctx, cfg); err != nil {
		log.Printf("theta export failed: %v", err)
		os.Exit(1)
	}
}

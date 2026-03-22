// export.go — queries market_snapshots from BigQuery and publishes JSON to
// GCS and (optionally) a Google Drive folder.
//
// GCS layout:
//
//	gs://<bucket>/index.json
//	gs://<bucket>/events/<event-slug>.json
//
// Drive layout (when DRIVE_PARENT_FOLDER_ID is set):
//
//	<parent-folder>/doomsday/index.json
//	<parent-folder>/doomsday/<event-slug>.json
package doomsday

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	googledrive "google.golang.org/api/drive/v3"
	"google.golang.org/api/iterator"
)

// ExportConfig holds destination parameters for RunExport.
type ExportConfig struct {
	Project       string // GCP project ID
	BucketName    string // GCS bucket name
	DriveFolderID string // Google Drive parent folder ID; empty = skip Drive
}

// DefaultExportConfig returns the standard production export config.
// Set DRIVE_PARENT_FOLDER_ID in the environment to enable Google Drive output.
func DefaultExportConfig() ExportConfig {
	return ExportConfig{
		Project:       "fg-polylabs",
		BucketName:    "doomsday-data",
		DriveFolderID: os.Getenv("DRIVE_PARENT_FOLDER_ID"),
	}
}

type exportBQRow struct {
	EventSlug           string                 `bigquery:"event_slug"`
	EventTitle          string                 `bigquery:"event_title"`
	Question            string                 `bigquery:"question"`
	Category            string                 `bigquery:"category"`
	SnapshotTimestamp   time.Time              `bigquery:"snapshot_timestamp"`
	ExpirationTimestamp bigquery.NullTimestamp `bigquery:"expiration_timestamp"`
	YesPrice            float64                `bigquery:"yes_price"`
	NoPrice             float64                `bigquery:"no_price"`
}

// SnapshotExport is one price point in the JSON output.
type SnapshotExport struct {
	Question            string  `json:"question"`
	ExpirationTimestamp string  `json:"expiration_timestamp,omitempty"`
	SnapshotTimestamp   string  `json:"snapshot_timestamp"`
	YesPrice            float64 `json:"yes_price"`
	NoPrice             float64 `json:"no_price"`
}

// EventExport is the per-event JSON file written to GCS/Drive.
type EventExport struct {
	EventSlug   string           `json:"event_slug"`
	EventTitle  string           `json:"event_title"`
	Category    string           `json:"category"`
	GeneratedAt string           `json:"generated_at"`
	Snapshots   []SnapshotExport `json:"snapshots"`
}

// ExportIndexEntry is one entry in the index manifest.
type ExportIndexEntry struct {
	EventSlug      string `json:"event_slug"`
	EventTitle     string `json:"event_title"`
	Category       string `json:"category"`
	SnapshotCount  int    `json:"snapshot_count"`
	LatestSnapshot string `json:"latest_snapshot,omitempty"`
	File           string `json:"file"`
}

// ExportIndex is the top-level index.json manifest.
type ExportIndex struct {
	GeneratedAt string             `json:"generated_at"`
	Events      []ExportIndexEntry `json:"events"`
}

type pendingExport struct {
	filename string
	data     any
}

// RunExport queries BigQuery market_snapshots, writes JSON files to GCS, and
// (if cfg.DriveFolderID is set) mirrors them to a "doomsday" subfolder in Drive.
// Drive failures are logged but not returned as errors — GCS is considered authoritative.
func RunExport(ctx context.Context, cfg ExportConfig) error {
	bqClient, err := bigquery.NewClient(ctx, cfg.Project)
	if err != nil {
		return fmt.Errorf("creating BQ client: %w", err)
	}
	defer bqClient.Close()

	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("creating GCS client: %w", err)
	}
	defer gcsClient.Close()

	q := bqClient.Query(fmt.Sprintf(
		"SELECT event_slug, event_title, question, category,"+
			" snapshot_timestamp, expiration_timestamp, yes_price, no_price"+
			" FROM `%s.doomsday.market_snapshots`"+
			" ORDER BY event_slug, expiration_timestamp, snapshot_timestamp",
		cfg.Project,
	))
	it, err := q.Read(ctx)
	if err != nil {
		return fmt.Errorf("running BQ query: %w", err)
	}

	type eventAcc struct {
		title     string
		category  string
		snapshots []SnapshotExport
	}
	events := make(map[string]*eventAcc)
	var order []string

	for {
		var row exportBQRow
		if err := it.Next(&row); err == iterator.Done {
			break
		} else if err != nil {
			return fmt.Errorf("reading BQ row: %w", err)
		}
		if _, ok := events[row.EventSlug]; !ok {
			events[row.EventSlug] = &eventAcc{title: row.EventTitle, category: row.Category}
			order = append(order, row.EventSlug)
		}
		snap := SnapshotExport{
			Question:          row.Question,
			SnapshotTimestamp: row.SnapshotTimestamp.UTC().Format(time.RFC3339),
			YesPrice:          row.YesPrice,
			NoPrice:           row.NoPrice,
		}
		if row.ExpirationTimestamp.Valid {
			snap.ExpirationTimestamp = row.ExpirationTimestamp.Timestamp.UTC().Format(time.RFC3339)
		}
		events[row.EventSlug].snapshots = append(events[row.EventSlug].snapshots, snap)
	}
	log.Printf("export: %d event(s) loaded from BigQuery", len(order))

	now := time.Now().UTC().Format(time.RFC3339)
	bucket := gcsClient.Bucket(cfg.BucketName)
	var indexEntries []ExportIndexEntry
	var driveFiles []pendingExport

	for _, slug := range order {
		acc := events[slug]
		export := EventExport{
			EventSlug:   slug,
			EventTitle:  acc.title,
			Category:    acc.category,
			GeneratedAt: now,
			Snapshots:   acc.snapshots,
		}
		gcsPath := fmt.Sprintf("events/%s.json", slug)
		if err := gcsWriteJSON(ctx, bucket, gcsPath, export); err != nil {
			log.Printf("export: warning: GCS %s failed: %v", gcsPath, err)
			continue
		}
		log.Printf("export: GCS %s (%d snapshots)", gcsPath, len(acc.snapshots))
		driveFiles = append(driveFiles, pendingExport{filename: slug + ".json", data: export})

		latest := ""
		if n := len(acc.snapshots); n > 0 {
			latest = acc.snapshots[n-1].SnapshotTimestamp
		}
		indexEntries = append(indexEntries, ExportIndexEntry{
			EventSlug:      slug,
			EventTitle:     acc.title,
			Category:       acc.category,
			SnapshotCount:  len(acc.snapshots),
			LatestSnapshot: latest,
			File:           gcsPath,
		})
	}

	index := ExportIndex{GeneratedAt: now, Events: indexEntries}
	if err := gcsWriteJSON(ctx, bucket, "index.json", index); err != nil {
		return fmt.Errorf("export: GCS index.json: %w", err)
	}
	driveFiles = append(driveFiles, pendingExport{filename: "index.json", data: index})
	log.Printf("export: GCS done — %d event(s) in gs://%s", len(order), cfg.BucketName)

	if cfg.DriveFolderID != "" {
		if err := driveWrite(ctx, cfg.DriveFolderID, driveFiles); err != nil {
			log.Printf("export: warning: Drive upload failed: %v", err)
		} else {
			log.Printf("export: Drive done — %d file(s) written", len(driveFiles))
		}
	}
	return nil
}

func gcsWriteJSON(ctx context.Context, bucket *storage.BucketHandle, path string, v any) error {
	w := bucket.Object(path).NewWriter(ctx)
	w.ContentType = "application/json"
	w.CacheControl = "public, max-age=3600"
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

// driveWrite finds or creates a "doomsday" subfolder inside parentFolderID and
// upserts each file there.
func driveWrite(ctx context.Context, parentFolderID string, files []pendingExport) error {
	svc, err := googledrive.NewService(ctx)
	if err != nil {
		return fmt.Errorf("creating Drive client: %w", err)
	}

	// Find or create the "doomsday" subfolder.
	q := fmt.Sprintf("name='doomsday' and '%s' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false", parentFolderID)
	r, err := svc.Files.List().Q(q).Fields("files(id)").Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("searching for doomsday folder: %w", err)
	}
	var folderID string
	if len(r.Files) > 0 {
		folderID = r.Files[0].Id
	} else {
		f, err := svc.Files.Create(&googledrive.File{
			Name:     "doomsday",
			MimeType: "application/vnd.google-apps.folder",
			Parents:  []string{parentFolderID},
		}).Context(ctx).Do()
		if err != nil {
			return fmt.Errorf("creating doomsday folder: %w", err)
		}
		folderID = f.Id
		log.Printf("export: Drive: created folder 'doomsday' (%s)", folderID)
	}

	for _, pf := range files {
		data, err := json.MarshalIndent(pf.data, "", "  ")
		if err != nil {
			log.Printf("export: Drive: marshal %s: %v", pf.filename, err)
			continue
		}
		data = append(data, '\n')

		fq := fmt.Sprintf("name='%s' and '%s' in parents and trashed=false", pf.filename, folderID)
		res, err := svc.Files.List().Q(fq).Fields("files(id)").Context(ctx).Do()
		if err != nil {
			log.Printf("export: Drive: search %s: %v", pf.filename, err)
			continue
		}
		reader := bytes.NewReader(data)
		if len(res.Files) > 0 {
			_, err = svc.Files.Update(res.Files[0].Id, &googledrive.File{}).
				Media(reader).Context(ctx).Do()
		} else {
			_, err = svc.Files.Create(&googledrive.File{
				Name:    pf.filename,
				Parents: []string{folderID},
			}).Media(reader).Context(ctx).Do()
		}
		if err != nil {
			log.Printf("export: Drive: write %s: %v", pf.filename, err)
		}
	}
	return nil
}

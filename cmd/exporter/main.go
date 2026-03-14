// exporter queries the doomsday.market_snapshots BigQuery table and writes
// one JSON file per event slug to GCS, plus an index.json manifest.
//
// GCS layout:
//
//	gs://fg-polylabs-doomsday/index.json
//	gs://fg-polylabs-doomsday/events/<event-slug>.json
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

const (
	project    = "fg-polylabs"
	dataset    = "doomsday"
	table      = "market_snapshots"
	bucketName = "fg-polylabs-doomsday"
)

type bqRow struct {
	EventSlug           string                 `bigquery:"event_slug"`
	EventTitle          string                 `bigquery:"event_title"`
	Question            string                 `bigquery:"question"`
	Category            string                 `bigquery:"category"`
	SnapshotTimestamp   time.Time              `bigquery:"snapshot_timestamp"`
	ExpirationTimestamp bigquery.NullTimestamp `bigquery:"expiration_timestamp"`
	YesPrice            float64                `bigquery:"yes_price"`
	NoPrice             float64                `bigquery:"no_price"`
}

type SnapshotExport struct {
	Question            string  `json:"question"`
	ExpirationTimestamp string  `json:"expiration_timestamp,omitempty"`
	SnapshotTimestamp   string  `json:"snapshot_timestamp"`
	YesPrice            float64 `json:"yes_price"`
	NoPrice             float64 `json:"no_price"`
}

type EventExport struct {
	EventSlug   string           `json:"event_slug"`
	EventTitle  string           `json:"event_title"`
	Category    string           `json:"category"`
	GeneratedAt string           `json:"generated_at"`
	Snapshots   []SnapshotExport `json:"snapshots"`
}

type IndexEntry struct {
	EventSlug      string `json:"event_slug"`
	EventTitle     string `json:"event_title"`
	Category       string `json:"category"`
	SnapshotCount  int    `json:"snapshot_count"`
	LatestSnapshot string `json:"latest_snapshot,omitempty"`
	File           string `json:"file"`
}

type Index struct {
	GeneratedAt string       `json:"generated_at"`
	Events      []IndexEntry `json:"events"`
}

func main() {
	ctx := context.Background()

	bqClient, err := bigquery.NewClient(ctx, project)
	if err != nil {
		log.Fatalf("creating BQ client: %v", err)
	}
	defer bqClient.Close()

	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("creating GCS client: %v", err)
	}
	defer gcsClient.Close()

	q := bqClient.Query(fmt.Sprintf(
		"SELECT event_slug, event_title, question, category,"+
			" snapshot_timestamp, expiration_timestamp, yes_price, no_price"+
			" FROM `%s.%s.%s`"+
			" ORDER BY event_slug, expiration_timestamp, snapshot_timestamp",
		project, dataset, table,
	))

	it, err := q.Read(ctx)
	if err != nil {
		log.Fatalf("executing BQ query: %v", err)
	}

	// Collect rows grouped by event_slug, preserving BQ order.
	type eventData struct {
		title     string
		category  string
		snapshots []SnapshotExport
	}
	events := make(map[string]*eventData)
	var eventOrder []string

	for {
		var row bqRow
		if err := it.Next(&row); err == iterator.Done {
			break
		} else if err != nil {
			log.Fatalf("reading BQ row: %v", err)
		}

		if _, ok := events[row.EventSlug]; !ok {
			events[row.EventSlug] = &eventData{title: row.EventTitle, category: row.Category}
			eventOrder = append(eventOrder, row.EventSlug)
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

	log.Printf("loaded %d event(s) from BigQuery", len(eventOrder))

	now := time.Now().UTC().Format(time.RFC3339)
	bucket := gcsClient.Bucket(bucketName)
	var indexEntries []IndexEntry

	for _, slug := range eventOrder {
		data := events[slug]
		export := EventExport{
			EventSlug:   slug,
			EventTitle:  data.title,
			Category:    data.category,
			GeneratedAt: now,
			Snapshots:   data.snapshots,
		}

		path := fmt.Sprintf("events/%s.json", slug)
		if err := writeJSON(ctx, bucket, path, export); err != nil {
			log.Printf("warning: failed to write %s: %v", path, err)
			continue
		}
		log.Printf("wrote %s (%d snapshots)", path, len(data.snapshots))

		latest := ""
		if n := len(data.snapshots); n > 0 {
			latest = data.snapshots[n-1].SnapshotTimestamp
		}
		indexEntries = append(indexEntries, IndexEntry{
			EventSlug:      slug,
			EventTitle:     data.title,
			Category:       data.category,
			SnapshotCount:  len(data.snapshots),
			LatestSnapshot: latest,
			File:           path,
		})
	}

	if err := writeJSON(ctx, bucket, "index.json", Index{GeneratedAt: now, Events: indexEntries}); err != nil {
		log.Fatalf("writing index.json: %v", err)
	}
	log.Printf("done: exported %d event(s) to gs://%s", len(eventOrder), bucketName)
}

func writeJSON(ctx context.Context, bucket *storage.BucketHandle, path string, v any) error {
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

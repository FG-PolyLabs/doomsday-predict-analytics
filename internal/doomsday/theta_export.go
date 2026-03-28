// theta_export.go — queries the market_theta view from BigQuery and publishes
// per-event theta (time-decay) JSON files to GCS.
//
// GCS layout:
//
//	gs://<bucket>/theta/index.json
//	gs://<bucket>/theta/<event-slug>.json
package doomsday

import (
	"context"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// ThetaPoint is one row of the market_theta view: the daily price change for a
// single question on a single day.
type ThetaPoint struct {
	Question          string  `json:"question"`
	SnapshotDate      string  `json:"snapshot_date"`
	ExpirationDate    string  `json:"expiration_date"`
	DaysToExpiry      int64   `json:"days_to_expiry"`
	YesPrice          float64 `json:"yes_price"`
	PrevYesPrice      float64 `json:"prev_yes_price"`
	PrevSnapshotDate  string  `json:"prev_snapshot_date"`
	PrevDaysToExpiry  int64   `json:"prev_days_to_expiry"`
	DaysElapsed       int64   `json:"days_elapsed"`
	Theta             float64 `json:"theta"`
	ThetaPerDay       float64 `json:"theta_per_day"`
	ThetaPct          float64 `json:"theta_pct"`
}

// ThetaExport is the per-event JSON file written to GCS.
type ThetaExport struct {
	EventSlug   string       `json:"event_slug"`
	EventTitle  string       `json:"event_title"`
	Category    string       `json:"category"`
	GeneratedAt string       `json:"generated_at"`
	Points      []ThetaPoint `json:"points"`
}

// ThetaIndexEntry is one entry in the theta index manifest.
type ThetaIndexEntry struct {
	EventSlug   string `json:"event_slug"`
	EventTitle  string `json:"event_title"`
	Category    string `json:"category"`
	PointCount  int    `json:"point_count"`
	LatestDate  string `json:"latest_date,omitempty"`
	File        string `json:"file"`
}

// ThetaIndex is the top-level theta/index.json manifest.
type ThetaIndex struct {
	GeneratedAt string            `json:"generated_at"`
	Events      []ThetaIndexEntry `json:"events"`
}

// thetaBQRow mirrors the market_theta view columns.
// DATE columns are cast to TIMESTAMP in the query to avoid the civil.Date dependency.
type thetaBQRow struct {
	EventSlug         string                 `bigquery:"event_slug"`
	EventTitle        string                 `bigquery:"event_title"`
	Question          string                 `bigquery:"question"`
	Category          string                 `bigquery:"category"`
	SnapshotDate      time.Time              `bigquery:"snapshot_date"`
	ExpirationDate    bigquery.NullTimestamp `bigquery:"expiration_date"`
	DaysToExpiry      int64                  `bigquery:"days_to_expiry"`
	YesPrice          float64                `bigquery:"yes_price"`
	PrevYesPrice      float64                `bigquery:"prev_yes_price"`
	PrevSnapshotDate  bigquery.NullTimestamp `bigquery:"prev_snapshot_date"`
	PrevDaysToExpiry  bigquery.NullInt64     `bigquery:"prev_days_to_expiry"`
	DaysElapsed       bigquery.NullInt64     `bigquery:"days_elapsed"`
	Theta             float64                `bigquery:"theta"`
	ThetaPerDay       bigquery.NullFloat64   `bigquery:"theta_per_day"`
	ThetaPct          bigquery.NullFloat64   `bigquery:"theta_pct"`
}

// RunThetaExport queries the market_theta view and writes per-event JSON files
// to GCS under the theta/ prefix. It uses the same ExportConfig as RunExport.
func RunThetaExport(ctx context.Context, cfg ExportConfig) error {
	bqClient, err := bigquery.NewClient(ctx, cfg.Project)
	if err != nil {
		return fmt.Errorf("theta export: creating BQ client: %w", err)
	}
	defer bqClient.Close()

	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("theta export: creating GCS client: %w", err)
	}
	defer gcsClient.Close()

	// Cast DATE columns to TIMESTAMP so they scan into time.Time without
	// requiring the cloud.google.com/go/civil package.
	q := bqClient.Query(fmt.Sprintf(`
		SELECT
			event_slug, event_title, question, category,
			TIMESTAMP(snapshot_date)    AS snapshot_date,
			TIMESTAMP(expiration_date)  AS expiration_date,
			days_to_expiry,
			yes_price, prev_yes_price,
			TIMESTAMP(prev_snapshot_date) AS prev_snapshot_date,
			prev_days_to_expiry,
			days_elapsed,
			theta, theta_per_day, theta_pct
		FROM `+"`%s.doomsday.market_theta`"+`
		ORDER BY event_slug, question, snapshot_date
	`, cfg.Project))

	it, err := q.Read(ctx)
	if err != nil {
		return fmt.Errorf("theta export: running BQ query: %w", err)
	}

	type eventAcc struct {
		title    string
		category string
		points   []ThetaPoint
	}
	events := make(map[string]*eventAcc)
	var order []string

	for {
		var row thetaBQRow
		if err := it.Next(&row); err == iterator.Done {
			break
		} else if err != nil {
			return fmt.Errorf("theta export: reading BQ row: %w", err)
		}

		if _, ok := events[row.EventSlug]; !ok {
			events[row.EventSlug] = &eventAcc{title: row.EventTitle, category: row.Category}
			order = append(order, row.EventSlug)
		}

		p := ThetaPoint{
			Question:     row.Question,
			SnapshotDate: row.SnapshotDate.UTC().Format("2006-01-02"),
			DaysToExpiry: row.DaysToExpiry,
			YesPrice:     row.YesPrice,
			PrevYesPrice: row.PrevYesPrice,
			Theta:        row.Theta,
		}
		if row.ExpirationDate.Valid {
			p.ExpirationDate = row.ExpirationDate.Timestamp.UTC().Format("2006-01-02")
		}
		if row.PrevSnapshotDate.Valid {
			p.PrevSnapshotDate = row.PrevSnapshotDate.Timestamp.UTC().Format("2006-01-02")
		}
		if row.PrevDaysToExpiry.Valid {
			p.PrevDaysToExpiry = row.PrevDaysToExpiry.Int64
		}
		if row.DaysElapsed.Valid {
			p.DaysElapsed = row.DaysElapsed.Int64
		}
		if row.ThetaPerDay.Valid {
			p.ThetaPerDay = row.ThetaPerDay.Float64
		}
		if row.ThetaPct.Valid {
			p.ThetaPct = row.ThetaPct.Float64
		}

		events[row.EventSlug].points = append(events[row.EventSlug].points, p)
	}
	log.Printf("theta export: %d event(s) loaded from BigQuery", len(order))

	now := time.Now().UTC().Format(time.RFC3339)
	bucket := gcsClient.Bucket(cfg.BucketName)
	var indexEntries []ThetaIndexEntry

	for _, slug := range order {
		acc := events[slug]
		export := ThetaExport{
			EventSlug:   slug,
			EventTitle:  acc.title,
			Category:    acc.category,
			GeneratedAt: now,
			Points:      acc.points,
		}
		gcsPath := fmt.Sprintf("theta/%s.json", slug)
		if err := gcsWriteJSON(ctx, bucket, gcsPath, export); err != nil {
			log.Printf("theta export: warning: GCS %s failed: %v", gcsPath, err)
			continue
		}
		log.Printf("theta export: GCS %s (%d points)", gcsPath, len(acc.points))

		latest := ""
		if n := len(acc.points); n > 0 {
			latest = acc.points[n-1].SnapshotDate
		}
		indexEntries = append(indexEntries, ThetaIndexEntry{
			EventSlug:  slug,
			EventTitle: acc.title,
			Category:   acc.category,
			PointCount: len(acc.points),
			LatestDate: latest,
			File:       gcsPath,
		})
	}

	index := ThetaIndex{GeneratedAt: now, Events: indexEntries}
	if err := gcsWriteJSON(ctx, bucket, "theta/index.json", index); err != nil {
		return fmt.Errorf("theta export: GCS theta/index.json: %w", err)
	}
	log.Printf("theta export: GCS done — %d event(s) in gs://%s/theta/", len(order), cfg.BucketName)
	return nil
}

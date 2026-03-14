package doomsday

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
)

// BQLoader loads MarketSnapshots into BigQuery using a staging-table MERGE strategy.
type BQLoader struct {
	client    *bigquery.Client
	projectID string
	datasetID string
	tableID   string
}

// NewBQLoader creates a loader using Application Default Credentials.
func NewBQLoader(ctx context.Context, projectID, datasetID, tableID string) (*BQLoader, error) {
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("creating bigquery client: %w", err)
	}
	return &BQLoader{
		client:    client,
		projectID: projectID,
		datasetID: datasetID,
		tableID:   tableID,
	}, nil
}

// Close releases the underlying BigQuery client.
func (l *BQLoader) Close() error {
	return l.client.Close()
}

// snapshotRow is the JSONL-encodable form of a MarketSnapshot, with field names
// matching BigQuery column names exactly.
type snapshotRow struct {
	EventSlug           string   `json:"event_slug"`
	EventTitle          string   `json:"event_title"`
	Question            string   `json:"question"`
	Category            *string  `json:"category"`
	SnapshotDate        string   `json:"snapshot_date"`        // YYYY-MM-DD
	SnapshotTimestamp   string   `json:"snapshot_timestamp"`   // RFC3339
	ExpirationTimestamp *string  `json:"expiration_timestamp"` // RFC3339; null if market has no end date
	YesPrice            float64  `json:"yes_price"`
	NoPrice             float64  `json:"no_price"`
	BestBid             *float64 `json:"best_bid"`
	BestAsk             *float64 `json:"best_ask"`
	Spread              *float64 `json:"spread"`
	Volume24h           *float64 `json:"volume_24h"`
	VolumeTotal         *float64 `json:"volume_total"`
	Liquidity           *float64 `json:"liquidity"`
}

func toRow(s MarketSnapshot) snapshotRow {
	r := snapshotRow{
		EventSlug:         s.EventSlug,
		EventTitle:        s.EventTitle,
		Question:          s.Question,
		SnapshotDate:      s.SnapshotTimestamp.UTC().Format("2006-01-02"),
		SnapshotTimestamp: s.SnapshotTimestamp.UTC().Format(time.RFC3339),
		YesPrice:          s.YesPrice,
		NoPrice:           s.NoPrice,
		BestBid:           s.BestBid,
		BestAsk:           s.BestAsk,
		Spread:            s.Spread,
		Volume24h:         s.Volume24h,
		VolumeTotal:       s.VolumeTotal,
		Liquidity:         s.Liquidity,
	}
	if s.Category != "" {
		r.Category = &s.Category
	}
	if !s.ExpirationTimestamp.IsZero() {
		exp := s.ExpirationTimestamp.UTC().Format(time.RFC3339)
		r.ExpirationTimestamp = &exp
	}
	return r
}

// MergeSnapshots loads snapshots into a temporary table then merges them into the
// target table, deduplicating on (event_slug, snapshot_timestamp).
// Returns the count of newly inserted rows.
func (l *BQLoader) MergeSnapshots(ctx context.Context, snapshots []MarketSnapshot) (int, error) {
	if len(snapshots) == 0 {
		return 0, nil
	}

	// Encode all snapshots as newline-delimited JSON.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, s := range snapshots {
		if err := enc.Encode(toRow(s)); err != nil {
			return 0, fmt.Errorf("encoding snapshot: %w", err)
		}
	}

	// Create a short-lived staging table (expires in 1 hour).
	tmpTableID := fmt.Sprintf("_tmp_doomsday_%d", time.Now().UnixNano())
	tmpRef := l.client.Dataset(l.datasetID).Table(tmpTableID)
	schema := snapshotSchema()
	if err := tmpRef.Create(ctx, &bigquery.TableMetadata{
		Schema:         schema,
		ExpirationTime: time.Now().Add(time.Hour),
	}); err != nil {
		return 0, fmt.Errorf("creating staging table: %w", err)
	}

	// Load JSONL into the staging table.
	rs := bigquery.NewReaderSource(&buf)
	rs.SourceFormat = bigquery.JSON
	rs.Schema = schema
	loadJob, err := tmpRef.LoaderFrom(rs).Run(ctx)
	if err != nil {
		return 0, fmt.Errorf("starting load job: %w", err)
	}
	loadStatus, err := loadJob.Wait(ctx)
	if err != nil {
		return 0, fmt.Errorf("waiting for load job: %w", err)
	}
	if err := loadStatus.Err(); err != nil {
		return 0, fmt.Errorf("load job failed: %w", err)
	}

	// MERGE staging → target, inserting only rows with a new (event_slug, snapshot_timestamp).
	dst := fmt.Sprintf("`%s.%s.%s`", l.projectID, l.datasetID, l.tableID)
	src := fmt.Sprintf("`%s.%s.%s`", l.projectID, l.datasetID, tmpTableID)
	mergeSQL := fmt.Sprintf(`
MERGE %s T
USING %s S
ON T.event_slug = S.event_slug
   AND T.snapshot_timestamp = S.snapshot_timestamp
WHEN NOT MATCHED THEN INSERT (
  event_slug, event_title, question, category,
  snapshot_date, snapshot_timestamp, expiration_timestamp,
  yes_price, no_price,
  best_bid, best_ask, spread, volume_24h, volume_total, liquidity
) VALUES (
  S.event_slug, S.event_title, S.question, S.category,
  S.snapshot_date, S.snapshot_timestamp, S.expiration_timestamp,
  S.yes_price, S.no_price,
  S.best_bid, S.best_ask, S.spread, S.volume_24h, S.volume_total, S.liquidity
)`, dst, src)

	q := l.client.Query(mergeSQL)
	q.Location = "US"
	mergeJob, err := q.Run(ctx)
	if err != nil {
		return 0, fmt.Errorf("starting merge job: %w", err)
	}
	mergeStatus, err := mergeJob.Wait(ctx)
	if err != nil {
		return 0, fmt.Errorf("waiting for merge job: %w", err)
	}
	if err := mergeStatus.Err(); err != nil {
		return 0, fmt.Errorf("merge job failed: %w", err)
	}

	stats, ok := mergeStatus.Statistics.Details.(*bigquery.QueryStatistics)
	if !ok || stats == nil {
		return 0, nil
	}
	return int(stats.DMLStats.InsertedRowCount), nil
}

func snapshotSchema() bigquery.Schema {
	return bigquery.Schema{
		{Name: "event_slug", Type: bigquery.StringFieldType, Required: true},
		{Name: "event_title", Type: bigquery.StringFieldType, Required: true},
		{Name: "question", Type: bigquery.StringFieldType, Required: true},
		{Name: "category", Type: bigquery.StringFieldType, Required: false},
		{Name: "snapshot_date", Type: bigquery.DateFieldType, Required: true},
		{Name: "snapshot_timestamp", Type: bigquery.TimestampFieldType, Required: true},
		{Name: "expiration_timestamp", Type: bigquery.TimestampFieldType, Required: false},
		{Name: "yes_price", Type: bigquery.FloatFieldType, Required: true},
		{Name: "no_price", Type: bigquery.FloatFieldType, Required: true},
		{Name: "best_bid", Type: bigquery.FloatFieldType, Required: false},
		{Name: "best_ask", Type: bigquery.FloatFieldType, Required: false},
		{Name: "spread", Type: bigquery.FloatFieldType, Required: false},
		{Name: "volume_24h", Type: bigquery.FloatFieldType, Required: false},
		{Name: "volume_total", Type: bigquery.FloatFieldType, Required: false},
		{Name: "liquidity", Type: bigquery.FloatFieldType, Required: false},
	}
}

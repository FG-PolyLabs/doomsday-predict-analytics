// snapshots.go — read-side queries on market_snapshots for the API layer.
package doomsday

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

// EventSummary is a lightweight summary of one event from market_snapshots.
type EventSummary struct {
	EventSlug      string    `json:"event_slug"`
	EventTitle     string    `json:"event_title"`
	Category       string    `json:"category"`
	SnapshotCount  int       `json:"snapshot_count"`
	LatestSnapshot time.Time `json:"latest_snapshot"`
}

// EventDetail holds the full snapshot history for one event.
type EventDetail struct {
	EventSlug  string          `json:"event_slug"`
	EventTitle string          `json:"event_title"`
	Category   string          `json:"category"`
	Snapshots  []SnapshotPoint `json:"snapshots"`
}

// SnapshotPoint is one price point returned by the API.
type SnapshotPoint struct {
	Question            string     `json:"question"`
	SnapshotTimestamp   time.Time  `json:"snapshot_timestamp"`
	ExpirationTimestamp *time.Time `json:"expiration_timestamp,omitempty"`
	YesPrice            float64    `json:"yes_price"`
	NoPrice             float64    `json:"no_price"`
}

// ListEvents returns one summary row per distinct event_slug in market_snapshots.
func (l *BQLoader) ListEvents(ctx context.Context) ([]EventSummary, error) {
	sql := fmt.Sprintf(`
		SELECT
			event_slug,
			MAX(event_title)       AS event_title,
			MAX(category)          AS category,
			COUNT(*)               AS snapshot_count,
			MAX(snapshot_timestamp) AS latest_snapshot
		FROM `+"`%s.%s.%s`"+`
		GROUP BY event_slug
		ORDER BY event_slug`,
		l.projectID, l.datasetID, l.tableID,
	)
	q := l.client.Query(sql)
	q.Location = "US"
	it, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("running events query: %w", err)
	}

	type summaryRow struct {
		EventSlug      string    `bigquery:"event_slug"`
		EventTitle     string    `bigquery:"event_title"`
		Category       string    `bigquery:"category"`
		SnapshotCount  int64     `bigquery:"snapshot_count"`
		LatestSnapshot time.Time `bigquery:"latest_snapshot"`
	}

	var summaries []EventSummary
	for {
		var row summaryRow
		if err := it.Next(&row); err == iterator.Done {
			break
		} else if err != nil {
			return nil, fmt.Errorf("reading events row: %w", err)
		}
		summaries = append(summaries, EventSummary{
			EventSlug:      row.EventSlug,
			EventTitle:     row.EventTitle,
			Category:       row.Category,
			SnapshotCount:  int(row.SnapshotCount),
			LatestSnapshot: row.LatestSnapshot,
		})
	}
	return summaries, nil
}

// GetEventSnapshots returns the full price history for one event slug.
// Returns nil if no snapshots exist for that slug.
func (l *BQLoader) GetEventSnapshots(ctx context.Context, slug string) (*EventDetail, error) {
	q := l.client.Query(fmt.Sprintf(`
		SELECT
			event_slug, event_title, category,
			question, snapshot_timestamp, expiration_timestamp,
			yes_price, no_price
		FROM `+"`%s.%s.%s`"+`
		WHERE event_slug = @slug
		ORDER BY question, snapshot_timestamp`,
		l.projectID, l.datasetID, l.tableID,
	))
	q.Parameters = []bigquery.QueryParameter{{Name: "slug", Value: slug}}
	q.Location = "US"
	it, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("running snapshots query: %w", err)
	}

	type snapRow struct {
		EventSlug           string                 `bigquery:"event_slug"`
		EventTitle          string                 `bigquery:"event_title"`
		Category            string                 `bigquery:"category"`
		Question            string                 `bigquery:"question"`
		SnapshotTimestamp   time.Time              `bigquery:"snapshot_timestamp"`
		ExpirationTimestamp bigquery.NullTimestamp `bigquery:"expiration_timestamp"`
		YesPrice            float64                `bigquery:"yes_price"`
		NoPrice             float64                `bigquery:"no_price"`
	}

	var detail *EventDetail
	for {
		var row snapRow
		if err := it.Next(&row); err == iterator.Done {
			break
		} else if err != nil {
			return nil, fmt.Errorf("reading snapshot row: %w", err)
		}
		if detail == nil {
			detail = &EventDetail{
				EventSlug:  row.EventSlug,
				EventTitle: row.EventTitle,
				Category:   row.Category,
			}
		}
		pt := SnapshotPoint{
			Question:          row.Question,
			SnapshotTimestamp: row.SnapshotTimestamp,
			YesPrice:          row.YesPrice,
			NoPrice:           row.NoPrice,
		}
		if row.ExpirationTimestamp.Valid {
			t := row.ExpirationTimestamp.Timestamp
			pt.ExpirationTimestamp = &t
		}
		detail.Snapshots = append(detail.Snapshots, pt)
	}
	return detail, nil
}

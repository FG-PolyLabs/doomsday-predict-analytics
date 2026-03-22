// market_store.go — CRUD operations on the doomsday.markets BigQuery table.
//
// The markets table is the single source of truth for which Polymarket events
// the pipeline tracks. Each row is either a direct slug (single-event mode) or
// a tag+slug_prefix pair (series-discovery mode).
package doomsday

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

// MarketConfig is one row in the doomsday.markets BQ table.
// Set Slug for a single named event, or Tag+SlugPrefix to auto-discover a series.
type MarketConfig struct {
	ID         string
	Slug       string // exact event slug (single-event mode)
	Tag        string // tag to search (series mode)
	SlugPrefix string // filter tag results by slug prefix (series mode)
	Category   string
	Active     bool
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// NewID generates a random hex ID suitable for use as a MarketConfig ID.
func NewID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

// MarketStore manages CRUD on the doomsday.markets BQ table.
type MarketStore struct {
	client    *bigquery.Client
	projectID string
	datasetID string
}

// NewMarketStore creates a MarketStore using Application Default Credentials.
func NewMarketStore(ctx context.Context, projectID, datasetID string) (*MarketStore, error) {
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("creating bigquery client: %w", err)
	}
	return &MarketStore{client: client, projectID: projectID, datasetID: datasetID}, nil
}

// Close releases the underlying BigQuery client.
func (s *MarketStore) Close() error {
	return s.client.Close()
}

// EnsureTable creates the markets table if it does not already exist.
func (s *MarketStore) EnsureTable(ctx context.Context) error {
	ref := s.client.Dataset(s.datasetID).Table("markets")
	if _, err := ref.Metadata(ctx); err == nil {
		return nil // already exists
	}
	return ref.Create(ctx, &bigquery.TableMetadata{Schema: marketsSchema()})
}

// ListActive returns all active market configs ordered by category, id.
func (s *MarketStore) ListActive(ctx context.Context) ([]MarketConfig, error) {
	return s.read(ctx, fmt.Sprintf(
		"SELECT * FROM `%s.%s.markets` WHERE active = TRUE ORDER BY category, id",
		s.projectID, s.datasetID,
	))
}

// ListAll returns all market configs including inactive ones.
func (s *MarketStore) ListAll(ctx context.Context) ([]MarketConfig, error) {
	return s.read(ctx, fmt.Sprintf(
		"SELECT * FROM `%s.%s.markets` ORDER BY category, id",
		s.projectID, s.datasetID,
	))
}

// GetByID returns a single market config by ID, or nil if not found.
func (s *MarketStore) GetByID(ctx context.Context, id string) (*MarketConfig, error) {
	q := s.client.Query(fmt.Sprintf(
		"SELECT * FROM `%s.%s.markets` WHERE id = @id",
		s.projectID, s.datasetID,
	))
	q.Parameters = []bigquery.QueryParameter{{Name: "id", Value: id}}
	q.Location = "US"
	configs, err := s.readQuery(ctx, q)
	if err != nil {
		return nil, err
	}
	if len(configs) == 0 {
		return nil, nil
	}
	return &configs[0], nil
}

// Create inserts a new market config. cfg.ID must be set before calling.
func (s *MarketStore) Create(ctx context.Context, cfg MarketConfig) error {
	now := time.Now().UTC()
	q := s.client.Query(fmt.Sprintf(
		"INSERT INTO `%s.%s.markets` (id, slug, tag, slug_prefix, category, active, created_at, updated_at)"+
			" VALUES (@id, @slug, @tag, @slug_prefix, @category, @active, @created_at, @updated_at)",
		s.projectID, s.datasetID,
	))
	q.Parameters = []bigquery.QueryParameter{
		{Name: "id", Value: cfg.ID},
		{Name: "slug", Value: toNullString(cfg.Slug)},
		{Name: "tag", Value: toNullString(cfg.Tag)},
		{Name: "slug_prefix", Value: toNullString(cfg.SlugPrefix)},
		{Name: "category", Value: cfg.Category},
		{Name: "active", Value: cfg.Active},
		{Name: "created_at", Value: now},
		{Name: "updated_at", Value: now},
	}
	q.Location = "US"
	return s.dml(ctx, q)
}

// Update changes the category and active flag for a market config.
// The slug/tag/slug_prefix fields are immutable after creation.
func (s *MarketStore) Update(ctx context.Context, id, category string, active bool) error {
	q := s.client.Query(fmt.Sprintf(
		"UPDATE `%s.%s.markets` SET category = @category, active = @active, updated_at = @updated_at WHERE id = @id",
		s.projectID, s.datasetID,
	))
	q.Parameters = []bigquery.QueryParameter{
		{Name: "category", Value: category},
		{Name: "active", Value: active},
		{Name: "updated_at", Value: time.Now().UTC()},
		{Name: "id", Value: id},
	}
	q.Location = "US"
	return s.dml(ctx, q)
}

// Delete removes a market config by ID.
func (s *MarketStore) Delete(ctx context.Context, id string) error {
	q := s.client.Query(fmt.Sprintf(
		"DELETE FROM `%s.%s.markets` WHERE id = @id",
		s.projectID, s.datasetID,
	))
	q.Parameters = []bigquery.QueryParameter{{Name: "id", Value: id}}
	q.Location = "US"
	return s.dml(ctx, q)
}

// --- internal helpers ---

type marketsBQRow struct {
	ID         string              `bigquery:"id"`
	Slug       bigquery.NullString `bigquery:"slug"`
	Tag        bigquery.NullString `bigquery:"tag"`
	SlugPrefix bigquery.NullString `bigquery:"slug_prefix"`
	Category   string              `bigquery:"category"`
	Active     bool                `bigquery:"active"`
	CreatedAt  time.Time           `bigquery:"created_at"`
	UpdatedAt  time.Time           `bigquery:"updated_at"`
}

func (s *MarketStore) read(ctx context.Context, sql string) ([]MarketConfig, error) {
	q := s.client.Query(sql)
	q.Location = "US"
	return s.readQuery(ctx, q)
}

func (s *MarketStore) readQuery(ctx context.Context, q *bigquery.Query) ([]MarketConfig, error) {
	it, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("running query: %w", err)
	}
	var configs []MarketConfig
	for {
		var row marketsBQRow
		if err := it.Next(&row); err == iterator.Done {
			break
		} else if err != nil {
			return nil, fmt.Errorf("reading row: %w", err)
		}
		cfg := MarketConfig{
			ID:        row.ID,
			Category:  row.Category,
			Active:    row.Active,
			CreatedAt: row.CreatedAt,
			UpdatedAt: row.UpdatedAt,
		}
		if row.Slug.Valid {
			cfg.Slug = row.Slug.StringVal
		}
		if row.Tag.Valid {
			cfg.Tag = row.Tag.StringVal
		}
		if row.SlugPrefix.Valid {
			cfg.SlugPrefix = row.SlugPrefix.StringVal
		}
		configs = append(configs, cfg)
	}
	return configs, nil
}

func (s *MarketStore) dml(ctx context.Context, q *bigquery.Query) error {
	job, err := q.Run(ctx)
	if err != nil {
		return fmt.Errorf("running DML: %w", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("waiting for DML: %w", err)
	}
	return status.Err()
}

func marketsSchema() bigquery.Schema {
	return bigquery.Schema{
		{Name: "id", Type: bigquery.StringFieldType, Required: true},
		{Name: "slug", Type: bigquery.StringFieldType},
		{Name: "tag", Type: bigquery.StringFieldType},
		{Name: "slug_prefix", Type: bigquery.StringFieldType},
		{Name: "category", Type: bigquery.StringFieldType, Required: true},
		{Name: "active", Type: bigquery.BooleanFieldType, Required: true},
		{Name: "created_at", Type: bigquery.TimestampFieldType, Required: true},
		{Name: "updated_at", Type: bigquery.TimestampFieldType, Required: true},
	}
}

func toNullString(s string) bigquery.NullString {
	return bigquery.NullString{StringVal: s, Valid: s != ""}
}

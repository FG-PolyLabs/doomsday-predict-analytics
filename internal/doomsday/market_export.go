// market_export.go — exports the markets table to GCS (data/markets.jsonl)
// and commits the same file to the GitHub data repo.
package doomsday

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/storage"
)

const marketsExportPath = "data/markets.jsonl"

// MarketExportConfig holds destinations for ExportMarkets.
type MarketExportConfig struct {
	GCSBucket  string // GCS bucket name
	GitHubRepo string // e.g. "FG-PolyLabs/doomsday-predict-data"
	GitHubPAT  string // personal access token; empty = skip GitHub
}

// DefaultMarketExportConfig returns config populated from environment variables.
func DefaultMarketExportConfig() MarketExportConfig {
	repo := os.Getenv("GITHUB_DATA_REPO")
	if repo == "" {
		repo = "FG-PolyLabs/doomsday-predict-data"
	}
	return MarketExportConfig{
		GCSBucket:  "fg-polylabs-doomsday",
		GitHubRepo: repo,
		GitHubPAT:  os.Getenv("DATA_SYNC_PAT"),
	}
}

// MarketExportResult summarises what was written.
type MarketExportResult struct {
	Count      int    `json:"count"`
	GCSWritten bool   `json:"gcs_written"`
	GitHubSHA  string `json:"github_sha,omitempty"`
}

// marketExportRow is the JSON shape for each line in the JSONL file.
type marketExportRow struct {
	ID         string  `json:"id"`
	Slug       *string `json:"slug"`
	Tag        *string `json:"tag"`
	SlugPrefix *string `json:"slug_prefix"`
	Category   string  `json:"category"`
	Active     bool    `json:"active"`
	CreatedAt  string  `json:"created_at"`
	UpdatedAt  *string `json:"updated_at"`
}

func nullableStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func buildMarketsJSONL(markets []MarketConfig) ([]byte, error) {
	var buf bytes.Buffer
	for _, m := range markets {
		row := marketExportRow{
			ID:         m.ID,
			Slug:       nullableStr(m.Slug),
			Tag:        nullableStr(m.Tag),
			SlugPrefix: nullableStr(m.SlugPrefix),
			Category:   m.Category,
			Active:     m.Active,
			CreatedAt:  m.CreatedAt.UTC().Format(time.RFC3339),
		}
		if !m.UpdatedAt.IsZero() {
			s := m.UpdatedAt.UTC().Format(time.RFC3339)
			row.UpdatedAt = &s
		}
		line, err := json.Marshal(row)
		if err != nil {
			return nil, fmt.Errorf("marshaling market %s: %w", m.ID, err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}

// ExportMarkets reads all markets from BigQuery, writes data/markets.jsonl to
// GCS, and (when GitHubPAT is set) commits the same file to GitHubRepo.
func ExportMarkets(ctx context.Context, store *MarketStore, cfg MarketExportConfig) (*MarketExportResult, error) {
	markets, err := store.ListAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing markets: %w", err)
	}

	data, err := buildMarketsJSONL(markets)
	if err != nil {
		return nil, err
	}

	result := &MarketExportResult{Count: len(markets)}

	// --- GCS ---
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating GCS client: %w", err)
	}
	defer gcsClient.Close()

	w := gcsClient.Bucket(cfg.GCSBucket).Object(marketsExportPath).NewWriter(ctx)
	w.ContentType = "application/x-ndjson"
	w.CacheControl = "public, max-age=300"
	if _, err := w.Write(data); err != nil {
		w.Close()
		return nil, fmt.Errorf("writing to GCS: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("closing GCS writer: %w", err)
	}
	result.GCSWritten = true

	// --- GitHub ---
	if cfg.GitHubRepo != "" && cfg.GitHubPAT != "" {
		sha, err := githubCommitFile(ctx, cfg.GitHubPAT, cfg.GitHubRepo, marketsExportPath, data,
			fmt.Sprintf("chore: export %d markets from BigQuery", len(markets)))
		if err != nil {
			// GCS succeeded; return partial result with the error.
			return result, fmt.Errorf("github commit: %w", err)
		}
		result.GitHubSHA = sha
	}

	return result, nil
}

// githubCommitFile creates or updates path in repo via the GitHub Contents API.
// Returns the new commit SHA.
func githubCommitFile(ctx context.Context, pat, repo, path string, content []byte, message string) (string, error) {
	apiURL := fmt.Sprintf("https://api.github.com/repos/%s/contents/%s", repo, path)

	currentSHA, err := githubFileSHA(ctx, pat, apiURL)
	if err != nil {
		return "", err
	}

	type putBody struct {
		Message string `json:"message"`
		Content string `json:"content"`
		SHA     string `json:"sha,omitempty"`
	}
	bodyBytes, _ := json.Marshal(putBody{
		Message: message,
		Content: base64.StdEncoding.EncodeToString(content),
		SHA:     currentSHA,
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, apiURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+pat)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("PUT request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("GitHub API %d: %s", resp.StatusCode, string(b))
	}

	var result struct {
		Commit struct {
			SHA string `json:"sha"`
		} `json:"commit"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decoding response: %w", err)
	}
	return result.Commit.SHA, nil
}

// githubFileSHA returns the blob SHA of path in the repo, or "" if it doesn't exist.
func githubFileSHA(ctx context.Context, pat, apiURL string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+pat)
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", nil // file doesn't exist yet — create it
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("GitHub GET %d: %s", resp.StatusCode, string(b))
	}

	var result struct {
		SHA string `json:"sha"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	return result.SHA, nil
}

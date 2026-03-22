// api is an HTTP server that exposes the doomsday data and market config
// management endpoints for the frontend admin.
//
// Environment variables:
//
//	PORT            HTTP listen port (default: 8080)
//	ALLOWED_ORIGIN  CORS allowed origin (default: *)
//	API_KEY         If set, all requests must include Authorization: Bearer <key>
//
// Routes:
//
//	GET    /api/v1/health
//	GET    /api/v1/markets[?active=true|false]
//	POST   /api/v1/markets
//	GET    /api/v1/markets/{id}
//	PATCH  /api/v1/markets/{id}
//	DELETE /api/v1/markets/{id}
//	GET    /api/v1/events
//	GET    /api/v1/events/{slug}
//	POST   /api/v1/jobs/run
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/FutureGadgetLabs/doomsday-predict-analytics/internal/doomsday"
	"golang.org/x/oauth2/google"
)

const (
	gcpProject = "fg-polylabs"
	gcpRegion  = "us-central1"
	bqDataset  = "doomsday"
	jobName    = "doomsday-polymarket"
)

type server struct {
	store  *doomsday.MarketStore
	loader *doomsday.BQLoader
	ctx    context.Context
}

func main() {
	ctx := context.Background()
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	store, err := doomsday.NewMarketStore(ctx, gcpProject, bqDataset)
	if err != nil {
		log.Fatalf("creating market store: %v", err)
	}
	defer store.Close()

	// Ensure the markets table exists on startup.
	if err := store.EnsureTable(ctx); err != nil {
		log.Fatalf("ensuring markets table: %v", err)
	}

	loader, err := doomsday.NewBQLoader(ctx, gcpProject, bqDataset, "market_snapshots")
	if err != nil {
		log.Fatalf("creating BQ loader: %v", err)
	}
	defer loader.Close()

	s := &server{store: store, loader: loader, ctx: ctx}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/health", s.health)
	mux.HandleFunc("GET /api/v1/markets", s.listMarkets)
	mux.HandleFunc("POST /api/v1/markets", s.createMarket)
	mux.HandleFunc("GET /api/v1/markets/{id}", s.getMarket)
	mux.HandleFunc("PATCH /api/v1/markets/{id}", s.updateMarket)
	mux.HandleFunc("DELETE /api/v1/markets/{id}", s.deleteMarket)
	mux.HandleFunc("GET /api/v1/events", s.listEvents)
	mux.HandleFunc("GET /api/v1/events/{slug}", s.getEvent)
	mux.HandleFunc("POST /api/v1/jobs/run", s.runJob)

	log.Printf("doomsday API listening on :%s", port)
	if err := http.ListenAndServe(":"+port, corsMiddleware(authMiddleware(mux))); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

// --- middleware ---

func corsMiddleware(next http.Handler) http.Handler {
	origin := os.Getenv("ALLOWED_ORIGIN")
	if origin == "" {
		origin = "*"
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func authMiddleware(next http.Handler) http.Handler {
	apiKey := os.Getenv("API_KEY")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if apiKey == "" {
			next.ServeHTTP(w, r)
			return
		}
		bearer := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if bearer != apiKey {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// --- handlers ---

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "time": time.Now().UTC().Format(time.RFC3339)})
}

// GET /api/v1/markets[?active=true|false]
func (s *server) listMarkets(w http.ResponseWriter, r *http.Request) {
	var configs []doomsday.MarketConfig
	var err error

	switch r.URL.Query().Get("active") {
	case "true":
		configs, err = s.store.ListActive(r.Context())
	default:
		configs, err = s.store.ListAll(r.Context())
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"markets": configs})
}

// POST /api/v1/markets
// Body: {"slug":"...","tag":"...","slug_prefix":"...","category":"...","active":true}
func (s *server) createMarket(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Slug       string `json:"slug"`
		Tag        string `json:"tag"`
		SlugPrefix string `json:"slug_prefix"`
		Category   string `json:"category"`
		Active     *bool  `json:"active"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if body.Slug == "" && body.Tag == "" {
		writeError(w, http.StatusBadRequest, "slug or tag is required")
		return
	}
	if body.Category == "" {
		writeError(w, http.StatusBadRequest, "category is required")
		return
	}
	active := true
	if body.Active != nil {
		active = *body.Active
	}
	cfg := doomsday.MarketConfig{
		ID:         doomsday.NewID(),
		Slug:       body.Slug,
		Tag:        body.Tag,
		SlugPrefix: body.SlugPrefix,
		Category:   body.Category,
		Active:     active,
	}
	if err := s.store.Create(r.Context(), cfg); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, cfg)
}

// GET /api/v1/markets/{id}
func (s *server) getMarket(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	cfg, err := s.store.GetByID(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if cfg == nil {
		writeError(w, http.StatusNotFound, "market not found")
		return
	}
	writeJSON(w, http.StatusOK, cfg)
}

// PATCH /api/v1/markets/{id}
// Body: {"category":"...","active":true|false}
func (s *server) updateMarket(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	existing, err := s.store.GetByID(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if existing == nil {
		writeError(w, http.StatusNotFound, "market not found")
		return
	}

	var body struct {
		Category *string `json:"category"`
		Active   *bool   `json:"active"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	category := existing.Category
	if body.Category != nil {
		category = *body.Category
	}
	active := existing.Active
	if body.Active != nil {
		active = *body.Active
	}

	if err := s.store.Update(r.Context(), id, category, active); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	existing.Category = category
	existing.Active = active
	writeJSON(w, http.StatusOK, existing)
}

// DELETE /api/v1/markets/{id}
func (s *server) deleteMarket(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.store.Delete(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /api/v1/events
func (s *server) listEvents(w http.ResponseWriter, r *http.Request) {
	events, err := s.loader.ListEvents(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

// GET /api/v1/events/{slug}
func (s *server) getEvent(w http.ResponseWriter, r *http.Request) {
	slug := r.PathValue("slug")
	detail, err := s.loader.GetEventSnapshots(r.Context(), slug)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if detail == nil {
		writeError(w, http.StatusNotFound, "event not found")
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

// POST /api/v1/jobs/run
// Body: {"mode":"all"|"slug"|"tag","slug":"...","tag":"...","category":"...","no_volume":false,"yesterday":false,"active_only":true}
func (s *server) runJob(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Mode       string `json:"mode"`        // "all", "slug", "tag"
		Slug       string `json:"slug"`
		Tag        string `json:"tag"`
		Category   string `json:"category"`
		NoVolume   bool   `json:"no_volume"`
		Yesterday  bool   `json:"yesterday"`
		ActiveOnly bool   `json:"active_only"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	// Build the args list for the Cloud Run job container.
	var args []string
	switch body.Mode {
	case "all", "":
		args = append(args, "--all")
	case "slug":
		if body.Slug == "" {
			writeError(w, http.StatusBadRequest, "slug is required for mode=slug")
			return
		}
		args = append(args, "--slug="+body.Slug)
		if body.Category != "" {
			args = append(args, "--category="+body.Category)
		}
	case "tag":
		if body.Tag == "" {
			writeError(w, http.StatusBadRequest, "tag is required for mode=tag")
			return
		}
		args = append(args, "--tag="+body.Tag)
		if body.Category != "" {
			args = append(args, "--category="+body.Category)
		}
	default:
		writeError(w, http.StatusBadRequest, fmt.Sprintf("unknown mode %q; use all, slug, or tag", body.Mode))
		return
	}
	if body.Yesterday {
		args = append(args, "--yesterday")
	}
	if body.ActiveOnly {
		args = append(args, "--active-only")
	}
	if body.NoVolume {
		args = append(args, "--no-volume")
	}

	if err := triggerCloudRunJob(r.Context(), gcpProject, gcpRegion, jobName, args); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("triggering job: %v", err))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"message": "job triggered",
		"args":    args,
	})
}

// triggerCloudRunJob calls the Cloud Run Jobs v2 API to run the job with overridden args.
func triggerCloudRunJob(ctx context.Context, project, region, job string, args []string) error {
	creds, err := google.FindDefaultCredentials(ctx, "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		return fmt.Errorf("getting credentials: %w", err)
	}
	token, err := creds.TokenSource.Token()
	if err != nil {
		return fmt.Errorf("getting token: %w", err)
	}

	// Build the run request body with container arg overrides.
	argsJSON, _ := json.Marshal(args)
	body := fmt.Sprintf(
		`{"overrides":{"containerOverrides":[{"args":%s}]}}`,
		string(argsJSON),
	)

	url := fmt.Sprintf(
		"https://run.googleapis.com/v2/projects/%s/locations/%s/jobs/%s:run",
		project, region, job,
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		return fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token.AccessToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("calling Cloud Run API: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("Cloud Run API returned %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

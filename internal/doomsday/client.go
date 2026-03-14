package doomsday

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const (
	gammaBaseURL = "https://gamma-api.polymarket.com"
	clobBaseURL  = "https://clob.polymarket.com"
)

// Client is an HTTP client for the Polymarket Gamma and CLOB APIs.
type Client struct {
	http *http.Client
}

// NewClient creates a new Polymarket API client.
func NewClient() *Client {
	return &Client{
		http: &http.Client{Timeout: 30 * time.Second},
	}
}

// GetEventBySlug fetches a Polymarket event and its markets by event slug.
func (c *Client) GetEventBySlug(slug string) (*GammaEvent, error) {
	u := fmt.Sprintf("%s/events?slug=%s", gammaBaseURL, url.QueryEscape(slug))
	body, err := c.get(u)
	if err != nil {
		return nil, fmt.Errorf("fetching event: %w", err)
	}

	var events []GammaEvent
	if err := json.Unmarshal(body, &events); err != nil {
		return nil, fmt.Errorf("parsing event response: %w", err)
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("no event found for slug %q", slug)
	}
	return &events[0], nil
}

// GetEventsByTag fetches all Polymarket events for a given tag slug.
func (c *Client) GetEventsByTag(tagSlug string) ([]GammaEvent, error) {
	u := fmt.Sprintf("%s/events?tag_slug=%s&limit=100", gammaBaseURL, url.QueryEscape(tagSlug))
	body, err := c.get(u)
	if err != nil {
		return nil, fmt.Errorf("fetching events for tag %q: %w", tagSlug, err)
	}
	var events []GammaEvent
	if err := json.Unmarshal(body, &events); err != nil {
		return nil, fmt.Errorf("parsing events response: %w", err)
	}
	return events, nil
}

// GetMarketByID fetches a single market's full details (including clobTokenIds) by its Gamma market ID.
func (c *Client) GetMarketByID(marketID string) (*GammaMarket, error) {
	u := fmt.Sprintf("%s/markets/%s", gammaBaseURL, url.PathEscape(marketID))
	body, err := c.get(u)
	if err != nil {
		return nil, fmt.Errorf("fetching market %s: %w", marketID, err)
	}
	var m GammaMarket
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("parsing market response: %w", err)
	}
	return &m, nil
}

// GetPriceHistory fetches the price history for a CLOB token over a time range.
// fidelity is the granularity in minutes (e.g., 60 = hourly, 1 = per-minute).
func (c *Client) GetPriceHistory(tokenID string, startTs, endTs int64, fidelity int) ([]CLOBPricePoint, error) {
	params := url.Values{}
	params.Set("market", tokenID)
	params.Set("startTs", strconv.FormatInt(startTs, 10))
	params.Set("endTs", strconv.FormatInt(endTs, 10))
	params.Set("fidelity", strconv.Itoa(fidelity))

	u := fmt.Sprintf("%s/prices-history?%s", clobBaseURL, params.Encode())
	body, err := c.get(u)
	if err != nil {
		return nil, fmt.Errorf("fetching price history: %w", err)
	}

	var resp CLOBPriceHistoryResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parsing price history response: %w", err)
	}
	return resp.History, nil
}

func (c *Client) get(rawURL string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "doomsday-predict-analytics/1.0")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", rawURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s returned status %d", rawURL, resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

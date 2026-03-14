package doomsday

import (
	"encoding/json"
	"time"
)

// StringSlice handles Polymarket's quirk of returning JSON arrays as encoded strings
// (e.g., `"[\"Yes\",\"No\"]"`) or as actual JSON arrays interchangeably.
type StringSlice []string

func (s *StringSlice) UnmarshalJSON(data []byte) error {
	var arr []string
	if err := json.Unmarshal(data, &arr); err == nil {
		*s = arr
		return nil
	}
	var encoded string
	if err := json.Unmarshal(data, &encoded); err != nil {
		return err
	}
	return json.Unmarshal([]byte(encoded), s)
}

// GammaEvent represents a Polymarket event from the Gamma API.
type GammaEvent struct {
	ID      string        `json:"id"`
	Slug    string        `json:"slug"`
	Title   string        `json:"title"`
	Markets []GammaMarket `json:"markets"`
}

// GammaMarket represents a single market (outcome) within a Polymarket event.
type GammaMarket struct {
	ID              string      `json:"id"`
	Question        string      `json:"question"`
	ConditionID     string      `json:"conditionId"`
	ClobTokenIDs    StringSlice `json:"clobTokenIds"`
	Outcomes        StringSlice `json:"outcomes"`
	OutcomePrices   StringSlice `json:"outcomePrices"`
	Active          bool        `json:"active"`
	Closed          bool        `json:"closed"`
	AcceptingOrders bool        `json:"acceptingOrders"`
	NegRisk         bool        `json:"negRisk"`
	BestBid         float64     `json:"bestBid"`
	BestAsk         float64     `json:"bestAsk"`
	LastTradePrice  float64     `json:"lastTradePrice"`
	Volume24hr      float64     `json:"volume24hrNum"`
	VolumeTotal     float64     `json:"volumeNum"`
	Liquidity       float64     `json:"liquidityNum"`
	EndDateIso      string      `json:"endDateIso"`
	StartDateIso    string      `json:"startDateIso"`
}

// YesTokenID returns the CLOB token ID for the YES outcome.
func (m *GammaMarket) YesTokenID() string {
	if len(m.ClobTokenIDs) > 0 {
		return m.ClobTokenIDs[0]
	}
	return ""
}

// NoTokenID returns the CLOB token ID for the NO outcome.
func (m *GammaMarket) NoTokenID() string {
	if len(m.ClobTokenIDs) > 1 {
		return m.ClobTokenIDs[1]
	}
	return ""
}

// CLOBPriceHistoryResponse is the response from /prices-history.
type CLOBPriceHistoryResponse struct {
	History []CLOBPricePoint `json:"history"`
}

// CLOBPricePoint is a single timestamped price from the CLOB API.
type CLOBPricePoint struct {
	T int64   `json:"t"` // unix timestamp
	P float64 `json:"p"` // price (0.0–1.0)
}

// MarketSnapshot is a single row of doomsday prediction data — maps 1:1 to the BQ table.
type MarketSnapshot struct {
	EventSlug           string
	EventTitle          string
	Question            string
	Category            string    // optional; empty string stored as NULL
	SnapshotTimestamp   time.Time // UTC time of this price snapshot
	ExpirationTimestamp time.Time // market resolution time; zero value stored as NULL
	YesPrice            float64
	NoPrice             float64
	BestBid             *float64 // nil when --no-volume is set
	BestAsk             *float64
	Spread              *float64
	Volume24h           *float64
	VolumeTotal         *float64
	Liquidity           *float64
}

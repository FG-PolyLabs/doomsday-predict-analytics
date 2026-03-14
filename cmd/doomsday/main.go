// doomsday is a CLI runner that pulls binary yes/no prediction market data from
// Polymarket and lands it into BigQuery (fg-polylabs.doomsday.market_snapshots).
//
// Usage:
//
//	doomsday --slug=us-x-iran-ceasefire-by-march-31 --category=war
//	doomsday --slug=us-x-iran-ceasefire-by-march-31 --fidelity=1 --dry-run
//	doomsday --slug=will-there-be-a-nuclear-event-in-2025 --category=nuclear --no-volume
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"time"

	"github.com/FutureGadgetLabs/doomsday-predict-analytics/internal/doomsday"
)

func main() {
	slug := flag.String("slug", "", "Polymarket event slug (required)")
	category := flag.String("category", "", "Event category for filtering/grouping (e.g. war, nuclear, political)")
	fidelity := flag.Int("fidelity", 60, "Price snapshot granularity in minutes (e.g., 60=hourly, 1=per-minute)")
	dryRun := flag.Bool("dry-run", false, "Print rows as JSONL to stdout instead of loading to BigQuery")
	noVolume := flag.Bool("no-volume", false, "Store NULL for volume/liquidity/bid-ask fields (use for historical backfills)")
	flag.Parse()

	if *slug == "" {
		fmt.Fprintln(os.Stderr, "Usage: doomsday --slug=<event-slug> [--category=<category>] [--fidelity=60] [--dry-run] [--no-volume]")
		os.Exit(1)
	}

	client := doomsday.NewClient()

	event, err := client.GetEventBySlug(*slug)
	if err != nil {
		log.Fatalf("could not find event for slug %q: %v", *slug, err)
	}
	log.Printf("found event: %q (%d market(s))", event.Title, len(event.Markets))

	if len(event.Markets) == 0 {
		log.Fatalf("no markets found for event slug %q", *slug)
	}

	// Hydrate any markets missing CLOB token IDs (the event endpoint sometimes omits them).
	markets := event.Markets
	for i, m := range markets {
		if len(m.ClobTokenIDs) == 0 && m.ID != "" {
			full, err := client.GetMarketByID(m.ID)
			if err != nil {
				log.Printf("warning: could not hydrate market %s: %v", m.ID, err)
				continue
			}
			markets[i] = *full
		}
	}

	// For a binary event there is typically one market. If there are multiple
	// (e.g., the event groups related binary markets), we process all of them.
	now := time.Now().UTC()

	var snapshots []doomsday.MarketSnapshot

	for _, market := range markets {
		if market.YesTokenID() == "" {
			log.Printf("skipping market %q — no CLOB token IDs available", market.Question)
			continue
		}

		// Skip markets with zero trading activity.
		if market.VolumeTotal == 0 && market.Liquidity == 0 {
			log.Printf("skipping market %q — no trading activity", market.Question)
			continue
		}

		// Determine history time window.
		histStart := now.AddDate(-1, 0, 0) // fallback: 1 year ago
		if market.StartDateIso != "" {
			if t, err := time.Parse("2006-01-02", market.StartDateIso[:10]); err == nil {
				histStart = t
			}
		}

		// Parse expiration. For resolved markets, cap history end at expiration + 1 day.
		var expiration time.Time
		histEnd := now
		if market.EndDateIso != "" {
			if t, err := time.Parse("2006-01-02", market.EndDateIso[:10]); err == nil {
				expiration = t.Add(24 * time.Hour) // end of that day
				if expiration.Before(now) {
					histEnd = expiration
				}
			}
		}

		log.Printf("pulling price history for %q from %s to %s",
			market.Question,
			histStart.Format("2006-01-02"),
			histEnd.Format("2006-01-02"),
		)

		yesHistory, err := client.GetPriceHistory(
			market.YesTokenID(),
			histStart.Unix(),
			histEnd.Unix(),
			*fidelity,
		)
		if err != nil {
			log.Printf("warning: could not fetch YES price history for market %q: %v", market.Question, err)
			continue
		}

		noHistory, err := client.GetPriceHistory(
			market.NoTokenID(),
			histStart.Unix(),
			histEnd.Unix(),
			*fidelity,
		)
		if err != nil {
			log.Printf("warning: could not fetch NO price history for market %q, deriving from YES: %v", market.Question, err)
			noHistory = deriveNoHistory(yesHistory)
		}

		noPriceByTs := make(map[int64]float64, len(noHistory))
		for _, pt := range noHistory {
			noPriceByTs[pt.T] = pt.P
		}

		var lastYesPrice float64 = -1 // sentinel so first point always passes

		for _, pt := range yesHistory {
			ts := time.Unix(pt.T, 0).UTC().Round(15 * time.Minute)

			// Skip snapshots after market resolution.
			if !expiration.IsZero() && ts.After(expiration) {
				continue
			}

			// Skip rows where the YES price hasn't meaningfully changed from the prior snapshot.
			// Tolerance of 0.001 (0.1%) avoids storing noise-level fluctuations.
			if math.Abs(pt.P-lastYesPrice) < 0.001 {
				continue
			}
			lastYesPrice = pt.P

			noPrice := noPriceByTs[pt.T]
			if noPrice == 0 {
				noPrice = 1.0 - pt.P
			}

			snap := doomsday.MarketSnapshot{
				EventSlug:           *slug,
				EventTitle:          event.Title,
				Question:            market.Question,
				Category:            *category,
				SnapshotTimestamp:   ts,
				ExpirationTimestamp: expiration,
				YesPrice:            pt.P,
				NoPrice:             noPrice,
			}
			if !*noVolume {
				bid := market.BestBid
				ask := market.BestAsk
				spr := market.BestAsk - market.BestBid
				vol24 := market.Volume24hr
				volTotal := market.VolumeTotal
				liq := market.Liquidity
				snap.BestBid = &bid
				snap.BestAsk = &ask
				snap.Spread = &spr
				snap.Volume24h = &vol24
				snap.VolumeTotal = &volTotal
				snap.Liquidity = &liq
			}
			snapshots = append(snapshots, snap)
		}
	}

	log.Printf("collected %d snapshots", len(snapshots))

	if *dryRun {
		enc := json.NewEncoder(os.Stdout)
		for _, s := range snapshots {
			if err := enc.Encode(s); err != nil {
				log.Printf("warning: failed to encode snapshot: %v", err)
			}
		}
		return
	}

	ctx := context.Background()
	loader, err := doomsday.NewBQLoader(ctx, "fg-polylabs", "doomsday", "market_snapshots")
	if err != nil {
		log.Fatalf("creating BigQuery loader: %v", err)
	}
	defer loader.Close()

	inserted, err := loader.MergeSnapshots(ctx, snapshots)
	if err != nil {
		log.Fatalf("merging snapshots into BigQuery: %v", err)
	}
	skipped := len(snapshots) - inserted
	log.Printf("done: %d new rows inserted, %d duplicates skipped", inserted, skipped)
}

// deriveNoHistory computes NO prices as 1 - YES price for each point.
func deriveNoHistory(yesHistory []doomsday.CLOBPricePoint) []doomsday.CLOBPricePoint {
	no := make([]doomsday.CLOBPricePoint, len(yesHistory))
	for i, pt := range yesHistory {
		no[i] = doomsday.CLOBPricePoint{T: pt.T, P: 1.0 - pt.P}
	}
	return no
}

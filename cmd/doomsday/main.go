// doomsday is a CLI runner that pulls binary yes/no prediction market data from
// Polymarket and lands it into BigQuery (fg-polylabs.doomsday.market_snapshots).
//
// Usage:
//
//	doomsday --slug=us-x-iran-ceasefire-by-march-31 --category=war
//	doomsday --tag=iran-ceasefire --category=war --no-volume          # historical backfill
//	doomsday --tag=iran-ceasefire --category=war --yesterday --active-only  # daily incremental
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
	slug := flag.String("slug", "", "Polymarket event slug (mutually exclusive with --tag)")
	tag := flag.String("tag", "", "Polymarket tag slug — auto-discovers all events for this tag")
	category := flag.String("category", "", "Event category for filtering/grouping (e.g. war, nuclear, political)")
	fidelity := flag.Int("fidelity", 60, "Price snapshot granularity in minutes (e.g., 60=hourly, 1440=daily)")
	dryRun := flag.Bool("dry-run", false, "Print rows as JSONL to stdout instead of loading to BigQuery")
	noVolume := flag.Bool("no-volume", false, "Store NULL for volume/liquidity/bid-ask fields (use for historical backfills)")
	startDate := flag.String("start-date", "", "Override history start date YYYY-MM-DD")
	endDate := flag.String("end-date", "", "Override history end date YYYY-MM-DD (exclusive)")
	yesterday := flag.Bool("yesterday", false, "Fetch only yesterday's end-of-day price (sets fidelity=1440); ideal for daily cron")
	activeOnly := flag.Bool("active-only", false, "Skip closed/resolved markets (use for daily incremental loads)")
	flag.Parse()

	if *slug == "" && *tag == "" {
		fmt.Fprintln(os.Stderr, "Usage: doomsday --slug=<event-slug> | --tag=<tag-slug> [options]")
		os.Exit(1)
	}
	if *slug != "" && *tag != "" {
		fmt.Fprintln(os.Stderr, "Error: --slug and --tag are mutually exclusive")
		os.Exit(1)
	}

	// --yesterday sets the window to yesterday 00:00–00:00 UTC and forces daily fidelity.
	now := time.Now().UTC()
	var overrideStart, overrideEnd time.Time
	if *yesterday {
		todayMidnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		overrideStart = todayMidnight.AddDate(0, 0, -1)
		overrideEnd = todayMidnight
		*fidelity = 1440
	}
	// Explicit date flags override --yesterday.
	if *startDate != "" {
		t, err := time.Parse("2006-01-02", *startDate)
		if err != nil {
			log.Fatalf("invalid --start-date %q: %v", *startDate, err)
		}
		overrideStart = t
	}
	if *endDate != "" {
		t, err := time.Parse("2006-01-02", *endDate)
		if err != nil {
			log.Fatalf("invalid --end-date %q: %v", *endDate, err)
		}
		overrideEnd = t
	}

	client := doomsday.NewClient()

	// Collect events from either a single slug or tag-based discovery.
	var events []doomsday.GammaEvent
	if *slug != "" {
		event, err := client.GetEventBySlug(*slug)
		if err != nil {
			log.Fatalf("could not find event for slug %q: %v", *slug, err)
		}
		events = []doomsday.GammaEvent{*event}
	} else {
		var err error
		events, err = client.GetEventsByTag(*tag)
		if err != nil {
			log.Fatalf("could not find events for tag %q: %v", *tag, err)
		}
		if len(events) == 0 {
			log.Fatalf("no events found for tag %q", *tag)
		}
		log.Printf("discovered %d event(s) for tag %q", len(events), *tag)
	}

	var snapshots []doomsday.MarketSnapshot

	for _, event := range events {
		log.Printf("processing event %q (%d market(s))", event.Title, len(event.Markets))

		if len(event.Markets) == 0 {
			log.Printf("skipping event %q — no markets", event.Title)
			continue
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

		for _, market := range markets {
			if *activeOnly && market.Closed {
				log.Printf("skipping closed market %q", market.Question)
				continue
			}
			if market.YesTokenID() == "" {
				log.Printf("skipping market %q — no CLOB token IDs available", market.Question)
				continue
			}
			if market.VolumeTotal == 0 && market.Liquidity == 0 {
				log.Printf("skipping market %q — no trading activity", market.Question)
				continue
			}

			// Determine history time window; overrides take precedence over market dates.
			histStart := now.AddDate(-1, 0, 0) // fallback: 1 year ago
			if market.StartDateIso != "" {
				if t, err := time.Parse("2006-01-02", market.StartDateIso[:10]); err == nil {
					histStart = t
				}
			}
			if !overrideStart.IsZero() {
				histStart = overrideStart
			}

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
			if !overrideEnd.IsZero() {
				histEnd = overrideEnd
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

				if !expiration.IsZero() && ts.After(expiration) {
					continue
				}
				if math.Abs(pt.P-lastYesPrice) < 0.001 {
					continue
				}
				lastYesPrice = pt.P

				noPrice := noPriceByTs[pt.T]
				if noPrice == 0 {
					noPrice = 1.0 - pt.P
				}

				snap := doomsday.MarketSnapshot{
					EventSlug:           event.Slug,
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
	}

	log.Printf("collected %d snapshot(s) across %d event(s)", len(snapshots), len(events))

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

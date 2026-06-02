package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"

	"github.com/gtamizhs14/eventmind/internal/events"
	"github.com/gtamizhs14/eventmind/internal/messaging"
	"github.com/gtamizhs14/eventmind/pkg/logger"
)

func main() {
	_ = godotenv.Load()
	log := logger.New()

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9094"
	}
	topic := os.Getenv("KAFKA_TOPIC_EVENTS")
	if topic == "" {
		topic = "events"
	}

	producer, err := messaging.NewProducer(brokers, topic, log)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create producer")
	}
	defer producer.Close()

	ctx := context.Background()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	evs := buildSeedEvents(rng)
	log.Info().Int("count", len(evs)).Str("topic", topic).Msg("seeding events")

	for i, ev := range evs {
		if err := producer.Publish(ctx, ev); err != nil {
			log.Error().Err(err).Str("event_id", ev.ID).Msg("publish failed")
			continue
		}
		log.Info().
			Int("n", i+1).
			Str("type", string(ev.Type)).
			Str("id", ev.ID).
			Msg("published")

		// small delay so the agent doesn't get slammed
		time.Sleep(time.Duration(50+rng.Intn(150)) * time.Millisecond)
	}

	log.Info().Msg("seed complete")
}

// buildSeedEvents returns 50 realistic events — 10 of each type, in shuffled order.
func buildSeedEvents(rng *rand.Rand) []*events.Event {
	var all []*events.Event

	for i := 0; i < 10; i++ {
		all = append(all, seedOrderPlaced(rng))
		all = append(all, seedSupportTicket(rng))
		all = append(all, seedPaymentFailed(rng))
		all = append(all, seedUserSignup(rng))
		all = append(all, seedInventoryLow(rng))
	}

	// shuffle so the agent sees a mix, not all of one type at a time
	rng.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	return all
}

func seedOrderPlaced(rng *rand.Rand) *events.Event {
	items := []map[string]any{}
	for n := 0; n < 1+rng.Intn(4); n++ {
		items = append(items, map[string]any{
			"sku":      fmt.Sprintf("SKU-%04d", rng.Intn(9999)),
			"quantity": 1 + rng.Intn(5),
			"price":    float64(499+rng.Intn(5000)) / 100.0,
		})
	}
	total := 0.0
	for _, item := range items {
		total += item["price"].(float64) * float64(item["quantity"].(int))
	}
	return mkEvent(events.OrderPlaced, map[string]any{
		"order_id":    fmt.Sprintf("ord_%s", shortID()),
		"customer_id": fmt.Sprintf("cust_%s", shortID()),
		"amount":      fmt.Sprintf("%.2f", total),
		"items":       items,
	})
}

func seedSupportTicket(rng *rand.Rand) *events.Event {
	priorities := []string{"low", "medium", "high", "critical"}
	subjects := []string{
		"Can't log into my account",
		"Charge on my card I didn't make",
		"Order hasn't arrived after 2 weeks",
		"App keeps crashing on iOS 17",
		"Wrong item shipped",
		"Need to change delivery address",
		"Discount code not working at checkout",
	}
	return mkEvent(events.SupportTicketCreated, map[string]any{
		"ticket_id":   fmt.Sprintf("tkt_%s", shortID()),
		"customer_id": fmt.Sprintf("cust_%s", shortID()),
		"subject":     subjects[rng.Intn(len(subjects))],
		"priority":    priorities[rng.Intn(len(priorities))],
		"body":        "Customer submitted via support portal.",
	})
}

func seedPaymentFailed(rng *rand.Rand) *events.Event {
	reasons := []string{"insufficient_funds", "card_declined", "expired_card", "fraud_suspected", "bank_unreachable"}
	return mkEvent(events.PaymentFailed, map[string]any{
		"payment_id":  fmt.Sprintf("pay_%s", shortID()),
		"order_id":    fmt.Sprintf("ord_%s", shortID()),
		"customer_id": fmt.Sprintf("cust_%s", shortID()),
		"amount":      float64(999+rng.Intn(9000)) / 100.0,
		"reason":      reasons[rng.Intn(len(reasons))],
		"attempts":    1 + rng.Intn(3),
	})
}

func seedUserSignup(rng *rand.Rand) *events.Event {
	plans := []string{"free", "pro", "enterprise"}
	sources := []string{"organic", "referral", "paid_search", "social", "email_campaign"}
	names := []string{"alex", "sam", "jordan", "taylor", "morgan", "casey", "riley", "quinn"}
	domains := []string{"gmail.com", "yahoo.com", "outlook.com", "protonmail.com", "hey.com"}
	name := names[rng.Intn(len(names))]
	return mkEvent(events.UserSignup, map[string]any{
		"user_id": fmt.Sprintf("usr_%s", shortID()),
		"email":   fmt.Sprintf("%s%d@%s", name, 100+rng.Intn(900), domains[rng.Intn(len(domains))]),
		"plan":    plans[rng.Intn(len(plans))],
		"source":  sources[rng.Intn(len(sources))],
	})
}

func seedInventoryLow(rng *rand.Rand) *events.Event {
	products := []struct{ name, sku string }{
		{"Wireless Headphones Pro", "WHP-001"},
		{"USB-C Hub 7-in-1", "HUB-007"},
		{"Mechanical Keyboard TKL", "MKB-TKL"},
		{"27-inch Monitor Stand", "MON-STD"},
		{"Laptop Cooling Pad", "LCP-200"},
		{"Webcam 4K", "WEB-4K1"},
	}
	p := products[rng.Intn(len(products))]
	threshold := 10 + rng.Intn(40)
	current := rng.Intn(threshold)
	return mkEvent(events.InventoryLow, map[string]any{
		"sku":           p.sku,
		"product_name":  p.name,
		"current_stock": current,
		"threshold":     threshold,
		"warehouse_id":  fmt.Sprintf("wh-%02d", 1+rng.Intn(5)),
	})
}

func mkEvent(typ events.Type, payload map[string]any) *events.Event {
	data, _ := json.Marshal(payload)
	return &events.Event{
		ID:        uuid.New().String(),
		Type:      typ,
		Payload:   data,
		Source:    "seed-script",
		Timestamp: time.Now().UTC(),
	}
}

func shortID() string { return uuid.New().String()[:8] }

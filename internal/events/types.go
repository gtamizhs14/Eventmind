package events

import (
	"encoding/json"
	"time"
)

type Type string

const (
	OrderPlaced          Type = "order_placed"
	SupportTicketCreated Type = "support_ticket_created"
	PaymentFailed        Type = "payment_failed"
	UserSignup           Type = "user_signup"
	InventoryLow         Type = "inventory_low"
)

func (t Type) Valid() bool {
	switch t {
	case OrderPlaced, SupportTicketCreated, PaymentFailed, UserSignup, InventoryLow:
		return true
	}
	return false
}

type Event struct {
	ID        string          `json:"id"`
	Type      Type            `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Source    string          `json:"source,omitempty"`
	Timestamp time.Time       `json:"timestamp"`
}

// Specific payload shapes — used by the agent to build context

type OrderPayload struct {
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
	Items      []Item  `json:"items"`
}

type Item struct {
	SKU      string  `json:"sku"`
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
}

type SupportTicketPayload struct {
	TicketID   string `json:"ticket_id"`
	CustomerID string `json:"customer_id"`
	Subject    string `json:"subject"`
	Priority   string `json:"priority"` // low, medium, high, critical
	Body       string `json:"body"`
}

type PaymentFailedPayload struct {
	PaymentID  string  `json:"payment_id"`
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
	Reason     string  `json:"reason"`
	Attempts   int     `json:"attempts"`
}

type UserSignupPayload struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
	Plan   string `json:"plan"`   // free, pro, enterprise
	Source string `json:"source"` // organic, referral, paid
}

type InventoryLowPayload struct {
	SKU          string `json:"sku"`
	ProductName  string `json:"product_name"`
	CurrentStock int    `json:"current_stock"`
	Threshold    int    `json:"threshold"`
	WarehouseID  string `json:"warehouse_id"`
}

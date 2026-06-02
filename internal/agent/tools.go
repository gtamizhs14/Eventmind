package agent

import (
	"encoding/json"
	"fmt"
)

type Action string

const (
	ActionSendNotification Action = "send_notification"
	ActionEscalateTicket   Action = "escalate_ticket"
	ActionFlagForReview    Action = "flag_for_review"
	ActionUpdateInventory  Action = "update_inventory"
	ActionSendWelcomeSeq   Action = "send_welcome_sequence"
)

var validActions = map[Action]bool{
	ActionSendNotification: true,
	ActionEscalateTicket:   true,
	ActionFlagForReview:    true,
	ActionUpdateInventory:  true,
	ActionSendWelcomeSeq:   true,
}

func (a Action) Valid() bool { return validActions[a] }

type ToolResult struct {
	Action  Action
	Success bool
	Details string
}

// executeAction dispatches to the right handler. Each tool is a stub that logs
// what it would do — real implementations would call downstream services (email,
// ticketing system, inventory API, etc.).
func executeAction(action Action, payload json.RawMessage) ToolResult {
	switch action {
	case ActionSendNotification:
		return sendNotification(payload)
	case ActionEscalateTicket:
		return escalateTicket(payload)
	case ActionFlagForReview:
		return flagForReview(payload)
	case ActionUpdateInventory:
		return updateInventory(payload)
	case ActionSendWelcomeSeq:
		return sendWelcomeSequence(payload)
	default:
		return ToolResult{Action: action, Success: false, Details: fmt.Sprintf("no handler for action %q", action)}
	}
}

func sendNotification(payload json.RawMessage) ToolResult {
	var p struct {
		CustomerID string `json:"customer_id"`
		OrderID    string `json:"order_id"`
	}
	_ = json.Unmarshal(payload, &p)
	// TODO: call notification service (email/SMS/push)
	return ToolResult{Action: ActionSendNotification, Success: true, Details: fmt.Sprintf("notification queued for customer %s", p.CustomerID)}
}

func escalateTicket(payload json.RawMessage) ToolResult {
	var p struct {
		TicketID string `json:"ticket_id"`
		Priority string `json:"priority"`
	}
	_ = json.Unmarshal(payload, &p)
	// TODO: call support ticketing system
	return ToolResult{Action: ActionEscalateTicket, Success: true, Details: fmt.Sprintf("ticket %s escalated (priority: %s)", p.TicketID, p.Priority)}
}

func flagForReview(payload json.RawMessage) ToolResult {
	var p struct {
		PaymentID string `json:"payment_id"`
	}
	_ = json.Unmarshal(payload, &p)
	// TODO: write to review queue / ops dashboard
	return ToolResult{Action: ActionFlagForReview, Success: true, Details: fmt.Sprintf("event flagged for review — payment %s", p.PaymentID)}
}

func updateInventory(payload json.RawMessage) ToolResult {
	var p struct {
		SKU          string `json:"sku"`
		CurrentStock int    `json:"current_stock"`
		Threshold    int    `json:"threshold"`
	}
	_ = json.Unmarshal(payload, &p)
	// TODO: call inventory management API
	return ToolResult{Action: ActionUpdateInventory, Success: true, Details: fmt.Sprintf("inventory reorder triggered for SKU %s (%d below threshold %d)", p.SKU, p.CurrentStock, p.Threshold)}
}

func sendWelcomeSequence(payload json.RawMessage) ToolResult {
	var p struct {
		UserID string `json:"user_id"`
		Email  string `json:"email"`
		Plan   string `json:"plan"`
	}
	_ = json.Unmarshal(payload, &p)
	// TODO: enqueue welcome email sequence in email service
	return ToolResult{Action: ActionSendWelcomeSeq, Success: true, Details: fmt.Sprintf("welcome sequence started for %s (plan: %s)", p.Email, p.Plan)}
}

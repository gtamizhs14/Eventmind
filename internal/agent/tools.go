package agent

// Action is what the agent tells us to do with an event.
type Action string

const (
	ActionSendNotification  Action = "send_notification"
	ActionEscalateTicket    Action = "escalate_ticket"
	ActionFlagForReview     Action = "flag_for_review"
	ActionUpdateInventory   Action = "update_inventory"
	ActionSendWelcomeSeq    Action = "send_welcome_sequence"
)

var validActions = map[Action]bool{
	ActionSendNotification: true,
	ActionEscalateTicket:   true,
	ActionFlagForReview:    true,
	ActionUpdateInventory:  true,
	ActionSendWelcomeSeq:   true,
}

func (a Action) Valid() bool {
	return validActions[a]
}

// ToolResult is what we hand back after executing an action.
type ToolResult struct {
	Action  Action
	Success bool
	Details string
}

// executeAction dispatches to the right tool handler.
// These are stubs — real implementations log + call downstream services.
// Implemented in step 4.
func executeAction(a Action, payload []byte) ToolResult {
	panic("not implemented — see step 4")
}

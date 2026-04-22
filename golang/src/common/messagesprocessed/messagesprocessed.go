package messagesprocessed

import "github.com/google/uuid"

type MessagesProcessed struct {
	ActualAmount uint32
	RealAmount   uint32
	ClientId     uuid.UUID
	Leader       uint32
}

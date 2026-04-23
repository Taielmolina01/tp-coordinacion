package eofringmessage

import "github.com/google/uuid"

type EofRingMessage struct {
	ActualAmount uint32
	RealAmount   uint32
	ClientId     uuid.UUID
	Leader       uint32
}

type EofMessageCommit struct {
	ClientID uuid.UUID
	Hops     int
}

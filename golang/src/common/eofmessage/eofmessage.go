package eofmessage

import "github.com/google/uuid"

type EofMessage struct {
	TotalMessages uint32
	ClientID      uuid.UUID
}

type EofMessageCommit struct {
	ClientID uuid.UUID
	Hops     int
}

package eofmessage

import "github.com/google/uuid"

type EofMessage struct {
	TotalMessages uint32
	ClientID      uuid.UUID
}

type AggregationEofMessage struct {
	ClientID      uuid.UUID
	AggregationID int
}

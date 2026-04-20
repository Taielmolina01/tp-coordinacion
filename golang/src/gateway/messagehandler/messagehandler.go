package messagehandler

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/google/uuid"
)

type MessageHandler struct {
	clientId uuid.UUID
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{clientId: uuid.New()}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := fruititem.FruitItemFromClient{
		ClientId:   messageHandler.clientId,
		FruitItems: []fruititem.FruitItem{fruitRecord},
	}
	return inner.SerializeMessage(data)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	data := fruititem.FruitItemFromClient{
		ClientId:   messageHandler.clientId,
		FruitItems: []fruititem.FruitItem{},
	}
	return inner.SerializeMessage(data)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) (*fruititem.FruitItemFromClient, error) {
	fruitRecords, _, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}

	if fruitRecords == nil || fruitRecords.ClientId != messageHandler.clientId {
		return nil, nil
	}

	return fruitRecords, nil
}

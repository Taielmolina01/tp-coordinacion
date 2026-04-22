package messagehandler

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/eofmessage"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messagesprocessed"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/google/uuid"
)

type MessageHandler struct {
	clientId          uuid.UUID
	processedMessages uint32
}

type MessageEof struct {
	actualAmount uint32
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{clientId: uuid.New()}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := fruititem.FruitItemFromClient{
		ClientId:   messageHandler.clientId,
		FruitItems: []fruititem.FruitItem{fruitRecord},
	}
	messageHandler.processedMessages += 1
	return inner.SerializeMessage(data)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	data := eofmessage.EofMessage{
		ClientID:      messageHandler.clientId,
		TotalMessages: messageHandler.processedMessages,
	}
	return inner.SerializeEofMessage(data)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) (*fruititem.FruitItemFromClient, error) {
	fruitRecords, _, _, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}

	if fruitRecords == nil || fruitRecords.ClientId != messageHandler.clientId {
		return nil, nil
	}
	return fruitRecords, nil
}

func (messageHandler *MessageHandler) DeserializeEofFromQueueMsg(message *middleware.Message) (*messagesprocessed.MessagesProcessed, *eofmessage.EofMessageCommit, error) {
	return inner.DeserializeEOFMessage(message)
}

func (messageHandler *MessageHandler) SerializeEofFromQueueMsg(msg messagesprocessed.MessagesProcessed) (*middleware.Message, error) {
	return inner.SerializeEofFromQueueMsg(msg)
}

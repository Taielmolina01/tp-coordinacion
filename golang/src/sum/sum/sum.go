package sum

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/eofmessage"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messagesprocessed"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/google/uuid"
)

const (
	_EOF_EXCHANGE_MIDD_NAME = "EOF_EXCHANGE"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	id                        uint32
	inputQueue                middleware.Middleware
	outputExchange            middleware.Middleware
	fruitItemMapPerClient     map[uuid.UUID]map[string]fruititem.FruitItem
	fruitItemMutex            sync.Mutex
	processedMessagesMutex    sync.Mutex
	eofOutput                 middleware.Middleware
	eofInput                  middleware.Middleware
	processedMessagesByClient map[uuid.UUID]uint32
	shutdown                  chan struct{}
	shutdownOnce              sync.Once
	sumAmount                 int
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	next := config.Id + 1

	if config.Id == config.SumAmount-1 {
		next = 0
	}

	eofInput, err := middleware.CreateExchangeMiddleware(
		_EOF_EXCHANGE_MIDD_NAME,
		[]string{strconv.Itoa(config.Id)},
		connSettings,
	)

	eofOutput, err := middleware.CreateExchangeMiddleware(
		_EOF_EXCHANGE_MIDD_NAME,
		[]string{strconv.Itoa(next)},
		connSettings,
	)

	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Sum{
		id:                        uint32(config.Id),
		inputQueue:                inputQueue,
		outputExchange:            outputExchange,
		fruitItemMutex:            sync.Mutex{},
		processedMessagesMutex:    sync.Mutex{},
		fruitItemMapPerClient:     map[uuid.UUID]map[string]fruititem.FruitItem{},
		eofInput:                  eofInput,
		eofOutput:                 eofOutput,
		processedMessagesByClient: map[uuid.UUID]uint32{},
		shutdown:                  make(chan struct{}),
		sumAmount:                 config.SumAmount,
	}, nil
}

func (sum *Sum) Run() {
	slog.Info("Starting sum consumers", "sum_id", sum.id)
	go sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
	go sum.eofInput.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleEofMessageFromQueue(msg, ack, nack)
	})

	<-sum.shutdown
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	fruitsFromClient, eofMessage, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing input message", "sum_id", sum.id, "err", err)
		return
	}

	if isEof {
		slog.Info("Received EOF from input queue", "sum_id", sum.id, "client_id", eofMessage.ClientID, "total_messages", eofMessage.TotalMessages)
		if err := sum.handleEndOfRecordMessage(*eofMessage); err != nil {
			slog.Error("While handling end of record message", "sum_id", sum.id, "client_id", eofMessage.ClientID, "err", err)
		}
		return
	}

	slog.Debug("Received data message", "sum_id", sum.id, "client_id", fruitsFromClient.ClientId, "items", len(fruitsFromClient.FruitItems))

	if err := sum.handleDataMessage(*fruitsFromClient); err != nil {
		slog.Error("While handling data message", "sum_id", sum.id, "client_id", fruitsFromClient.ClientId, "err", err)
	}
}

func (sum *Sum) handleEofMessageFromQueue(msg middleware.Message, ack, nack func()) {
	processed, commit, err := inner.DeserializeEOFMessage(&msg)
	if err != nil {
		slog.Error("Error deserializing EOF ring message", "sum_id", sum.id, "err", err)
		nack()
		return
	}

	if commit != nil {
		slog.Info("Received EOF commit from ring", "sum_id", sum.id, "client_id", commit.ClientID)
		if err := sum.handleEndOfRecordCommitMessage(commit); err != nil {
			nack()
			slog.Error("Error handling EOF commit", "sum_id", sum.id, "client_id", commit.ClientID, "err", err)
		} else {
			slog.Info("EOF commit sent to aggregation", "sum_id", sum.id, "client_id", commit.ClientID)
			ack()
		}
		return
	}

	if processed == nil {
		slog.Error("EOF ring message without processed payload", "sum_id", sum.id)
		nack()
		return
	}

	sum.processedMessagesMutex.Lock()
	defer sum.processedMessagesMutex.Unlock()
	localAmount := sum.processedMessagesByClient[processed.ClientId]
	slog.Info(
		"Processing EOF ring message",
		"sum_id", sum.id,
		"client_id", processed.ClientId,
		"leader", processed.Leader,
		"ring_actual", processed.ActualAmount,
		"local_actual", localAmount,
		"real_total", processed.RealAmount,
	)

	if processed.Leader == sum.id && processed.ActualAmount == processed.RealAmount {
		slog.Info("EOF ring closed, emitting commit", "sum_id", sum.id, "client_id", processed.ClientId)
		msg, err := inner.SerializeEofMessageCommit(eofmessage.EofMessageCommit{ClientID: processed.ClientId, Hops: 0})
		if err != nil {
			nack()
			slog.Error("Error serializing EOF commit", "sum_id", sum.id, "client_id", processed.ClientId, "err", err)
			return
		}
		if err = sum.eofOutput.Send(*msg); err != nil {
			nack()
			slog.Error("Error sending EOF commit to ring", "sum_id", sum.id, "client_id", processed.ClientId, "err", err)
			return
		}
		ack()
		slog.Info("EOF commit sent to ring", "sum_id", sum.id, "client_id", processed.ClientId)
	} else {
		value, ok := sum.processedMessagesByClient[processed.ClientId]
		if ok {
			processed.ActualAmount += value
		}
		slog.Info("Forwarding EOF ring message", "sum_id", sum.id, "client_id", processed.ClientId, "forwarded_actual", processed.ActualAmount, "real_total", processed.RealAmount)

		newMsg, err := inner.SerializeEofFromQueueMsg(*processed)
		if err != nil {
			nack()
			slog.Error("Error serializing forwarded EOF ring message", "sum_id", sum.id, "client_id", processed.ClientId, "err", err)
			return
		}
		if err := sum.eofOutput.Send(*newMsg); err != nil {
			nack()
			slog.Error("Error forwarding EOF ring message", "sum_id", sum.id, "client_id", processed.ClientId, "err", err)
			return
		}

		ack()
	}
}

func (sum *Sum) handleEndOfRecordCommitMessage(msg *eofmessage.EofMessageCommit) error {
	slog.Info("Sending aggregated client result", "sum_id", sum.id, "client_id", msg.ClientID, "fruits", len(sum.fruitItemMapPerClient[msg.ClientID]))
	sum.fruitItemMutex.Lock()
	for _, value := range sum.fruitItemMapPerClient[msg.ClientID] {
		message, err := inner.SerializeMessage(fruititem.FruitItemFromClient{
			ClientId:   msg.ClientID,
			FruitItems: []fruititem.FruitItem{value},
		})
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		if err := sum.outputExchange.Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}
	sum.fruitItemMutex.Unlock()

	eofMessage := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(fruititem.FruitItemFromClient{
		ClientId:   msg.ClientID,
		FruitItems: eofMessage,
	})
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	slog.Info("Finished sending client result and EOF", "sum_id", sum.id, "client_id", msg.ClientID)
	msg.Hops++
	if msg.Hops == sum.sumAmount {
		return nil
	}

	toSend, err := inner.SerializeEofMessageCommit(*msg)

	if err != nil {
		slog.Debug("While sending EOF commit", "err", err)
		return err
	}

	if err = sum.eofOutput.Send(*toSend); err != nil {
		slog.Error("Error sending EOF commit to ring", "sum_id", sum.id, "client_id", msg.ClientID, "err", err)
	}
	return nil

}

func (sum *Sum) handleEndOfRecordMessage(eofMessage eofmessage.EofMessage) error {
	slog.Info("Received End Of Records message", "sum_id", sum.id, "client_id", eofMessage.ClientID, "real_total", eofMessage.TotalMessages)

	sum.processedMessagesMutex.Lock()
	amount, ok := sum.processedMessagesByClient[eofMessage.ClientID]
	sum.processedMessagesMutex.Unlock()
	if !ok {
		amount = 0
	}
	slog.Info("Publishing EOF ring message", "sum_id", sum.id, "client_id", eofMessage.ClientID, "local_actual", amount, "real_total", eofMessage.TotalMessages)

	eofMessageRequest, err := inner.SerializeEofFromQueueMsg(
		messagesprocessed.MessagesProcessed{
			ActualAmount: amount,
			RealAmount:   eofMessage.TotalMessages,
			ClientId:     eofMessage.ClientID,
			Leader:       uint32(sum.id),
		},
	)
	if err != nil {
		return err
	}
	if err := sum.eofOutput.Send(*eofMessageRequest); err != nil {
		return err
	}
	return nil
}

func (sum *Sum) handleDataMessage(fruitsFromClient fruititem.FruitItemFromClient) error {
	sum.fruitItemMutex.Lock()
	if _, ok := sum.fruitItemMapPerClient[fruitsFromClient.ClientId]; !ok {
		sum.fruitItemMapPerClient[fruitsFromClient.ClientId] = map[string]fruititem.FruitItem{}
	}

	for _, fruitRecord := range fruitsFromClient.FruitItems {
		_, ok := sum.fruitItemMapPerClient[fruitsFromClient.ClientId][fruitRecord.Fruit]
		if ok {
			sum.fruitItemMapPerClient[fruitsFromClient.ClientId][fruitRecord.Fruit] = sum.fruitItemMapPerClient[fruitsFromClient.ClientId][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			sum.fruitItemMapPerClient[fruitsFromClient.ClientId][fruitRecord.Fruit] = fruitRecord
		}
	}
	sum.fruitItemMutex.Unlock()

	sum.processedMessagesMutex.Lock()
	defer sum.processedMessagesMutex.Unlock()
	_, ok := sum.processedMessagesByClient[fruitsFromClient.ClientId]
	if ok {
		sum.processedMessagesByClient[fruitsFromClient.ClientId] += 1
	} else {
		sum.processedMessagesByClient[fruitsFromClient.ClientId] = 1
	}

	slog.Debug(
		"Updated client partials",
		"sum_id", sum.id,
		"client_id", fruitsFromClient.ClientId,
		"processed_messages", sum.processedMessagesByClient[fruitsFromClient.ClientId],
		"distinct_fruits", len(sum.fruitItemMapPerClient[fruitsFromClient.ClientId]),
	)
	return nil
}

func (sum *Sum) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	if err := sum.Close(); err != nil {
		slog.Error("While closing sum", "err", err)
	}
	sum.shutdownOnce.Do(func() {
		close(sum.shutdown)
	})
}

func (sum *Sum) Close() error {
	if err := sum.inputQueue.StopConsuming(); err != nil {
		return err
	}
	if err := sum.inputQueue.Close(); err != nil {
		return err
	}
	if err := sum.outputExchange.StopConsuming(); err != nil {
		return err
	}
	if err := sum.outputExchange.Close(); err != nil {
		return err
	}
	return nil
}

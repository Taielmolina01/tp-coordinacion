package sum

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/google/uuid"
)

const (
	_INPUT_QUEUE_INDEX      = 0
	_OUTPUT_QUEUE_INDEX     = 1
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
	inputQueue            middleware.Middleware
	outputExchange        middleware.Middleware
	fruitItemMapPerClient map[uuid.UUID]map[string]fruititem.FruitItem
	// bullyExchange         middleware.Middleware
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

	// toPublishKeys := make([]string, 0, config.SumAmount-1)
	// for i := range config.SumAmount {
	// 	if i == config.Id {
	// 		continue
	// 	}
	// 	toPublishKeys = append(toPublishKeys, fmt.Sprintf("input_key_%d", i+1))
	// }

	// bullyExchange, err := middleware.CreateExchangeMiddleware(
	// 	_EOF_EXCHANGE_MIDD_NAME,
	// 	toPublishKeys,
	// 	connSettings,
	// )

	// if err != nil {
	// 	inputQueue.Close()
	// 	return nil, err
	// }

	return &Sum{
		inputQueue:            inputQueue,
		outputExchange:        outputExchange,
		fruitItemMapPerClient: map[uuid.UUID]map[string]fruititem.FruitItem{},
		// bullyExchange:         bullyExchange,
	}, nil
}

func (sum *Sum) Run() {
	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	fruitsFromClient, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		if err := sum.handleEndOfRecordMessage(*fruitsFromClient); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
	}

	if err := sum.handleDataMessage(*fruitsFromClient); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleEndOfRecordMessage(fruitsFromClient fruititem.FruitItemFromClient) error {
	slog.Info("Received End Of Records message")
	for _, items := range sum.fruitItemMapPerClient[fruitsFromClient.ClientId] {
		message, err := inner.SerializeMessage(fruititem.FruitItemFromClient{
			ClientId:   fruitsFromClient.ClientId,
			FruitItems: []fruititem.FruitItem{items},
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

	message, err := inner.SerializeMessage(fruitsFromClient)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func (sum *Sum) handleDataMessage(fruitsFromClient fruititem.FruitItemFromClient) error {
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
	return nil
}

func (sum *Sum) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	sum.Close()
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

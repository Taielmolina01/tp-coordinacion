package aggregation

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/google/uuid"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	outputQueue           middleware.Middleware
	inputExchange         middleware.Middleware
	fruitItemMapPerClient map[uuid.UUID]map[string]fruititem.FruitItem
	topSize               int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:           outputQueue,
		inputExchange:         inputExchange,
		fruitItemMapPerClient: map[uuid.UUID]map[string]fruititem.FruitItem{},
		topSize:               config.TopSize,
	}, nil
}

func (aggregation *Aggregation) Run() {
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		if err := aggregation.handleEndOfRecordsMessage(*fruitRecords); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
	}

	aggregation.handleDataMessage(*fruitRecords)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(fruitItemsFromClient fruititem.FruitItemFromClient) error {
	slog.Info("Received End Of Records message")

	fruitTopRecords := aggregation.buildFruitTop(fruitItemsFromClient.ClientId)

	message, err := inner.SerializeMessage(fruitTopRecords)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	message, err = inner.SerializeMessage(fruititem.FruitItemFromClient{
		ClientId:   fruitItemsFromClient.ClientId,
		FruitItems: []fruititem.FruitItem{},
	})
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func (aggregation *Aggregation) handleDataMessage(fruitItemsForClient fruititem.FruitItemFromClient) {
	if _, ok := aggregation.fruitItemMapPerClient[fruitItemsForClient.ClientId]; !ok {
		aggregation.fruitItemMapPerClient[fruitItemsForClient.ClientId] = map[string]fruititem.FruitItem{}
	}

	for _, fruitRecord := range fruitItemsForClient.FruitItems {
		if _, ok := aggregation.fruitItemMapPerClient[fruitItemsForClient.ClientId][fruitRecord.Fruit]; ok {
			aggregation.fruitItemMapPerClient[fruitItemsForClient.ClientId][fruitRecord.Fruit] = aggregation.fruitItemMapPerClient[fruitItemsForClient.ClientId][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			aggregation.fruitItemMapPerClient[fruitItemsForClient.ClientId][fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(clientId uuid.UUID) fruititem.FruitItemFromClient {
	fruitItemsFromClient := fruititem.FruitItemFromClient{
		ClientId:   clientId,
		FruitItems: make([]fruititem.FruitItem, 0, len(aggregation.fruitItemMapPerClient[clientId])),
	}
	for _, item := range aggregation.fruitItemMapPerClient[clientId] {
		fruitItemsFromClient.FruitItems = append(fruitItemsFromClient.FruitItems, item)
	}
	sort.SliceStable(fruitItemsFromClient.FruitItems, func(i, j int) bool {
		return fruitItemsFromClient.FruitItems[j].Less(fruitItemsFromClient.FruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItemsFromClient.FruitItems))
	fruitItemsFromClient.FruitItems = fruitItemsFromClient.FruitItems[:finalTopSize]
	return fruitItemsFromClient
}

func (aggregation *Aggregation) HandleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	aggregation.Close()
}

func (aggregation *Aggregation) Close() error {
	if err := aggregation.inputExchange.StopConsuming(); err != nil {
		return err
	}
	if err := aggregation.inputExchange.Close(); err != nil {
		return err
	}
	if err := aggregation.outputQueue.StopConsuming(); err != nil {
		return err
	}
	if err := aggregation.outputQueue.Close(); err != nil {
		return err
	}
	return nil
}

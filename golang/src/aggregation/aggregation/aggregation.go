package aggregation

import (
	"fmt"
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
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
	outputQueue        middleware.Middleware
	inputExchange      middleware.Middleware
	clientFruitItemMap map[uint64]map[string]fruititem.FruitItem
	topSize            int
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
		outputQueue:        outputQueue,
		inputExchange:      inputExchange,
		clientFruitItemMap: map[uint64]map[string]fruititem.FruitItem{},
		topSize:            config.TopSize,
	}, nil
}

func (aggregation *Aggregation) Run() {
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	msgBody, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if msgBody.IsEof {
		if err := aggregation.handleEndOfRecordsMessage(msgBody); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	aggregation.handleDataMessage(msgBody)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(msgBody *inner.MessageBody) error {
	slog.Info("Received End Of Records message")

	fruitTopRecords := aggregation.buildFruitTop(msgBody.ClientId)
	message, err := inner.SerializeMessage(inner.MessageBody{
		ClientId: msgBody.ClientId,
		Records:  fruitTopRecords,
	})
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	message, err = inner.SerializeMessage(inner.MessageBody{
		ClientId: msgBody.ClientId,
		IsEof:    true,
	})
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}

	delete(aggregation.clientFruitItemMap, msgBody.ClientId)

	return nil
}

func (aggregation *Aggregation) handleDataMessage(msgBody *inner.MessageBody) {
	if _, ok := aggregation.clientFruitItemMap[msgBody.ClientId]; !ok {
		aggregation.clientFruitItemMap[msgBody.ClientId] = map[string]fruititem.FruitItem{}
	}

	for _, fruitRecord := range msgBody.Records {
		if _, ok := aggregation.clientFruitItemMap[msgBody.ClientId][fruitRecord.Fruit]; ok {
			aggregation.clientFruitItemMap[msgBody.ClientId][fruitRecord.Fruit] = aggregation.clientFruitItemMap[msgBody.ClientId][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			aggregation.clientFruitItemMap[msgBody.ClientId][fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(clientId uint64) []fruititem.FruitItem {
	fruitItemMap, ok := aggregation.clientFruitItemMap[clientId]
	if !ok {
		return []fruititem.FruitItem{}
	}

	fruitItems := make([]fruititem.FruitItem, 0, len(fruitItemMap))
	for _, item := range fruitItemMap {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}

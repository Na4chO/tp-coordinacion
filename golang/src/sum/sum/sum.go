package sum

import (
	"fmt"
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
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
	id                      int
	inputQueue              middleware.Middleware
	outputExchange          middleware.Middleware
	ringInputQueue          middleware.Middleware
	ringOutputQueue         middleware.Middleware
	clientFruitItemMap      map[uint64]map[string]fruititem.FruitItem
	clientProcessedMsgCount map[uint64]uint64
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

	ringInputQueue, err := middleware.CreateQueueMiddleware(
		fmt.Sprintf("ring_%s_%d", config.SumPrefix, config.Id),
		connSettings,
	)
	ringOutputQueue, err := middleware.CreateQueueMiddleware(
		fmt.Sprintf("ring_%s_%d", config.SumPrefix, (config.Id+1)%config.SumAmount),
		connSettings,
	)

	if err != nil {
		return nil, err
	}

	return &Sum{
		id:                      config.Id,
		inputQueue:              inputQueue,
		outputExchange:          outputExchange,
		ringInputQueue:          ringInputQueue,
		ringOutputQueue:         ringOutputQueue,
		clientFruitItemMap:      map[uint64]map[string]fruititem.FruitItem{},
		clientProcessedMsgCount: map[uint64]uint64{},
	}, nil
}

func (sum *Sum) Run() {
	go sum.ringInputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleRingMessage(msg, ack, nack)
	})

	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	msgBody, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if msgBody.IsEof {
		if err := sum.handleEndOfRecordMessage(msgBody); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	if err := sum.handleDataMessage(msgBody); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleEndOfRecordMessage(msgBody *inner.MessageBody) error {
	slog.Info("Received End Of Records message")

	err := sum.sendRingMessage(&inner.RingMessage{
		ClientId: msgBody.ClientId,
		Total:    msgBody.Total,
	})

	if err != nil {
		slog.Error("While sending first ring message", "err", err)
		return err
	}

	return nil
}

func (sum *Sum) handleDataMessage(msgBody *inner.MessageBody) error {
	if _, ok := sum.clientFruitItemMap[msgBody.ClientId]; !ok {
		sum.clientFruitItemMap[msgBody.ClientId] = map[string]fruititem.FruitItem{}
	}

	for _, fruitRecord := range msgBody.Records {
		_, ok := sum.clientFruitItemMap[msgBody.ClientId][fruitRecord.Fruit]
		if ok {
			sum.clientFruitItemMap[msgBody.ClientId][fruitRecord.Fruit] = sum.clientFruitItemMap[msgBody.ClientId][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			sum.clientFruitItemMap[msgBody.ClientId][fruitRecord.Fruit] = fruitRecord
		}
	}

	sum.clientProcessedMsgCount[msgBody.ClientId]++
	return nil
}

func (sum *Sum) handleRingMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	ringMsg, err := inner.DeserializeRingMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if ringMsg.IsFinal {
		sum.handleRingFinalMessage(ringMsg)
	} else {
		sum.handeRingNonFinalMessage(ringMsg)
	}

	return
}

func (sum *Sum) handleRingFinalMessage(msg *inner.RingMessage) error {
	slog.Info("Received Final ring message")

	if err := sum.sendClientRecords(msg.ClientId); err != nil {
		slog.Error("While sending records to Aggregation", "err", err)
		return err
	}

	if msg.CoordinatorId == sum.id {
		if err := sum.sendEndOfTheRecordsMessage(msg.ClientId); err != nil {
			slog.Error("While sending EOF to Aggregation", "err", err)
			return err
		}
	} else {
		if err := sum.sendRingMessage(msg); err != nil {
			slog.Error("While sending Final ring message", "err", err)
			return err
		}
	}

	return nil
}

func (sum *Sum) handeRingNonFinalMessage(ringMsg *inner.RingMessage) {
	processedCount := sum.clientProcessedMsgCount[ringMsg.ClientId]
	ringMsg.Processed += processedCount
	sum.clientProcessedMsgCount[ringMsg.ClientId] = 0

	if ringMsg.Processed == ringMsg.Total {
		ringMsg.IsFinal = true
		ringMsg.CoordinatorId = sum.id
	}

	if err := sum.sendRingMessage(ringMsg); err != nil {
		slog.Error("While sending ring message", "err", err)
	}
}

func (sum *Sum) sendRingMessage(msg *inner.RingMessage) error {
	message, err := inner.SerializeRingMessage(*msg)
	if err != nil {
		return err
	}

	return sum.ringOutputQueue.Send(*message)
}

func (sum *Sum) sendEndOfTheRecordsMessage(clientId uint64) error {
	message, err := inner.SerializeMessage(inner.MessageBody{
		ClientId: clientId,
		IsEof:    true,
	})
	if err != nil {
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		return err
	}
	return nil
}

func (sum *Sum) sendClientRecords(clientId uint64) error {
	fruitItemMap, ok := sum.clientFruitItemMap[clientId]
	if ok {
		for key := range fruitItemMap {
			records := []fruititem.FruitItem{fruitItemMap[key]}
			message, err := inner.SerializeMessage(inner.MessageBody{
				ClientId: clientId,
				Records:  records,
			})
			if err != nil {
				return err
			}
			if err := sum.outputExchange.Send(*message); err != nil {
				return err
			}
		}
		delete(sum.clientFruitItemMap, clientId)
	}
	return nil
}

package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"

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
	id                  int
	inputQueue          middleware.Middleware
	outputExchange      middleware.Router
	coordExchange       middleware.Router
	aggregationPrefix   string
	aggregationAmount   int
	stateLock           sync.Mutex
	clientFruitItemMap  map[uint64]map[string]fruititem.FruitItem
	clientRemainingMsg  map[uint64]uint64
	clientNewMsg        map[uint64]uint64
	clientCoordinatorId map[uint64]int
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

	coordExchangeRouteKeys := []string{coordBroadcastKey(), coordRouteKey(config.Id)}
	coordExchange, err := middleware.CreateExchangeMiddleware(
		fmt.Sprintf("coord_%s", config.SumPrefix),
		coordExchangeRouteKeys,
		connSettings,
	)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}

	return &Sum{
		id:                  config.Id,
		inputQueue:          inputQueue,
		outputExchange:      outputExchange,
		coordExchange:       coordExchange,
		aggregationPrefix:   config.AggregationPrefix,
		aggregationAmount:   config.AggregationAmount,
		stateLock:           sync.Mutex{},
		clientFruitItemMap:  map[uint64]map[string]fruititem.FruitItem{},
		clientRemainingMsg:  map[uint64]uint64{},
		clientNewMsg:        map[uint64]uint64{},
		clientCoordinatorId: map[uint64]int{},
	}, nil
}

func (sum *Sum) Run() {
	go sum.coordExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleCoordMessage(msg, ack, nack)
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

	sum.stateLock.Lock()
	sum.clientRemainingMsg[msgBody.ClientId] = msgBody.Total - sum.clientNewMsg[msgBody.ClientId]
	delete(sum.clientNewMsg, msgBody.ClientId)
	sum.stateLock.Unlock()

	if err := sum.sendClientRecords(msgBody.ClientId); err != nil {
		slog.Error("While sending client records", "err", err)
		return err
	}

	if err := sum.sendEndOfTheRecordsMessage(msgBody.ClientId); err != nil {
		slog.Error("While sending end of the-records message", "err", err)
		return err
	}

	message := inner.CoordinationMessage{Type: inner.Coordinator, ClientId: msgBody.ClientId, CoordinatorId: sum.id}
	if err := sum.sendCoordBroadcastMessage(&message); err != nil {
		slog.Error("While sending Coordinator message", "err", err)
		return err
	}

	return nil
}

func (sum *Sum) handleDataMessage(msgBody *inner.MessageBody) error {
	sum.stateLock.Lock()
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

	sum.clientNewMsg[msgBody.ClientId]++
	coordinatorId, isParticipant := sum.clientCoordinatorId[msgBody.ClientId]
	processed := sum.clientNewMsg[msgBody.ClientId]
	if isParticipant {
		sum.clientNewMsg[msgBody.ClientId] = 0
	}
	sum.stateLock.Unlock()

	if isParticipant {
		message := inner.CoordinationMessage{
			Type:          inner.Count,
			ClientId:      msgBody.ClientId,
			CoordinatorId: coordinatorId,
			Processed:     processed,
		}
		if err := sum.sendCoordMessageTo(coordinatorId, &message); err != nil {
			slog.Error("While sending Count coordination message", "err", err)
		}
	}

	return nil
}

func (sum *Sum) handleCoordMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	coordMsg, err := inner.DeserializeCoordinationMessage(&msg)
	if err != nil {
		slog.Error("While deserializing coordination message", "err", err)
		return
	}

	// TODO: Ver que pasa cuando me llegan mensajes que YO mande, se tienen que descartar

	if coordMsg.CoordinatorId == sum.id {
		sum.coordinatorHandler(coordMsg)
	} else {
		sum.participantHandler(coordMsg)
	}

	return
}

func (sum *Sum) coordinatorHandler(msg *inner.CoordinationMessage) error {
	if msg.Type != inner.Count {
		return nil
	}

	sum.stateLock.Lock()
	_, ok := sum.clientRemainingMsg[msg.ClientId]
	if !ok {
		sum.stateLock.Unlock()
		slog.Warn("Messages to process not found for this client", "clientId", msg.ClientId)
		return nil
	}
	sum.clientRemainingMsg[msg.ClientId] -= msg.Processed
	shouldEnd := sum.clientRemainingMsg[msg.ClientId] == 0
	if shouldEnd {
		delete(sum.clientRemainingMsg, msg.ClientId)
	}
	sum.stateLock.Unlock()

	if shouldEnd {
		message := inner.CoordinationMessage{
			Type:          inner.End,
			ClientId:      msg.ClientId,
			CoordinatorId: sum.id,
		}

		if err := sum.sendCoordBroadcastMessage(&message); err != nil {
			slog.Error("While sending End coordination message", "err", err)
			return err
		}
	}
	return nil
}

func (sum *Sum) participantHandler(msg *inner.CoordinationMessage) error {
	if msg.Type == inner.Coordinator {
		sum.stateLock.Lock()
		sum.clientCoordinatorId[msg.ClientId] = msg.CoordinatorId
		processed := sum.clientNewMsg[msg.ClientId]
		sum.clientNewMsg[msg.ClientId] = 0
		sum.stateLock.Unlock()

		message := inner.CoordinationMessage{
			Type:          inner.Count,
			ClientId:      msg.ClientId,
			Processed:     processed,
			CoordinatorId: msg.CoordinatorId,
		}

		if err := sum.sendCoordMessageTo(msg.CoordinatorId, &message); err != nil {
			slog.Error("While sending Count coordination message", "err", err)
			return err
		}
	} else if msg.Type == inner.End {
		sum.stateLock.Lock()
		delete(sum.clientNewMsg, msg.ClientId)
		delete(sum.clientCoordinatorId, msg.ClientId)
		sum.stateLock.Unlock()

		if err := sum.sendClientRecords(msg.ClientId); err != nil {
			slog.Error("While sending client records", "err", err)
			return err
		}
		if err := sum.sendEndOfTheRecordsMessage(msg.ClientId); err != nil {
			slog.Error("While sending end of the-records message", "err", err)
			return err
		}
	}

	return nil
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

func (sum *Sum) takeClientRecords(clientId uint64) []fruititem.FruitItem {
	sum.stateLock.Lock()
	defer sum.stateLock.Unlock()

	fruitItemMap, ok := sum.clientFruitItemMap[clientId]
	if !ok {
		return nil
	}
	delete(sum.clientFruitItemMap, clientId)

	records := make([]fruititem.FruitItem, 0, len(fruitItemMap))
	for _, item := range fruitItemMap {
		records = append(records, item)
	}
	return records
}

func (sum *Sum) sendClientRecords(clientId uint64) error {
	recordsByGroup := map[int][]fruititem.FruitItem{}
	for _, record := range sum.takeClientRecords(clientId) {
		group := fruitGroup(record.Fruit, sum.aggregationAmount)
		recordsByGroup[group] = append(recordsByGroup[group], record)
	}

	for group, records := range recordsByGroup {
		message, err := inner.SerializeMessage(inner.MessageBody{ClientId: clientId, Records: records})
		if err != nil {
			return err
		}

		routingKey := fmt.Sprintf("%s_%d", sum.aggregationPrefix, group)
		if err := sum.outputExchange.SendTo(routingKey, *message); err != nil {
			return err
		}
	}
	return nil
}

func (sum *Sum) sendCoordBroadcastMessage(msg *inner.CoordinationMessage) error {
	message, err := inner.SerializeCoordinationMessage(*msg)
	if err != nil {
		return err
	}

	return sum.coordExchange.SendTo(coordBroadcastKey(), *message)
}

func (sum *Sum) sendCoordMessageTo(sumId int, msg *inner.CoordinationMessage) error {
	message, err := inner.SerializeCoordinationMessage(*msg)
	if err != nil {
		return err
	}

	routingKey := coordRouteKey(sumId)
	return sum.coordExchange.SendTo(routingKey, *message)
}

func coordRouteKey(id int) string {
	return fmt.Sprintf("coord_%d", id)
}

func coordBroadcastKey() string {
	return "coord_broadcast"
}

func fruitGroup(fruit string, groups int) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(fruit))
	return int(h.Sum64() % uint64(groups))
}

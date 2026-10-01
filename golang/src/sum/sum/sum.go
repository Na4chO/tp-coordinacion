package sum

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

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

type clientState struct {
	fruitItems    map[string]fruititem.FruitItem
	newMsg        uint64
	remainingMsg  uint64
	coordinatorId int
	isCoordinator bool
	isParticipant bool
}

type Sum struct {
	id                int
	inputQueue        middleware.Middleware
	outputExchange    middleware.Router
	coordExchange     middleware.Router
	aggregationPrefix string
	aggregationAmount int
	sumAmount         int
	stateLock         sync.Mutex
	clientStates      map[uint64]*clientState
	running           atomic.Bool
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
		_ = inputQueue.Close()
		return nil, err
	}

	coordExchangeRouteKeys := []string{coordBroadcastKey(), coordRouteKey(config.Id)}
	coordExchange, err := middleware.CreateExchangeMiddleware(
		fmt.Sprintf("coord_%s", config.SumPrefix),
		coordExchangeRouteKeys,
		connSettings,
	)
	if err != nil {
		_ = inputQueue.Close()
		_ = outputExchange.Close()
		return nil, err
	}

	sum := Sum{
		id:                config.Id,
		inputQueue:        inputQueue,
		outputExchange:    outputExchange,
		coordExchange:     coordExchange,
		aggregationPrefix: config.AggregationPrefix,
		aggregationAmount: config.AggregationAmount,
		sumAmount:         config.SumAmount,
		stateLock:         sync.Mutex{},
		clientStates:      map[uint64]*clientState{},
	}
	sum.running.Store(true)
	return &sum, nil
}

func (sum *Sum) Run() error {
	go func() {
		if err := sum.coordExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			sum.handleCoordMessage(msg, ack, nack)
		}); err != nil && sum.running.Load() {
			slog.Error("Coordination consumer stopped with error", "err", err)
		}
	}()

	go sum.handleSignals()

	err := errors.Join(
		sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			sum.handleMessage(msg, ack, nack)
		}),
		sum.closeMiddlewares(),
	)

	if sum.running.Load() {
		return err
	}
	return nil
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
	state := sum.clientStateFor(msgBody.ClientId)
	state.remainingMsg = msgBody.Total - state.newMsg
	state.newMsg = 0
	state.isCoordinator = true
	sum.stateLock.Unlock()

	if err := sum.sendClientRecords(msgBody.ClientId); err != nil {
		slog.Error("While sending client records", "err", err)
		return err
	}

	if err := sum.sendEndOfTheRecordsMessage(msgBody.ClientId); err != nil {
		slog.Error("While sending end of the-records message", "err", err)
		return err
	}

	if sum.sumAmount == 1 {
		sum.stateLock.Lock()
		delete(sum.clientStates, msgBody.ClientId)
		sum.stateLock.Unlock()
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
	state := sum.clientStateFor(msgBody.ClientId)

	for _, fruitRecord := range msgBody.Records {
		if current, ok := state.fruitItems[fruitRecord.Fruit]; ok {
			state.fruitItems[fruitRecord.Fruit] = current.Sum(fruitRecord)
		} else {
			state.fruitItems[fruitRecord.Fruit] = fruitRecord
		}
	}

	state.newMsg++
	coordinatorId := state.coordinatorId
	isParticipant := state.isParticipant
	processed := state.newMsg
	if isParticipant {
		state.newMsg = 0
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

	state, ok := sum.clientStates[clientId]
	if !ok {
		return nil
	}

	records := make([]fruititem.FruitItem, 0, len(state.fruitItems))
	for _, item := range state.fruitItems {
		records = append(records, item)
	}
	state.fruitItems = map[string]fruititem.FruitItem{}
	return records
}

func (sum *Sum) sendClientRecords(clientId uint64) error {
	recordsByGroup := map[int][]fruititem.FruitItem{}
	for _, record := range sum.takeClientRecords(clientId) {
		group := clientFruitGroup(clientId, record.Fruit, sum.aggregationAmount)
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

func clientFruitGroup(clientId uint64, fruit string, groups int) int {
	if groups < 1 {
		return 0
	}
	h := fnv.New64a()

	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, clientId)
	key = append(key, fruit...)

	_, _ = h.Write(key)

	return int(h.Sum64() % uint64(groups))
}

func (sum *Sum) clientStateFor(clientId uint64) *clientState {
	state, ok := sum.clientStates[clientId]
	if !ok {
		state = &clientState{fruitItems: map[string]fruititem.FruitItem{}}
		sum.clientStates[clientId] = state
	}
	return state
}

func (sum *Sum) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	sum.running.Store(false)
	_ = sum.inputQueue.StopConsuming()
	_ = sum.coordExchange.StopConsuming()
}

func (sum *Sum) closeMiddlewares() error {
	return errors.Join(
		sum.inputQueue.StopConsuming(),
		sum.inputQueue.Close(),
		sum.coordExchange.StopConsuming(),
		sum.coordExchange.Close(),
		sum.outputExchange.Close(),
	)
}

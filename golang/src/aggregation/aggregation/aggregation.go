package aggregation

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"sync/atomic"
	"syscall"

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

type clientState struct {
	fruitItems map[string]fruititem.FruitItem
	eofAmount  int
}

type Aggregation struct {
	outputQueue       middleware.Middleware
	inputExchange     middleware.Middleware
	clientStates      map[uint64]*clientState
	topSize           int
	expectedEOFAmount int
	running           atomic.Bool
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

	aggregation := Aggregation{
		outputQueue:       outputQueue,
		inputExchange:     inputExchange,
		clientStates:      map[uint64]*clientState{},
		topSize:           config.TopSize,
		expectedEOFAmount: config.SumAmount,
	}
	aggregation.running.Store(true)
	return &aggregation, nil
}

func (aggregation *Aggregation) Run() error {
	go aggregation.handleSignals()

	err := errors.Join(
		aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			aggregation.handleMessage(msg, ack, nack)
		}),
		aggregation.closeMiddlewares(),
	)

	if aggregation.running.Load() {
		return err
	}
	return nil
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

	state := aggregation.clientStateFor(msgBody.ClientId)
	state.eofAmount++
	if state.eofAmount < aggregation.expectedEOFAmount {
		return nil
	}

	fruitTopRecords := aggregation.buildFruitTop(state.fruitItems)
	delete(aggregation.clientStates, msgBody.ClientId)

	if err := aggregation.sendClientTop(msgBody.ClientId, fruitTopRecords); err != nil {
		slog.Error("While sending top message", "err", err)
		return err
	}

	return nil
}

func (aggregation *Aggregation) handleDataMessage(msgBody *inner.MessageBody) {
	state := aggregation.clientStateFor(msgBody.ClientId)

	for _, fruitRecord := range msgBody.Records {
		if current, ok := state.fruitItems[fruitRecord.Fruit]; ok {
			state.fruitItems[fruitRecord.Fruit] = current.Sum(fruitRecord)
		} else {
			state.fruitItems[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(fruitItemMap map[string]fruititem.FruitItem) []fruititem.FruitItem {
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

func (aggregation *Aggregation) sendClientTop(clientId uint64, fruitTopRecords []fruititem.FruitItem) error {
	message, err := inner.SerializeMessage(inner.MessageBody{
		ClientId: clientId,
		Records:  fruitTopRecords,
	})
	if err != nil {
		return err
	}
	return aggregation.outputQueue.Send(*message)
}

func (aggregation *Aggregation) clientStateFor(clientId uint64) *clientState {
	state, ok := aggregation.clientStates[clientId]
	if !ok {
		state = &clientState{fruitItems: map[string]fruititem.FruitItem{}}
		aggregation.clientStates[clientId] = state
	}
	return state
}

func (aggregation *Aggregation) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	aggregation.running.Store(false)
	_ = aggregation.inputExchange.StopConsuming()
}

func (aggregation *Aggregation) closeMiddlewares() error {
	return errors.Join(
		aggregation.inputExchange.StopConsuming(),
		aggregation.inputExchange.Close(),
		aggregation.outputQueue.Close(),
	)
}

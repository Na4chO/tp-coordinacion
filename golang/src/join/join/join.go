package join

import (
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type clientState struct {
	finalTop         []fruititem.FruitItem
	partialTopAmount int
}

type Join struct {
	inputQueue               middleware.Middleware
	outputQueue              middleware.Middleware
	topSize                  int
	expectedPartialTopAmount int
	clientStates             map[uint64]*clientState
	running                  atomic.Bool
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		_ = inputQueue.Close()
		return nil, err
	}

	join := Join{
		inputQueue:               inputQueue,
		outputQueue:              outputQueue,
		topSize:                  config.TopSize,
		expectedPartialTopAmount: config.AggregationAmount,
		clientStates:             map[uint64]*clientState{},
	}
	join.running.Store(true)
	return &join, nil
}

func (join *Join) Run() error {
	go join.handleSignals()

	err := errors.Join(
		join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			join.handleMessage(msg, ack, nack)
		}),
		join.closeMiddlewares(),
	)

	if join.running.Load() {
		return err
	}
	return nil
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	msgBody, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	state := join.clientStateFor(msgBody.ClientId)
	join.updateClientFinalTop(state, msgBody.Records)

	if state.partialTopAmount == join.expectedPartialTopAmount {
		if err := join.sendClientFinalTop(msgBody.ClientId, state); err != nil {
			slog.Error("While sending final top", "err", err)
			return
		}
	}
	return
}

func (join *Join) updateClientFinalTop(state *clientState, partialTop []fruititem.FruitItem) {
	state.partialTopAmount++
	if len(partialTop) == 0 {
		return
	}

	newTop := make([]fruititem.FruitItem, min(join.topSize, len(state.finalTop)+len(partialTop)))

	i, j := 0, 0
	for (i+j < join.topSize) && (i < len(state.finalTop) || j < len(partialTop)) {
		switch {
		case j >= len(partialTop):
			newTop[i+j] = state.finalTop[i]
			i++
		case i >= len(state.finalTop):
			newTop[i+j] = partialTop[j]
			j++
		case state.finalTop[i].Less(partialTop[j]):
			newTop[i+j] = partialTop[j]
			j++
		default:
			newTop[i+j] = state.finalTop[i]
			i++
		}
	}

	state.finalTop = newTop
}

func (join *Join) sendClientFinalTop(clientId uint64, state *clientState) error {
	finalTop := state.finalTop
	if finalTop == nil {
		finalTop = []fruititem.FruitItem{}
	}

	message, err := inner.SerializeMessage(inner.MessageBody{
		ClientId: clientId,
		Records:  finalTop,
	})
	if err != nil {
		return err
	}
	if err = join.outputQueue.Send(*message); err != nil {
		return err
	}

	delete(join.clientStates, clientId)
	return nil
}

func (join *Join) clientStateFor(clientId uint64) *clientState {
	state, ok := join.clientStates[clientId]
	if !ok {
		state = &clientState{}
		join.clientStates[clientId] = state
	}
	return state
}

func (join *Join) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	join.running.Store(false)
	_ = join.inputQueue.StopConsuming()
}

func (join *Join) closeMiddlewares() error {
	return errors.Join(
		join.inputQueue.StopConsuming(),
		join.inputQueue.Close(),
		join.outputQueue.Close(),
	)
}

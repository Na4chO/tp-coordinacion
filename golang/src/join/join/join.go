package join

import (
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

type Join struct {
	inputQueue               middleware.Middleware
	outputQueue              middleware.Middleware
	topSize                  int
	clientFinalTop           map[uint64][]fruititem.FruitItem
	expectedPartialTopAmount int
	clientPartialTopAmount   map[uint64]int
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
		inputQueue.Close()
		return nil, err
	}

	join := Join{
		inputQueue:               inputQueue,
		outputQueue:              outputQueue,
		topSize:                  config.TopSize,
		clientFinalTop:           map[uint64][]fruititem.FruitItem{},
		expectedPartialTopAmount: config.AggregationAmount,
		clientPartialTopAmount:   map[uint64]int{},
	}
	join.running.Store(true)
	return &join, nil
}

func (join *Join) Run() error {
	go join.handleSignals()

	err := join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})

	join.closeMiddlewares()

	if join.running.Load() {
		return err
	}
	return nil
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) error {
	defer ack()

	msgBody, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return err
	}

	join.updateCLientFinalTop(msgBody.ClientId, msgBody.Records)

	if join.clientPartialTopAmount[msgBody.ClientId] == join.expectedPartialTopAmount {
		if err := join.sendClientFinalTop(msgBody.ClientId); err != nil {
			slog.Error("While sending final top", "err", err)
			return err
		}
	}
	return nil
}

func (join *Join) updateCLientFinalTop(clientId uint64, partialTop []fruititem.FruitItem) {
	join.clientPartialTopAmount[clientId]++
	actualTop, ok := join.clientFinalTop[clientId]
	if !ok {
		join.clientFinalTop[clientId] = partialTop
		return
	}
	if len(partialTop) == 0 {
		return
	}

	newTop := make([]fruititem.FruitItem, min(join.topSize, len(actualTop)+len(partialTop)))

	i, j := 0, 0
	for (i+j < join.topSize) && (i < len(actualTop) || j < len(partialTop)) {
		switch {
		case j >= len(partialTop):
			newTop[i+j] = actualTop[i]
			i++
		case i >= len(actualTop):
			newTop[i+j] = partialTop[j]
			j++
		case actualTop[i].Less(partialTop[j]):
			newTop[i+j] = partialTop[j]
			j++
		default:
			newTop[i+j] = actualTop[i]
			i++
		}
	}

	join.clientFinalTop[clientId] = newTop
}

func (join *Join) sendClientFinalTop(clientId uint64) error {
	finalTop, _ := join.clientFinalTop[clientId]
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

	delete(join.clientFinalTop, clientId)
	delete(join.clientPartialTopAmount, clientId)
	return nil
}

func (join *Join) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	join.running.Store(false)
	_ = join.inputQueue.StopConsuming()
}

func (join *Join) closeMiddlewares() {
	_ = join.inputQueue.StopConsuming()
	_ = join.inputQueue.Close()
	_ = join.outputQueue.Close()
}

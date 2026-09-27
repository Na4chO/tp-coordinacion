package messagehandler

import (
	"sync/atomic"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

var idSeq atomic.Uint64

type MessageHandler struct {
	id           uint64
	messageCount uint64
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{id: idSeq.Add(1)}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	messageHandler.messageCount++
	records := []fruititem.FruitItem{fruitRecord}
	return inner.SerializeMessage(inner.MessageBody{
		ClientId: messageHandler.id,
		Records:  records,
	})
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	return inner.SerializeMessage(inner.MessageBody{
		ClientId: messageHandler.id,
		IsEof:    true,
		Total:    messageHandler.messageCount,
	})
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	body, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}
	if body.ClientId != messageHandler.id {
		return nil, nil
	}

	return body.Records, nil
}

package middleware

import (
	"errors"
	"fmt"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	conn        *amqp.Connection
	channel     *amqp.Channel
	queue       amqp.Queue
	isConsuming atomic.Bool
}

func NewQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%d", connectionSettings.Hostname, connectionSettings.Port))
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	queue, err := queueDeclare(queueName, channel)
	if err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return nil, err
	}

	return &QueueMiddleware{
		conn:    conn,
		channel: channel,
		queue:   queue,
	}, nil
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	defer func() { qm.isConsuming.Store(false) }()
	qm.isConsuming.Store(true)

	msgs, err := qm.consume()
	if err != nil {
		return err
	}

	for d := range msgs {
		msg := Message{Body: string(d.Body)}
		ack := func() { _ = d.Ack(false) }
		nack := func() { _ = d.Nack(false, true) }

		callbackFunc(msg, ack, nack)
	}

	if qm.conn.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (qm *QueueMiddleware) StopConsuming() error {
	if !qm.isConsuming.Load() {
		return nil
	}

	if err := qm.channel.Cancel(qm.queue.Name, false); err != nil {
		if qm.isDisconnectedErr(err) {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}

	return nil
}

func (qm *QueueMiddleware) Send(msg Message) error {
	err := qm.channel.Publish(
		"",
		qm.queue.Name,
		false,
		false,
		amqp.Publishing{
			Body: []byte(msg.Body),
		},
	)

	if err != nil {
		if qm.isDisconnectedErr(err) {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}

	return nil
}

func (qm *QueueMiddleware) Close() error {
	var closeErr error

	if err := qm.channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
		closeErr = err
	}
	if err := qm.conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
		closeErr = err
	}

	if closeErr != nil {
		return ErrMessageMiddlewareClose
	}

	return nil
}

func (qm *QueueMiddleware) isDisconnectedErr(err error) bool {
	return qm.conn.IsClosed() || errors.Is(err, amqp.ErrClosed)
}

func queueDeclare(name string, channel *amqp.Channel) (amqp.Queue, error) {
	return channel.QueueDeclare(
		name,
		false,
		false,
		false,
		false,
		nil,
	)
}

func (qm *QueueMiddleware) consume() (<-chan amqp.Delivery, error) {
	msgs, err := qm.channel.Consume(
		qm.queue.Name,
		qm.queue.Name,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		if qm.isDisconnectedErr(err) {
			return nil, ErrMessageMiddlewareDisconnected
		}
		return nil, ErrMessageMiddlewareMessage
	}

	return msgs, nil
}

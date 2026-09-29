package sum

import (
	"fmt"
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func (sum *Sum) handleCoordMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	coordMsg, err := inner.DeserializeCoordinationMessage(&msg)
	if err != nil {
		slog.Error("While deserializing coordination message", "err", err)
		return
	}

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
	state, ok := sum.clientStates[msg.ClientId]
	if !ok || !state.isCoordinator {
		sum.stateLock.Unlock()
		slog.Warn("Messages to process not found for this client", "clientId", msg.ClientId)
		return nil
	}
	state.remainingMsg -= msg.Processed
	shouldEnd := state.remainingMsg == 0
	if shouldEnd {
		delete(sum.clientStates, msg.ClientId)
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
		state := sum.clientStateFor(msg.ClientId)
		state.isParticipant = true
		state.coordinatorId = msg.CoordinatorId
		processed := state.newMsg
		state.newMsg = 0
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
		if err := sum.sendClientRecords(msg.ClientId); err != nil {
			slog.Error("While sending client records", "err", err)
			return err
		}
		if err := sum.sendEndOfTheRecordsMessage(msg.ClientId); err != nil {
			slog.Error("While sending end of-the-records message", "err", err)
			return err
		}

		sum.stateLock.Lock()
		delete(sum.clientStates, msg.ClientId)
		sum.stateLock.Unlock()
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

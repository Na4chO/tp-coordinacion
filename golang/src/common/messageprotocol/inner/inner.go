package inner

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func SerializeMessage(body MessageBody) (*middleware.Message, error) {
	jsonBody, err := body.serializeJson()
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(jsonBody)}

	return &message, nil
}

func DeserializeMessage(message *middleware.Message) (*MessageBody, error) {
	body, err := deserializeJson([]byte((*message).Body))
	if err != nil {
		return nil, err
	}
	return body, nil
}

func SerializeCoordinationMessage(msg CoordinationMessage) (*middleware.Message, error) {
	jsonBody, err := msg.serializeJson()
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(jsonBody)}

	return &message, nil
}

func DeserializeCoordinationMessage(message *middleware.Message) (*CoordinationMessage, error) {
	msg, err := deserializeJsonCoordination([]byte((*message).Body))
	if err != nil {
		return nil, err
	}
	return msg, nil
}

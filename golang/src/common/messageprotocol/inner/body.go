package inner

import (
	"encoding/json"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
)

type MessageBody struct {
	ClientId uint64                `json:"client"`
	IsEof    bool                  `json:"is_eof"`
	Total    uint64                `json:"t,omitempty"`
	Records  []fruititem.FruitItem `json:"r"`
}

func (b *MessageBody) serializeJson() ([]byte, error) {
	return json.Marshal(b)
}

func deserializeJson(data []byte) (*MessageBody, error) {
	var body MessageBody
	if err := json.Unmarshal(data, &body); err != nil {
		return nil, err
	}
	return &body, nil
}

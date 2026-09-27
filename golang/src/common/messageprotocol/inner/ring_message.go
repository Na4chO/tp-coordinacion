package inner

import "encoding/json"

type RingMessage struct {
	ClientId      uint64 `json:"client"`
	Total         uint64 `json:"t"`
	Processed     uint64 `json:"p"`
	IsFinal       bool   `json:"f"`
	CoordinatorId int    `json:"c"`
}

func (r *RingMessage) serializeJson() ([]byte, error) {
	return json.Marshal(r)
}

func deserializeJsonRing(data []byte) (*RingMessage, error) {
	var msg RingMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

package inner

import "encoding/json"

type CoordinationType string

const (
	Coordinator CoordinationType = "coordinator"
	Count       CoordinationType = "count"
	End         CoordinationType = "end"
)

type CoordinationMessage struct {
	Type          CoordinationType `json:"type"`
	ClientId      uint64           `json:"client"`
	Processed     uint64           `json:"p"`
	CoordinatorId int              `json:"c"`
}

func (m *CoordinationMessage) serializeJson() ([]byte, error) {
	return json.Marshal(m)
}

func deserializeJsonCoordination(data []byte) (*CoordinationMessage, error) {
	var msg CoordinationMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

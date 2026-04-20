package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
	"github.com/google/uuid"
)

func serializeJson(message []interface{}) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJson(message []byte) ([]interface{}, error) {
	var data []interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func SerializeMessage(fruitRecords fruititem.FruitItemFromClient) (*middleware.Message, error) {
	data := []interface{}{}
	data = append(data, fruitRecords.ClientId)
	for _, fruitRecord := range fruitRecords.FruitItems {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		data = append(data, datum)
	}

	body, err := serializeJson(data)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeMessage(message *middleware.Message) (*fruititem.FruitItemFromClient, bool, error) {
	data, err := deserializeJson([]byte((*message).Body))
	if err != nil {
		return nil, false, err
	}
	result := fruititem.FruitItemFromClient{}
	for i, datum := range data {
		if i == 0 {
			clientId, err := uuid.Parse(datum.(string))
			if err != nil {
				return nil, false, errors.New("Error parsing clientId to float64")
			}
			result.ClientId = clientId
			continue
		}

		fruitPair, ok := datum.([]interface{})
		if !ok {
			return nil, false, errors.New("Datum is not an array")
		}

		fruit, ok := fruitPair[0].(string)
		if !ok {
			return nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := fruitPair[1].(float64)
		if !ok {
			return nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
		result.FruitItems = append(result.FruitItems, fruitRecord)
	}

	return &result, len(result.FruitItems) == 0, nil
}

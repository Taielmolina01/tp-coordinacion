package fruititem

import (
	"fmt"

	"github.com/google/uuid"
)

type FruitItem struct {
	Fruit  string
	Amount uint32
}

func (f FruitItem) String() string {
	return fmt.Sprintf(f.Fruit, f.Amount)
}

func (fruitItem FruitItem) Sum(other FruitItem) FruitItem {
	return FruitItem{Fruit: fruitItem.Fruit, Amount: fruitItem.Amount + other.Amount}
}

func (fruitItem FruitItem) Less(other FruitItem) bool {
	return fruitItem.Amount < other.Amount
}

type FruitItemFromClient struct {
	ClientId   uuid.UUID
	FruitItems []FruitItem
}

func (f FruitItemFromClient) String() string {
	response := fmt.Sprintf("Client_id: %d, FruitItems: ", f.ClientId)
	for _, item := range f.FruitItems {
		response += item.String() + "\n"
	}
	return response
}

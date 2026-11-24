package handler

import (
	"log"
	"sync"

	"github.com.br/devfullcycle/fc-ms-wallet/pkg/events"
	"github.com.br/devfullcycle/fc-ms-wallet/pkg/kafka"
)

type TransactionCreatedKafkaHandler struct {
	Kafka *kafka.Producer
}

func NewTransactionCreatedKafkaHandler(kafka *kafka.Producer) *TransactionCreatedKafkaHandler {
	return &TransactionCreatedKafkaHandler{
		Kafka: kafka,
	}
}

func (h *TransactionCreatedKafkaHandler) Handle(message events.EventInterface, wg *sync.WaitGroup) {
	defer wg.Done()
	if err := h.Kafka.Publish(message, nil, "transactions"); err != nil {
		log.Printf("publish to transactions: %v", err)
	}
}

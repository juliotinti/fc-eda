package kafka

import (
	"encoding/json"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer struct {
	ConfigMap *ckafka.ConfigMap
}

func NewKafkaProducer(configMap *ckafka.ConfigMap) *Producer {
	return &Producer{ConfigMap: configMap}
}

func (p *Producer) Publish(msg interface{}, key []byte, topic string) error {
	producer, err := ckafka.NewProducer(p.ConfigMap)
	if err != nil {
		return err
	}

	defer producer.Close()

	msgJson, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	message := &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: ckafka.PartitionAny},
		Value:          msgJson,
		Key:            key,
	}
	delivery := make(chan ckafka.Event, 1)
	if err := producer.Produce(message, delivery); err != nil {
		return err
	}
	return (<-delivery).(*ckafka.Message).TopicPartition.Error
}

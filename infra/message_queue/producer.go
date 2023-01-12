package message_queue

import (
	"fmt"

	"github.com/Shopify/sarama"
)

type SyncProducer interface {
	SendMsg(key string, val []byte) (int32, int64, error)
}

type syncProducer struct {
	sarama.SyncProducer
	topic string
}

func NewSyncProducer(addrs []string, config *sarama.Config, topic string) (SyncProducer, error) {
	producer, err := sarama.NewSyncProducer(addrs, config)
	if err != nil {
		return nil, err
	}
	return &syncProducer{producer, topic}, nil
}

func (p *syncProducer) SendMsg(key string, val []byte) (int32, int64, error) {
	partition, offset, err := p.SendMessage(&sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(val),
	})

	if err != nil {
		return -1, -1, err
	}
	fmt.Printf("Send successful at partition of %d, offset of %d\n", partition, offset)
	return partition, offset, err
}

package main

import (
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/leminhviett/message-bus-prototype/domain/user"
	"github.com/leminhviett/message-bus-prototype/infra/message_queue"
)

func main() {
	consumer, err :=
		sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()
	partitionConsumer, err :=
		consumer.ConsumePartition("User_Tab", 0,
			sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	defer partitionConsumer.Close()
	for {
		msg := <-partitionConsumer.Messages()
		fmt.Printf("Consumed message: \"%s\" at offset: %d\n",
			msg.Value, msg.Offset)

		valueB := msg.Value

		message := &message_queue.Message{}
		json.Unmarshal(valueB, message)

		oldUser := &user.DTO{}
		newUser := &user.DTO{}

		err = json.Unmarshal(message.OldValue, oldUser)
		fmt.Println(err)
		err = json.Unmarshal(message.NewValue, newUser)
		fmt.Println(err)

		fmt.Printf("Table: %s; Action: %s \n", message.TableName, message.Action)
		fmt.Printf("Old value: %v; New Value: %v \n", oldUser, newUser)

	}
}

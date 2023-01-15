package main

import (
	"github.com/Shopify/sarama"
	"github.com/leminhviett/message-bus-prototype/domain/user"
	infra_canal "github.com/leminhviett/message-bus-prototype/infra/db_canal"
	"github.com/leminhviett/message-bus-prototype/infra/message_queue"
)

func main() {
	canal, err := infra_canal.NewDefaultCanal("127.0.0.1:3306", "root", "my-secret-pw")
	if err != nil {
		panic("init default db_canal fail")
	}

	userHandler := initUserHandler()
	canal.SetEventHandler(userHandler)
	err = canal.Run()
	if err != nil {
		panic("default db_canal run fail")
	}
}

func initUserHandler() *user.Handler {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Producer.Retry.Max = 3

	producer, err :=
		message_queue.NewSyncProducer([]string{"localhost:9092"}, kafkaConfig, "user_tab")
	if err != nil {
		panic(err)
	}

	handler := user.NewHandler(producer)
	handler.RegisterToDTO(user.DAOToDTO)

	return handler
}

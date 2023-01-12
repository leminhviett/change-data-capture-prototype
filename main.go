package main

import (
	"github.com/leminhviett/message-bus-prototype/domain/user"
	canal2 "github.com/leminhviett/message-bus-prototype/infra/canal"
	"github.com/leminhviett/message-bus-prototype/infra/message_queue"
)

func main() {
	canal, err := canal2.NewDefaultCanal("127.0.0.1:3306", "root", "my-secret-pw")
	if err != nil {
		panic("init default canal")
	}

	producer, err :=
		message_queue.NewSyncProducer([]string{"localhost:9092"}, nil, "User_Tab")
	if err != nil {
		panic(err)
	}

	convertDTO := func(dao *user.DAO) interface{} {
		return &user.DTO{
			Id:         dao.Id,
			Status:     dao.Status,
			CreateTime: dao.CreateTime,
		}
	}
	handler := &user.Handler{
		Producer:     producer,
		ConvertToDTO: []func(dao *user.DAO) interface{}{convertDTO},
	}

	canal.SetEventHandler(handler)
	canal.Run()

}

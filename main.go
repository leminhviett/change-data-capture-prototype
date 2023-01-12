package main

import (
	"github.com/leminhviett/message-bus-prototype/domain/user"
	canal2 "github.com/leminhviett/message-bus-prototype/infra/canal"
)

func main() {
	canal, err := canal2.NewDefaultCanal("127.0.0.1:3306", "root", "my-secret-pw")
	if err != nil {
		panic("init default canal")
	}

	handler := &user.Handler{}
	canal.SetEventHandler(handler)
	canal.Run()
}

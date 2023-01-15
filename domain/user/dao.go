package user

import (
	"encoding/json"
	"strconv"

	"github.com/go-mysql-org/go-mysql/canal"
	infra_canal "github.com/leminhviett/message-bus-prototype/infra/db_canal"
	"github.com/leminhviett/message-bus-prototype/infra/message_queue"
)

var TABLE_NAME = "User"

//DAO coupled with physical Database format
type DAO struct {
	Id         int64
	Name       string
	Status     uint32
	CreateTime uint64
}

type Handler struct {
	canal.DummyEventHandler
	Producer message_queue.SyncProducer
	ToDTO    []func(*DAO) interface{}
}

func NewHandler(producer message_queue.SyncProducer) *Handler {
	return &Handler{
		Producer: producer,
	}
}

func (h *Handler) RegisterToDTO(funcs ...func(*DAO) interface{}) {
	for _, f := range funcs {
		h.ToDTO = append(h.ToDTO, f)
	}
}

func (h *Handler) String() string { return "user binlog handler" }

func (h *Handler) OnRow(e *canal.RowsEvent) error {
	if e.Table.Name != TABLE_NAME {
		return nil
	}

	switch e.Action {
	case canal.DeleteAction:
		return nil
	case canal.UpdateAction:
		h.updateHandler(e)
	case canal.InsertAction:
		h.insertHandler(e)
	}

	return nil
}

func (h *Handler) updateHandler(e *canal.RowsEvent) {
	oldRow := e.Rows[0]
	oldUser := &DAO{}
	infra_canal.Unmarshal(oldUser, oldRow, e.Table.Columns)

	newRow := e.Rows[1]
	newUser := &DAO{}
	infra_canal.Unmarshal(newUser, newRow, e.Table.Columns)

	h.produceMsg(oldUser, newUser)
}

func (h *Handler) produceMsg(oldUser, newUser *DAO) {
	for _, f := range h.ToDTO {
		oldUserDTO := f(oldUser)
		newUserDTO := f(newUser)

		oldUserDTOB, _ := json.Marshal(oldUserDTO)
		newUserDTOB, _ := json.Marshal(newUserDTO)

		message := message_queue.Message{
			TableName: TABLE_NAME,
			Action:    canal.UpdateAction,
			OldValue:  oldUserDTOB,
			NewValue:  newUserDTOB,
		}
		messageData, _ := json.Marshal(message)

		h.Producer.SendMsg(strconv.FormatInt(oldUser.Id, 10), messageData)
	}

}

func (h *Handler) insertHandler(e *canal.RowsEvent) {

}

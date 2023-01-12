package user

import (
	"fmt"
	"reflect"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
)

type DAO struct {
	Id         int64
	Name       string
	Status     uint32
	CreateTime uint64
}

type DTO struct {
	Id         int64
	CreateTime uint64
}

type Handler struct {
	canal.DummyEventHandler
}

func (u *Handler) RegisterDTOConversion() {
	//Convert()
}

func (u *Handler) String() string { return "user binlog handler" }

func (u *Handler) OnRow(e *canal.RowsEvent) error {
	if e.Table.Name != "User" {
		return nil
	}

	switch e.Action {
	case canal.DeleteAction:
		return nil
	case canal.UpdateAction:
		u.updateHandler(e)
	case canal.InsertAction:
		u.insertHandler(e)
	}

	return nil
}

func (u *Handler) updateHandler(e *canal.RowsEvent) {
	oldRow := e.Rows[0]
	oldUser := &DAO{}
	Unmarshal(oldUser, oldRow, e.Table.Columns)

	fmt.Println(oldUser)

	newRow := e.Rows[1]
	newUser := &DAO{}
	Unmarshal(newUser, newRow, e.Table.Columns)

	fmt.Println(newUser)
	fmt.Println("====")
}

func Unmarshal(dest interface{}, rowSrc []interface{}, columns []schema.TableColumn) {
	destVal := reflect.ValueOf(dest).Elem()

	for i, val := range rowSrc {
		switch columns[i].Type {
		case schema.TYPE_NUMBER:
			switch val.(type) {
			case int32:
				destVal.FieldByIndex([]int{i}).SetInt(int64(val.(int32)))
			case int64:
				destVal.FieldByIndex([]int{i}).SetInt(val.(int64))
			default:
				fmt.Printf("Not supported number type %T \n", val)
			}
		case schema.TYPE_STRING:
			destVal.FieldByIndex([]int{i}).SetString(val.(string))
		case schema.TYPE_ENUM:
			destVal.FieldByIndex([]int{i}).SetUint(uint64(val.(int64)))
		case schema.TYPE_TIMESTAMP:
			timeStamp, err := time.Parse("2006-01-02 15:04:05", val.(string))
			if err != nil {
				return
			}
			destVal.FieldByIndex([]int{i}).SetUint(uint64(timeStamp.Unix()))
		default:
			fmt.Printf("Not supported %v \n", columns[i].Type)
		}
	}

}

func (u *Handler) insertHandler(e *canal.RowsEvent) {

}

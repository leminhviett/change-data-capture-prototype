package db_canal

import (
	"fmt"
	"reflect"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
)

type Canal interface {
	Run() error
	SetEventHandler(h canal.EventHandler)
}

type defaultCanal struct {
	*canal.Canal
}

func NewDefaultCanal(addr, usr, pw string) (Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = addr
	cfg.User = usr
	cfg.Password = pw
	cfg.Flavor = "mysql"
	cfg.Dump.ExecutionPath = ""

	newCanal, err := canal.NewCanal(cfg)
	if err != nil {
		return nil, err
	}

	return &defaultCanal{
		Canal: newCanal,
	}, nil
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

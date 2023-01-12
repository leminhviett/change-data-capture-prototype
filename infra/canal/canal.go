package canal

import "github.com/go-mysql-org/go-mysql/canal"

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

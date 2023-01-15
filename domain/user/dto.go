package user

//DTO business data model; different team can register different DTO format
type DTO struct {
	Id         int64
	Status     uint32
	CreateTime uint64
}

var DAOToDTO = func(dao *DAO) interface{} {
	return &DTO{
		Id:         dao.Id,
		Status:     dao.Status,
		CreateTime: dao.CreateTime,
	}
}

package vm

type Scan interface {
	Init() error
	Next() (bool, error)
	GetInt16(colName string) (int16, error)
	GetInt32(colName string) (int32, error)
	GetInt64(colName string) (int64, error)
	GetFloat32(colName string) (float32, error)
	GetFloat64(colName string) (float64, error)
	GetString(colName string) (string, error)
	HasColumn(colName string) bool
	Close()
}

package processor

type Scan interface {
	BeforeFirst() error
	Next() (bool, error)
	GetInt16(colName string) (int16, error)
	GetInt32(colName string) (int32, error)
	GetInt64(colName string) (int64, error)
	GetFloat32(colName string) (float32, error)
	GetFloat64(colName string) (float64, error)
	GetString(colName string) (string, error)
	GetType(colName string) (string, error)
	HasColumn(colName string) bool
}

type UpdateScan interface {
	Scan
	SetInt16(colName string, val int16)
	SetInt32(colName string, val int32)
	SetInt64(colName string, val int64)
	SetFloat32(colName string, val float32)
	SetFloat64(colName string, val float64)
	SetString(colName string, val string)
	Insert()
	Delete()
	GetRowKey() []byte
	MoveToRow(key []byte)
}

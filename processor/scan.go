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
	GetRow() string
	GetFields() string
	HasColumn(colName string) bool
}

type ModifyScan interface {
	Scan
	SetInt16(colName string, val int16) error
	SetInt32(colName string, val int32) error
	SetInt64(colName string, val int64) error
	SetFloat32(colName string, val float32) error
	SetFloat64(colName string, val float64) error
	SetString(colName string, val string) error
	InsertRow() error
	DeleteRow() error
	GetRowKey() []byte
}

package record

import (
	"encoding/binary"
	"math"
)

type Value struct {
	data     []byte
	datatype DataType
}

func NewValue(data []byte, datatype DataType) *Value {
	return &Value{
		data:     data,
		datatype: datatype,
	}
}

func (v *Value) ToInt16() int16 {
	if len(v.data) < 2 {
		return 0
	}
	return int16(binary.BigEndian.Uint16(v.data))
}

func (v *Value) ToInt32() int32 {
	if len(v.data) < 4 {
		return 0
	}
	return int32(binary.BigEndian.Uint32(v.data))
}

func (v *Value) ToInt64() int64 {
	if len(v.data) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(v.data))
}

func (v *Value) ToFloat32() float32 {
	if len(v.data) < 4 {
		return 0
	}
	return math.Float32frombits(binary.BigEndian.Uint32(v.data))
}

func (v *Value) ToFloat64() float64 {
	if len(v.data) < 8 {
		return 0
	}
	return math.Float64frombits(binary.BigEndian.Uint64(v.data))
}

func (v *Value) ToString() string {
	if len(v.data) < 2 {
		return ""
	}
	return string(v.data[2:]) // first 2 bytes are the char/varchar length
}

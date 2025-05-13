package processor

import (
	"errors"
	"strings"
)

type ProjectScan struct {
	inputScan Scan
	fields    map[string]struct{}
}

func NewProjectScan(inputScan Scan, fields []string) *ProjectScan {

	fieldsMap := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		fieldsMap[field] = struct{}{}
	}
	return &ProjectScan{
		inputScan: inputScan,
		fields:    fieldsMap,
	}
}

func (ps *ProjectScan) BeforeFirst() error {
	return ps.inputScan.BeforeFirst()
}

func (ps *ProjectScan) Next() (bool, error) {
	return ps.inputScan.Next()
}

func (ps *ProjectScan) GetInt16(colName string) (int16, error) {
	if ps.HasColumn(colName) {
		return ps.inputScan.GetInt16(colName)
	}

	return 0, errors.New("field not included in projection")
}

func (ps *ProjectScan) GetInt32(colName string) (int32, error) {
	if ps.HasColumn(colName) {
		return ps.inputScan.GetInt32(colName)
	}

	return 0, errors.New("field not included in projection")
}

func (ps *ProjectScan) GetInt64(colName string) (int64, error) {
	if ps.HasColumn(colName) {
		return ps.inputScan.GetInt64(colName)
	}

	return 0, errors.New("field not included in projection")
}

func (ps *ProjectScan) GetFloat32(colName string) (float32, error) {
	if ps.HasColumn(colName) {
		return ps.inputScan.GetFloat32(colName)
	}

	return 0, errors.New("field not included in projection")
}

func (ps *ProjectScan) GetFloat64(colName string) (float64, error) {
	if ps.HasColumn(colName) {
		return ps.inputScan.GetFloat64(colName)
	}

	return 0, errors.New("field not included in projection")
}

func (ps *ProjectScan) GetString(colName string) (string, error) {
	if ps.HasColumn(colName) {
		return ps.inputScan.GetString(colName)
	}

	return "", errors.New("field not included in projection")
}

func (ps *ProjectScan) GetType(colName string) (string, error) {
	if ps.HasColumn(colName) {
		return ps.inputScan.GetType(colName)
	}

	return "", errors.New("field not included in projection")
}

func (ps *ProjectScan) GetRow() string {
	row := ps.inputScan.GetRow()
	if _, star := ps.fields["*"]; star {
		return row
	}

	fields := ps.inputScan.GetFields()
	rowVals := strings.Split(row, "|")
	fieldVals := strings.Split(fields, "|")
	var sb strings.Builder

	if len(rowVals) != len(fieldVals) {
		return ""
	}

	for i, f := range fieldVals {
		if _, ok := ps.fields[f]; ok {
			sb.WriteString(rowVals[i])
			sb.WriteRune('|')
		}
	}

	r := []rune(sb.String())    // There is guaranteed to be at least one field
	return string(r[:len(r)-1]) // Remove unnecessaru '|' at the end of the string
}

func (ps *ProjectScan) GetFields() string {
	if _, star := ps.fields["*"]; star {
		return ps.inputScan.GetFields()
	}

	var sb strings.Builder
	for f := range ps.fields {
		sb.WriteString(f)
		sb.WriteRune('|')
	}

	r := []rune(sb.String())
	return string(r[:len(r)-1]) // Remove unnecessaru '|' at the end of the string
}

func (ps *ProjectScan) HasColumn(colName string) bool {
	if _, star := ps.fields["*"]; star {
		return ps.inputScan.HasColumn(colName) // Should always be true because the planner should've checked semantic correctness
	}

	if _, ok := ps.fields[colName]; ok {
		return ps.inputScan.HasColumn(colName) // Should always be true because the planner should've checked semantic correctness
	}

	return false
}

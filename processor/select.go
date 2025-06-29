package processor

import (
	"errors"

	"github.com/m0hossam/rocketsql/parser"
)

type SelectScan struct {
	inputScan Scan
	predicate *parser.Predicate
}

func NewSelectScan(inputScan Scan, predicate *parser.Predicate) *SelectScan {
	return &SelectScan{
		inputScan: inputScan,
		predicate: predicate,
	}
}

func evalIntTerm(l int64, r int64, op string) bool {
	switch op {
	case "=":
		return l == r
	case "!=", "<>":
		return l != r
	case ">":
		return l > r
	case ">=":
		return l >= r
	case "<":
		return l < r
	case "<=":
		return l <= r
	default:
		panic("unreachable state, the planner should have checked the semantics of the query beforehand")
	}
}

func evalFloatTerm(l float64, r float64, op string) bool {
	switch op {
	case "=":
		return l == r
	case "!=", "<>":
		return l != r
	case ">":
		return l > r
	case ">=":
		return l >= r
	case "<":
		return l < r
	case "<=":
		return l <= r
	default:
		panic("unreachable state, the planner should have checked the semantics of the query beforehand")
	}
}

func evalStringTerm(l string, r string, op string) bool {
	switch op {
	case "=":
		return l == r
	case "!=", "<>":
		return l != r
	case ">":
		return l > r
	case ">=":
		return l >= r
	case "<":
		return l < r
	case "<=":
		return l <= r
	default:
		panic("unreachable state, the planner should have checked the semantics of the query beforehand")
	}
}

func (ss *SelectScan) resolveExpression(expr *parser.Expression) *parser.Constant {
	if expr.IsField {
		fieldType, err := ss.inputScan.GetType(expr.Field.Name)
		if err != nil {
			panic("unreachable state, the planner should have checked the semantics of the query beforehand")
		}
		switch fieldType {
		case "SMALLINT", "INT", "BIGINT":
			val, err := ss.inputScan.GetInt64(expr.Field.Name)
			if err != nil {
				panic("unreachable state, the planner should have checked the semantics of the query beforehand")
			}
			return &parser.Constant{
				Type:   parser.IntegerToken,
				IntVal: val,
			}
		case "FLOAT", "DOUBLE":
			val, err := ss.inputScan.GetFloat64(expr.Field.Name)
			if err != nil {
				panic("unreachable state, the planner should have checked the semantics of the query beforehand")
			}
			return &parser.Constant{
				Type:     parser.FloatToken,
				FloatVal: val,
			}
		case "CHAR", "VARCHAR":
			val, err := ss.inputScan.GetString(expr.Field.Name)
			if err != nil {
				panic("unreachable state, the planner should have checked the semantics of the query beforehand")
			}
			return &parser.Constant{
				Type:   parser.StringToken,
				StrVal: val,
			}
		default:
			panic("unreachable state, the planner should have checked the semantics of the query beforehand")
		}
	}

	// Expression is a constant
	return expr.Constant
}

func (ss *SelectScan) isPredicateSatisfied(predicate *parser.Predicate) (bool, error) {
	if predicate == nil {
		return true, nil
	}

	var termRes bool
	leftConst := ss.resolveExpression(predicate.Term.Lhs)
	rightConst := ss.resolveExpression(predicate.Term.Rhs)

	switch {
	case leftConst.Type == parser.IntegerToken && rightConst.Type == parser.IntegerToken:
		termRes = evalIntTerm(leftConst.IntVal, rightConst.IntVal, predicate.Term.Op)
	case leftConst.Type == parser.IntegerToken && rightConst.Type == parser.FloatToken:
		termRes = evalFloatTerm(float64(leftConst.IntVal), rightConst.FloatVal, predicate.Term.Op)
	case leftConst.Type == parser.FloatToken && rightConst.Type == parser.IntegerToken:
		termRes = evalFloatTerm(leftConst.FloatVal, float64(rightConst.IntVal), predicate.Term.Op)
	case leftConst.Type == parser.FloatToken && rightConst.Type == parser.FloatToken:
		termRes = evalFloatTerm(leftConst.FloatVal, rightConst.FloatVal, predicate.Term.Op)
	case leftConst.Type == parser.StringToken && rightConst.Type == parser.StringToken:
		termRes = evalStringTerm(leftConst.StrVal, rightConst.StrVal, predicate.Term.Op)
	default:
		return false, errors.New("type mismatch in predicate")
	}

	if predicate.Next != nil {
		nextRes, err := ss.isPredicateSatisfied(predicate.Next)
		if err != nil {
			return false, err
		}
		switch predicate.Op {
		case "AND":
			return termRes && nextRes, nil
		case "OR":
			return termRes || nextRes, nil
		default:
			return false, errors.New("invalid operator")
		}
	}

	return termRes, nil
}

func (ss *SelectScan) BeforeFirst() error {
	return ss.inputScan.BeforeFirst()
}

func (ss *SelectScan) Next() (bool, error) {
	// Loop through records until we reach one that satisfies the predicate or the end of the scan or an error
	for {
		next, err := ss.inputScan.Next()

		if !next || err != nil {
			return false, err
		}

		predTrue, err := ss.isPredicateSatisfied(ss.predicate)
		if err != nil {
			return false, err
		}

		if predTrue {
			return true, nil
		}
	}

}

func (ss *SelectScan) GetInt16(colName string) (int16, error) {
	return ss.inputScan.GetInt16(colName)
}

func (ss *SelectScan) GetInt32(colName string) (int32, error) {
	return ss.inputScan.GetInt32(colName)
}

func (ss *SelectScan) GetInt64(colName string) (int64, error) {
	return ss.inputScan.GetInt64(colName)
}

func (ss *SelectScan) GetFloat32(colName string) (float32, error) {
	return ss.inputScan.GetFloat32(colName)
}

func (ss *SelectScan) GetFloat64(colName string) (float64, error) {
	return ss.inputScan.GetFloat64(colName)
}

func (ss *SelectScan) GetString(colName string) (string, error) {
	return ss.inputScan.GetString(colName)
}

func (ss *SelectScan) GetType(colName string) (string, error) {
	return ss.inputScan.GetType(colName)
}

func (ss *SelectScan) GetRow() string {
	return ss.inputScan.GetRow()
}

func (ss *SelectScan) GetFields() string {
	return ss.inputScan.GetFields()
}

func (ss *SelectScan) HasColumn(colName string) bool {
	return ss.inputScan.HasColumn(colName)
}

func (ss *SelectScan) SetInt16(colName string, val int16) error {
	if ms, ok := ss.inputScan.(ModifyScan); ok {
		return ms.SetInt16(colName, val)
	}
	return errors.New("modify scan not supported")
}

func (ss *SelectScan) SetInt32(colName string, val int32) error {
	if ms, ok := ss.inputScan.(ModifyScan); ok {
		return ms.SetInt32(colName, val)
	}
	return errors.New("modify scan not supported")
}

func (ss *SelectScan) SetInt64(colName string, val int64) error {
	if ms, ok := ss.inputScan.(ModifyScan); ok {
		return ms.SetInt64(colName, val)
	}
	return errors.New("modify scan not supported")
}

func (ss *SelectScan) SetFloat32(colName string, val float32) error {
	if ms, ok := ss.inputScan.(ModifyScan); ok {
		return ms.SetFloat32(colName, val)
	}
	return errors.New("modify scan not supported")
}

func (ss *SelectScan) SetFloat64(colName string, val float64) error {
	if ms, ok := ss.inputScan.(ModifyScan); ok {
		return ms.SetFloat64(colName, val)
	}
	return errors.New("modify scan not supported")
}

func (ss *SelectScan) SetString(colName string, val string) error {
	if ms, ok := ss.inputScan.(ModifyScan); ok {
		return ms.SetString(colName, val)
	}
	return errors.New("modify scan not supported")
}

func (ss *SelectScan) InsertRow() error {
	if ms, ok := ss.inputScan.(ModifyScan); ok {
		return ms.InsertRow()
	}
	return errors.New("modify scan not supported")
}

func (ss *SelectScan) DeleteRow() error {
	if ms, ok := ss.inputScan.(ModifyScan); ok {
		return ms.DeleteRow()
	}
	return errors.New("modify scan not supported")
}

func (ss *SelectScan) GetRowKey() []byte {
	if ms, ok := ss.inputScan.(ModifyScan); ok {
		return ms.GetRowKey()
	}
	return nil
}

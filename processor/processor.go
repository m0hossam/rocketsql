package processor

import (
	"errors"

	"github.com/m0hossam/rocketsql/btree"
	"github.com/m0hossam/rocketsql/metadata"
	"github.com/m0hossam/rocketsql/parser"
	"github.com/m0hossam/rocketsql/record"
)

type Processor struct {
	btree      *btree.Btree
	tblManager *metadata.TableManager
}

func NewProcessor(btree *btree.Btree) *Processor {
	return &Processor{
		btree:      btree,
		tblManager: metadata.NewTableManager(btree),
	}
}

// rows affected, result table
func (p *Processor) ExecuteSQL(parseTree *parser.ParseTree) (int, Scan, error) {
	switch parseTree.Type {
	case parser.SelectTree:
		scan, err := p.ExecuteQuery(parseTree.Query)
		return 0, scan, err
	case parser.InsertTree:
		if err := p.ExecuteInsert(parseTree.InsertData); err != nil {
			return 0, nil, err
		}
		return 1, nil, nil
	case parser.CreateTableTree:
		return 0, nil, p.ExecuteCreateTable(parseTree.CreateTableData)
	case parser.DeleteTree:
		rows, err := p.ExecuteDelete(parseTree.DeleteData)
		return rows, nil, err
	case parser.UpdateTree:
		rows, err := p.ExecuteUpdate(parseTree.UpdateData)
		return rows, nil, err
	default:
		return 0, nil, errors.New("todo")
	}
}

func (p *Processor) ExecuteUpdate(updateData *parser.UpdateData) (int, error) {
	metadata, err := p.tblManager.GetTableMetadata(updateData.TableName)
	if err != nil {
		return 0, err
	}
	tableScan := NewTableScan(metadata, p.btree)
	selectScan := NewSelectScan(tableScan, updateData.Predicate)

	if err = selectScan.BeforeFirst(); err != nil {
		return 0, err
	}

	rowsAffected := 0
	for {
		ok, err := selectScan.Next()
		if err != nil {
			return 0, err
		}

		if !ok {
			break
		}

		if err = selectScan.DeleteRow(); err != nil {
			return 0, err
		}

		fieldType, err := selectScan.GetType(updateData.Field.Name)
		if err != nil {
			return 0, err
		}

		switch fieldType {
		case "SMALLINT":
			err = selectScan.SetInt16(updateData.Field.Name, int16(updateData.Constant.IntVal))
		case "INT":
			err = selectScan.SetInt32(updateData.Field.Name, int32(updateData.Constant.IntVal))
		case "BIGINT":
			err = selectScan.SetInt64(updateData.Field.Name, updateData.Constant.IntVal)
		case "FLOAT":
			err = selectScan.SetFloat32(updateData.Field.Name, float32(updateData.Constant.FloatVal))
		case "DOUBLE":
			err = selectScan.SetFloat64(updateData.Field.Name, updateData.Constant.FloatVal)
		case "CHAR", "VARCHAR":
			err = selectScan.SetString(updateData.Field.Name, updateData.Constant.StrVal)
		default:
			err = errors.New("invalid type")
		}

		if err != nil {
			return 0, err
		}

		if err = selectScan.InsertRow(); err != nil {
			return 0, err
		}

		rowsAffected++
	}

	return rowsAffected, nil
}

func (p *Processor) ExecuteDelete(deleteData *parser.DeleteData) (int, error) {
	metadata, err := p.tblManager.GetTableMetadata(deleteData.TableName)
	if err != nil {
		return 0, err
	}
	tableScan := NewTableScan(metadata, p.btree)
	selectScan := NewSelectScan(tableScan, deleteData.Predicate)

	if err = selectScan.BeforeFirst(); err != nil {
		return 0, err
	}

	rowsAffected := 0
	for {
		ok, err := selectScan.Next()
		if err != nil {
			return 0, err
		}

		if !ok {
			break
		}

		if err = selectScan.DeleteRow(); err != nil {
			return 0, err
		}
		rowsAffected++
	}

	return rowsAffected, nil
}

func (p *Processor) ExecuteInsert(insertData *parser.InsertData) error {
	metadata, err := p.tblManager.GetTableMetadata(insertData.TableName)
	if err != nil {
		return err
	}

	if len(insertData.Values) != len(metadata.TableSchema.FieldDefs) {
		return errors.New("too few parameters in VALUES")
	}

	// Get ordered column indices
	mp := make(map[string]int, len(metadata.TableSchema.FieldDefs))
	for i, fieldDef := range metadata.TableSchema.FieldDefs {
		mp[fieldDef.Name] = i
	}

	// Rearrange columns and values according to the order column indices
	cols := make([]*parser.TypeDef, len(metadata.TableSchema.FieldDefs))
	vals := make([]*parser.Constant, len(insertData.Values))

	if insertData.Fields != nil {
		if len(insertData.Fields) != len(metadata.TableSchema.FieldDefs) {
			return errors.New("too few fields")
		}

		for i, f := range insertData.Fields {
			pos, ok := mp[f.Name]
			if !ok {
				return errors.New("no field with this name")
			}
			cols[pos] = metadata.TableSchema.FieldDefs[pos].TypeDef
			vals[pos] = insertData.Values[i]
		}
	} else {
		for i, f := range metadata.TableSchema.FieldDefs {
			cols[i] = f.TypeDef
			vals[i] = insertData.Values[i]
		}
	}

	keyRec := &record.Record{
		Columns: []*parser.TypeDef{cols[0]},
		Values:  []*parser.Constant{vals[0]},
	}
	valueRec := &record.Record{
		Columns: cols,
		Values:  vals,
	}

	key, err := keyRec.Serialize()
	if err != nil {
		return err
	}
	value, err := valueRec.Serialize()
	if err != nil {
		return err
	}

	return p.btree.Insert(metadata.RootPageNo, key, value)
}

func (p *Processor) ExecuteCreateTable(tableData *parser.CreateTableData) error {
	newPgNo := p.btree.GetNewPagePtr()
	keyRec := &record.Record{
		Columns: []*parser.TypeDef{{Type: "VARCHAR", Size: 255}},
		Values:  []*parser.Constant{{StrVal: tableData.TableName}},
	}
	valueRec := &record.Record{
		Columns: []*parser.TypeDef{
			{Type: "VARCHAR", Size: 255},
			{Type: "INT"},
			{Type: "VARCHAR", Size: 255}},
		Values: []*parser.Constant{
			{Type: parser.StringToken, StrVal: tableData.TableName},
			{Type: parser.IntegerToken, IntVal: int64(*newPgNo)},
			{Type: parser.StringToken, StrVal: tableData.SchemaSql}},
	}
	key, err := keyRec.Serialize()
	if err != nil {
		return err
	}
	value, err := valueRec.Serialize()
	if err != nil {
		return err
	}
	return p.btree.Create(key, value)
}

func (p *Processor) ExecuteQuery(query *parser.Query) (Scan, error) {
	metadata, err := p.tblManager.GetTableMetadata(query.TableList[0].Name)
	if err != nil {
		return nil, err
	}
	tableScan := NewTableScan(metadata, p.btree)
	var productScan *ProductScan
	for i := 1; i < len(query.TableList); i++ {
		metadata, err = p.tblManager.GetTableMetadata(query.TableList[i].Name)
		if err != nil {
			return nil, err
		}
		newTableScan := NewTableScan(metadata, p.btree)
		productScan = NewProductScan(tableScan, newTableScan)
		tableScan = newTableScan
	}

	var selectScan *SelectScan
	if productScan != nil {
		selectScan = NewSelectScan(productScan, query.Predicate)
	} else {
		selectScan = NewSelectScan(tableScan, query.Predicate)
	}

	selectList := []string{}
	for _, field := range query.SelectList {
		selectList = append(selectList, field.Name)
	}
	projectScan := NewProjectScan(selectScan, selectList)
	return projectScan, nil
}

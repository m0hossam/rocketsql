package parser

import (
	"errors"
)

/*

#################################################################
# TODO: Remove unnecessary matches                              #
# TODO: Refactor structs and functions into query, dml, and ddl #
#################################################################

*/

/* Grammar:
<Field> := IdTok
<Constant> := StrTok | IntTok | FloatTok
<Expression> := <Field> | <Constant>
<Term> := <Expression> = <Expression>
<Predicate> := <Term> [ AND <Predicate> ]

<FieldList> := <Field> [ , <FieldList> ]
<ConstList> := <Constant> [ , <ConstList> ]

<Query> := SELECT <FieldList> FROM <FieldList> [ WHERE <Predicate> ]

<DML> := <Insert> | <Delete> | <Update>

<Insert> := INSERT INTO IdTok ( <FieldList> ) VALUES ( <ConstList> )

<Delete> := DELETE FROM IdTok [ WHERE <Predicate> ]

<Update> := UPDATE IdTok SET <Field> = <Expression> [ WHERE <Predicate> ]

<DDL> := <CreateTable>

<CreateTable> := CREATE TABLE IdTok ( <FieldDefs> )

<FieldDefs> := <FieldDef> [ , <FieldDefs> ]
<FieldDef> := IdTok <TypeDef>
<TypeDef> := SMALLINT | INT | BIGINT | FLOAT | DOUBLE | CHAR ( IntTok ) | VARCHAR ( IntTok )
*/

// <Field> := IdTok
type Field struct {
	Name string
}

// <Constant> := StrTok | IntTok | FloatTok
type Constant struct {
	Type     TokenType
	StrVal   string
	IntVal   int
	FloatVal float64
}

// <Expression> := <Field> | <Constant>
type Expression struct {
	IsField  bool
	Field    *Field
	Constant *Constant
}

// <Term> := <Expression> <Op> <Expression>
type Term struct {
	Lhs *Expression
	Op  string // Comparison operators (=, <, >, <=, >=, !=, <>)
	Rhs *Expression
}

// <Predicate> := <Term> [ AND | OR <Predicate> ]
type Predicate struct {
	Term *Term
	Op   string     // Logical operators (AND, OR)
	Next *Predicate // Optional predicate
}

// <FieldList> := <Field> [ , <FieldList> ]
type FieldList struct {
	Fields []*Field
}

// <ConstList> := <Constant> [ , <ConstList> ]
type ConstList struct {
	Constants []*Constant
}

// <Query> := SELECT <SelectList> FROM <TableList> [ WHERE <Predicate> ]
type Query struct {
	SelectList *FieldList
	TableList  *FieldList
	Predicate  *Predicate
}

// <TypeDef> := SMALLINT | INT | BIGINT | FLOAT | DOUBLE | CHAR ( IntTok ) | VARCHAR ( IntTok )
type TypeDef struct {
	Type string
	Size int // Size for CHAR and VARCHAR types
}

// <FieldDef> := IdTok <TypeDef>
type FieldDef struct {
	Name    string
	TypeDef *TypeDef
}

// <FieldDefs> := <FieldDef> [ , <FieldDefs> ]
type FieldDefs struct {
	FieldDefs []*FieldDef
}

// <CreateTable> := CREATE TABLE IdTok ( <FieldDefs> )
type CreateTableData struct {
	TableName string
	FieldDefs *FieldDefs
}

// <Insert> := INSERT INTO IdTok ( <FieldList> ) VALUES ( <ConstList> )
type InsertData struct {
	TableName string
	Fields    *FieldList
	Values    *ConstList
}

// <Delete> := DELETE FROM IdTok [ WHERE <Predicate> ]
type DeleteData struct {
	TableName string
	Predicate *Predicate
}

// <Update> := UPDATE IdTok SET <Field> = <Expression> [ WHERE <Predicate> ]
type UpdateData struct {
	TableName  string
	Field      *Field
	Expression *Expression
	Predicate  *Predicate
}

type ParseTreeType int

const (
	SelectTree ParseTreeType = iota
	InsertTree
	DeleteTree
	UpdateTree
	CreateTableTree
)

type ParseTree struct {
	Type            ParseTreeType
	Query           *Query
	CreateTableData *CreateTableData
	InsertData      *InsertData
	DeleteData      *DeleteData
	UpdateData      *UpdateData
}

type Parser struct {
	lexer *lexer
}

func NewParser(sql string) *Parser {
	return &Parser{
		lexer: newLexer(sql),
	}
}

func (p *Parser) parseField() (*Field, error) {
	if !p.lexer.matchIdentifier() {
		return nil, errors.New("invalid syntax")
	}

	id, err := p.lexer.eatIdentifier()
	if err != nil {
		return nil, err
	}
	return &Field{Name: id}, nil
}

func (p *Parser) parseConstant() (*Constant, error) {
	switch {
	case p.lexer.matchStringConstant():
		str, err := p.lexer.eatStringConstant()
		if err != nil {
			return nil, err
		}
		return &Constant{Type: StringToken, StrVal: str}, nil
	case p.lexer.matchIntConstant():
		i, err := p.lexer.eatIntConstant()
		if err != nil {
			return nil, err
		}
		return &Constant{Type: IntegerToken, IntVal: i}, nil
	case p.lexer.matchFloatConstant():
		f, err := p.lexer.eatFloatConstant()
		if err != nil {
			return nil, err
		}
		return &Constant{Type: FloatToken, FloatVal: f}, nil
	default:
		return nil, errors.New("invalid syntax")
	}
}

func (p *Parser) parseExpression() (*Expression, error) {
	f, err := p.parseField()
	if err == nil {
		return &Expression{IsField: true, Field: f}, nil
	}

	c, err := p.parseConstant()
	if err == nil {
		return &Expression{IsField: false, Constant: c}, nil
	}

	return nil, errors.New("invalid syntax")
}

func (p *Parser) parseComparisonOperator() (string, error) {
	switch {
	case p.lexer.matchOperator("="):
		if err := p.lexer.eatOperator("="); err != nil {
			return "", err
		}
		return "=", nil
	case p.lexer.matchOperator("<"):
		if err := p.lexer.eatOperator("<"); err != nil {
			return "", err
		}
		return "<", nil
	case p.lexer.matchOperator(">"):
		if err := p.lexer.eatOperator(">"); err != nil {
			return "", err
		}
		return ">", nil
	case p.lexer.matchOperator("<="):
		if err := p.lexer.eatOperator("<="); err != nil {
			return "", err
		}
		return "<=", nil
	case p.lexer.matchOperator(">="):
		if err := p.lexer.eatOperator(">="); err != nil {
			return "", err
		}
		return ">=", nil
	case p.lexer.matchOperator("!="):
		if err := p.lexer.eatOperator("!="); err != nil {
			return "", err
		}
		return "!=", nil
	case p.lexer.matchOperator("<>"):
		if err := p.lexer.eatOperator("<>"); err != nil {
			return "", err
		}
		return "<>", nil
	default:
		return "", errors.New("invalid syntax")
	}
}

func (p *Parser) parseTerm() (*Term, error) {
	lhs, err := p.parseExpression()
	if err != nil {
		return nil, err
	}

	op, err := p.parseComparisonOperator()
	if err != nil {
		return nil, err
	}

	rhs, err := p.parseExpression()
	if err != nil {
		return nil, err
	}
	return &Term{
		Lhs: lhs,
		Op:  op,
		Rhs: rhs,
	}, nil
}

func (p *Parser) parsePredicate() (*Predicate, error) {
	term, err := p.parseTerm()
	if err != nil {
		return nil, err
	}

	if p.lexer.matchKeyword("AND") {
		if err = p.lexer.eatKeyword("AND"); err != nil {
			return nil, err
		}
		next, err := p.parsePredicate()
		if err != nil {
			return nil, err
		}
		return &Predicate{
			Term: term,
			Op:   "AND",
			Next: next,
		}, nil
	}

	if p.lexer.matchKeyword("OR") {
		if err = p.lexer.eatKeyword("OR"); err != nil {
			return nil, err
		}
		next, err := p.parsePredicate()
		if err != nil {
			return nil, err
		}
		return &Predicate{
			Term: term,
			Op:   "OR",
			Next: next,
		}, nil
	}

	return &Predicate{
		Term: term,
	}, nil
}

func (p *Parser) parseFieldList() (*FieldList, error) {
	fields := make([]*Field, 0)

	f, err := p.parseField()
	if err != nil {
		return nil, err
	}
	fields = append(fields, f)

	if p.lexer.matchDelim(',') {
		if err = p.lexer.eatDelim(','); err != nil {
			return nil, err
		}
		fl, err := p.parseFieldList() // TODO: Can this be done using a loop instead of recursion?
		if err != nil {
			return nil, err
		}
		fields = append(fields, fl.Fields...)
	}

	return &FieldList{Fields: fields}, nil
}

func (p *Parser) parseQuery() (*Query, error) {
	if !p.lexer.matchKeyword("SELECT") {
		return nil, errors.New("invalid syntax")
	}
	if err := p.lexer.eatKeyword("SELECT"); err != nil {
		return nil, err
	}

	var selectList *FieldList
	if p.lexer.matchOperator("*") { // SELECT *
		if err := p.lexer.eatOperator("*"); err != nil {
			return nil, err
		}
		selectList = &FieldList{Fields: []*Field{{Name: "*"}}}
	} else { // SELECT <FieldList>
		sl, err := p.parseFieldList()
		if err != nil {
			return nil, err
		}
		selectList = sl
	}

	if !p.lexer.matchKeyword("FROM") {
		return nil, errors.New("invalid syntax")
	}
	if err := p.lexer.eatKeyword("FROM"); err != nil {
		return nil, err
	}

	tableList, err := p.parseFieldList()
	if err != nil {
		return nil, err
	}

	if p.lexer.matchKeyword("WHERE") {
		if err := p.lexer.eatKeyword("WHERE"); err != nil {
			return nil, err
		}
		predicate, err := p.parsePredicate()
		if err != nil {
			return nil, err
		}
		return &Query{
			SelectList: selectList,
			TableList:  tableList,
			Predicate:  predicate,
		}, nil
	}

	return &Query{
		SelectList: selectList,
		TableList:  tableList,
	}, nil
}

func (p *Parser) parseConstList() (*ConstList, error) {
	consts := make([]*Constant, 0)

	c, err := p.parseConstant()
	if err != nil {
		return nil, err
	}
	consts = append(consts, c)

	if p.lexer.matchDelim(',') {
		if err = p.lexer.eatDelim(','); err != nil {
			return nil, err
		}
		cl, err := p.parseConstList() // TODO: Can this be done using a loop instead of recursion?
		if err != nil {
			return nil, err
		}
		consts = append(consts, cl.Constants...)
	}

	return &ConstList{Constants: consts}, nil
}

func (p *Parser) parseTypeDef() (*TypeDef, error) {
	switch {
	case p.lexer.matchKeyword("SMALLINT"):
		if err := p.lexer.eatKeyword("SMALLINT"); err != nil {
			return nil, err
		}
		return &TypeDef{Type: "SMALLINT"}, nil
	case p.lexer.matchKeyword("INT"):
		if err := p.lexer.eatKeyword("INT"); err != nil {
			return nil, err
		}
		return &TypeDef{Type: "INT"}, nil
	case p.lexer.matchKeyword("BIGINT"):
		if err := p.lexer.eatKeyword("BIGINT"); err != nil {
			return nil, err
		}
		return &TypeDef{Type: "BIGINT"}, nil
	case p.lexer.matchKeyword("FLOAT"):
		if err := p.lexer.eatKeyword("FLOAT"); err != nil {
			return nil, err
		}
		return &TypeDef{Type: "FLOAT"}, nil
	case p.lexer.matchKeyword("DOUBLE"):
		if err := p.lexer.eatKeyword("DOUBLE"); err != nil {
			return nil, err
		}
		return &TypeDef{Type: "DOUBLE"}, nil
	case p.lexer.matchKeyword("CHAR") || p.lexer.matchKeyword("VARCHAR"):
		var kw string
		if p.lexer.matchKeyword("CHAR") {
			kw = "CHAR"
		} else {
			kw = "VARCHAR"
		}

		if err := p.lexer.eatKeyword(kw); err != nil {
			return nil, err
		}

		if !p.lexer.matchDelim('(') {
			return nil, errors.New("invalid syntax")
		}
		if err := p.lexer.eatDelim('('); err != nil {
			return nil, err
		}

		if !p.lexer.matchIntConstant() {
			return nil, errors.New("invalid syntax")
		}
		size, err := p.lexer.eatIntConstant()
		if err != nil {
			return nil, err
		}

		if !p.lexer.matchDelim(')') {
			return nil, errors.New("invalid syntax")
		}
		if err := p.lexer.eatDelim(')'); err != nil {
			return nil, err
		}

		return &TypeDef{Type: kw, Size: size}, nil
	default:
		return nil, errors.New("invalid syntax")
	}
}

func (p *Parser) parseFieldDef() (*FieldDef, error) {
	if !p.lexer.matchIdentifier() {
		return nil, errors.New("invalid syntax")
	}
	id, err := p.lexer.eatIdentifier()
	if err != nil {
		return nil, err
	}

	typeDef, err := p.parseTypeDef()
	if err != nil {
		return nil, err
	}

	return &FieldDef{
		Name:    id,
		TypeDef: typeDef,
	}, nil
}

func (p *Parser) parseFieldDefs() (*FieldDefs, error) {
	defs := make([]*FieldDef, 0)

	d, err := p.parseFieldDef()
	if err != nil {
		return nil, err
	}
	defs = append(defs, d)

	if p.lexer.matchDelim(',') {
		if err = p.lexer.eatDelim(','); err != nil {
			return nil, err
		}
		fds, err := p.parseFieldDefs() // TODO: Can this be done using a loop instead of recursion?
		if err != nil {
			return nil, err
		}
		defs = append(defs, fds.FieldDefs...)
	}

	return &FieldDefs{FieldDefs: defs}, nil
}

func (p *Parser) parseCreateTable() (*CreateTableData, error) {
	if !p.lexer.matchKeyword("CREATE") {
		return nil, errors.New("invalid syntax")
	}
	if err := p.lexer.eatKeyword("CREATE"); err != nil {
		return nil, err
	}

	if !p.lexer.matchKeyword("TABLE") {
		return nil, errors.New("invalid syntax")
	}
	if err := p.lexer.eatKeyword("TABLE"); err != nil {
		return nil, err
	}

	if !p.lexer.matchIdentifier() {
		return nil, errors.New("invalid syntax")
	}
	tableName, err := p.lexer.eatIdentifier()
	if err != nil {
		return nil, err
	}

	if !p.lexer.matchDelim('(') {
		return nil, errors.New("invalid syntax")
	}
	if err := p.lexer.eatDelim('('); err != nil {
		return nil, err
	}

	fieldDefs, err := p.parseFieldDefs()
	if err != nil {
		return nil, err
	}

	if !p.lexer.matchDelim(')') {
		return nil, errors.New("invalid syntax")
	}
	if err := p.lexer.eatDelim(')'); err != nil {
		return nil, err
	}

	return &CreateTableData{
		TableName: tableName,
		FieldDefs: fieldDefs,
	}, nil
}

func (p *Parser) parseInsert() (*InsertData, error) {
	if !p.lexer.matchKeyword("INSERT") {
		return nil, errors.New("invalid syntax")
	}
	if err := p.lexer.eatKeyword("INSERT"); err != nil {
		return nil, err
	}

	if !p.lexer.matchKeyword("INTO") {
		return nil, errors.New("invalid syntax")
	}
	if err := p.lexer.eatKeyword("INTO"); err != nil {
		return nil, err
	}

	if !p.lexer.matchIdentifier() {
		return nil, errors.New("invalid syntax")
	}
	tableName, err := p.lexer.eatIdentifier()
	if err != nil {
		return nil, err
	}

	if !p.lexer.matchDelim('(') {
		return nil, errors.New("invalid syntax")
	}
	if err := p.lexer.eatDelim('('); err != nil {
		return nil, err
	}

	fieldList, err := p.parseFieldList()
	if err != nil {
		return nil, err
	}

	if !p.lexer.matchDelim(')') {
		return nil, errors.New("invalid syntax")
	}
	if err := p.lexer.eatDelim(')'); err != nil {
		return nil, err
	}

	if !p.lexer.matchKeyword("VALUES") {
		return nil, errors.New("invalid syntax")
	}
	if err := p.lexer.eatKeyword("VALUES"); err != nil {
		return nil, err
	}

	if !p.lexer.matchDelim('(') {
		return nil, errors.New("invalid syntax")
	}
	if err := p.lexer.eatDelim('('); err != nil {
		return nil, err
	}

	constList, err := p.parseConstList()
	if err != nil {
		return nil, err
	}

	if !p.lexer.matchDelim(')') {
		return nil, errors.New("invalid syntax")
	}
	if err := p.lexer.eatDelim(')'); err != nil {
		return nil, err
	}

	return &InsertData{
		TableName: tableName,
		Fields:    fieldList,
		Values:    constList,
	}, nil
}

func (p *Parser) parseDelete() (*DeleteData, error) {
	if !p.lexer.matchKeyword("DELETE") {
		return nil, errors.New("invalid syntax")
	}
	if err := p.lexer.eatKeyword("DELETE"); err != nil {
		return nil, err
	}

	if !p.lexer.matchKeyword("FROM") {
		return nil, errors.New("invalid syntax")
	}
	if err := p.lexer.eatKeyword("FROM"); err != nil {
		return nil, err
	}

	if !p.lexer.matchIdentifier() {
		return nil, errors.New("invalid syntax")
	}
	tableName, err := p.lexer.eatIdentifier()
	if err != nil {
		return nil, err
	}

	var predicate *Predicate
	if p.lexer.matchKeyword("WHERE") {
		if err := p.lexer.eatKeyword("WHERE"); err != nil {
			return nil, err
		}
		pred, err := p.parsePredicate()
		if err != nil {
			return nil, err
		}
		predicate = pred
	}

	return &DeleteData{
		TableName: tableName,
		Predicate: predicate,
	}, nil
}

func (p *Parser) parseUpdate() (*UpdateData, error) {
	if !p.lexer.matchKeyword("UPDATE") {
		return nil, errors.New("invalid syntax")
	}
	if err := p.lexer.eatKeyword("UPDATE"); err != nil {
		return nil, err
	}

	if !p.lexer.matchIdentifier() {
		return nil, errors.New("invalid syntax")
	}
	tableName, err := p.lexer.eatIdentifier()
	if err != nil {
		return nil, err
	}

	if !p.lexer.matchKeyword("SET") {
		return nil, errors.New("invalid syntax")
	}
	if err := p.lexer.eatKeyword("SET"); err != nil {
		return nil, err
	}

	field, err := p.parseField()
	if err != nil {
		return nil, err
	}

	if !p.lexer.matchOperator("=") {
		return nil, errors.New("invalid syntax")
	}
	if err := p.lexer.eatOperator("="); err != nil {
		return nil, err
	}

	expression, err := p.parseExpression()
	if err != nil {
		return nil, err
	}

	var predicate *Predicate
	if p.lexer.matchKeyword("WHERE") {
		if err := p.lexer.eatKeyword("WHERE"); err != nil {
			return nil, err
		}
		pred, err := p.parsePredicate()
		if err != nil {
			return nil, err
		}
		predicate = pred
	}

	return &UpdateData{
		TableName:  tableName,
		Field:      field,
		Expression: expression,
		Predicate:  predicate,
	}, nil
}

func (p *Parser) Parse() (*ParseTree, error) {
	switch {
	case p.lexer.matchKeyword("SELECT"):
		q, err := p.parseQuery()
		if err != nil {
			return nil, err
		}
		return &ParseTree{Type: SelectTree, Query: q}, nil
	case p.lexer.matchKeyword("CREATE"):
		data, err := p.parseCreateTable()
		if err != nil {
			return nil, err
		}
		return &ParseTree{Type: CreateTableTree, CreateTableData: data}, nil
	case p.lexer.matchKeyword("INSERT"):
		data, err := p.parseInsert()
		if err != nil {
			return nil, err
		}
		return &ParseTree{Type: InsertTree, InsertData: data}, nil
	case p.lexer.matchKeyword("UPDATE"):
		data, err := p.parseUpdate()
		if err != nil {
			return nil, err
		}
		return &ParseTree{Type: UpdateTree, UpdateData: data}, nil
	case p.lexer.matchKeyword("DELETE"):
		data, err := p.parseDelete()
		if err != nil {
			return nil, err
		}
		return &ParseTree{Type: DeleteTree, DeleteData: data}, nil
	default:
		return nil, errors.New("invalid syntax")
	}
}

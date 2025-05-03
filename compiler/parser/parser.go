package parser

import "errors"

/* Grammar:
<Field> := IdTok
<Constant> := StrTok | IntTok | FloatTok
<Expression> := <Field> | <Constant>
<Term> := <Expression> = <Expression>
<Predicate> := <Term> [ AND <Predicate> ]

<Query> := SELECT <SelectList> FROM <TableList> [ WHERE <Predicate> ]
<SelectList> := <Field> [ , <SelectList> ]
<TableList> := IdTok [ , <TableList> ]

<UpdateCmd> := <Insert> | <Delete> | <Modify> | <Create>
<Create> := <CreateTable> | <CreateView> | <CreateIndex>

<Insert> := INSERT INTO IdTok ( <FieldList> ) VALUES ( <ConstList> )
<FieldList> := <Field> [ , <FieldList> ]
<ConstList> := <Constant> [ , <ConstList> ]

<Delete> := DELETE FROM IdTok [ WHERE <Predicate> ]

<Modify> := UPDATE IdTok SET <Field> = <Expression> [ WHERE <Predicate> ]

<CreateTable> := CREATE TABLE IdTok ( <FieldDefs> )
<FieldDefs> := <FieldDef> [ , <FieldDefs> ]
<FieldDef> := IdTok <TypeDef>
<TypeDef> := INT | BIGINT | SMALLINT | FLOAT | DOUBLE | CHAR ( IntTok ) | VARCHAR ( IntTok )
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

// <Query> := SELECT <SelectList> FROM <TableList> [ WHERE <Predicate> ]
type Query struct {
	SelectList *FieldList
	TableList  *FieldList
	Predicate  *Predicate
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
		return &Expression{Field: f}, nil
	}

	c, err := p.parseConstant()
	if err == nil {
		return &Expression{Constant: c}, nil
	}

	return nil, errors.New("invalid syntax")
}

func (p *Parser) parseTerm() (*Term, error) {
	lhs, err := p.parseExpression()
	if err != nil {
		return nil, err
	}

	var op string
	switch {
	case p.lexer.matchOperator("="):
		if err = p.lexer.eatOperator("="); err != nil {
			return nil, err
		}
		op = "="
	case p.lexer.matchOperator("<"):
		if err = p.lexer.eatOperator("<"); err != nil {
			return nil, err
		}
		op = "<"
	case p.lexer.matchOperator(">"):
		if err = p.lexer.eatOperator(">"); err != nil {
			return nil, err
		}
		op = ">"
	case p.lexer.matchOperator("<="):
		if err = p.lexer.eatOperator("<="); err != nil {
			return nil, err
		}
		op = "<="
	case p.lexer.matchOperator(">="):
		if err = p.lexer.eatOperator(">="); err != nil {
			return nil, err
		}
		op = ">="
	case p.lexer.matchOperator("!="):
		if err = p.lexer.eatOperator("!="); err != nil {
			return nil, err
		}
		op = "!="
	case p.lexer.matchOperator("<>"):
		if err = p.lexer.eatOperator("<>"); err != nil {
			return nil, err
		}
		op = "<>"
	default:
		return nil, errors.New("invalid syntax")
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

	field, err := p.parseField()
	if err != nil {
		return nil, err
	}
	fields = append(fields, field)

	if p.lexer.matchDelim(',') {
		if err = p.lexer.eatDelim(','); err != nil {
			return nil, err
		}
		selectList, err := p.parseFieldList() // TODO: Can this be done using a loop instead of recursion?
		if err != nil {
			return nil, err
		}
		fields = append(fields, selectList.Fields...)
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

func (p *Parser) Parse() (*Query, error) {
	if p.lexer.matchKeyword("SELECT") {
		return p.parseQuery()
	}

	return nil, errors.New("invalid syntax")
}

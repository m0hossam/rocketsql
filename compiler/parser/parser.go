package parser

import (
	"errors"
)

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

	if p.lexer.matchKeyword("AND") || p.lexer.matchKeyword("OR") {
		var kw string
		if p.lexer.matchKeyword("AND") {
			kw = "AND"
		} else {
			kw = "OR"
		}

		if err = p.lexer.eatKeyword(kw); err != nil {
			return nil, err
		}

		next, err := p.parsePredicate()
		if err != nil {
			return nil, err
		}

		return &Predicate{
			Term: term,
			Op:   kw,
			Next: next,
		}, nil
	}

	return &Predicate{
		Term: term,
	}, nil
}

func (p *Parser) parseFieldList() ([]*Field, error) {
	fields := make([]*Field, 0)

	f, err := p.parseField()
	if err != nil {
		return nil, err
	}
	fields = append(fields, f)

	for p.lexer.matchDelim(',') {
		if err = p.lexer.eatDelim(','); err != nil {
			return nil, err
		}
		f, err := p.parseField()
		if err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}

	return fields, nil
}

func (p *Parser) parseConstList() ([]*Constant, error) {
	consts := make([]*Constant, 0)

	c, err := p.parseConstant()
	if err != nil {
		return nil, err
	}
	consts = append(consts, c)

	for p.lexer.matchDelim(',') {
		if err = p.lexer.eatDelim(','); err != nil {
			return nil, err
		}
		c, err := p.parseConstant()
		if err != nil {
			return nil, err
		}
		consts = append(consts, c)
	}

	return consts, nil
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

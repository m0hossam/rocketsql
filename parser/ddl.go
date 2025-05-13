package parser

import (
	"errors"
	"strconv"
	"strings"
)

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

// <CreateTable> := CREATE TABLE IdTok ( <FieldDefs> )
type CreateTableData struct {
	TableName string
	FieldDefs []*FieldDef
	SchemaSql string
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

		if err := p.lexer.eatDelim('('); err != nil {
			return nil, err
		}

		size, err := p.lexer.eatIntConstant()
		if err != nil {
			return nil, err
		}

		if err := p.lexer.eatDelim(')'); err != nil {
			return nil, err
		}

		return &TypeDef{Type: kw, Size: int(size)}, nil
	default:
		return nil, errors.New("invalid syntax")
	}
}

func (p *Parser) parseFieldDef() (*FieldDef, error) {
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

func (p *Parser) parseFieldDefs() ([]*FieldDef, error) {
	fds := make([]*FieldDef, 0)

	fd, err := p.parseFieldDef()
	if err != nil {
		return nil, err
	}
	fds = append(fds, fd)

	for p.lexer.matchDelim(',') {
		if err = p.lexer.eatDelim(','); err != nil {
			return nil, err
		}

		fd, err := p.parseFieldDef()
		if err != nil {
			return nil, err
		}
		fds = append(fds, fd)
	}

	return fds, nil
}

func (p *Parser) parseCreateTable() (*CreateTableData, error) {
	if err := p.lexer.eatKeyword("CREATE"); err != nil {
		return nil, err
	}

	if err := p.lexer.eatKeyword("TABLE"); err != nil {
		return nil, err
	}

	tableName, err := p.lexer.eatIdentifier()
	if err != nil {
		return nil, err
	}

	if err := p.lexer.eatDelim('('); err != nil {
		return nil, err
	}

	fieldDefs, err := p.parseFieldDefs()
	if err != nil {
		return nil, err
	}

	if err := p.lexer.eatDelim(')'); err != nil {
		return nil, err
	}

	return &CreateTableData{
		TableName: tableName,
		FieldDefs: fieldDefs,
		SchemaSql: getSqlSchema(tableName, fieldDefs),
	}, nil
}

func getSqlSchema(tblName string, fieldDefs []*FieldDef) string {
	var b strings.Builder

	b.WriteString("CREATE TABLE ")
	b.WriteString(tblName)
	b.WriteString(" (")
	for i, fieldDef := range fieldDefs {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fieldDef.Name)
		b.WriteString(" ")
		b.WriteString(fieldDef.TypeDef.Type)
		if fieldDef.TypeDef.Size > 0 {
			b.WriteString("(")
			b.WriteString(strconv.Itoa(fieldDef.TypeDef.Size))
			b.WriteString(")")
		}
	}
	b.WriteString(")")

	return b.String()
}

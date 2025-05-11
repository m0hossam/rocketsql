package parser

// <Insert> := INSERT INTO IdTok [ ( <FieldList> ) ] VALUES ( <ConstList> )
type InsertData struct {
	TableName string
	Fields    []*Field // optional
	Values    []*Constant
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

func (p *Parser) parseInsert() (*InsertData, error) {
	if err := p.lexer.eatKeyword("INSERT"); err != nil {
		return nil, err
	}

	if err := p.lexer.eatKeyword("INTO"); err != nil {
		return nil, err
	}

	tableName, err := p.lexer.eatIdentifier()
	if err != nil {
		return nil, err
	}

	var fieldList []*Field // optional field list
	if p.lexer.matchDelim('(') {
		if err := p.lexer.eatDelim('('); err != nil {
			return nil, err
		}

		fieldList, err = p.parseFieldList()
		if err != nil {
			return nil, err
		}

		if err := p.lexer.eatDelim(')'); err != nil {
			return nil, err
		}
	}

	if err := p.lexer.eatKeyword("VALUES"); err != nil {
		return nil, err
	}

	if err := p.lexer.eatDelim('('); err != nil {
		return nil, err
	}

	constList, err := p.parseConstList()
	if err != nil {
		return nil, err
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
	if err := p.lexer.eatKeyword("DELETE"); err != nil {
		return nil, err
	}

	if err := p.lexer.eatKeyword("FROM"); err != nil {
		return nil, err
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
	if err := p.lexer.eatKeyword("UPDATE"); err != nil {
		return nil, err
	}

	tableName, err := p.lexer.eatIdentifier()
	if err != nil {
		return nil, err
	}

	if err := p.lexer.eatKeyword("SET"); err != nil {
		return nil, err
	}

	field, err := p.parseField()
	if err != nil {
		return nil, err
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

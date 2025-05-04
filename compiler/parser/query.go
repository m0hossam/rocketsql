package parser

// <Query> := SELECT <SelectList> FROM <TableList> [ WHERE <Predicate> ]
type Query struct {
	SelectList []*Field
	TableList  []*Field
	Predicate  *Predicate
}

func (p *Parser) parseQuery() (*Query, error) {
	if err := p.lexer.eatKeyword("SELECT"); err != nil {
		return nil, err
	}

	var selectList []*Field
	if p.lexer.matchOperator("*") { // SELECT *
		if err := p.lexer.eatOperator("*"); err != nil {
			return nil, err
		}
		selectList = []*Field{{Name: "*"}}
	} else { // SELECT <FieldList>
		sl, err := p.parseFieldList()
		if err != nil {
			return nil, err
		}
		selectList = sl
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

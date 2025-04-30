package parser

type Parser struct {
	lexer *lexer
}

func NewParser(sql string) *Parser {
	return &Parser{
		lexer: newLexer(sql),
	}
}

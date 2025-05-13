package parser

import (
	"errors"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

type TokenType int

const (
	KeywordToken TokenType = iota
	IdentifierToken
	DelimiterToken
	IntegerToken
	FloatToken
	StringToken
	OperatorToken
	InvalidToken
	EndToken
)

type token struct {
	tType     TokenType
	intVal    int64
	floatVal  float64
	stringVal string
	runeVal   rune
}

type lexer struct {
	stream   string
	pos      int
	curToken token
	keywords map[string]struct{}
}

func newLexer(inputStr string) *lexer {
	l := &lexer{stream: inputStr, pos: 0}
	l.initKeywords()
	l.nextToken()
	return l
}

func (l *lexer) initKeywords() {
	kws := []string{
		"CREATE", "TABLE",
		"INSERT", "INTO", "VALUES",
		"SELECT", "FROM", "WHERE",
		"UPDATE", "SET",
		"DELETE", "DROP", "TRUNCATE",
		"NULL", "SMALLINT", "INT", "BIGINT",
		"FLOAT", "DOUBLE", "CHAR", "VARCHAR",
		"AND", "OR", "NOT",
	}
	l.keywords = make(map[string]struct{}, len(kws))
	for _, kw := range kws {
		l.keywords[kw] = struct{}{}
	}
}

func isDelimiter(r rune) bool {
	return r == '(' || r == ')' || r == ',' || r == ';'
}

func (l *lexer) getOperatorToken() token {
	start := l.pos
	r, sz := utf8.DecodeRuneInString(l.stream[l.pos:])

	if r == '=' || r == '+' || r == '-' || r == '*' || r == '/' || r == '%' {
		l.pos += sz
		return token{tType: OperatorToken, stringVal: l.stream[start:l.pos]}
	}

	if r == '<' || r == '>' || r == '!' {
		l.pos += sz
		rprev := r
		if l.pos < len(l.stream) {
			r, sz = utf8.DecodeRuneInString(l.stream[l.pos:])
			if (rprev == '<' && (r == '=' || r == '>')) ||
				(rprev == '>' && r == '=') ||
				(rprev == '!' && r == '=') {
				l.pos += sz
			}
		}
		return token{tType: OperatorToken, stringVal: l.stream[start:l.pos]}
	}

	return token{tType: InvalidToken}
}

func (l *lexer) getStringToken() token {
	hasEndQuote := false
	start := l.pos
	l.pos++ // Starting quote

	for l.pos < len(l.stream) {
		r, sz := utf8.DecodeRuneInString(l.stream[l.pos:])
		l.pos += sz
		if r == '\'' {
			hasEndQuote = true
			break
		}
	}

	if !hasEndQuote {
		return token{tType: InvalidToken}
	}

	return token{tType: StringToken, stringVal: l.stream[start+1 : l.pos-1]} // Exclude start/end quotes
}

func (l *lexer) getWordToken() token {
	start := l.pos

	for l.pos < len(l.stream) {
		r, sz := utf8.DecodeRuneInString(l.stream[l.pos:])
		if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_') {
			break
		}
		l.pos += sz
	}

	word := l.stream[start:l.pos]

	if _, isKeyword := l.keywords[strings.ToUpper(word)]; isKeyword {
		return token{tType: KeywordToken, stringVal: strings.ToUpper(word)}
	}

	return token{tType: IdentifierToken, stringVal: word}
}

func (l *lexer) getNumberToken() token {
	start := l.pos
	pos := l.pos
	hasDot := false
	hasDigit := false

	r, sz := utf8.DecodeRuneInString(l.stream[pos:])
	if r == '-' || r == '+' { // Skip sign
		pos += sz
	}

	for pos < len(l.stream) {
		r, sz = utf8.DecodeRuneInString(l.stream[pos:])
		if unicode.IsDigit(r) {
			hasDigit = true
			pos += sz
		} else if r == '.' && !hasDot { // Must have only one decimal point
			hasDot = true
			pos += sz
		} else {
			break
		}
	}

	if !hasDigit {
		return token{tType: InvalidToken}
	}

	numStr := l.stream[start:pos]
	l.pos = pos

	if hasDot {
		floatVal, err := strconv.ParseFloat(numStr, 64)
		if err != nil {
			return token{tType: InvalidToken}
		}
		return token{tType: FloatToken, floatVal: floatVal}
	}

	intVal, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return token{tType: InvalidToken}
	}
	return token{tType: IntegerToken, intVal: intVal}
}

func (l *lexer) skipWhitespace() {
	for l.pos < len(l.stream) {
		r, sz := utf8.DecodeRuneInString(l.stream[l.pos:])
		if !unicode.IsSpace(r) {
			break
		}
		l.pos += sz
	}
}

func (l *lexer) nextToken() error {
	l.skipWhitespace()

	if l.pos >= len(l.stream) {
		l.curToken = token{tType: EndToken}
		return nil
	}

	r, _ := utf8.DecodeRuneInString(l.stream[l.pos:])
	if r == utf8.RuneError {
		return errors.New("inalid syntax")
	}
	newToken := token{tType: InvalidToken}

	switch {
	case isDelimiter(r): // Delimiters
		newToken = token{tType: DelimiterToken, runeVal: r}
		l.pos++
	case unicode.IsLetter(r): // Keywords and Identifiers
		newToken = l.getWordToken()
	case r == '\'': // String literals
		newToken = l.getStringToken()
	case unicode.IsDigit(r) || r == '-' || r == '+' || r == '.': // Int and Float constants
		newToken = l.getNumberToken()
		fallthrough // Starting with '+' or '-' could be an operator
	default:
		if newToken.tType == InvalidToken {
			newToken = l.getOperatorToken()
		}
	}

	if newToken.tType == InvalidToken {
		return errors.New("invalid syntax")
	}

	l.curToken = newToken
	return nil
}

// Interface to be used by the Parser

// Matching methods:

func (l *lexer) matchDelim(d rune) bool {
	return l.curToken.tType == DelimiterToken && l.curToken.runeVal == d
}
func (l *lexer) matchIntConstant() bool {
	return l.curToken.tType == IntegerToken
}

func (l *lexer) matchFloatConstant() bool {
	return l.curToken.tType == FloatToken
}

func (l *lexer) matchStringConstant() bool {
	return l.curToken.tType == StringToken
}

func (l *lexer) matchOperator(op string) bool {
	return l.curToken.tType == OperatorToken && l.curToken.stringVal == op
}

func (l *lexer) matchKeyword(kw string) bool {
	return l.curToken.tType == KeywordToken && l.curToken.stringVal == strings.ToUpper(kw)
}

func (l *lexer) matchIdentifier() bool {
	_, isKeyword := l.keywords[l.curToken.stringVal]
	return l.curToken.tType == IdentifierToken && !isKeyword
}

// Consuming methods:

func (l *lexer) eatDelim(d rune) error {
	if !l.matchDelim(d) {
		return errors.New("invalid syntax")
	}
	return l.nextToken()
}

func (l *lexer) eatIntConstant() (int64, error) {
	if !l.matchIntConstant() {
		return 0, errors.New("invalid syntax")
	}
	i := l.curToken.intVal
	return i, l.nextToken()
}

func (l *lexer) eatFloatConstant() (float64, error) {
	if !l.matchFloatConstant() {
		return 0, errors.New("invalid syntax")
	}
	f := l.curToken.floatVal
	return f, l.nextToken()
}

func (l *lexer) eatStringConstant() (string, error) {
	if !l.matchStringConstant() {
		return "", errors.New("invalid syntax")
	}
	str := l.curToken.stringVal
	return str, l.nextToken()
}

func (l *lexer) eatOperator(op string) error {
	if !l.matchOperator(op) {
		return errors.New("invalid syntax")
	}
	return l.nextToken()
}

func (l *lexer) eatKeyword(kw string) error {
	if !l.matchKeyword(kw) {
		return errors.New("invalid syntax")
	}
	return l.nextToken()
}

func (l *lexer) eatIdentifier() (string, error) {
	if !l.matchIdentifier() {
		return "", errors.New("invalid syntax")
	}
	id := l.curToken.stringVal
	return id, l.nextToken()
}

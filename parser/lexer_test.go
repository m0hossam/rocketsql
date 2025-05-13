package parser

import (
	"testing"
)

func TestLexerQuery(t *testing.T) {
	l := newLexer("SELECT * FROM employees WHERE name = 'Mohamed';")
	if err := l.eatKeyword("SELECT"); err != nil {
		t.Fatalf("Expected SELECT keyword, got error: %v", err)
	}
	if err := l.eatOperator("*"); err != nil {
		t.Fatalf("Expected '*' operator, got error: %v", err)
	}
	if err := l.eatKeyword("FROM"); err != nil {
		t.Fatalf("Expected FROM keyword, got error: %v", err)
	}
	id, err := l.eatIdentifier()
	if err != nil {
		t.Fatalf("Expected identifier, got error: %v", err)
	}
	if id != "employees" {
		t.Fatalf("Expected identifier 'employees', got '%s'", id)
	}
	if err := l.eatKeyword("WHERE"); err != nil {
		t.Fatalf("Expected WHERE keyword, got error: %v", err)
	}
	id, err = l.eatIdentifier()
	if err != nil {
		t.Fatalf("Expected identifier, got error: %v", err)
	}
	if id != "name" {
		t.Fatalf("Expected identifier 'name', got '%s'", id)
	}
	if err := l.eatOperator("="); err != nil {
		t.Fatalf("Expected '=' operator, got error: %v", err)
	}
	name, err := l.eatStringConstant()
	if err != nil {
		t.Fatalf("Expected string constant, got error: %v", err)
	}
	if name != "Mohamed" {
		t.Fatalf("Expected string constant 'Mohamed', got '%s'", name)
	}
	if err := l.eatDelim(';'); err != nil {
		t.Fatalf("Expected ';' delimiter, got error: %v", err)
	}
}

func TestLexerKeywords(t *testing.T) {
	l := newLexer("SELECT FROM WHERE")
	if !l.matchKeyword("SELECT") {
		t.Fatalf("Expected SELECT keyword")
	}
	_ = l.nextToken()
	if !l.matchKeyword("FROM") {
		t.Fatalf("Expected FROM keyword")
	}
	_ = l.nextToken()
	if !l.matchKeyword("WHERE") {
		t.Fatalf("Expected WHERE keyword")
	}
}

func TestLexerIdentifiers(t *testing.T) {
	l := newLexer("myTable123")
	if !l.matchIdentifier() {
		t.Fatalf("Expected identifier")
	}
	if val, _ := l.eatIdentifier(); val != "myTable123" {
		t.Fatalf("Expected identifier 'myTable123', got '%s'", val)
	}
}

func TestLexerDelimiters(t *testing.T) {
	l := newLexer("( , ) ;")
	if !l.matchDelim('(') {
		t.Fatalf("Expected '(' delimiter")
	}
	_ = l.nextToken()
	if !l.matchDelim(',') {
		t.Fatalf("Expected ',' delimiter")
	}
	_ = l.nextToken()
	if !l.matchDelim(')') {
		t.Fatalf("Expected ')' delimiter")
	}
	_ = l.nextToken()
	if !l.matchDelim(';') {
		t.Fatalf("Expected ';' delimiter")
	}
}

func TestLexerOperators(t *testing.T) {
	ops := []string{"=", "<", ">", "<=", ">=", "<>", "!=", "+", "-", "*", "/", "%"}
	for _, op := range ops {
		l := newLexer(op)
		if !l.matchOperator(op) {
			t.Fatalf("Expected operator '%s'", op)
		}
	}
}

func TestLexerNumbers(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{"123", 123},
		{"-456", -456},
		{"+789", 789},
	}

	for _, test := range tests {
		l := newLexer(test.input)
		if !l.matchIntConstant() {
			t.Fatalf("Expected integer constant for input '%s'", test.input)
		}
		val, _ := l.eatIntConstant()
		if int(val) != test.expected {
			t.Fatalf("Expected %d, got %d", test.expected, val)
		}
	}
}

func TestLexerFloats(t *testing.T) {
	tests := []struct {
		input    string
		expected float64
	}{
		{"3.14", 3.14},
		{"-2.5", -2.5},
		{"+0.99", 0.99},
	}

	for _, test := range tests {
		l := newLexer(test.input)
		if !l.matchFloatConstant() {
			t.Fatalf("Expected float constant for input '%s'", test.input)
		}
		val, _ := l.eatFloatConstant()
		if val != test.expected {
			t.Fatalf("Expected %f, got %f", test.expected, val)
		}
	}
}

func TestLexerString(t *testing.T) {
	l := newLexer("'hello world'")
	if !l.matchStringConstant() {
		t.Fatalf("Expected string constant")
	}
	val, _ := l.eatStringConstant()
	if val != "hello world" {
		t.Fatalf("Expected 'hello world', got '%s'", val)
	}
}

func TestLexerInvalid(t *testing.T) {
	l := newLexer("!@#")
	err := l.nextToken()
	if err == nil {
		t.Fatalf("Expected error for invalid input")
	}
}

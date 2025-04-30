package parser

import (
	"testing"
)

func TestLexerKeywords(t *testing.T) {
	l := newLexer("SELECT FROM WHERE")
	if !l.matchKeyword("SELECT") {
		t.Errorf("Expected SELECT keyword")
	}
	_ = l.nextToken()
	if !l.matchKeyword("FROM") {
		t.Errorf("Expected FROM keyword")
	}
	_ = l.nextToken()
	if !l.matchKeyword("WHERE") {
		t.Errorf("Expected WHERE keyword")
	}
}

func TestLexerIdentifiers(t *testing.T) {
	l := newLexer("myTable123")
	if !l.matchIdentifier() {
		t.Errorf("Expected identifier")
	}
	if val, _ := l.eatIdentifier(); val != "myTable123" {
		t.Errorf("Expected identifier 'myTable123', got '%s'", val)
	}
}

func TestLexerDelimiters(t *testing.T) {
	l := newLexer("( , ) ;")
	if !l.matchDelim('(') {
		t.Errorf("Expected '(' delimiter")
	}
	_ = l.nextToken()
	if !l.matchDelim(',') {
		t.Errorf("Expected ',' delimiter")
	}
	_ = l.nextToken()
	if !l.matchDelim(')') {
		t.Errorf("Expected ')' delimiter")
	}
	_ = l.nextToken()
	if !l.matchDelim(';') {
		t.Errorf("Expected ';' delimiter")
	}
}

func TestLexerOperators(t *testing.T) {
	ops := []string{"=", "<", ">", "<=", ">=", "<>", "!=", "+", "-", "*", "/", "%"}
	for _, op := range ops {
		l := newLexer(op)
		if !l.matchOperator(op) {
			t.Errorf("Expected operator '%s'", op)
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
			t.Errorf("Expected integer constant for input '%s'", test.input)
		}
		val, _ := l.eatIntConstant()
		if val != test.expected {
			t.Errorf("Expected %d, got %d", test.expected, val)
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
			t.Errorf("Expected float constant for input '%s'", test.input)
		}
		val, _ := l.eatFloatConstant()
		if val != test.expected {
			t.Errorf("Expected %f, got %f", test.expected, val)
		}
	}
}

func TestLexerString(t *testing.T) {
	l := newLexer("'hello world'")
	if !l.matchStringConstant() {
		t.Errorf("Expected string constant")
	}
	val, _ := l.eatStringConstant()
	if val != "hello world" {
		t.Errorf("Expected 'hello world', got '%s'", val)
	}
}

func TestLexerInvalid(t *testing.T) {
	l := newLexer("!@#")
	err := l.nextToken()
	if err == nil {
		t.Errorf("Expected error for invalid input")
	}
}

package parser

import (
	"strings"
	"testing"
)

func TestParserSelectStar(t *testing.T) {
	sql := "SELECT * FROM employees WHERE name = 'Mohamed'"
	p := NewParser(sql)

	q, err := p.Parse()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	var b strings.Builder

	b.WriteString("SELECT ")
	for i, field := range q.SelectList.Fields {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(field.Name)
	}

	b.WriteString(" FROM ")
	for i, field := range q.TableList.Fields {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(field.Name)
	}

	b.WriteString(" WHERE ")
	b.WriteString(q.Predicate.Term.Lhs.Field.Name)
	b.WriteString(" ")
	b.WriteString(q.Predicate.Term.Op)
	b.WriteString(" ")
	b.WriteString("'")
	b.WriteString(q.Predicate.Term.Rhs.Constant.StrVal)
	b.WriteString("'")

	if b.String() != sql {
		t.Errorf("Expected 'SELECT * FROM employees WHERE name = 'Mohamed'', got: '%s'", b.String())
	}
}

func TestParserSelect(t *testing.T) {
	sql := "SELECT name, age, dept FROM employees WHERE name = 'Mohamed'"
	p := NewParser(sql)

	q, err := p.Parse()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	var b strings.Builder

	b.WriteString("SELECT ")
	for i, field := range q.SelectList.Fields {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(field.Name)
	}

	b.WriteString(" FROM ")
	for i, field := range q.TableList.Fields {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(field.Name)
	}

	b.WriteString(" WHERE ")
	b.WriteString(q.Predicate.Term.Lhs.Field.Name)
	b.WriteString(" ")
	b.WriteString(q.Predicate.Term.Op)
	b.WriteString(" ")
	b.WriteString("'")
	b.WriteString(q.Predicate.Term.Rhs.Constant.StrVal)
	b.WriteString("'")

	if b.String() != sql {
		t.Errorf("Expected 'SELECT * FROM employees WHERE name = 'Mohamed'', got: '%s'", b.String())
	}
}

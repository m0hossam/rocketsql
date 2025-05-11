package parser

import (
	"strconv"
	"strings"
	"testing"
)

// TODO: Implement tests for invalid SQL

func TestParserSelectStar(t *testing.T) {
	sql := "SELECT * FROM employees WHERE name = 'Mohamed'"
	p := NewParser(sql)

	pt, err := p.Parse()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if pt.Type != SelectTree {
		t.Fatalf("Expected SelectTree, got: %v", pt.Type)
	}

	q := pt.Query
	if q == nil {
		t.Fatalf("Expected a query, got: nil")
	}

	var b strings.Builder

	b.WriteString("SELECT ")
	for i, field := range q.SelectList {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(field.Name)
	}

	b.WriteString(" FROM ")
	for i, field := range q.TableList {
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

	pt, err := p.Parse()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if pt.Type != SelectTree {
		t.Fatalf("Expected SelectTree, got: %v", pt.Type)
	}

	q := pt.Query
	if q == nil {
		t.Fatalf("Expected a query, got: nil")
	}

	var b strings.Builder

	b.WriteString("SELECT ")
	for i, field := range q.SelectList {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(field.Name)
	}

	b.WriteString(" FROM ")
	for i, field := range q.TableList {
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

func TestParserCreateTable(t *testing.T) {
	sql := "CREATE TABLE employees (name VARCHAR(32), age INT, dept CHAR(3))"
	p := NewParser(sql)

	pt, err := p.Parse()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if pt.Type != CreateTableTree {
		t.Fatalf("Expected CreateTableTree, got: %v", pt.Type)
	}

	data := pt.CreateTableData
	if data == nil {
		t.Fatalf("Expected a create table statement, got: nil")
	}

	var b strings.Builder

	b.WriteString("CREATE TABLE ")
	b.WriteString(data.TableName)
	b.WriteString(" (")
	for i, fieldDef := range data.FieldDefs {
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

	if b.String() != sql {
		t.Errorf("Expected 'CREATE TABLE employees (name VARCHAR(50), age INT, dept VARCHAR(50))', got: '%s'", b.String())
	}
}

func TestParserInsert(t *testing.T) {
	sql := "INSERT INTO employees (name, age, dept) VALUES ('Mohamed', 30, 'HR')"
	p := NewParser(sql)

	pt, err := p.Parse()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if pt.Type != InsertTree {
		t.Fatalf("Expected InsertTree, got: %v", pt.Type)
	}

	data := pt.InsertData
	if data == nil {
		t.Fatalf("Expected an insert statement, got: nil")
	}

	if data.Fields == nil {
		t.Fatalf("Expected fields, got: nil")
	}

	var b strings.Builder

	b.WriteString("INSERT INTO ")
	b.WriteString(data.TableName)
	b.WriteString(" (")
	for i, field := range data.Fields {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(field.Name)
	}
	b.WriteString(") VALUES (")
	for i, constant := range data.Values {
		if i > 0 {
			b.WriteString(", ")
		}
		switch constant.Type {
		case StringToken:
			b.WriteString("'")
			b.WriteString(constant.StrVal)
			b.WriteString("'")
		case IntegerToken:
			b.WriteString(strconv.Itoa(constant.IntVal))
		case FloatToken:
			b.WriteString(strconv.FormatFloat(constant.FloatVal, 'f', -1, 64))
		default:
			t.Fatalf("Unexpected value type: %T", constant.Type)
		}
	}
	b.WriteString(")")

	if b.String() != sql {
		t.Errorf("Expected 'INSERT INTO employees (name, age, dept) VALUES ('Mohamed', 30, 'HR')', got: '%s'", b.String())
	}
}

func TestParserInsertWithoutFields(t *testing.T) {
	sql := "INSERT INTO employees VALUES ('Mohamed', 30, 'HR')"
	p := NewParser(sql)

	pt, err := p.Parse()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if pt.Type != InsertTree {
		t.Fatalf("Expected InsertTree, got: %v", pt.Type)
	}

	data := pt.InsertData
	if data == nil {
		t.Fatalf("Expected an insert statement, got: nil")
	}

	if data.Fields != nil {
		t.Fatalf("Expected nil, got: %v", data.Fields)
	}

	var b strings.Builder

	b.WriteString("INSERT INTO ")
	b.WriteString(data.TableName)
	b.WriteString(" VALUES (")
	for i, constant := range data.Values {
		if i > 0 {
			b.WriteString(", ")
		}
		switch constant.Type {
		case StringToken:
			b.WriteString("'")
			b.WriteString(constant.StrVal)
			b.WriteString("'")
		case IntegerToken:
			b.WriteString(strconv.Itoa(constant.IntVal))
		case FloatToken:
			b.WriteString(strconv.FormatFloat(constant.FloatVal, 'f', -1, 64))
		default:
			t.Fatalf("Unexpected value type: %T", constant.Type)
		}
	}
	b.WriteString(")")

	if b.String() != sql {
		t.Errorf("Expected 'INSERT INTO employees VALUES ('Mohamed', 30, 'HR')', got: '%s'", b.String())
	}
}

func TestParserDelete(t *testing.T) {
	sql := "DELETE FROM employees WHERE name = 'Mohamed'"
	p := NewParser(sql)

	pt, err := p.Parse()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if pt.Type != DeleteTree {
		t.Fatalf("Expected DeleteTree, got: %v", pt.Type)
	}

	data := pt.DeleteData
	if data == nil {
		t.Fatalf("Expected a delete statement, got: nil")
	}

	var b strings.Builder

	b.WriteString("DELETE FROM ")
	b.WriteString(data.TableName)
	b.WriteString(" WHERE ")
	b.WriteString(data.Predicate.Term.Lhs.Field.Name)
	b.WriteString(" ")
	b.WriteString(data.Predicate.Term.Op)
	b.WriteString(" ")
	b.WriteString("'")
	b.WriteString(data.Predicate.Term.Rhs.Constant.StrVal)
	b.WriteString("'")

	if b.String() != sql {
		t.Errorf("Expected 'DELETE FROM employees WHERE name = 'Mohamed'', got: '%s'", b.String())
	}
}

func TestParserUpdate(t *testing.T) {
	sql := "UPDATE employees SET age = 31 WHERE name = 'Mohamed'"
	p := NewParser(sql)

	pt, err := p.Parse()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if pt.Type != UpdateTree {
		t.Fatalf("Expected UpdateTree, got: %v", pt.Type)
	}

	data := pt.UpdateData
	if data == nil {
		t.Fatalf("Expected a update statement, got: nil")
	}

	var b strings.Builder

	b.WriteString("UPDATE ")
	b.WriteString(data.TableName)
	b.WriteString(" SET ")
	b.WriteString(data.Field.Name)
	b.WriteString(" = ")
	if data.Expression.IsField {
		b.WriteString(data.Expression.Field.Name)
	} else {
		switch data.Expression.Constant.Type {
		case StringToken:
			b.WriteString("'")
			b.WriteString(data.Expression.Constant.StrVal)
			b.WriteString("'")
		case IntegerToken:
			b.WriteString(strconv.Itoa(data.Expression.Constant.IntVal))
		case FloatToken:
			b.WriteString(strconv.FormatFloat(data.Expression.Constant.FloatVal, 'f', -1, 64))
		default:
			t.Fatalf("Unexpected value type: %T", data.Expression.Constant.Type)
		}
	}
	b.WriteString(" WHERE ")
	b.WriteString(data.Predicate.Term.Lhs.Field.Name)
	b.WriteString(" ")
	b.WriteString(data.Predicate.Term.Op)
	b.WriteString(" ")
	b.WriteString("'")
	b.WriteString(data.Predicate.Term.Rhs.Constant.StrVal)
	b.WriteString("'")

	if b.String() != sql {
		t.Errorf("Expected 'UPDATE employees SET age = 31 WHERE name = 'Mohamed'', got: '%s'", b.String())
	}
}

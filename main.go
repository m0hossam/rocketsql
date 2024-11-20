package main

import (
	"fmt"
)

func main() {
	err := createDB("db.rocketsql")
	if err != nil {
		return
	}

	_, err = getFirstFreePagePtr(dbFilePath)
	if err != nil {
		return
	}

	//colNames := []string{"ID", "Name", "Gender", "Age", "Salary"}
	colTypes := []string{"INT", "VARCHAR(255)", "VARCHAR(255)", "SMALLINT", "NULL", "FLOAT"}
	colVals := []string{"42", "Mohamed Hossam", "Male", "22", "1337.66"}
	serRow := serializeRow(colTypes, colVals)
	fmt.Println(serRow)
	fmt.Println(deserializeRow(serRow))
}

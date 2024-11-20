package main

import "fmt"

func main() {
	err := openDB(dbFilePath)
	if err != nil {
		err = createDB("db.rocketsql")
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	_, err = getFirstFreePagePtr(dbFilePath)
	if err != nil {
		fmt.Println(err)
		return
	}

	tblName := "Students"
	colNames := []string{"ID", "Name", "Gender", "Age", "Salary"}
	colTypes := []string{"INT", "VARCHAR(255)", "VARCHAR(255)", "SMALLINT", "FLOAT"}
	colVals := []string{"42", "Mohamed Hossam", "Male", "22", "1337.66"}

	err = createTable(tblName, colNames, colTypes)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = insertIntoTable(tblName, colTypes, colVals)
	if err != nil {
		fmt.Println(err)
		return
	}

	result, err := searchTable(tblName, "42")
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(result)
}

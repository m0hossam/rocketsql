package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/m0hossam/rocketsql/db"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("rocketSQL> Welcome to RocketSQL")
	fmt.Println("rocketSQL> Type '.exit' to quit")
	fmt.Println("rocketSQL> You are connected to an in-memory database")

	rocketsql, err := db.NewDb()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer rocketsql.Close()

	// REPL: (Read - Eval - Print) Loop
	for {
		// Get input
		fmt.Print("rocketSQL> ")
		if !scanner.Scan() {
			break
		}
		input := strings.TrimSpace(scanner.Text())

		// Quitting (meta-command)
		if input == ".exit" {
			break
		}

		// Other meta-commands
		if len(input) >= 1 {
			if input[0] == '.' {
				fmt.Println(rocketsql.ExecuteMetaCommand(input))
				continue
			}
		}

		// Execute SQL
		rowsAffected, resultTable, err := rocketsql.ExecuteSQL(input)
		if err != nil {
			fmt.Println(err)
			continue
		}

		// Print results of SQL
		if resultTable == nil { // DML, DDL
			fmt.Printf("%d row(s) affected\n", rowsAffected)
		} else { // Queries
			if err = resultTable.BeforeFirst(); err != nil {
				fmt.Println(err)
				continue
			}

			for {
				next, err := resultTable.Next()

				if err != nil {
					fmt.Println(err)
					break
				}

				if !next {
					break
				}

				fmt.Println(resultTable.GetRow())
			}
		}
	}

	if err = scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Error reading input:", err)
	}
}

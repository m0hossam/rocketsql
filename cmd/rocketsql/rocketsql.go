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

	db, err := db.NewDb("rocketsql.db")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	for {
		fmt.Print("rocketSQL> ")

		if !scanner.Scan() {
			break
		}
		input := strings.TrimSpace(scanner.Text())

		if input == ".exit" {
			break
		}

		rowsAffected, resultTable, err := db.ExecuteSQL(input)
		if err != nil {
			fmt.Println(err)
			continue
		}

		if resultTable == nil {
			fmt.Printf("%d row(s) affected\n", rowsAffected)
		} else {
			if err := resultTable.BeforeFirst(); err != nil {
				fmt.Println(err)
				continue
			}

			for {
				next, err := resultTable.Next()

				if err != nil {
					fmt.Println(err)
					continue
				}

				if !next {
					break
				}

				fmt.Println(resultTable.GetRow())
			}
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Error reading input:", err)
	}
}

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

	var rocketsql *db.Db
	var err error

	for {
		fmt.Print("rocketSQL> ")

		if !scanner.Scan() {
			break
		}
		input := strings.TrimSpace(scanner.Text())

		if input == ".exit" {
			break
		}

		if len(input) >= 7 {
			if input[:6] == ".open " {
				rocketsql, err = db.NewDb(input[6:])
				if err != nil {
					fmt.Println(err)
					continue
				}
				defer rocketsql.Close()
				fmt.Printf("Connected to database '%s'\n", input[6:])
				continue
			}
		}

		rowsAffected, resultTable, err := rocketsql.ExecuteSQL(input)
		if err != nil {
			fmt.Println(err)
			continue
		}

		if resultTable == nil {
			fmt.Printf("%d row(s) affected\n", rowsAffected)
		} else {
			if err = resultTable.BeforeFirst(); err != nil {
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

	if err = scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Error reading input:", err)
	}
}

package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func ExecuteSQLFile(db *sqlvibe.Database, filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var sql strings.Builder

	for scanner.Scan() {
		line := scanner.Text()
		sql.WriteString(line)
		sql.WriteString(" ")

		if strings.Contains(line, ";") {
			query := strings.TrimSpace(sql.String())
			if query != "" && !strings.HasPrefix(query, "--") {
				_, err := db.Exec(query)
				if err != nil {
					return fmt.Errorf("error executing %s: %v", query, err)
				}
			}
			sql.Reset()
		}
	}

	return scanner.Err()
}

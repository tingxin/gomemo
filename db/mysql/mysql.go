package mysql

import (
	"database/sql"
	"fmt"
	"strings"

	// used to import mysql driver
	_ "github.com/go-sql-driver/mysql"
)

// GetConnStr used to build db connection string
func GetConnStr(url, user, pass, dbName string, port int) string {
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, pass, url, port, dbName)
	return connStr
}

// FetchWithConnStr used to query data with a established connection
func FetchWithConnStr(connStr string, command string, rowHandel func(rowIndex int, row []sql.RawBytes) (interface{}, error)) ([]interface{}, error) {

	conn, err := sql.Open("mysql", connStr)
	if err != nil {
		return nil, nil
	}
	return FetchWithConn(conn, command, rowHandel)
}

// FetchWithConn used to query data with a established connection
func FetchWithConn(conn *sql.DB, command string, rowHandel func(rowIndex int, row []sql.RawBytes) (interface{}, error)) ([]interface{}, error) {
	// Execute the query
	rows, err := conn.Query(command)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	// Make a slice for the values
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Fetch rows
	rowIndex := 0

	cache := make([]interface{}, 0)
	for rows.Next() {
		// get RawBytes from data
		err = rows.Scan(scanArgs...)
		if err != nil {
			fmt.Printf("Error: get mysql columns met %s", err.Error())
			continue
		}

		obj, handelErr := rowHandel(rowIndex, values)
		if handelErr != nil {
			panic(handelErr)
		}
		rowIndex++
		cache = append(cache, obj)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return cache, nil
}

// ExecuteWithConn used to excuete sql command such as insert, delete
func ExecuteWithConn(conn *sql.DB, command string) error {
	// Execute the query
	_, err := conn.Exec(command)
	return err
}

// PushBulk used to push bulk data to mysql
func PushBulk(conn *sql.DB, command string, items []string) error {
	data := strings.Join(items, ",")
	_, err := conn.Exec(command + data)
	if err != nil {
		return err
	}
	return nil
}

package sql

import (
	"database/sql"
	"strings"

	"github.com/tingxin/gomemo/log"
)

// DataHandler used process the raw data to target object
type DataHandler func(rowIndex int, row []sql.RawBytes) (interface{}, error)

// GenRow used to process the raw data GenRow
type GenRow struct {
	Err  error
	Data []sql.RawBytes
}

// FetchWithConn used to query data with a established connection
func FetchWithConn(conn *sql.DB, command string, rowHandel func(rowIndex int, row []sql.RawBytes) (interface{}, error)) ([]interface{}, error) {
	// Execute the query
	rows, err := conn.Query(command)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := rows.Close()
		panic(err)
	}()

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
			log.ERROR.Printf("get columns met %s", err.Error())
			continue
		}

		obj, hError := rowHandel(rowIndex, values)
		if hError != nil {
			continue
		}
		rowIndex++
		cache = append(cache, obj)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return cache, nil
}

// FetchRawWithConn (NOT SUGGEST TO USE DUE TO MEMORY BUG)used to query data with a established connection
func FetchRawWithConn(conn *sql.DB, command string) ([][]sql.RawBytes, error) {
	rows, err := conn.Query(command)
	if err != nil {
		return nil, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	columnsCount := len(columns)
	// Make a slice for the values
	scanArgs := make([]interface{}, columnsCount)

	// Fetch rows
	rowIndex := 0
	cache := make([][]sql.RawBytes, 0)

	for rows.Next() {
		// get RawBytes from data
		values := make([]sql.RawBytes, columnsCount)
		for i := range values {
			scanArgs[i] = &values[i]
		}
		err = rows.Scan(scanArgs...)
		if err != nil {
			log.ERROR.Printf("failed to get  columns met %s", err.Error())
			continue
		}

		rowIndex++
		cache = append(cache, values)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return cache, nil
}

// FetchRawGenerator used to query data with a established connection
func FetchRawGenerator(conn *sql.DB, command string) <-chan *GenRow {
	req := make(chan *GenRow)
	go fetchRawGen(conn, command, req)
	return req
}

// fetchRawWithConn used to query data with a established connection
func fetchRawGen(conn *sql.DB, command string, result chan<- *GenRow) {
	// Execute the query
	rows, err := conn.Query(command)
	if err != nil {
		result <- &GenRow{Err: err, Data: nil}
		return
	}

	columns, err := rows.Columns()
	if err != nil {
		result <- &GenRow{Err: err, Data: nil}
		return
	}
	columnsCount := len(columns)

	// Fetch rows
	rowIndex := 0

	// Make a slice for the values
	scanArgs := make([]interface{}, columnsCount)

	for rows.Next() {

		// get RawBytes from data
		values := make([]sql.RawBytes, columnsCount)
		for i := range values {
			scanArgs[i] = &values[i]
		}
		err = rows.Scan(scanArgs...)
		if err != nil {
			log.ERROR.Printf("failed to get columns met %s", err.Error())
			continue
		}

		rowIndex++
		output := make([]sql.RawBytes, columnsCount)
		copy(output, values)
		result <- &GenRow{Err: nil, Data: output}
	}
	if err = rows.Err(); err != nil {
		result <- &GenRow{Err: err, Data: nil}
		return
	}
	close(result)
	return
}

// ExecuteWithConn used to execute sql command such as insert, delete
func ExecuteWithConn(conn *sql.DB, command string) error {
	// Execute the query
	_, err := conn.Exec(command)
	return err
}

// PushBulk used to push bulk data to db
func PushBulk(conn *sql.DB, command string, items []string) error {
	data := strings.Join(items, ",")
	_, err := conn.Exec(command + data)
	if err != nil {
		return err
	}
	return nil
}

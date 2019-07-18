package mysql

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	// used to import mysql driver

	"github.com/RichardKnop/machinery/v1/log"
	_ "github.com/go-sql-driver/mysql"
)

var (
	retryTimes = 3
)

// DataHandler used process the raw data to target object
type DataHandler func(rowIndex int, row []sql.RawBytes) (interface{}, error)

// GenRow used to process the mysql raw data GenRow
type GenRow struct {
	Err  error
	Data []sql.RawBytes
}

// GetConn used to generate a connection to datebase
func GetConn(connStr string) (*sql.DB, error) {
	conn, err := sql.Open("mysql", connStr)
	if err != nil {
		return nil, err
	}
	conn.SetConnMaxLifetime(time.Second * 20)
	conn.SetMaxOpenConns(1000)
	conn.SetMaxIdleConns(1000)

	err = conn.Ping()
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// FetchWithConn used to query data with a established connection
func FetchWithConn(conn *sql.DB, command string, rowHandel func(rowIndex int, row []sql.RawBytes) (interface{}, error)) ([]interface{}, error) {
	// Execute the query
	var rows *sql.Rows
	for i := 0; i < retryTimes; i++ {
		_rows, err := conn.Query(command)
		if err == nil {
			rows = _rows
			break
		}
		errStr := fmt.Sprintf("%v", err)
		if i == retryTimes-1 || strings.Contains(errStr, "in your SQL syntax") {
			log.ERROR.Printf("Failed to query in mysql due to %v, retry ...", err, retryTimes)
			return nil, err
		}
		log.WARNING.Printf("Failed to query in mysql due to \n %v\n, retry ...", err, i+1)
		sleepTime := time.Duration(100 * (i + 1))
		time.Sleep(time.Millisecond * sleepTime)
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
			log.ERROR.Printf("get mysql columns met %s", err.Error())
			continue
		}

		obj, handelErr := rowHandel(rowIndex, values)
		if handelErr != nil {
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
	// Execute the query
	var rows *sql.Rows
	for i := 0; i < retryTimes; i++ {
		_rows, err := conn.Query(command)
		if err == nil {
			rows = _rows
			break
		}
		errStr := fmt.Sprintf("%v", err)
		if i == retryTimes-1 || strings.Contains(errStr, "in your SQL syntax") {
			log.ERROR.Printf("Failed to query in mysql due to %v, retry ...", err, retryTimes)
			return nil, err
		}
		log.WARNING.Printf("Failed to query in mysql due to \n %v\n, retry ...", err, i+1)
		sleepTime := time.Duration(100 * (i + 1))
		time.Sleep(time.Millisecond * sleepTime)
	}
	defer rows.Close()

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
			log.ERROR.Printf("get mysql columns met %s", err.Error())
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
	var rows *sql.Rows
	for i := 0; i < retryTimes; i++ {
		_rows, err := conn.Query(command)
		if err == nil {
			rows = _rows
			break
		}
		errStr := fmt.Sprintf("%v", err)
		if i == retryTimes-1 || strings.Contains(errStr, "in your SQL syntax") {
			log.ERROR.Printf("Failed to query in mysql due to %v, retry ...", err, retryTimes)
			result <- &GenRow{Err: err, Data: nil}
			return
		}
		log.WARNING.Printf("Failed to query in mysql due to \n %v\n, retry ...", err, i+1)
		sleepTime := time.Duration(100 * (i + 1))
		time.Sleep(time.Millisecond * sleepTime)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		result <- &GenRow{Err: err, Data: nil}
		return
	}
	columnsCount := len(columns)
	// Make a slice for the values
	scanArgs := make([]interface{}, columnsCount)

	// Fetch rows
	rowIndex := 0

	for rows.Next() {
		// get RawBytes from data
		values := make([]sql.RawBytes, columnsCount)
		for i := range values {
			scanArgs[i] = &values[i]
		}
		err = rows.Scan(scanArgs...)
		if err != nil {
			log.ERROR.Printf("get mysql columns met %s", err.Error())
			continue
		}

		rowIndex++
		result <- &GenRow{Err: nil, Data: values}
	}
	if err = rows.Err(); err != nil {
		result <- &GenRow{Err: err, Data: nil}
		return
	}
	close(result)
	return
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

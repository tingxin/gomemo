package mysql

import (
	"database/sql"
	"fmt"
	"strings"

	// used to import mysql driver
	"git.nevint.com/nio-foundation/db"
	"github.com/RichardKnop/machinery/v1/log"
	_ "github.com/go-sql-driver/mysql"
)

// DataHandler used process the raw data to target object
type DataHandler func(rowIndex int, row []sql.RawBytes) (interface{}, error)

var (
	udp       *sql.DB
	lifestyle *sql.DB
	udpStr    string
	lifeStr   string
)

func init() {
	initUDPConnStr()
	initLifeStyleConnStr()
	conn, err := GetUDPConn()
	if err != nil {
		panic(err)
	}
	udp = conn

	conn, err = GetLifeStyleConn()
	if err != nil {
		panic(err)
	}
	lifestyle = conn

	log.INFO.Println("Init Mysql Done")
}

// GetUDPConn used to generate a connection to udp datebase
func GetUDPConn() (*sql.DB, error) {
	conn, err := sql.Open("mysql", udpStr)
	if err != nil {
		return nil, err
	}

	conn.SetMaxOpenConns(500)
	conn.SetMaxIdleConns(100)
	err = conn.Ping()
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// GetSharedUDPConn used to generate a shared connection to udp datebase
func GetSharedUDPConn() (*sql.DB, error) {
	return udp, nil
}

// GetLifeStyleConn used to generate a connection to udp datebase
func GetLifeStyleConn() (*sql.DB, error) {
	conn, err := sql.Open("mysql", lifeStr)
	if err != nil {
		return nil, err
	}

	conn.SetMaxOpenConns(500)
	conn.SetMaxIdleConns(100)
	err = conn.Ping()
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// GetSharedLifeStyleConn used to generate a shared connection to udp datebase
func GetSharedLifeStyleConn() (*sql.DB, error) {
	return lifestyle, nil
}

// FetchWithConn used to query data with a established connection
func FetchWithConn(conn *sql.DB, command string, rowHandel func(rowIndex int, row []sql.RawBytes) (interface{}, error)) ([]interface{}, error) {
	// Execute the query
	rows, err := conn.Query(command)
	defer rows.Close()
	if err != nil {
		return nil, err
	}

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

// FetchRawWithConn used to query data with a established connection
func FetchRawWithConn(conn *sql.DB, command string) ([][]sql.RawBytes, error) {
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

	cache := make([][]sql.RawBytes, 0)
	for rows.Next() {
		// get RawBytes from data
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

// initUDPConnStr used to build db connection string
func initUDPConnStr() {
	info, _ := db.GetDBInfoBy("udp_aws")
	udpStr = fmt.Sprintf("%s:%s@tcp(%s:3306)/%s", info.Account, info.Pass, info.Address[1], info.DBName)
}

// initLifeStyleConnStr used to build db connection string
func initLifeStyleConnStr() {
	info, _ := db.GetDBInfoBy("lifestyle_prod1")
	lifeStr = fmt.Sprintf("%s:%s@tcp(%s:3306)/%s", info.Account, info.Pass, info.Address[0], info.DBName)
}

package mysql

import (
	"fmt"
	// used to import mysql driver
	_ "github.com/go-sql-driver/mysql"
)

// GetConnStr used to build the connection string
func GetConnStr(address, user, pass, db string) string {
	dbStr := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s", user, pass, address, db)
	return dbStr
}

package mongo

import (
	"github.com/globalsign/mgo"
)

// Connect used to connect mongo db server
func Connect(addr []string, user, pass, dbName string) (*mgo.Session, error) {
	// dialInfo, err := mgo.ParseURL(irisdb)
	dialInfo := &mgo.DialInfo{Username: user, Password: pass, Addrs: addr, Mechanism: "SCRAM-SHA-1", Database: dbName}
	return mgo.DialWithInfo(dialInfo)
}

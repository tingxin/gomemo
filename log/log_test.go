package log

import "testing"

func TestLog(t *testing.T) {
	DEBUG.Println("IN DEBUG MODE")
	ERROR.Println("IN ERROR MODE")
	WARNING.Println("IN WARING MODE")
	INFO.Println("IN INFO MODE")
}

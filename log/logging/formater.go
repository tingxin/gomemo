package logging

import (
	"fmt"
	"path/filepath"
	"runtime"
)

const (
	// Runtime caller depth
	depth = 3
)
const (
	// For colouring
	resetSeq  = "\033[0m"
	colourSeq = "\033[0;%dm"
)

// Formatter interface
type Formatter interface {
	GetPrefix(lvl level) string
	Format(lvl level, v ...interface{}) []interface{}
	GetSuffix(lvl level) string
}

// Returns header including filename and line number
func header() string {
	_, fn, line, ok := runtime.Caller(depth)
	if !ok {
		fn = "???"
		line = 1
	}

	return fmt.Sprintf("%s:%d ", filepath.Base(fn), line)
}

// Colour map
var colour = map[level]string{
	INFO:    fmt.Sprintf(colourSeq, 94), // blue
	WARNING: fmt.Sprintf(colourSeq, 95), // pink
	ERROR:   fmt.Sprintf(colourSeq, 91), // red
	FATAL:   fmt.Sprintf(colourSeq, 91), // red
}

// DefaultFormatter adds filename and line number before the log message
type DefaultFormatter struct {
}

// GetPrefix returns ""
func (f *DefaultFormatter) GetPrefix(lvl level) string {
	return ""
}

// GetSuffix returns ""
func (f *DefaultFormatter) GetSuffix(lvl level) string {
	return ""
}

// Format adds filename and line number before the log message
func (f *DefaultFormatter) Format(lvl level, v ...interface{}) []interface{} {
	return append([]interface{}{header()}, v...)
}

// ColouredFormatter colours log messages with ASCI escape codes
// and adds filename and line number before the log message
// See https://en.wikipedia.org/wiki/ANSI_escape_code
type ColouredFormatter struct {
}

// GetPrefix returns colour escape code
func (f *ColouredFormatter) GetPrefix(lvl level) string {
	return colour[lvl]
}

// GetSuffix returns reset sequence code
func (f *ColouredFormatter) GetSuffix(lvl level) string {
	return resetSeq
}

// Format adds filename and line number before the log message
func (f *ColouredFormatter) Format(lvl level, v ...interface{}) []interface{} {
	return append([]interface{}{header()}, v...)
}

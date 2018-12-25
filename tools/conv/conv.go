package conv

import (
	"database/sql"
	"strconv"
	"strings"
	"time"
)

const (
	timeFormat = "2006-01-02 15:04:05"
)

// ToLocalFromTimestamp  used to convert stamp to local time string
func ToLocalFromTimestamp(stamp int64) string {
	time1 := time.Unix(stamp, 0)
	local, _ := time.LoadLocation("Local")
	return time1.In(local).Format(timeFormat)
}

// ToIntFromBytes used to convert  bytes to int
func ToIntFromBytes(raw []byte) int {
	i, err := strconv.Atoi(string(raw))
	if err != nil {
		return 0
	}
	return i
}

// ToIntFromFloatBytes used to convert  bytes to int
func ToIntFromFloatBytes(raw []byte) int {
	str := string(raw)
	index := strings.Index(str, ".")
	if index > 0 {
		str = str[0:index]
	}
	i, err := strconv.Atoi(str)
	if err != nil {
		return 0
	}
	return i
}

// ToStrFromObj used to convert  bytes to int
func ToStrFromObj(raw interface{}) string {
	t := raw.(sql.RawBytes)
	return string(t)
}

// ToInt64FromObj used to convert  bytes to int
func ToInt64FromObj(raw interface{}) int64 {
	t := raw.(sql.RawBytes)
	i, err := strconv.ParseInt(string(t), 10, 64)
	if err != nil {
		return 0
	}
	return i
}

// ToInt64FromBytes used to convert  bytes to int
func ToInt64FromBytes(raw []byte) int64 {
	i, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return 0
	}
	return i
}

// ToFloat64FromBytes used to convert  bytes to int
func ToFloat64FromBytes(raw []byte) float64 {
	i, err := strconv.ParseFloat(string(raw), 64)
	if err != nil {
		return 0
	}
	return i
}

// ToUTCFromLocal used to convert local time string to UTC time string
func ToUTCFromLocal(localTimeStr string) string {
	localDocTime, _ := time.Parse("2006-01-02 15:04:05", localTimeStr)
	utcTime := localDocTime.Add(-8 * time.Hour)
	utcTimeStr := utcTime.Format("2006-01-02T15:04:05")
	return utcTimeStr
}

// ToTimestamp used to convert utc time string to local time string
func ToTimestamp(timeStr string) int64 {
	docTime, _ := time.Parse("2006-01-02 15:04:05", timeStr)
	return docTime.Unix()
}

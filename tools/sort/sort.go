package sort

// BinarySearch64 used to binary search int64 array
func BinarySearch64(key int64, arr []int64) int {
	length := len(arr)
	var begin int
	var end int
	end = length - 1
	for {
		if end < begin {
			return -1
		}
		middleIndex := (end + begin) / 2
		middleValue := arr[middleIndex]
		if middleValue == key {
			return middleIndex
		} else if middleValue > key {
			end = middleIndex - 1
		} else {
			begin = middleIndex + 1
		}
	}
}

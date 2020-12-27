package sort

import "fmt"

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

// Partition used to split the arr by povit with desc
func Partition(arr []int64, pivotIndex int, asc bool) int {
	index := pivotIndex - 1
	for pivotIndex > 1 && index >= 0 {
		if arr[index] < arr[pivotIndex] {
			arr[pivotIndex-1], arr[index] = arr[index], arr[pivotIndex-1]
			arr[pivotIndex], arr[pivotIndex-1] = arr[pivotIndex-1], arr[pivotIndex]
			pivotIndex--
		}
		index--
	}
	length := len(arr)
	index = pivotIndex + 1
	for index < length {
		if arr[index] > arr[pivotIndex] {
			arr[pivotIndex+1], arr[index] = arr[index], arr[pivotIndex+1]
			arr[pivotIndex], arr[pivotIndex+1] = arr[pivotIndex+1], arr[pivotIndex]
			pivotIndex++
		}
		index++
	}
	if asc {
		i := 0
		j := length - 1
		for i < j {
			arr[i], arr[j] = arr[j], arr[i]
			i++
			j--
		}
		return length - 1 - pivotIndex
	}
	return pivotIndex
}

// GetTop used to get top k number
func GetTop(arr []int64, k int) []int64 {
	length := len(arr)
	if k > length {
		return arr
	}
	if k <= 0 {
		return make([]int64, 0)
	}

	split := Partition(arr, length-1, false)
	fmt.Printf("%v\n", arr)
	if k <= split {
		return arr[0:k]
	}

	p := arr[0:split]
	s1 := GetTop(arr[split:length], k-split)
	u := append(p, s1...)
	return u
}

package array

// CompareFunc used to express compare item fit some conditions
type CompareFunc func(item interface{}) bool

// ConvFunc used conv item to another
type ConvFunc func(item interface{}) interface{}

// FindIndex returns the index of the first element in the array that satisfies the provided testing function
func FindIndex(list []interface{}, compare CompareFunc) int {
	for i, obj := range list {
		if compare(obj) {
			return i
		}
	}
	return -1
}

// IndexOf returns the first index at which a given element can be found in the
func IndexOf(list []interface{}, target interface{}) int {
	for i, obj := range list {
		if obj == target {
			return i
		}
	}
	return -1
}

// Filter return all values in the array that satisfies the provided testing function
func Filter(list []interface{}, compare CompareFunc) []interface{} {
	result := make([]interface{}, 0)
	for _, obj := range list {
		if compare(obj) {
			result = append(result, obj)
		}
	}
	return result
}

// First return the value of the first element in the array that satisfies the provided testing function
func First(list []interface{}, compare CompareFunc) interface{} {
	for _, obj := range list {
		if compare(obj) {
			return obj
		}
	}
	return nil
}

// Any return if any item in list that satisfies the provided testing function
func Any(list []interface{}, compare CompareFunc) bool {
	for _, obj := range list {
		if compare(obj) {
			return true
		}
	}
	return false
}

// Contain return if list contain the target element
func Contain(list []interface{}, target interface{}) bool {
	for _, obj := range list {
		if obj == target {
			return true
		}
	}
	return false
}

// Map return all the converted value by conv function from the element in the list
func Map(list []interface{}, conv ConvFunc) []interface{} {
	result := make([]interface{}, len(list))
	for i, obj := range list {
		conved := conv(obj)
		result[i] = conved
	}
	return result
}

// StringsToInterfaces used to conv string array to interface array
func StringsToInterfaces(a []string) []interface{} {
	b := make([]interface{}, len(a), len(a))
	for i := range a {
		b[i] = a[i]
	}
	return b
}

// IntsToInterfaces used to conv int array to interface array
func IntsToInterfaces(a []int) []interface{} {
	b := make([]interface{}, len(a), len(a))
	for i := range a {
		b[i] = a[i]
	}
	return b
}

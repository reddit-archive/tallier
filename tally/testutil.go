package tally

import (
	"fmt"
	"reflect"
)

func assertDeepEqual(expected, result interface{}) (s string, b bool) {
	if !reflect.DeepEqual(expected, result) {
		return fmt.Sprintf("\nexpected: %+v\n     got: %+v",
			expected, result), false
	}
	return "", true
}

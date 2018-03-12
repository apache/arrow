// +build debug assert

package debug

import "fmt"

func getStringValue(v interface{}) string {
	switch a := v.(type) {
	case func() string:
		return a()

	case string:
		return a

	case fmt.Stringer:
		return a.String()

	default:
		panic(fmt.Sprintf("unexpected type, %t", v))
	}
}

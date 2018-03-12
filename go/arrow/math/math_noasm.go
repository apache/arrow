// +build noasm

package math

func init() {
	initGo()
}

func initGo() {
	initFloat64Go()
	initInt64Go()
	initUint64Go()
}

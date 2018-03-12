// +build assert

package debug

// Assert will panic with msg if cond is false.
//
// msg must be a string, func() string or fmt.Stringer.
func Assert(cond bool, msg interface{}) {
	if !cond {
		panic(getStringValue(msg))
	}
}

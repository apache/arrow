package pqarrow

import "sync"

// I would propose this should be done as a buffer using generics.  However,
// that would require upgrading to go 1.18+

// When concurrently processing, it's often necessary to accumulate errors
// to pass back to main routine.  This is a convenience way to accumulate.
type ErrBuffer struct {
	sync.Mutex
	errs []error
}


// Add an error to the error buffer
func (e *ErrBuffer) Append(err error) {
	e.Lock()
	defer e.Unlock()
	e.errs = append(e.errs,err)
}

// Retrieve accumulated errors and clear
func (e *ErrBuffer) Errors() []error {
	e.Lock() 
	defer e.Unlock()
	temp := e.errs
	e.errs = nil
	return temp
}

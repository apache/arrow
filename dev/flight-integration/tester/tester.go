package tester

import (
	"fmt"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const RequirementFailedMsg = "tester: requirement failed"

func NewTester() *Tester {
	req := requireT{}
	return &Tester{req: &req, assert: assert.New(&req), require: require.New(&req)}
}

type Tester struct {
	req *requireT

	assert  *assert.Assertions
	require *require.Assertions
}

func (t *Tester) Errors() []error {
	return t.req.errors
}

func (t *Tester) Assert() *assert.Assertions {
	return t.assert
}

func (t *Tester) Require() *require.Assertions {
	return t.require
}

type requireT struct {
	errors []error
}

// FailNow implements require.TestingT.
func (a *requireT) FailNow() {
	panic(RequirementFailedMsg)
}

// Errorf implements assert.TestingT.
func (a *requireT) Errorf(format string, args ...interface{}) {
	a.errors = append(a.errors, fmt.Errorf(format, args...))
}

var _ require.TestingT = (*requireT)(nil)

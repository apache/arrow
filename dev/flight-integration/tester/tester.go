// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

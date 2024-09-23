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

package utils

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testError struct{}

var _ error = testError{}

func (testError) Error() string {
	return "test error"
}

func TestFormatRecoveredError(t *testing.T) {
	defer func() {
		thing := recover()
		assert.NotNil(t, thing)
		assert.Error(t, thing.(testError))

		err := FormatRecoveredError("recovered thing", thing)

		assert.Equal(t, "recovered thing: test error", err.Error())
		assert.True(t, errors.Is(err, testError{}))
		assert.Equal(t, "test error", errors.Unwrap(err).(testError).Error())
	}()

	panic(testError{})
}

func TestFormatRecoveredNonError(t *testing.T) {
	defer func() {
		thing := recover()
		assert.NotNil(t, thing)

		err := FormatRecoveredError("recovered thing", thing)

		assert.Equal(t, "recovered thing: just a message", err.Error())
		assert.False(t, errors.Is(err, testError{}))
	}()

	panic("just a message")
}

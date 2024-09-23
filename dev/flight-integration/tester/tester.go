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
	"io"

	"github.com/apache/arrow/dev/flight-integration/protocol/flight"
	"github.com/apache/arrow/dev/flight-integration/protocol/message/org/apache/arrow/flatbuf"
	"github.com/apache/arrow/dev/flight-integration/serialize"
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

func RequireStreamHeaderMatchesFields[T interface {
	Recv() (*flight.FlightData, error)
}](t *Tester, stream T, expectedFields []serialize.Field) {

	data, err := stream.Recv()
	t.Require().NoError(err)

	msg := flatbuf.GetRootAsMessage(data.DataHeader, 0)
	t.Require().Equal(flatbuf.MessageHeaderSchema, msg.HeaderType())

	fields, ok := serialize.ParseFlatbufferSchemaFields(msg)
	t.Require().True(ok)
	t.Require().Len(fields, len(expectedFields))

	AssertSchemaMatchesFields(t, fields, expectedFields)
}

func AssertSchemaMatchesFields(t *Tester, fields []flatbuf.Field, expectedFields []serialize.Field) {
	for _, expectedField := range expectedFields {
		field, found := matchFieldByName(fields, expectedField.Name)
		t.Assert().Truef(found, "no matching field with expected name \"%s\" found in flatbuffer schema", expectedField.Name)
		if !found {
			continue
		}

		t.Assert().Equal(expectedField.Name, string(field.Name()))
		t.Assert().Equal(expectedField.Type, field.TypeType())
		t.Assert().Equal(expectedField.Nullable, field.Nullable())
		// TODO: metadata?
	}
}

func RequireDrainStream[E any, S interface {
	Recv() (E, error)
}](t *Tester, stream S, eval func(*Tester, E)) {
	for {
		data, err := stream.Recv()
		if err == io.EOF {
			break
		}
		t.Require().NoError(err)

		if eval != nil {
			eval(t, data)
		}
	}
}

func RequireSchemaResultMatchesFields(t *Tester, res *flight.SchemaResult, expectedFields []serialize.Field) {
	metadata := serialize.ExtractFlatbufferPayload(res.Schema)

	msg := flatbuf.GetRootAsMessage(metadata, 0)
	t.Require().Equal(flatbuf.MessageHeaderSchema, msg.HeaderType())

	fields, ok := serialize.ParseFlatbufferSchemaFields(msg)
	t.Require().True(ok)
	t.Require().Len(fields, len(expectedFields))

	AssertSchemaMatchesFields(t, fields, expectedFields)
}

func matchFieldByName(fields []flatbuf.Field, name string) (flatbuf.Field, bool) {
	for _, f := range fields {
		fieldName := string(f.Name())
		if fieldName == name {
			return f, true
		}
	}
	return flatbuf.Field{}, false
}

var _ require.TestingT = (*requireT)(nil)

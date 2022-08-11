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

package example

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/flight"
	"github.com/apache/arrow/go/v10/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v10/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/apache/arrow/go/v10/arrow/scalar"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

func TestServer(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	s := flight.NewServerWithMiddleware(nil)
	srv, err := NewSQLiteFlightSQLServer()
	assert.NoError(t, err)
	assert.NotNil(t, srv)
	srv.mem = mem
	s.RegisterFlightService(flightsql.NewFlightServer(srv))
	s.Init("localhost:0")

	go s.Serve()
	defer s.Shutdown()

	cl, err := flightsql.NewClient(s.Addr().String(), nil, nil, dialOpts...)
	assert.NoError(t, err)
	assert.NotNil(t, cl)

	info, err := cl.Execute(context.Background(), "SELECT * FROM intTable")
	assert.NoError(t, err)

	rdr, err := cl.DoGet(context.Background(), info.Endpoint[0].Ticket)
	assert.NoError(t, err)
	defer rdr.Release()

	assert.True(t, rdr.Next())
	rec := rdr.Record()
	assert.NotNil(t, rec)

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "keyName", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "foreignId", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	assert.Truef(t, expectedSchema.Equal(rec.Schema()), "expected: %s\ngot: %s", expectedSchema, rec.Schema())

	idarr, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int64, strings.NewReader(`[1, 2, 3, 4]`))
	defer idarr.Release()
	keyarr, _, _ := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["one", "zero", "negative one", null]`))
	defer keyarr.Release()
	valarr, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int64, strings.NewReader(`[1, 0, -1, null]`))
	defer valarr.Release()
	foreignarr, _, _ := array.FromJSON(mem, arrow.PrimitiveTypes.Int64, strings.NewReader(`[1, 1, 1, null]`))
	defer foreignarr.Release()

	expectedRec := array.NewRecord(expectedSchema, []arrow.Array{idarr, keyarr, valarr, foreignarr}, 4)
	defer expectedRec.Release()

	assert.Truef(t, array.RecordEqual(expectedRec, rec), "expected: %s\ngot: %s", expectedRec, rec)

	info, err = cl.GetTables(context.Background(), &flightsql.GetTablesOpts{})
	assert.NoError(t, err)
	assert.NotNil(t, info)

	rdr, err = cl.DoGet(context.Background(), info.Endpoint[0].Ticket)
	assert.NoError(t, err)
	defer rdr.Release()

	catalogName := scalar.MakeArrayOfNull(arrow.BinaryTypes.String, 3, mem)
	defer catalogName.Release()
	schemaName := scalar.MakeArrayOfNull(arrow.BinaryTypes.String, 3, mem)
	defer schemaName.Release()

	tableName, _, _ := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["foreignTable", "intTable", "sqlite_sequence"]`))
	defer tableName.Release()

	tableType, _, _ := array.FromJSON(mem, arrow.BinaryTypes.String, strings.NewReader(`["table", "table", "table"]`))
	defer tableType.Release()

	expectedRec = array.NewRecord(schema_ref.Tables, []arrow.Array{catalogName, schemaName, tableName, tableType}, 3)
	defer expectedRec.Release()

	assert.True(t, rdr.Next())
	rec = rdr.Record()
	assert.NotNil(t, rec)
	rec.Retain()
	assert.False(t, rdr.Next())

	assert.Truef(t, array.RecordEqual(expectedRec, rec), "expected: %s\ngot: %s", expectedRec, rec)
}

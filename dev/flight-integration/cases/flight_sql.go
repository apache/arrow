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

package cases

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/arrow/dev/flight-integration/protocol/flight"
	"github.com/apache/arrow/dev/flight-integration/protocol/message/org/apache/arrow/flatbuf"
	"github.com/apache/arrow/dev/flight-integration/scenario"
	"github.com/apache/arrow/dev/flight-integration/serialize"
	"github.com/apache/arrow/dev/flight-integration/tester"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func init() {
	var (
		catalog               = "catalog"
		dbSchemaFilterPattern = "db_schema_filter_pattern"
		tableFilterPattern    = "table_filter_pattern"
		table                 = "table"
		dbSchema              = "db_schema"
		tableTypes            = []string{"table", "view"}
		pkCatalog             = "pk_catalog"
		pkDbSchema            = "pk_db_schema"
		pkTable               = "pk_table"
		fkCatalog             = "fk_catalog"
		fkDbSchema            = "fk_db_schema"
		fkTable               = "fk_table"

		stmtQuery       = "SELECT STATEMENT"
		stmtQueryHandle = "SELECT STATEMENT HANDLE"
		stmtUpdate      = "UPDATE STATEMENT"
		stmtUpdateRows  = int64(10000)

		stmtPreparedQuery        = "SELECT PREPARED STATEMENT"
		stmtPreparedQueryHandle  = "SELECT PREPARED STATEMENT HANDLE"
		stmtPreparedUpdate       = "UPDATE PREPARED STATEMENT"
		stmtPreparedUpdateHandle = "UPDATE PREPARED STATEMENT HANDLE"
		stmtPreparedUpdateRows   = int64(20000)

		createPreparedStatementActionType = "CreatePreparedStatement"
		closePreparedStatementActionType  = "ClosePreparedStatement"
	)

	testcases := []struct {
		Command proto.Message
		Fields  []serialize.Field
	}{
		{
			Command: &flight.CommandGetCatalogs{},
			Fields:  serialize.SchemaRef.Catalogs,
		},
		{
			Command: &flight.CommandGetDbSchemas{Catalog: &catalog, DbSchemaFilterPattern: &dbSchemaFilterPattern},
			Fields:  serialize.SchemaRef.DBSchemas,
		},
		// {
		// 	Command: &flight.CommandGetTables{
		// 		Catalog:                &catalog,
		// 		DbSchemaFilterPattern:  &dbSchemaFilterPattern,
		// 		TableNameFilterPattern: &tableFilterPattern,
		// 		IncludeSchema:          false,
		// 		TableTypes:             tableTypes,
		// 	},
		// 	Fields: []field{
		// 		{Name: "catalog_name", Type: flatbuf.TypeUtf8, Nullable: true},
		// 		{Name: "db_schema_name", Type: flatbuf.TypeUtf8, Nullable: true},
		// 		{Name: "table_name", Type: flatbuf.TypeUtf8, Nullable: false},
		// 		{Name: "table_type", Type: flatbuf.TypeUtf8, Nullable: false},
		// 	},
		// },
		{
			Command: &flight.CommandGetTables{
				Catalog:                &catalog,
				DbSchemaFilterPattern:  &dbSchemaFilterPattern,
				TableNameFilterPattern: &tableFilterPattern,
				IncludeSchema:          true,
				TableTypes:             tableTypes,
			},
			Fields: serialize.SchemaRef.TablesWithIncludedSchema,
		},
		{
			Command: &flight.CommandGetTableTypes{},
			Fields:  serialize.SchemaRef.TableTypes,
		},
		{
			Command: &flight.CommandGetPrimaryKeys{Catalog: &catalog, DbSchema: &dbSchema, Table: table},
			Fields:  serialize.SchemaRef.PrimaryKeys,
		},
		{
			Command: &flight.CommandGetExportedKeys{Catalog: &catalog, DbSchema: &dbSchema, Table: table},
			Fields:  serialize.SchemaRef.ExportedKeys,
		},
		{
			Command: &flight.CommandGetImportedKeys{Catalog: &catalog, DbSchema: &dbSchema, Table: table},
			Fields:  serialize.SchemaRef.ImportedKeys,
		},
		{
			Command: &flight.CommandGetCrossReference{
				PkCatalog:  &pkCatalog,
				PkDbSchema: &pkDbSchema,
				PkTable:    pkTable,
				FkCatalog:  &fkCatalog,
				FkDbSchema: &fkDbSchema,
				FkTable:    fkTable,
			},
			Fields: serialize.SchemaRef.CrossReference,
		},
		{
			Command: &flight.CommandGetXdbcTypeInfo{},
			Fields:  serialize.SchemaRef.XdbcTypeInfo,
		},
		{
			Command: &flight.CommandGetSqlInfo{Info: []uint32{0, 3}},
			Fields:  serialize.SchemaRef.SqlInfo,
		},
	}

	steps := make([]scenario.ScenarioStep, 0)

	// ValidateMetadataRetrieval
	for _, tc := range testcases {
		name := proto.MessageName(tc.Command).Name()
		steps = append(
			steps,
			scenario.ScenarioStep{Name: fmt.Sprintf("GetFlightInfo/%s", name), ServerHandler: scenario.Handler{GetFlightInfo: echoFlightInfo}},
			scenario.ScenarioStep{Name: fmt.Sprintf("DoGet/%s", name), ServerHandler: scenario.Handler{DoGet: doGetFieldsForCommandFn(tc.Command, tc.Fields)}},
			scenario.ScenarioStep{Name: fmt.Sprintf("GetSchema/%s", name), ServerHandler: scenario.Handler{GetSchema: getSchemaFieldsForCommandFn(tc.Command, tc.Fields)}})
	}

	// ValidateStatementExecution
	steps = append(
		steps,
		scenario.ScenarioStep{
			Name: "GetFlightInfo/CommandStatementQuery",
			ServerHandler: scenario.Handler{GetFlightInfo: func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
				var cmd flight.CommandStatementQuery
				if err := serialize.DeserializeProtobufWrappedInAny(fd.Cmd, &cmd); err != nil {
					return nil, status.Errorf(codes.InvalidArgument, "failed to deserialize FlightDescriptor.Cmd: %s", err)
				}

				if cmd.GetQuery() != stmtQuery {
					return nil, status.Errorf(codes.InvalidArgument, "expected query: %s, found: %s", stmtQuery, cmd.GetQuery())
				}

				if len(cmd.GetTransactionId()) != 0 {
					return nil, status.Errorf(codes.InvalidArgument, "expected no TransactionID")
				}

				handle, err := serialize.SerializeProtobufWrappedInAny(&flight.TicketStatementQuery{StatementHandle: []byte(stmtQueryHandle)})
				if err != nil {
					return nil, status.Errorf(codes.Internal, "failed to create Ticket: %s", err)
				}

				return &flight.FlightInfo{
					Endpoint: []*flight.FlightEndpoint{{
						Ticket: &flight.Ticket{Ticket: handle},
					}},
				}, nil
			}}},
		scenario.ScenarioStep{Name: "DoGet/TicketStatementQuery", ServerHandler: scenario.Handler{DoGet: func(t *flight.Ticket, fs flight.FlightService_DoGetServer) error {
			var cmd flight.TicketStatementQuery
			if err := serialize.DeserializeProtobufWrappedInAny(t.Ticket, &cmd); err != nil {
				return status.Errorf(codes.InvalidArgument, "failed to deserialize Ticket.Ticket: %s", err)
			}

			if string(cmd.GetStatementHandle()) != stmtQueryHandle {
				return status.Errorf(codes.InvalidArgument, "expected handle: %s, found: %s", stmtQueryHandle, cmd.GetStatementHandle())
			}

			return fs.Send(&flight.FlightData{DataHeader: serialize.BuildFlatbufferSchema(serialize.SchemaExample.Query)})
		}}},
		scenario.ScenarioStep{Name: "GetSchema/CommandStatementQuery", ServerHandler: scenario.Handler{GetSchema: getSchemaFieldsForCommandFn(&flight.CommandStatementQuery{}, serialize.SchemaExample.Query)}},

		scenario.ScenarioStep{
			Name: "DoPut/CommandStatementUpdate",
			ServerHandler: scenario.Handler{DoPut: func(fs flight.FlightService_DoPutServer) error {
				data, err := fs.Recv()
				if err != nil {
					return status.Errorf(codes.Internal, "unable to read from stream: %s", err)
				}

				desc := data.FlightDescriptor
				var cmd flight.CommandStatementUpdate
				if err := serialize.DeserializeProtobufWrappedInAny(desc.Cmd, &cmd); err != nil {
					return status.Errorf(codes.InvalidArgument, "failed to deserialize FlightDescriptor.Cmd: %s", err)
				}

				if cmd.GetQuery() != stmtUpdate {
					return status.Errorf(codes.InvalidArgument, "expected query: %s, found: %s", stmtUpdate, cmd.GetQuery())
				}

				if len(cmd.GetTransactionId()) != 0 {
					return status.Errorf(codes.InvalidArgument, "expected no TransactionID")
				}

				appMetadata, err := proto.Marshal(&flight.DoPutUpdateResult{RecordCount: stmtUpdateRows})
				if err != nil {
					return status.Errorf(codes.Internal, "failed to marshal DoPutUpdateResult: %s", err)
				}

				return fs.Send(&flight.PutResult{AppMetadata: appMetadata})
			}}},
	)

	// ValidatePreparedStatementExecution
	steps = append(
		steps,
		scenario.ScenarioStep{
			Name: "DoAction/ActionCreatePreparedStatementRequest",
			ServerHandler: scenario.Handler{DoAction: func(a *flight.Action, fs flight.FlightService_DoActionServer) error {
				var req flight.ActionCreatePreparedStatementRequest
				if err := serialize.DeserializeProtobufWrappedInAny(a.Body, &req); err != nil {
					return status.Errorf(codes.InvalidArgument, "failed to deserialize Action.Body: %s", err)
				}

				if req.GetQuery() != stmtPreparedQuery {
					return status.Errorf(codes.InvalidArgument, "expected query: %s, found: %s", stmtPreparedQuery, req.GetQuery())
				}

				if len(req.GetTransactionId()) != 0 {
					return status.Errorf(codes.InvalidArgument, "expected no TransactionID")
				}

				body, err := serialize.SerializeProtobufWrappedInAny(&flight.ActionCreatePreparedStatementResult{PreparedStatementHandle: []byte(stmtPreparedQueryHandle)})
				if err != nil {
					return status.Errorf(codes.Internal, "failed to ActionCreatePreparedStatementResult: %s", err)
				}

				return fs.Send(&flight.Result{Body: body})
			}}},
		scenario.ScenarioStep{
			Name: "DoPut/CommandPreparedStatementQuery",
			ServerHandler: scenario.Handler{DoPut: func(fs flight.FlightService_DoPutServer) error {
				data, err := fs.Recv()
				if err != nil {
					return status.Errorf(codes.Internal, "unable to read from stream: %s", err)
				}

				msg := flatbuf.GetRootAsMessage(data.DataHeader, 0)
				if msg.HeaderType() != flatbuf.MessageHeaderSchema {
					return status.Errorf(codes.Internal, "invalid stream, expected first message to be Schema: %s", err)
				}

				fields, ok := serialize.ParseFlatbufferSchemaFields(msg)
				if !ok {
					return status.Errorf(codes.Internal, "failed to parse flatbuffer schema")
				}

				// TODO: maybe don't use tester here
				t := tester.NewTester()
				tester.AssertSchemaMatchesFields(t, fields, serialize.SchemaExample.Query)
				if len(t.Errors()) > 0 {
					return status.Errorf(codes.Internal, "flatbuffer schema mismatch: %s", errors.Join(t.Errors()...))
				}

				desc := data.FlightDescriptor
				var cmd flight.CommandPreparedStatementQuery
				if err := serialize.DeserializeProtobufWrappedInAny(desc.Cmd, &cmd); err != nil {
					return status.Errorf(codes.InvalidArgument, "failed to deserialize FlightDescriptor.Cmd: %s", err)
				}

				if string(cmd.GetPreparedStatementHandle()) != stmtPreparedQueryHandle {
					return status.Errorf(codes.InvalidArgument, "expected handle: %s, found: %s", stmtPreparedQueryHandle, cmd.GetPreparedStatementHandle())
				}

				appMetadata, err := proto.Marshal(&flight.DoPutPreparedStatementResult{PreparedStatementHandle: cmd.GetPreparedStatementHandle()})
				if err != nil {
					return status.Errorf(codes.Internal, "failed to marshal DoPutPreparedStatementResult: %s", err)
				}

				return fs.Send(&flight.PutResult{AppMetadata: appMetadata})
			}}},
		scenario.ScenarioStep{
			Name: "GetFlightInfo/CommandPreparedStatementQuery",
			ServerHandler: scenario.Handler{GetFlightInfo: func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
				var cmd flight.CommandPreparedStatementQuery
				if err := serialize.DeserializeProtobufWrappedInAny(fd.Cmd, &cmd); err != nil {
					return nil, status.Errorf(codes.InvalidArgument, "failed to deserialize FlightDescriptor.Cmd: %s", err)
				}

				if string(cmd.GetPreparedStatementHandle()) != stmtPreparedQueryHandle {
					return nil, status.Errorf(codes.InvalidArgument, "expected handle: %s, found: %s", stmtPreparedQueryHandle, cmd.GetPreparedStatementHandle())
				}

				return &flight.FlightInfo{
					Endpoint: []*flight.FlightEndpoint{{
						Ticket: &flight.Ticket{Ticket: fd.Cmd},
					}},
				}, nil
			}}},
		scenario.ScenarioStep{
			Name: "DoGet/CommandPreparedStatementQuery",
			ServerHandler: scenario.Handler{DoGet: func(t *flight.Ticket, fs flight.FlightService_DoGetServer) error {
				var cmd flight.CommandPreparedStatementQuery
				if err := serialize.DeserializeProtobufWrappedInAny(t.Ticket, &cmd); err != nil {
					return status.Errorf(codes.InvalidArgument, "failed to deserialize Ticket.Ticket: %s", err)
				}

				if string(cmd.GetPreparedStatementHandle()) != stmtPreparedQueryHandle {
					return status.Errorf(codes.InvalidArgument, "expected handle: %s, found: %s", stmtPreparedQueryHandle, cmd.GetPreparedStatementHandle())
				}

				return fs.Send(&flight.FlightData{DataHeader: serialize.BuildFlatbufferSchema(serialize.SchemaExample.Query)})
			}}},
		scenario.ScenarioStep{
			Name: "GetSchema/CommandPreparedStatementQuery",
			ServerHandler: scenario.Handler{GetSchema: func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.SchemaResult, error) {
				var cmd flight.CommandPreparedStatementQuery
				if err := serialize.DeserializeProtobufWrappedInAny(fd.Cmd, &cmd); err != nil {
					return nil, status.Errorf(codes.InvalidArgument, "failed to deserialize FlightDescriptor.Cmd: %s", err)
				}

				if string(cmd.GetPreparedStatementHandle()) != stmtPreparedQueryHandle {
					return nil, status.Errorf(codes.InvalidArgument, "expected handle: %s, found: %s", stmtPreparedQueryHandle, cmd.GetPreparedStatementHandle())
				}

				return &flight.SchemaResult{Schema: serialize.WriteFlatbufferPayload(serialize.SchemaExample.Query)}, nil
			}}},
		scenario.ScenarioStep{
			Name: "DoAction/ActionClosePreparedStatementRequest",
			ServerHandler: scenario.Handler{DoAction: func(a *flight.Action, fs flight.FlightService_DoActionServer) error {
				var req flight.ActionClosePreparedStatementRequest
				if err := serialize.DeserializeProtobufWrappedInAny(a.Body, &req); err != nil {
					return status.Errorf(codes.InvalidArgument, "failed to deserialize Action.Body: %s", err)
				}

				if string(req.GetPreparedStatementHandle()) != stmtPreparedQueryHandle {
					return status.Errorf(codes.InvalidArgument, "expected handle: %s, found: %s", stmtPreparedQueryHandle, req.GetPreparedStatementHandle())
				}

				return fs.Send(&flight.Result{})
			}}},

		scenario.ScenarioStep{
			Name: "DoAction/ActionCreatePreparedStatementRequest",
			ServerHandler: scenario.Handler{DoAction: func(a *flight.Action, fs flight.FlightService_DoActionServer) error {
				var req flight.ActionCreatePreparedStatementRequest
				if err := serialize.DeserializeProtobufWrappedInAny(a.Body, &req); err != nil {
					return status.Errorf(codes.InvalidArgument, "failed to deserialize Action.Body: %s", err)
				}

				if req.GetQuery() != stmtPreparedUpdate {
					return status.Errorf(codes.InvalidArgument, "expected query: %s, found: %s", stmtPreparedUpdate, req.GetQuery())
				}

				if len(req.GetTransactionId()) != 0 {
					return status.Errorf(codes.InvalidArgument, "expected no TransactionID")
				}

				body, err := serialize.SerializeProtobufWrappedInAny(&flight.ActionCreatePreparedStatementResult{PreparedStatementHandle: []byte(stmtPreparedUpdateHandle)})
				if err != nil {
					return status.Errorf(codes.Internal, "failed to ActionCreatePreparedStatementResult: %s", err)
				}

				return fs.Send(&flight.Result{Body: body})
			}}},
		scenario.ScenarioStep{
			Name: "DoPut/CommandPreparedStatementUpdate",
			ServerHandler: scenario.Handler{DoPut: func(fs flight.FlightService_DoPutServer) error {
				data, err := fs.Recv()
				if err != nil {
					return status.Errorf(codes.Internal, "unable to read from stream: %s", err)
				}

				msg := flatbuf.GetRootAsMessage(data.DataHeader, 0)
				if msg.HeaderType() != flatbuf.MessageHeaderSchema {
					return status.Errorf(codes.Internal, "invalid stream, expected first message to be Schema: %s", err)
				}

				fields, ok := serialize.ParseFlatbufferSchemaFields(msg)
				if !ok {
					return status.Errorf(codes.Internal, "failed to parse flatbuffer schema")
				}

				if len(fields) != 0 {
					return status.Errorf(codes.InvalidArgument, "bind schema not expected")
				}

				desc := data.FlightDescriptor
				var cmd flight.CommandPreparedStatementUpdate
				if err := serialize.DeserializeProtobufWrappedInAny(desc.Cmd, &cmd); err != nil {
					return status.Errorf(codes.InvalidArgument, "failed to deserialize FlightDescriptor.Cmd: %s", err)
				}

				if string(cmd.GetPreparedStatementHandle()) != stmtPreparedUpdateHandle {
					return status.Errorf(codes.InvalidArgument, "expected handle: %s, found: %s", stmtPreparedUpdateHandle, cmd.GetPreparedStatementHandle())
				}

				appMetadata, err := proto.Marshal(&flight.DoPutUpdateResult{RecordCount: stmtPreparedUpdateRows})
				if err != nil {
					return status.Errorf(codes.Internal, "failed to marshal DoPutPreparedStatementResult: %s", err)
				}

				return fs.Send(&flight.PutResult{AppMetadata: appMetadata})
			}}},
		scenario.ScenarioStep{
			Name: "DoAction/ActionClosePreparedStatementRequest",
			ServerHandler: scenario.Handler{DoAction: func(a *flight.Action, fs flight.FlightService_DoActionServer) error {
				var req flight.ActionClosePreparedStatementRequest
				if err := serialize.DeserializeProtobufWrappedInAny(a.Body, &req); err != nil {
					return status.Errorf(codes.InvalidArgument, "failed to deserialize Action.Body: %s", err)
				}

				if string(req.GetPreparedStatementHandle()) != stmtPreparedUpdateHandle {
					return status.Errorf(codes.InvalidArgument, "expected handle: %s, found: %s", stmtPreparedUpdateHandle, req.GetPreparedStatementHandle())
				}

				return fs.Send(&flight.Result{})
			}}},
	)

	scenario.Register(
		scenario.Scenario{
			Name:  "flight_sql",
			Steps: steps,
			RunClient: func(ctx context.Context, client flight.FlightServiceClient, t *tester.Tester) {

				// ValidateMetadataRetrieval
				////////////////////////////
				for _, tc := range testcases {
					// pack the command
					desc, err := serialize.DescForCommand(tc.Command)
					t.Require().NoError(err)

					// submit query
					info, err := client.GetFlightInfo(ctx, desc)
					t.Require().NoError(err)

					t.Require().Greater(len(info.Endpoint), 0)
					t.Assert().Equal(desc.Cmd, info.Endpoint[0].Ticket.Ticket)

					// fetch result stream
					stream, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
					t.Require().NoError(err)

					// validate first message is properly formatted schema message
					tester.RequireStreamHeaderMatchesFields(t, stream, tc.Fields)

					// drain rest of stream
					tester.RequireDrainStream(t, stream, func(t *tester.Tester, data *flight.FlightData) {
						// no more schema messages
						t.Assert().Contains(
							[]flatbuf.MessageHeader{
								flatbuf.MessageHeaderRecordBatch,
								flatbuf.MessageHeaderDictionaryBatch,
							},
							flatbuf.GetRootAsMessage(data.DataHeader, 0).HeaderType(),
						)
					})

					// issue GetSchema
					res, err := client.GetSchema(ctx, desc)
					t.Require().NoError(err)

					// expect schema to be serialized as full IPC stream Schema message
					tester.RequireSchemaResultMatchesFields(t, res, tc.Fields)
				}

				// ValidateStatementExecution
				/////////////////////////////
				{
					{
						desc, err := serialize.DescForCommand(&flight.CommandStatementQuery{Query: stmtQuery})
						t.Require().NoError(err)

						info, err := client.GetFlightInfo(ctx, desc)
						t.Require().NoError(err)

						t.Require().Greater(len(info.Endpoint), 0)

						// fetch result stream
						stream, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
						t.Require().NoError(err)

						// validate result stream
						tester.RequireStreamHeaderMatchesFields(t, stream, serialize.SchemaExample.Query)
						tester.RequireDrainStream(t, stream, func(t *tester.Tester, data *flight.FlightData) {
							// no more schema messages
							t.Assert().Contains(
								[]flatbuf.MessageHeader{
									flatbuf.MessageHeaderRecordBatch,
									flatbuf.MessageHeaderDictionaryBatch,
								},
								flatbuf.GetRootAsMessage(data.DataHeader, 0).HeaderType(),
							)
						})

						res, err := client.GetSchema(ctx, desc)
						t.Require().NoError(err)

						// expect schema to be serialized as full IPC stream Schema message
						tester.RequireSchemaResultMatchesFields(t, res, serialize.SchemaExample.Query)
					}

					{
						desc, err := serialize.DescForCommand(&flight.CommandStatementUpdate{Query: stmtUpdate})
						t.Require().NoError(err)

						stream, err := client.DoPut(ctx)
						t.Require().NoError(err)

						t.Require().NoError(stream.Send(&flight.FlightData{FlightDescriptor: desc}))
						t.Require().NoError(stream.CloseSend())

						putResult, err := stream.Recv()
						t.Require().NoError(err)

						var updateResult flight.DoPutUpdateResult
						t.Require().NoError(proto.Unmarshal(putResult.GetAppMetadata(), &updateResult))

						t.Assert().Equal(stmtUpdateRows, updateResult.GetRecordCount())
					}
				}

				// ValidatePreparedStatementExecution
				/////////////////////////////////////
				{
					var prepareResult flight.ActionCreatePreparedStatementResult
					{
						prepareAction, err := serialize.PackAction(
							createPreparedStatementActionType,
							&flight.ActionCreatePreparedStatementRequest{Query: stmtPreparedQuery},
							serialize.SerializeProtobufWrappedInAny,
						)
						t.Require().NoError(err)

						stream, err := client.DoAction(ctx, prepareAction)
						t.Require().NoError(err)

						t.Require().NoError(stream.CloseSend())

						result, err := stream.Recv()
						t.Require().NoError(err)

						t.Require().NoError(serialize.DeserializeProtobufWrappedInAny(result.Body, &prepareResult))

						t.Require().Equal(stmtPreparedQueryHandle, string(prepareResult.GetPreparedStatementHandle()))
						tester.RequireDrainStream(t, stream, nil)
					}

					var doPutPreparedResult flight.DoPutPreparedStatementResult
					{
						stream, err := client.DoPut(ctx)
						t.Require().NoError(err)

						desc, err := serialize.DescForCommand(&flight.CommandPreparedStatementQuery{PreparedStatementHandle: prepareResult.GetPreparedStatementHandle()})
						t.Require().NoError(err)

						t.Require().NoError(stream.Send(&flight.FlightData{FlightDescriptor: desc, DataHeader: serialize.BuildFlatbufferSchema(serialize.SchemaExample.Query)}))
						t.Require().NoError(stream.CloseSend())

						putResult, err := stream.Recv()
						t.Require().NoError(err)

						// TODO: legacy server doesn't provide a response

						t.Require().NoError(proto.Unmarshal(putResult.GetAppMetadata(), &doPutPreparedResult))
						t.Require().Equal(stmtPreparedQueryHandle, string(doPutPreparedResult.GetPreparedStatementHandle()))
					}

					var (
						descPutPrepared *flight.FlightDescriptor
						ticket          *flight.Ticket
					)
					{
						desc, err := serialize.DescForCommand(&flight.CommandPreparedStatementQuery{PreparedStatementHandle: doPutPreparedResult.GetPreparedStatementHandle()})
						t.Require().NoError(err)

						info, err := client.GetFlightInfo(ctx, desc)
						t.Require().NoError(err)

						t.Require().Greater(len(info.Endpoint), 0)

						descPutPrepared = desc
						ticket = info.Endpoint[0].Ticket
					}

					{
						stream, err := client.DoGet(ctx, ticket)
						t.Require().NoError(err)

						// validate result stream
						tester.RequireStreamHeaderMatchesFields(t, stream, serialize.SchemaExample.Query)
						tester.RequireDrainStream(t, stream, func(t *tester.Tester, data *flight.FlightData) {
							// no more schema messages
							t.Assert().Contains(
								[]flatbuf.MessageHeader{
									flatbuf.MessageHeaderRecordBatch,
									flatbuf.MessageHeaderDictionaryBatch,
								},
								flatbuf.GetRootAsMessage(data.DataHeader, 0).HeaderType(),
							)
						})
					}

					{
						schema, err := client.GetSchema(ctx, descPutPrepared)
						t.Require().NoError(err)

						tester.RequireSchemaResultMatchesFields(t, schema, serialize.SchemaExample.Query)
					}

					{
						action, err := serialize.PackAction(
							closePreparedStatementActionType,
							&flight.ActionClosePreparedStatementRequest{PreparedStatementHandle: []byte(stmtPreparedQueryHandle)},
							serialize.SerializeProtobufWrappedInAny,
						)
						t.Require().NoError(err)

						stream, err := client.DoAction(ctx, action)
						t.Require().NoError(err)

						t.Require().NoError(stream.CloseSend())
						tester.RequireDrainStream(t, stream, nil)
					}

					{
						action, err := serialize.PackAction(
							createPreparedStatementActionType,
							&flight.ActionCreatePreparedStatementRequest{Query: stmtPreparedUpdate},
							serialize.SerializeProtobufWrappedInAny,
						)
						t.Require().NoError(err)

						stream, err := client.DoAction(ctx, action)
						t.Require().NoError(err)

						t.Require().NoError(stream.CloseSend())

						result, err := stream.Recv()
						t.Require().NoError(err)

						var actionResult flight.ActionCreatePreparedStatementResult
						t.Require().NoError(serialize.DeserializeProtobufWrappedInAny(result.Body, &actionResult))

						t.Require().Equal(stmtPreparedUpdateHandle, string(actionResult.GetPreparedStatementHandle()))
						tester.RequireDrainStream(t, stream, nil)
					}

					{
						stream, err := client.DoPut(ctx)
						t.Require().NoError(err)

						desc, err := serialize.DescForCommand(&flight.CommandPreparedStatementUpdate{PreparedStatementHandle: []byte(stmtPreparedUpdateHandle)})
						t.Require().NoError(err)

						t.Require().NoError(stream.Send(&flight.FlightData{FlightDescriptor: desc, DataHeader: serialize.BuildFlatbufferSchema(nil)}))
						t.Require().NoError(stream.CloseSend())

						putResult, err := stream.Recv()
						t.Require().NoError(err)

						var updateResult flight.DoPutUpdateResult
						t.Require().NoError(proto.Unmarshal(putResult.GetAppMetadata(), &updateResult))

						t.Assert().Equal(stmtPreparedUpdateRows, updateResult.GetRecordCount())
					}

					{
						action, err := serialize.PackAction(
							closePreparedStatementActionType,
							&flight.ActionClosePreparedStatementRequest{PreparedStatementHandle: []byte(stmtPreparedUpdateHandle)},
							serialize.SerializeProtobufWrappedInAny,
						)
						t.Require().NoError(err)

						stream, err := client.DoAction(ctx, action)
						t.Require().NoError(err)

						t.Require().NoError(stream.CloseSend())
						tester.RequireDrainStream(t, stream, nil)
					}
				}
			},
		},
	)
}

func echoFlightInfo(ctx context.Context, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return &flight.FlightInfo{
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: fd.Cmd},
		}},
	}, nil
}

func doGetFieldsForCommandFn(cmd proto.Message, fields []serialize.Field) func(t *flight.Ticket, fs flight.FlightService_DoGetServer) error {
	return func(t *flight.Ticket, fs flight.FlightService_DoGetServer) error {
		cmd := proto.Clone(cmd)
		proto.Reset(cmd)

		if err := serialize.DeserializeProtobufWrappedInAny(t.Ticket, cmd); err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to deserialize Ticket.Ticket: %s", err)
		}

		return fs.Send(&flight.FlightData{DataHeader: serialize.BuildFlatbufferSchema(fields)})
	}
}

func getSchemaFieldsForCommandFn(cmd proto.Message, fields []serialize.Field) func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	return func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.SchemaResult, error) {
		cmd := proto.Clone(cmd)
		proto.Reset(cmd)

		if err := serialize.DeserializeProtobufWrappedInAny(fd.Cmd, cmd); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "failed to deserialize FlightDescriptor.Cmd: %s", err)
		}

		schema := serialize.WriteFlatbufferPayload(fields)

		return &flight.SchemaResult{Schema: schema}, nil
	}
}

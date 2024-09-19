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
	"encoding/binary"
	"fmt"
	"io"

	"github.com/apache/arrow/dev/flight-integration/flight"
	"github.com/apache/arrow/dev/flight-integration/message/org/apache/arrow/flatbuf"
	"github.com/apache/arrow/dev/flight-integration/scenario"
	"github.com/apache/arrow/dev/flight-integration/tester"
	flatbuffers "github.com/google/flatbuffers/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
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

		queryFields = []field{
			{
				Name:         "id",
				Type:         flatbuf.TypeInt,
				GetTypeTable: int64TypeTable,
				Nullable:     true,
				Metadata: map[string]string{
					"ARROW:FLIGHT:SQL:TABLE_NAME":        "test",
					"ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT": "1",
					"ARROW:FLIGHT:SQL:IS_CASE_SENSITIVE": "0",
					"ARROW:FLIGHT:SQL:TYPE_NAME":         "type_test",
					"ARROW:FLIGHT:SQL:SCHEMA_NAME":       "schema_test",
					"ARROW:FLIGHT:SQL:IS_SEARCHABLE":     "1",
					"ARROW:FLIGHT:SQL:CATALOG_NAME":      "catalog_test",
					"ARROW:FLIGHT:SQL:PRECISION":         "100",
				},
			},
		}
	)

	testcases := []struct {
		Command proto.Message
		Fields  []field
	}{
		{
			Command: &flight.CommandGetCatalogs{},
			Fields: []field{
				{Name: "catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: false},
			},
		},
		{
			Command: &flight.CommandGetDbSchemas{Catalog: &catalog, DbSchemaFilterPattern: &dbSchemaFilterPattern},
			Fields: []field{
				{Name: "catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: false},
			},
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
			Fields: []field{
				{Name: "catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "table_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: false},
				{Name: "table_type", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: false},
				{Name: "table_schema", Type: flatbuf.TypeBinary, GetTypeTable: binaryTypeTable, Nullable: false},
			},
		},
		{
			Command: &flight.CommandGetTableTypes{},
			Fields: []field{
				{Name: "table_type", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: false},
			},
		},
		{
			Command: &flight.CommandGetPrimaryKeys{Catalog: &catalog, DbSchema: &dbSchema, Table: table},
			Fields: []field{
				{Name: "catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "table_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: false},
				{Name: "column_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: false},
				{Name: "key_sequence", Type: flatbuf.TypeInt, GetTypeTable: int32TypeTable, Nullable: false},
				{Name: "key_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
			},
		},
		{
			Command: &flight.CommandGetExportedKeys{Catalog: &catalog, DbSchema: &dbSchema, Table: table},
			Fields: []field{
				{Name: "pk_catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "pk_db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "pk_table_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: false},
				{Name: "pk_column_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: false},
				{Name: "fk_catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "fk_db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "fk_table_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: false},
				{Name: "fk_column_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: false},
				{Name: "key_sequence", Type: flatbuf.TypeInt, GetTypeTable: int32TypeTable, Nullable: false},
				{Name: "fk_key_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "pk_key_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "update_rule", Type: flatbuf.TypeInt, GetTypeTable: uint8TypeTable, Nullable: false},
				{Name: "delete_rule", Type: flatbuf.TypeInt, GetTypeTable: uint8TypeTable, Nullable: false},
			},
		},
		{
			Command: &flight.CommandGetImportedKeys{Catalog: &catalog, DbSchema: &dbSchema, Table: table},
			Fields: []field{
				{Name: "pk_catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "pk_db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "pk_table_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: false},
				{Name: "pk_column_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: false},
				{Name: "fk_catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "fk_db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "fk_table_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: false},
				{Name: "fk_column_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: false},
				{Name: "key_sequence", Type: flatbuf.TypeInt, GetTypeTable: int32TypeTable, Nullable: false},
				{Name: "fk_key_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "pk_key_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "update_rule", Type: flatbuf.TypeInt, GetTypeTable: uint8TypeTable, Nullable: false},
				{Name: "delete_rule", Type: flatbuf.TypeInt, GetTypeTable: uint8TypeTable, Nullable: false},
			},
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
			Fields: []field{
				{Name: "pk_catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "pk_db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "pk_table_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: false},
				{Name: "pk_column_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: false},
				{Name: "fk_catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "fk_db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "fk_table_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: false},
				{Name: "fk_column_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: false},
				{Name: "key_sequence", Type: flatbuf.TypeInt, GetTypeTable: int32TypeTable, Nullable: false},
				{Name: "fk_key_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "pk_key_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "update_rule", Type: flatbuf.TypeInt, GetTypeTable: uint8TypeTable, Nullable: false},
				{Name: "delete_rule", Type: flatbuf.TypeInt, GetTypeTable: uint8TypeTable, Nullable: false},
			},
		},
		{
			Command: &flight.CommandGetXdbcTypeInfo{},
			Fields: []field{
				{Name: "type_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: false},
				{Name: "data_type", Type: flatbuf.TypeInt, GetTypeTable: int32TypeTable, Nullable: false},
				{Name: "column_size", Type: flatbuf.TypeInt, GetTypeTable: int32TypeTable, Nullable: true},
				{Name: "literal_prefix", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "literal_suffix", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "create_params", Type: flatbuf.TypeList, GetTypeTable: createParamsTypeTable, Nullable: true},
				{Name: "nullable", Type: flatbuf.TypeInt, GetTypeTable: int32TypeTable, Nullable: false},
				{Name: "case_sensitive", Type: flatbuf.TypeBool, GetTypeTable: boolTypeTable, Nullable: false},
				{Name: "searchable", Type: flatbuf.TypeInt, GetTypeTable: int32TypeTable, Nullable: false},
				{Name: "unsigned_attribute", Type: flatbuf.TypeBool, GetTypeTable: boolTypeTable, Nullable: true},
				{Name: "fixed_prec_scale", Type: flatbuf.TypeBool, GetTypeTable: boolTypeTable, Nullable: false},
				{Name: "auto_increment", Type: flatbuf.TypeBool, GetTypeTable: boolTypeTable, Nullable: true},
				{Name: "local_type_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true},
				{Name: "minimum_scale", Type: flatbuf.TypeInt, GetTypeTable: int32TypeTable, Nullable: true},
				{Name: "maximum_scale", Type: flatbuf.TypeInt, GetTypeTable: int32TypeTable, Nullable: true},
				{Name: "sql_data_type", Type: flatbuf.TypeInt, GetTypeTable: int32TypeTable, Nullable: false},
				{Name: "datetime_subcode", Type: flatbuf.TypeInt, GetTypeTable: int32TypeTable, Nullable: true},
				{Name: "num_prec_radix", Type: flatbuf.TypeInt, GetTypeTable: int32TypeTable, Nullable: true},
				{Name: "interval_precision", Type: flatbuf.TypeInt, GetTypeTable: int32TypeTable, Nullable: true},
			},
		},
		{
			Command: &flight.CommandGetSqlInfo{Info: []uint32{0, 3}},
			Fields: []field{
				{Name: "info_name", Type: flatbuf.TypeInt, GetTypeTable: uint32TypeTable, Nullable: false},
				{Name: "value", Type: flatbuf.TypeUnion, GetTypeTable: sqlInfoValuesTypeTable, Nullable: false},
			},
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
				if err := deserializeProtobufPayload(fd.Cmd, &cmd); err != nil {
					return nil, status.Errorf(codes.InvalidArgument, "failed to deserialize FlightDescriptor.Cmd: %s", err)
				}

				if cmd.GetQuery() != stmtQuery {
					return nil, status.Errorf(codes.InvalidArgument, "expected query: %s, found: %s", stmtQuery, cmd.GetQuery())
				}

				if len(cmd.GetTransactionId()) != 0 {
					return nil, status.Errorf(codes.InvalidArgument, "expected no TransactionID")
				}

				handle, err := createStatementQueryTicket([]byte(stmtQueryHandle))
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
			if err := deserializeProtobufPayload(t.Ticket, &cmd); err != nil {
				return status.Errorf(codes.InvalidArgument, "failed to deserialize Ticket.Ticket: %s", err)
			}

			if string(cmd.GetStatementHandle()) != stmtQueryHandle {
				return status.Errorf(codes.InvalidArgument, "expected handle: %s, found: %s", stmtQueryHandle, cmd.GetStatementHandle())
			}

			return fs.Send(&flight.FlightData{DataHeader: buildFlatbufferSchema(queryFields)})
		}}},
		scenario.ScenarioStep{Name: "GetSchema/CommandStatementQuery", ServerHandler: scenario.Handler{GetSchema: getSchemaFieldsForCommandFn(&flight.CommandStatementQuery{}, queryFields)}},

		scenario.ScenarioStep{
			Name: "DoPut/CommandStatementUpdate",
			ServerHandler: scenario.Handler{DoPut: func(fs flight.FlightService_DoPutServer) error {
				data, err := fs.Recv()
				if err != nil {
					return status.Errorf(codes.Internal, "unable to read from stream: %s", err)
				}

				desc := data.FlightDescriptor
				var cmd flight.CommandStatementUpdate
				if err := deserializeProtobufPayload(desc.Cmd, &cmd); err != nil {
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

	scenario.Register(
		scenario.Scenario{
			Name:  "flight_sql",
			Steps: steps,
			RunClient: func(ctx context.Context, client flight.FlightServiceClient, t *tester.Tester) {

				// ValidateMetadataRetrieval
				for _, tc := range testcases {
					// pack the command
					desc, err := descForCommand(tc.Command)
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
					requireStreamHeaderMatchesFields(t, stream, tc.Fields)

					// drain rest of stream
					requireDrainStream(t, stream)

					// issue GetSchema
					res, err := client.GetSchema(ctx, desc)
					t.Require().NoError(err)

					// expect schema to be serialized as full IPC stream Schema message
					requireSchemaResultMatchesFields(t, res, tc.Fields)
				}

				// ValidateStatementExecution
				cmdQuery := flight.CommandStatementQuery{Query: stmtQuery}
				descQuery, err := descForCommand(&cmdQuery)
				t.Require().NoError(err)

				infoQuery, err := client.GetFlightInfo(ctx, descQuery)
				t.Require().NoError(err)

				t.Require().Greater(len(infoQuery.Endpoint), 0)

				// fetch result streamQuery
				streamQuery, err := client.DoGet(ctx, infoQuery.Endpoint[0].Ticket)
				t.Require().NoError(err)

				// validate result stream
				requireStreamHeaderMatchesFields(t, streamQuery, queryFields)
				requireDrainStream(t, streamQuery)

				schemaResultQuery, err := client.GetSchema(ctx, descQuery)
				t.Require().NoError(err)

				// expect schema to be serialized as full IPC stream Schema message
				requireSchemaResultMatchesFields(t, schemaResultQuery, queryFields)

				cmdUpdate := flight.CommandStatementUpdate{Query: stmtUpdate}
				descUpdate, err := descForCommand(&cmdUpdate)
				t.Require().NoError(err)

				streamUpdate, err := client.DoPut(ctx)
				t.Require().NoError(err)

				t.Require().NoError(streamUpdate.Send(&flight.FlightData{FlightDescriptor: descUpdate}))
				t.Require().NoError(streamUpdate.CloseSend())

				putResult, err := streamUpdate.Recv()
				t.Require().NoError(err)

				var updateResult flight.DoPutUpdateResult
				t.Require().NoError(proto.Unmarshal(putResult.GetAppMetadata(), &updateResult))

				t.Assert().Equal(stmtUpdateRows, updateResult.GetRecordCount())
			},
		},
	)
}

type field struct {
	Name         string
	Type         flatbuf.Type
	Nullable     bool
	Metadata     map[string]string
	GetTypeTable func(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT)
}

func utf8TypeTable(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
	flatbuf.Utf8Start(b)
	return flatbuf.Utf8End(b), 0
}

func binaryTypeTable(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
	flatbuf.BinaryStart(b)
	return flatbuf.BinaryEnd(b), 0
}

func int32TypeTable(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
	flatbuf.IntStart(b)
	flatbuf.IntAddBitWidth(b, 32)
	flatbuf.IntAddIsSigned(b, true)
	return flatbuf.IntEnd(b), 0
}

func int64TypeTable(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
	flatbuf.IntStart(b)
	flatbuf.IntAddBitWidth(b, 64)
	flatbuf.IntAddIsSigned(b, true)
	return flatbuf.IntEnd(b), 0
}

func uint32TypeTable(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
	flatbuf.IntStart(b)
	flatbuf.IntAddBitWidth(b, 32)
	flatbuf.IntAddIsSigned(b, false)
	return flatbuf.IntEnd(b), 0
}

func uint8TypeTable(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
	flatbuf.IntStart(b)
	flatbuf.IntAddBitWidth(b, 8)
	flatbuf.IntAddIsSigned(b, false)
	return flatbuf.IntEnd(b), 0
}

func boolTypeTable(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
	flatbuf.BoolStart(b)
	return flatbuf.BoolEnd(b), 0
}

func createParamsTypeTable(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
	return listTypeTable(b, field{Name: "item", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable})
}

func int32ListTypeTable(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
	return listTypeTable(b, field{Name: "item", Type: flatbuf.TypeInt, GetTypeTable: int32TypeTable, Nullable: true})
}

func listTypeTable(b *flatbuffers.Builder, child field) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
	childOffset := buildFlatbufferField(b, child)

	flatbuf.ListStart(b)
	listOffset := flatbuf.ListEnd(b)

	flatbuf.FieldStartChildrenVector(b, 1)
	b.PrependUOffsetT(childOffset)
	childVecOffset := b.EndVector(1)

	return listOffset, childVecOffset
}

func structTypeTable(b *flatbuffers.Builder, children []field) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
	nChildren := len(children)
	childOffsets := make([]flatbuffers.UOffsetT, nChildren)
	for i, child := range children {
		childOffsets[i] = buildFlatbufferField(b, child)
	}

	flatbuf.Struct_Start(b)
	for i := nChildren - 1; i >= 0; i-- {
		b.PrependUOffsetT(childOffsets[i])
	}
	structOffset := flatbuf.Struct_End(b)

	flatbuf.FieldStartChildrenVector(b, nChildren)
	for i := nChildren - 1; i >= 0; i-- {
		b.PrependUOffsetT(childOffsets[i])
	}
	childVecOffset := b.EndVector(nChildren)

	return structOffset, childVecOffset
}

func mapTypeTable(b *flatbuffers.Builder, key, val field) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
	childOffset := buildFlatbufferField(
		b,
		field{
			Name: "entries",
			Type: flatbuf.TypeStruct_,
			GetTypeTable: func(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
				return structTypeTable(b, []field{key, val})
			},
		},
	)

	flatbuf.MapStart(b)
	mapOffset := flatbuf.MapEnd(b)

	flatbuf.FieldStartChildrenVector(b, 1)
	b.PrependUOffsetT(childOffset)
	childVecOffset := b.EndVector(1)

	return mapOffset, childVecOffset
}

func stringListTypeTable(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
	return listTypeTable(b, field{Name: "item", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable, Nullable: true}) // TODO: nullable?
}

func int32ToInt32ListMapTypeTable(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
	return mapTypeTable(
		b,
		field{Name: "key", Type: flatbuf.TypeInt, GetTypeTable: int32TypeTable},
		field{Name: "val", Type: flatbuf.TypeList, GetTypeTable: int32ListTypeTable, Nullable: true}, // TODO: nullable?
	)
}

func sqlInfoValuesTypeTable(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
	children := []field{
		{Name: "string_value", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable},
		{Name: "bool_value", Type: flatbuf.TypeBool, GetTypeTable: boolTypeTable},
		{Name: "bigint_value", Type: flatbuf.TypeInt, GetTypeTable: int64TypeTable},
		{Name: "int32_bitmask", Type: flatbuf.TypeInt, GetTypeTable: int32TypeTable},
		{Name: "string_list", Type: flatbuf.TypeList, GetTypeTable: stringListTypeTable},
		{Name: "int32_to_int32_list_map", Type: flatbuf.TypeMap, GetTypeTable: int32ToInt32ListMapTypeTable},
	}

	childOffsets := make([]flatbuffers.UOffsetT, len(children))
	for i, child := range children {
		childOffsets[i] = buildFlatbufferField(b, child)
	}

	flatbuf.UnionStartTypeIdsVector(b, len(children))
	for i := len(children) - 1; i >= 0; i-- {
		b.PlaceInt32(int32(i))
	}
	typeIDVecOffset := b.EndVector(len(children))

	flatbuf.UnionStart(b)
	flatbuf.UnionAddMode(b, flatbuf.UnionModeDense)
	flatbuf.UnionAddTypeIds(b, typeIDVecOffset)
	unionOffset := flatbuf.UnionEnd(b)

	flatbuf.FieldStartChildrenVector(b, len(children))
	for i := len(children) - 1; i >= 0; i-- {
		b.PrependUOffsetT(childOffsets[i])
	}
	childVecOffset := b.EndVector(len(children))

	return unionOffset, childVecOffset
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

func writeFlatbufferPayload(fields []field) []byte {
	schema := buildFlatbufferSchema(fields)
	size := uint32(len(schema))

	res := make([]byte, 8+size)
	res[0] = 255
	res[1] = 255
	res[2] = 255
	res[3] = 255
	binary.LittleEndian.PutUint32(res[4:], size)
	copy(res[8:], schema)

	return res
}

func buildFlatbufferSchema(fields []field) []byte {
	b := flatbuffers.NewBuilder(1024)

	fieldOffsets := make([]flatbuffers.UOffsetT, len(fields))
	for i, f := range fields {
		fieldOffsets[len(fields)-i-1] = buildFlatbufferField(b, f)
	}

	flatbuf.SchemaStartFieldsVector(b, len(fields))

	for _, f := range fieldOffsets {
		b.PrependUOffsetT(f)
	}

	fieldsFB := b.EndVector(len(fields))

	flatbuf.SchemaStart(b)
	flatbuf.SchemaAddFields(b, fieldsFB)
	headerOffset := flatbuf.SchemaEnd(b)

	flatbuf.MessageStart(b)
	flatbuf.MessageAddVersion(b, flatbuf.MetadataVersionV5)
	flatbuf.MessageAddHeaderType(b, flatbuf.MessageHeaderSchema)
	flatbuf.MessageAddHeader(b, headerOffset)
	msg := flatbuf.MessageEnd(b)

	b.Finish(msg)

	return b.FinishedBytes()
}

func buildFlatbufferField(b *flatbuffers.Builder, f field) flatbuffers.UOffsetT {
	nameOffset := b.CreateString(f.Name)
	typOffset, childrenOffset := f.GetTypeTable(b)

	var kvOffsets []flatbuffers.UOffsetT
	for k, v := range f.Metadata {
		kk := b.CreateString(k)
		vv := b.CreateString(v)
		flatbuf.KeyValueStart(b)
		flatbuf.KeyValueAddKey(b, kk)
		flatbuf.KeyValueAddValue(b, vv)
		kvOffsets = append(kvOffsets, flatbuf.KeyValueEnd(b))
	}

	var metadataOffset flatbuffers.UOffsetT
	if len(kvOffsets) > 0 {
		flatbuf.FieldStartCustomMetadataVector(b, len(kvOffsets))
		for i := len(kvOffsets) - 1; i >= 0; i-- {
			b.PrependUOffsetT(kvOffsets[i])
		}
		metadataOffset = b.EndVector(len(kvOffsets))
	}

	flatbuf.FieldStart(b)
	flatbuf.FieldAddName(b, nameOffset)
	flatbuf.FieldAddTypeType(b, f.Type)
	flatbuf.FieldAddType(b, typOffset)
	flatbuf.FieldAddChildren(b, childrenOffset)
	flatbuf.FieldAddCustomMetadata(b, metadataOffset)
	flatbuf.FieldAddNullable(b, f.Nullable)
	return flatbuf.FieldEnd(b)
}

func parseFlatbufferSchemaFields(msg *flatbuf.Message) ([]flatbuf.Field, bool) {
	var schema flatbuf.Schema
	table := schema.Table()
	if ok := msg.Header(&table); !ok {
		return nil, false
	}
	schema.Init(table.Bytes, table.Pos)

	fields := make([]flatbuf.Field, schema.FieldsLength())
	for i := range fields {
		var field flatbuf.Field
		if ok := schema.Fields(&field, i); !ok {
			return nil, false
		}
		fields[i] = field
	}
	return fields, true
}

func extractFlatbufferPayload(b []byte) []byte {
	b = consumeContinuationIndicator(b)
	b, size := consumeMetadataSize(b)
	return b[:size]
}

func consumeMetadataSize(b []byte) ([]byte, int32) {
	size := int32(binary.LittleEndian.Uint32(b[:4]))
	return b[4:], size
}

func consumeContinuationIndicator(b []byte) []byte {
	indicator := []byte{255, 255, 255, 255}
	for i, v := range indicator {
		if b[i] != v {
			// indicator not found
			return b
		}
	}
	// indicator found, truncate leading bytes
	return b[4:]
}

func descForCommand(cmd proto.Message) (*flight.FlightDescriptor, error) {
	var any anypb.Any
	if err := any.MarshalFrom(cmd); err != nil {
		return nil, err
	}

	data, err := proto.Marshal(&any)
	if err != nil {
		return nil, err
	}
	return &flight.FlightDescriptor{
		Type: flight.FlightDescriptor_CMD,
		Cmd:  data,
	}, nil
}

func echoFlightInfo(ctx context.Context, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return &flight.FlightInfo{
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: fd.Cmd},
		}},
	}, nil
}

func doGetFieldsForCommandFn(cmd proto.Message, fields []field) func(t *flight.Ticket, fs flight.FlightService_DoGetServer) error {
	return func(t *flight.Ticket, fs flight.FlightService_DoGetServer) error {
		cmd := proto.Clone(cmd)
		proto.Reset(cmd)

		if err := deserializeProtobufPayload(t.Ticket, cmd); err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to deserialize Ticket.Ticket: %s", err)
		}

		return fs.Send(&flight.FlightData{DataHeader: buildFlatbufferSchema(fields)})
	}
}

func getSchemaFieldsForCommandFn(cmd proto.Message, fields []field) func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	return func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.SchemaResult, error) {
		cmd := proto.Clone(cmd)
		proto.Reset(cmd)

		if err := deserializeProtobufPayload(fd.Cmd, cmd); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "failed to deserialize FlightDescriptor.Cmd: %s", err)
		}

		schema := writeFlatbufferPayload(fields)

		return &flight.SchemaResult{Schema: schema}, nil
	}
}

func deserializeProtobufPayload(b []byte, dst proto.Message) error {
	var anycmd anypb.Any
	if err := proto.Unmarshal(b, &anycmd); err != nil {
		return fmt.Errorf("unable to unmarshal payload to proto.Any: %s", err)
	}

	if err := anycmd.UnmarshalTo(dst); err != nil {
		return fmt.Errorf("unable to unmarshal proto.Any: %s", err)
	}

	return nil
}

func createStatementQueryTicket(handle []byte) ([]byte, error) {
	query := &flight.TicketStatementQuery{StatementHandle: handle}

	var ticket anypb.Any
	if err := ticket.MarshalFrom(query); err != nil {
		return nil, fmt.Errorf("unable to marshal ticket proto to proto.Any: %s", err)
	}

	b, err := proto.Marshal(&ticket)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal proto.Any to bytes: %s", err)
	}

	return b, nil
}

func requireStreamHeaderMatchesFields[T interface {
	Recv() (*flight.FlightData, error)
}](t *tester.Tester, stream T, expectedFields []field) {

	data, err := stream.Recv()
	t.Require().NoError(err)

	msg := flatbuf.GetRootAsMessage(data.DataHeader, 0)
	t.Require().Equal(flatbuf.MessageHeaderSchema, msg.HeaderType())

	fields, ok := parseFlatbufferSchemaFields(msg)
	t.Require().True(ok)
	t.Require().Len(fields, len(expectedFields))

	for _, expectedField := range expectedFields {
		field, found := matchFieldByName(fields, expectedField.Name)
		t.Require().Truef(found, "no matching field with expected name \"%s\" found in flatbuffer schema", expectedField.Name)

		t.Assert().Equal(expectedField.Name, string(field.Name()))
		t.Assert().Equal(expectedField.Type, field.TypeType())
		t.Assert().Equal(expectedField.Nullable, field.Nullable())
	}
}

func requireDrainStream[T interface {
	Recv() (*flight.FlightData, error)
}](t *tester.Tester, stream T) {
	for {
		data, err := stream.Recv()
		if err == io.EOF {
			break
		}
		t.Require().NoError(err)

		// no more schema messages
		t.Assert().Contains(
			[]flatbuf.MessageHeader{
				flatbuf.MessageHeaderRecordBatch,
				flatbuf.MessageHeaderDictionaryBatch,
			},
			flatbuf.GetRootAsMessage(data.DataHeader, 0).HeaderType(),
		)
	}
}

func requireSchemaResultMatchesFields(t *tester.Tester, res *flight.SchemaResult, expectedFields []field) {
	metadata := extractFlatbufferPayload(res.Schema)

	msg := flatbuf.GetRootAsMessage(metadata, 0)
	t.Require().Equal(flatbuf.MessageHeaderSchema, msg.HeaderType())

	fields, ok := parseFlatbufferSchemaFields(msg)
	t.Require().True(ok)
	t.Require().Len(fields, len(expectedFields))

	for _, expectedField := range expectedFields {
		field, found := matchFieldByName(fields, expectedField.Name)
		t.Require().Truef(found, "no matching field with expected name \"%s\" found in flatbuffer schema", expectedField.Name)

		t.Assert().Equal(expectedField.Name, string(field.Name()))
		t.Assert().Equal(expectedField.Type, field.TypeType())
		t.Assert().Equal(expectedField.Nullable, field.Nullable())
	}
}

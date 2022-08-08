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

package flightsql

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/flight"
	"github.com/apache/arrow/go/v10/arrow/flight/flightsql/schema_ref"
	pb "github.com/apache/arrow/go/v10/arrow/flight/internal/flight"
	"github.com/apache/arrow/go/v10/arrow/internal/debug"
	"github.com/apache/arrow/go/v10/arrow/ipc"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type StatementQuery interface {
	GetQuery() string
}

type StatementUpdate interface {
	GetQuery() string
}

type StatementQueryTicket interface {
	GetStatementHandle() []byte
}

type PreparedStatementQuery interface {
	GetPreparedStatementHandle() []byte
}

type PreparedStatementUpdate interface {
	GetPreparedStatementHandle() []byte
}

type ActionClosePreparedStatementRequest interface {
	GetPreparedStatementHandle() []byte
}

type ActionCreatePreparedStatementRequest interface {
	GetQuery() string
}

type ActionCreatePreparedStatementResult struct {
	Handle          []byte
	DatasetSchema   *arrow.Schema
	ParameterSchema *arrow.Schema
}

type getXdbcTypeInfo struct {
	*pb.CommandGetXdbcTypeInfo
}

func (c *getXdbcTypeInfo) GetDataType() *int32 { return c.DataType }

type GetXdbcTypeInfo interface {
	GetDataType() *int32
}

type GetSqlInfo interface {
	GetInfo() []uint32
}

type getDBSchemas struct {
	*pb.CommandGetDbSchemas
}

func (c *getDBSchemas) GetCatalog() *string               { return c.Catalog }
func (c *getDBSchemas) GetDBSchemaFilterPattern() *string { return c.DbSchemaFilterPattern }

type GetDBSchemas interface {
	GetCatalog() *string
	GetDBSchemaFilterPattern() *string
}

type getTables struct {
	*pb.CommandGetTables
}

func (c *getTables) GetCatalog() *string                { return c.Catalog }
func (c *getTables) GetDBSchemaFilterPattern() *string  { return c.DbSchemaFilterPattern }
func (c *getTables) GetTableNameFilterPattern() *string { return c.TableNameFilterPattern }

type GetTables interface {
	GetCatalog() *string
	GetDBSchemaFilterPattern() *string
	GetTableNameFilterPattern() *string
	GetTableTypes() []string
	GetIncludeSchema() bool
}

type BaseServer struct {
	sqlInfoToResult SqlInfoResultMap
}

func (BaseServer) mustEmbedBaseServer() {}

func (b *BaseServer) RegisterSqlInfo(id uint32, result interface{}) error {
	switch result.(type) {
	case string, bool, int64, int32, []string, map[int32][]int32:
	default:
		return fmt.Errorf("invalid sql info type '%T' registered for id: %d", result, id)
	}
	return nil
}

func (BaseServer) GetFlightInfoStatement(context.Context, StatementQuery, *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return nil, status.Errorf(codes.Unimplemented, "GetFlightInfoStatement not implemented")
}

func (BaseServer) DoGetStatement(context.Context, StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return nil, nil, status.Errorf(codes.Unimplemented, "DoGetStatement not implemented")
}

func (BaseServer) GetFlightInfoPreparedStatement(context.Context, PreparedStatementQuery, *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return nil, status.Errorf(codes.Unimplemented, "GetFlightInfoPreparedStatement not implemented")
}

func (BaseServer) DoGetPreparedStatement(context.Context, PreparedStatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return nil, nil, status.Errorf(codes.Unimplemented, "DoGetPreparedStatement not implemented")
}

func (BaseServer) GetFlightInfoCatalogs(context.Context, *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return nil, status.Errorf(codes.Unimplemented, "GetFlightInfoCatalogs not implemented")
}

func (BaseServer) DoGetCatalogs(context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return nil, nil, status.Errorf(codes.Unimplemented, "DoGetCatalogs not implemented")
}

func (BaseServer) GetFlightInfoXdbcTypeInfo(context.Context, GetXdbcTypeInfo, *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return nil, status.Errorf(codes.Unimplemented, "GetFlightInfoXdbcTypeInfo not implemented")
}

func (BaseServer) DoGetXdbcTypeInfo(context.Context, GetXdbcTypeInfo) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return nil, nil, status.Errorf(codes.Unimplemented, "DoGetXdbcTypeInfo not implemented")
}

func (BaseServer) GetFlightInfoSqlInfo(context.Context, GetSqlInfo, *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return nil, status.Errorf(codes.Unimplemented, "GetFlightInfoSqlInfo not implemented")
}

func (b *BaseServer) DoGetSqlInfo(_ context.Context, cmd GetSqlInfo) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	mem := memory.DefaultAllocator
	bldr := array.NewRecordBuilder(mem, schema_ref.SqlInfo)
	defer bldr.Release()

	nameFieldBldr := bldr.Field(0).(*array.Uint32Builder)
	valFieldBldr := bldr.Field(1).(*array.DenseUnionBuilder)

	// doesn't take ownership, no calls to retain. so we don't need
	// extra releases.
	sqlInfoResultBldr := newSqlInfoResultBuilder(valFieldBldr)

	// populate both the nameFieldBldr and the values for each
	// element on command.info.
	// valueFieldBldr is populated depending on the data type
	// since it's a dense union. The population for each
	// data type is handled by the sqlInfoResultBuilder.
	for _, info := range cmd.GetInfo() {
		val, ok := b.sqlInfoToResult[info]
		if !ok {
			return nil, nil, status.Errorf(codes.NotFound, "no information for sql info number %d", info)
		}
		nameFieldBldr.Append(info)
		sqlInfoResultBldr.Append(val)
	}

	batch := bldr.NewRecord()
	defer batch.Release()
	debug.Assert(int(batch.NumRows()) == len(cmd.GetInfo()), "too many rows added to SqlInfo result")

	ch := make(chan flight.StreamChunk)
	rdr, err := array.NewRecordReader(schema_ref.SqlInfo, []arrow.Record{batch})
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "error producing record response: %s", err.Error())
	}

	go flight.StreamChunksFromReader(rdr, ch)
	return schema_ref.SqlInfo, ch, nil
}

func (BaseServer) GetFlightInfoSchemas(context.Context, GetDBSchemas, *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return nil, status.Errorf(codes.Unimplemented, "GetFlightInfoSchemas not implemented")
}

func (BaseServer) DoGetDBSchemas(context.Context, GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return nil, nil, status.Errorf(codes.Unimplemented, "DoGetDBSchemas not implemented")
}

func (BaseServer) GetFlightInfoTables(context.Context, GetTables, *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return nil, status.Errorf(codes.Unimplemented, "GetTables not implemented")
}

func (BaseServer) DoGetTables(context.Context, GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return nil, nil, status.Errorf(codes.Unimplemented, "DoGetTables not implemented")
}

func (BaseServer) GetFlightInfoTableTypes(context.Context, *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return nil, status.Errorf(codes.Unimplemented, "GetFlightInfoTableTypes not implemented")
}

func (BaseServer) DoGetTableTypes(context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return nil, nil, status.Errorf(codes.Unimplemented, "DoGetTableTypes not implemented")
}

func (BaseServer) GetFlightInfoPrimaryKeys(context.Context, TableRef, *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return nil, status.Error(codes.Unimplemented, "GetFlightInfoPrimaryKeys not implemented")
}

func (BaseServer) DoGetPrimaryKeys(context.Context, TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return nil, nil, status.Errorf(codes.Unimplemented, "DoGetPrimaryKeys not implemented")
}

func (BaseServer) GetFlightInfoExportedKeys(context.Context, TableRef, *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return nil, status.Error(codes.Unimplemented, "GetFlightInfoExportedKeys not implemented")
}

func (BaseServer) DoGetExportedKeys(context.Context, TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return nil, nil, status.Errorf(codes.Unimplemented, "DoGetExportedKeys not implemented")
}

func (BaseServer) GetFlightInfoImportedKeys(context.Context, TableRef, *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return nil, status.Error(codes.Unimplemented, "GetFlightInfoImportedKeys not implemented")
}

func (BaseServer) DoGetImportedKeys(context.Context, TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return nil, nil, status.Errorf(codes.Unimplemented, "DoGetImportedKeys not implemented")
}

func (BaseServer) GetFlightInfoCrossReference(context.Context, CrossTableRef, *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return nil, status.Error(codes.Unimplemented, "GetFlightInfoCrossReference not implemented")
}

func (BaseServer) DoGetCrossReference(context.Context, CrossTableRef) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	return nil, nil, status.Errorf(codes.Unimplemented, "DoGetCrossReference not implemented")
}

func (BaseServer) CreatePreparedStatement(context.Context, ActionCreatePreparedStatementRequest) (res ActionCreatePreparedStatementResult, err error) {
	return res, status.Error(codes.Unimplemented, "CreatePreparedStatement not implemented")
}

func (BaseServer) ClosePreparedStatement(context.Context, ActionClosePreparedStatementRequest) error {
	return status.Error(codes.Unimplemented, "ClosePreparedStatement not implemented")
}

func (BaseServer) DoPutCommandStatementUpdate(context.Context, StatementUpdate) (int64, error) {
	return 0, status.Error(codes.Unimplemented, "DoPutCommandStatementUpdate not implemented")
}
func (BaseServer) DoPutPreparedStatementQuery(context.Context, PreparedStatementQuery, flight.MessageReader, flight.MetadataWriter) error {
	return status.Error(codes.Unimplemented, "DoPutPreparedStatementQuery not implemented")
}

func (BaseServer) DoPutPreparedStatementUpdate(context.Context, PreparedStatementUpdate, flight.MessageReader) (int64, error) {
	return 0, status.Error(codes.Unimplemented, "DoPutPreparedStatementUpdate not implemented")
}

type Server interface {
	mustEmbedBaseServer()
	GetFlightInfoStatement(context.Context, StatementQuery, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	DoGetStatement(context.Context, StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error)
	GetFlightInfoPreparedStatement(context.Context, PreparedStatementQuery, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	DoGetPreparedStatement(context.Context, PreparedStatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error)
	GetFlightInfoCatalogs(context.Context, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	DoGetCatalogs(context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error)
	GetFlightInfoXdbcTypeInfo(context.Context, GetXdbcTypeInfo, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	DoGetXdbcTypeInfo(context.Context, GetXdbcTypeInfo) (*arrow.Schema, <-chan flight.StreamChunk, error)
	GetFlightInfoSqlInfo(context.Context, GetSqlInfo, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	DoGetSqlInfo(context.Context, GetSqlInfo) (*arrow.Schema, <-chan flight.StreamChunk, error)
	GetFlightInfoSchemas(context.Context, GetDBSchemas, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	DoGetDBSchemas(context.Context, GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error)
	GetFlightInfoTables(context.Context, GetTables, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	DoGetTables(context.Context, GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error)
	GetFlightInfoTableTypes(context.Context, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	DoGetTableTypes(context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error)
	GetFlightInfoPrimaryKeys(context.Context, TableRef, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	DoGetPrimaryKeys(context.Context, TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error)
	GetFlightInfoExportedKeys(context.Context, TableRef, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	DoGetExportedKeys(context.Context, TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error)
	GetFlightInfoImportedKeys(context.Context, TableRef, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	DoGetImportedKeys(context.Context, TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error)
	GetFlightInfoCrossReference(context.Context, CrossTableRef, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	DoGetCrossReference(context.Context, CrossTableRef) (*arrow.Schema, <-chan flight.StreamChunk, error)
	DoPutCommandStatementUpdate(context.Context, StatementUpdate) (int64, error)
	DoPutPreparedStatementQuery(context.Context, PreparedStatementQuery, flight.MessageReader, flight.MetadataWriter) error
	DoPutPreparedStatementUpdate(context.Context, PreparedStatementUpdate, flight.MessageReader) (int64, error)
	CreatePreparedStatement(context.Context, ActionCreatePreparedStatementRequest) (ActionCreatePreparedStatementResult, error)
	ClosePreparedStatement(context.Context, ActionClosePreparedStatementRequest) error
}

func NewFlightServer(srv Server) flight.FlightServer {
	return &flightSqlServer{srv: srv}
}

type flightSqlServer struct {
	flight.BaseFlightServer
	mem memory.Allocator
	srv Server
}

func (f *flightSqlServer) GetFlightInfo(ctx context.Context, request *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	var (
		anycmd anypb.Any
		cmd    proto.Message
		err    error
	)
	if err = proto.Unmarshal(request.Cmd, &anycmd); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unable to parse command: %s", err.Error())
	}

	if cmd, err = anycmd.UnmarshalNew(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "could not unmarshal Any to a command type: %s", err.Error())
	}

	switch cmd := cmd.(type) {
	case *pb.CommandStatementQuery:
		return f.srv.GetFlightInfoStatement(ctx, cmd, request)
	case *pb.CommandPreparedStatementQuery:
		return f.srv.GetFlightInfoPreparedStatement(ctx, cmd, request)
	case *pb.CommandGetCatalogs:
		return f.srv.GetFlightInfoCatalogs(ctx, request)
	case *pb.CommandGetDbSchemas:
		return f.srv.GetFlightInfoSchemas(ctx, &getDBSchemas{cmd}, request)
	case *pb.CommandGetTables:
		return f.srv.GetFlightInfoTables(ctx, &getTables{cmd}, request)
	case *pb.CommandGetTableTypes:
		return f.srv.GetFlightInfoTableTypes(ctx, request)
	case *pb.CommandGetXdbcTypeInfo:
		return f.srv.GetFlightInfoXdbcTypeInfo(ctx, &getXdbcTypeInfo{cmd}, request)
	case *pb.CommandGetSqlInfo:
		return f.srv.GetFlightInfoSqlInfo(ctx, cmd, request)
	case *pb.CommandGetPrimaryKeys:
		return f.srv.GetFlightInfoPrimaryKeys(ctx, pkToTableRef(cmd), request)
	case *pb.CommandGetExportedKeys:
		return f.srv.GetFlightInfoExportedKeys(ctx, exkToTableRef(cmd), request)
	case *pb.CommandGetImportedKeys:
		return f.srv.GetFlightInfoImportedKeys(ctx, impkToTableRef(cmd), request)
	case *pb.CommandGetCrossReference:
		return f.srv.GetFlightInfoCrossReference(ctx, toCrossTableRef(cmd), request)
	}

	return nil, status.Error(codes.InvalidArgument, "requested command is invalid")
}

func (f *flightSqlServer) DoGet(request *flight.Ticket, stream flight.FlightService_DoGetServer) (err error) {
	var (
		anycmd anypb.Any
		cmd    proto.Message
		cc     <-chan flight.StreamChunk
		sc     *arrow.Schema
	)
	if err = proto.Unmarshal(request.Ticket, &anycmd); err != nil {
		return status.Errorf(codes.InvalidArgument, "unable to parse ticket: %s", err.Error())
	}

	if cmd, err = anycmd.UnmarshalNew(); err != nil {
		return status.Errorf(codes.InvalidArgument, "unable to unmarshal proto.Any: %s", err.Error())
	}

	switch cmd := cmd.(type) {
	case *pb.TicketStatementQuery:
		sc, cc, err = f.srv.DoGetStatement(stream.Context(), cmd)
	case *pb.CommandPreparedStatementQuery:
		sc, cc, err = f.srv.DoGetPreparedStatement(stream.Context(), cmd)
	case *pb.CommandGetCatalogs:
		sc, cc, err = f.srv.DoGetCatalogs(stream.Context())
	case *pb.CommandGetDbSchemas:
		sc, cc, err = f.srv.DoGetDBSchemas(stream.Context(), &getDBSchemas{cmd})
	case *pb.CommandGetTables:
		sc, cc, err = f.srv.DoGetTables(stream.Context(), &getTables{cmd})
	case *pb.CommandGetTableTypes:
		sc, cc, err = f.srv.DoGetTableTypes(stream.Context())
	case *pb.CommandGetXdbcTypeInfo:
		sc, cc, err = f.srv.DoGetXdbcTypeInfo(stream.Context(), &getXdbcTypeInfo{cmd})
	case *pb.CommandGetSqlInfo:
		sc, cc, err = f.srv.DoGetSqlInfo(stream.Context(), cmd)
	case *pb.CommandGetPrimaryKeys:
		sc, cc, err = f.srv.DoGetPrimaryKeys(stream.Context(), pkToTableRef(cmd))
	case *pb.CommandGetExportedKeys:
		sc, cc, err = f.srv.DoGetExportedKeys(stream.Context(), exkToTableRef(cmd))
	case *pb.CommandGetImportedKeys:
		sc, cc, err = f.srv.DoGetImportedKeys(stream.Context(), impkToTableRef(cmd))
	case *pb.CommandGetCrossReference:
		sc, cc, err = f.srv.DoGetCrossReference(stream.Context(), toCrossTableRef(cmd))
	default:
		return status.Error(codes.InvalidArgument, "requested command is invalid")
	}

	if err != nil {
		return err
	}

	wr := flight.NewRecordWriter(stream, ipc.WithSchema(sc))
	defer wr.Close()

	for chunk := range cc {
		wr.SetFlightDescriptor(chunk.Desc)
		if err = wr.WriteWithAppMetadata(chunk.Data, chunk.AppMetadata); err != nil {
			return err
		}
		chunk.Data.Release()
	}

	return err
}

type putMetadataWriter struct {
	stream flight.FlightService_DoPutServer
}

func (p *putMetadataWriter) WriteMetadata(appMetadata []byte) error {
	return p.stream.Send(&flight.PutResult{AppMetadata: appMetadata})
}

func (f *flightSqlServer) DoPut(stream flight.FlightService_DoPutServer) error {
	rdr, err := flight.NewRecordReader(stream, ipc.WithAllocator(f.mem))
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "failed to read input stream: %s", err.Error())
	}

	// flight descriptor should have come with the schema message
	request := rdr.LatestFlightDescriptor()

	var (
		anycmd anypb.Any
		cmd    proto.Message
	)
	if err = proto.Unmarshal(request.Cmd, &anycmd); err != nil {
		return status.Errorf(codes.InvalidArgument, "unable to parse command: %s", err.Error())
	}

	if cmd, err = anycmd.UnmarshalNew(); err != nil {
		return status.Errorf(codes.InvalidArgument, "could not unmarshal google.protobuf.Any: %s", err.Error())
	}

	switch cmd := cmd.(type) {
	case *pb.CommandStatementUpdate:
		recordCount, err := f.srv.DoPutCommandStatementUpdate(stream.Context(), cmd)
		if err != nil {
			return err
		}

		result := pb.DoPutUpdateResult{RecordCount: recordCount}
		out := &flight.PutResult{}
		if out.AppMetadata, err = proto.Marshal(&result); err != nil {
			return status.Errorf(codes.Internal, "failed to marshal PutResult: %s", err.Error())
		}
		return stream.Send(out)
	case *pb.CommandPreparedStatementQuery:
		return f.srv.DoPutPreparedStatementQuery(stream.Context(), cmd, rdr, &putMetadataWriter{stream})
	case *pb.CommandPreparedStatementUpdate:
		recordCount, err := f.srv.DoPutPreparedStatementUpdate(stream.Context(), cmd, rdr)
		if err != nil {
			return err
		}

		result := pb.DoPutUpdateResult{RecordCount: recordCount}
		out := &flight.PutResult{}
		if out.AppMetadata, err = proto.Marshal(&result); err != nil {
			return status.Errorf(codes.Internal, "failed to marshal PutResult: %s", err.Error())
		}
		return stream.Send(out)
	default:
		return status.Error(codes.InvalidArgument, "the defined request is invalid")
	}
}

func (f *flightSqlServer) ListActions(_ *flight.Empty, stream flight.FlightService_ListActionsServer) error {
	actions := []string{CreatePreparedStatementActionType, ClosePreparedStatementActionType}

	for _, a := range actions {
		if err := stream.Send(&flight.ActionType{Type: a}); err != nil {
			return err
		}
	}
	return nil
}

func (f *flightSqlServer) DoAction(cmd *flight.Action, stream flight.FlightService_DoActionServer) error {
	var anycmd anypb.Any

	switch cmd.Type {
	case CreatePreparedStatementActionType:
		if err := proto.Unmarshal(cmd.Body, &anycmd); err != nil {
			return status.Errorf(codes.InvalidArgument, "unable to parse command: %s", err.Error())
		}

		var (
			request pb.ActionCreatePreparedStatementRequest
			result  pb.ActionCreatePreparedStatementResult
			ret     pb.Result
		)
		if err := anycmd.UnmarshalTo(&request); err != nil {
			return status.Errorf(codes.InvalidArgument, "unable to unmarshal google.protobuf.Any: %s", err.Error())
		}

		output, err := f.srv.CreatePreparedStatement(stream.Context(), &request)
		if err != nil {
			return err
		}

		result.PreparedStatementHandle = output.Handle
		if output.DatasetSchema != nil {
			result.DatasetSchema = flight.SerializeSchema(output.DatasetSchema, f.mem)
		}
		if output.ParameterSchema != nil {
			result.ParameterSchema = flight.SerializeSchema(output.ParameterSchema, f.mem)
		}

		if err := anycmd.MarshalFrom(&result); err != nil {
			return status.Errorf(codes.Internal, "unable to marshal final response: %s", err.Error())
		}

		if ret.Body, err = proto.Marshal(&anycmd); err != nil {
			return status.Errorf(codes.Internal, "unable to marshal result: %s", err.Error())
		}
		return stream.Send(&ret)
	case ClosePreparedStatementActionType:
		if err := proto.Unmarshal(cmd.Body, &anycmd); err != nil {
			return status.Errorf(codes.InvalidArgument, "unable to parse command: %s", err.Error())
		}

		var request pb.ActionClosePreparedStatementRequest
		if err := anycmd.UnmarshalTo(&request); err != nil {
			return status.Errorf(codes.InvalidArgument, "unable to unmarshal google.protobuf.Any: %s", err.Error())
		}

		if err := f.srv.ClosePreparedStatement(stream.Context(), &request); err != nil {
			return err
		}

		return stream.Send(&pb.Result{})
	default:
		return status.Error(codes.InvalidArgument, "the defined request is invalid.")
	}
}

var (
	_ Server = (*BaseServer)(nil)
)

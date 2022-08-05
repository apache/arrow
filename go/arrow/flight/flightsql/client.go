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
	"errors"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/flight"
	pb "github.com/apache/arrow/go/v10/arrow/flight/internal/flight"
	"github.com/apache/arrow/go/v10/arrow/ipc"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type Client struct {
	Client flight.Client
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
		Type: flight.DescriptorCMD,
		Cmd:  data,
	}, nil
}

func flightInfoForCommand(ctx context.Context, cl *Client, cmd proto.Message, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	desc, err := descForCommand(cmd)
	if err != nil {
		return nil, err
	}
	return cl.GetFlightInfo(ctx, desc, opts...)
}

// Execute executes the desired query on the server and returns a FlightInfo
// object describing where to retrieve the results.
func (c *Client) Execute(ctx context.Context, query string, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	cmd := pb.CommandStatementQuery{Query: query}
	return flightInfoForCommand(ctx, c, &cmd, opts...)
}

// ExecuteUpdate is for executing an update query and only returns the number of affected rows.
func (c *Client) ExecuteUpdate(ctx context.Context, query string, opts ...grpc.CallOption) (n int64, err error) {
	var (
		cmd          pb.CommandStatementQuery
		desc         *flight.FlightDescriptor
		stream       pb.FlightService_DoPutClient
		res          *pb.PutResult
		updateResult pb.DoPutUpdateResult
	)

	cmd.Query = query
	if desc, err = descForCommand(&cmd); err != nil {
		return
	}

	if stream, err = c.Client.DoPut(ctx, opts...); err != nil {
		return
	}

	if err = stream.Send(&flight.FlightData{FlightDescriptor: desc}); err != nil {
		return
	}

	if err = stream.CloseSend(); err != nil {
		return
	}

	if res, err = stream.Recv(); err != nil {
		return
	}

	if err = proto.Unmarshal(res.GetAppMetadata(), &updateResult); err != nil {
		return
	}

	return updateResult.GetRecordCount(), nil
}

// GetCatalogs requests the list of catalogs from the server and
// returns a flightInfo object where the response can be retrieved
func (c *Client) GetCatalogs(ctx context.Context, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	return flightInfoForCommand(ctx, c, &pb.CommandGetCatalogs{}, opts...)
}

// GetDBSchemas requests the list of schemas from the database and
// returns a FlightInfo object where the response can be retrieved
func (c *Client) GetDBSchemas(ctx context.Context, cmdOpts *GetDBSchemasOpts, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	return flightInfoForCommand(ctx, c, cmdOpts, opts...)
}

// DoGet uses the provided flight ticket to request the stream of data.
// It returns a recordbatch reader to stream the results. Release
// should be called on the reader when done.
func (c *Client) DoGet(ctx context.Context, in *flight.Ticket, opts ...grpc.CallOption) (*flight.Reader, error) {
	stream, err := c.Client.DoGet(ctx, in, opts...)
	if err != nil {
		return nil, err
	}

	return flight.NewRecordReader(stream)
}

// GetTables requests a list of tables from the server, with the provided
// options describing how to make the request (filter patterns, if the schema
// should be returned, etc.). Returns a FlightInfo object where the response
// can be retrieved.
func (c *Client) GetTables(ctx context.Context, reqOptions *GetTablesOpts, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	return flightInfoForCommand(ctx, c, reqOptions, opts...)
}

func (c *Client) GetPrimaryKeys(ctx context.Context, ref TableRef, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	cmd := pb.CommandGetPrimaryKeys{
		Catalog:  ref.Catalog,
		DbSchema: ref.DBSchema,
		Table:    ref.Table,
	}
	return flightInfoForCommand(ctx, c, &cmd, opts...)
}

func (c *Client) GetExportedKeys(ctx context.Context, ref TableRef, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	cmd := pb.CommandGetExportedKeys{
		Catalog:  ref.Catalog,
		DbSchema: ref.DBSchema,
		Table:    ref.Table,
	}
	return flightInfoForCommand(ctx, c, &cmd, opts...)
}

func (c *Client) GetImportedKeys(ctx context.Context, ref TableRef, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	cmd := pb.CommandGetImportedKeys{
		Catalog:  ref.Catalog,
		DbSchema: ref.DBSchema,
		Table:    ref.Table,
	}
	return flightInfoForCommand(ctx, c, &cmd, opts...)
}

func (c *Client) GetCrossReference(ctx context.Context, pkTable, fkTable TableRef, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	cmd := pb.CommandGetCrossReference{
		PkCatalog:  pkTable.Catalog,
		PkDbSchema: pkTable.DBSchema,
		PkTable:    pkTable.Table,
		FkCatalog:  fkTable.Catalog,
		FkDbSchema: fkTable.DBSchema,
		FkTable:    fkTable.Table,
	}
	return flightInfoForCommand(ctx, c, &cmd, opts...)
}

func (c *Client) GetTableTypes(ctx context.Context, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	return flightInfoForCommand(ctx, c, &pb.CommandGetTableTypes{}, opts...)
}

func (c *Client) GetXdbcTypeInfo(ctx context.Context, dataType *int32, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	return flightInfoForCommand(ctx, c, &pb.CommandGetXdbcTypeInfo{DataType: dataType}, opts...)
}

func (c *Client) GetSqlInfo(ctx context.Context, info []SqlInfo, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	cmd := &pb.CommandGetSqlInfo{Info: make([]uint32, len(info))}

	for i, v := range info {
		cmd.Info[i] = uint32(v)
	}
	return flightInfoForCommand(ctx, c, cmd, opts...)
}

func (c *Client) Prepare(ctx context.Context, mem memory.Allocator, query string, opts ...grpc.CallOption) (prep *PreparedStatement, err error) {
	const actionType = "CreatePreparedStatement"
	var (
		cmd, cmdResult        anypb.Any
		res                   *pb.Result
		request               pb.ActionCreatePreparedStatementRequest
		result                pb.ActionCreatePreparedStatementResult
		action                pb.Action
		stream                pb.FlightService_DoActionClient
		dsSchema, paramSchema *arrow.Schema
	)

	request.Query = query
	if err = cmd.MarshalFrom(&request); err != nil {
		return
	}

	action.Type = actionType
	if action.Body, err = proto.Marshal(&cmd); err != nil {
		return
	}

	if stream, err = c.Client.DoAction(ctx, &action, opts...); err != nil {
		return
	}

	if res, err = stream.Recv(); err != nil {
		return
	}

	if err = proto.Unmarshal(res.Body, &cmdResult); err != nil {
		return
	}

	if err = cmdResult.UnmarshalTo(&result); err != nil {
		return
	}

	dsSchema, err = flight.DeserializeSchema(result.DatasetSchema, mem)
	if err != nil {
		return
	}
	paramSchema, err = flight.DeserializeSchema(result.ParameterSchema, mem)
	if err != nil {
		return
	}

	prep = &PreparedStatement{
		client:        c,
		opts:          opts,
		handle:        result.PreparedStatementHandle,
		datasetSchema: dsSchema,
		paramSchema:   paramSchema,
	}
	return
}

func (c *Client) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	return c.Client.GetFlightInfo(ctx, desc, opts...)
}

func (c *Client) Close() error { return c.Client.Close() }

type PreparedStatement struct {
	client        *Client
	opts          []grpc.CallOption
	handle        []byte
	datasetSchema *arrow.Schema
	paramSchema   *arrow.Schema
	paramBinding  arrow.Record
	closed        bool
}

func (p *PreparedStatement) Execute(ctx context.Context) (*flight.FlightInfo, error) {
	if p.closed {
		return nil, errors.New("arrow/flightsql: prepared statement already closed")
	}

	cmd := &pb.CommandPreparedStatementQuery{PreparedStatementHandle: p.handle}

	desc, err := descForCommand(cmd)
	if err != nil {
		return nil, err
	}

	if p.paramBinding != nil && p.paramBinding.NumRows() > 0 {
		pstream, err := p.client.Client.DoPut(ctx, p.opts...)
		if err != nil {
			return nil, err
		}

		wr := flight.NewRecordWriter(pstream, ipc.WithSchema(p.paramBinding.Schema()))
		wr.SetFlightDescriptor(desc)
		if err = wr.Write(p.paramBinding); err != nil {
			return nil, err
		}
		if err = wr.Close(); err != nil {
			return nil, err
		}
		pstream.CloseSend()

		// wait for the server to ack the result
		if _, err = pstream.Recv(); err != nil {
			return nil, err
		}
	}

	return p.client.GetFlightInfo(ctx, desc, p.opts...)
}

func (p *PreparedStatement) ExecuteUpdate(ctx context.Context) (nrecords int64, err error) {
	if p.closed {
		return 0, errors.New("arrow/flightsql: prepared statement already closed")
	}

	var (
		execCmd      = &pb.CommandPreparedStatementUpdate{PreparedStatementHandle: p.handle}
		desc         *flight.FlightDescriptor
		pstream      pb.FlightService_DoPutClient
		wr           *flight.Writer
		res          *pb.PutResult
		updateResult pb.DoPutUpdateResult
	)

	desc, err = descForCommand(execCmd)
	if err != nil {
		return
	}

	if pstream, err = p.client.Client.DoPut(ctx, p.opts...); err != nil {
		return
	}
	if p.paramBinding != nil && p.paramBinding.NumRows() > 0 {
		wr = flight.NewRecordWriter(pstream, ipc.WithSchema(p.paramBinding.Schema()))
		wr.SetFlightDescriptor(desc)
		if err = wr.Write(p.paramBinding); err != nil {
			return
		}
	} else {
		schema := arrow.NewSchema([]arrow.Field{}, nil)
		wr = flight.NewRecordWriter(pstream, ipc.WithSchema(p.paramBinding.Schema()))
		wr.SetFlightDescriptor(desc)
		rec := array.NewRecord(schema, []arrow.Array{}, 0)
		if err = wr.Write(rec); err != nil {
			return
		}
	}

	if err = wr.Close(); err != nil {
		return
	}
	if err = pstream.CloseSend(); err != nil {
		return
	}
	if res, err = pstream.Recv(); err != nil {
		return
	}

	if err = proto.Unmarshal(res.GetAppMetadata(), &updateResult); err != nil {
		return
	}

	return updateResult.GetRecordCount(), nil
}

func (p *PreparedStatement) DatasetSchema() *arrow.Schema   { return p.datasetSchema }
func (p *PreparedStatement) ParameterSchema() *arrow.Schema { return p.paramSchema }

func (p *PreparedStatement) SetParameters(binding arrow.Record) {
	if p.paramBinding != nil {
		p.paramBinding.Release()
		p.paramBinding = nil
	}
	p.paramBinding = binding
	p.paramBinding.Retain()
}

func (p *PreparedStatement) Close(ctx context.Context) error {
	if p.closed {
		return errors.New("arrow/flightsql: already closed")
	}

	if p.paramBinding != nil {
		p.paramBinding.Release()
		p.paramBinding = nil
	}

	const actionType = "ClosePreparedStatement"
	var (
		cmd     anypb.Any
		request pb.ActionClosePreparedStatementRequest
	)

	request.PreparedStatementHandle = p.handle
	if err := cmd.MarshalFrom(&request); err != nil {
		return err
	}

	body, err := proto.Marshal(&cmd)
	if err != nil {
		return err
	}

	action := &flight.Action{Type: actionType, Body: body}
	_, err = p.client.Client.DoAction(ctx, action, p.opts...)
	if err != nil {
		return err
	}

	p.closed = true
	return nil
}

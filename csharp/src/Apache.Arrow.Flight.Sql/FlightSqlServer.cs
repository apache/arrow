// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow.Flight.Server;
using Apache.Arrow.Types;
using Arrow.Flight.Protocol.Sql;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace Apache.Arrow.Flight.Sql;

public abstract class FlightSqlServer : FlightServer
{
    private ILogger<FlightSqlServer>? Logger { get; }
    public static readonly Schema CatalogSchema = new(new List<Field> {new("catalog_name", StringType.Default, false)}, null);
    public static readonly Schema TableTypesSchema = new(new List<Field> {new("table_type", StringType.Default, false)}, null);
    public static readonly Schema DbSchemaFlightSchema = new(new List<Field> {new("catalog_name", StringType.Default, true), new("db_schema_name", StringType.Default, false)}, null);

    public static readonly Schema PrimaryKeysSchema = new(new List<Field>
    {
        new("catalog_name", StringType.Default, true),
        new("db_schema_name", StringType.Default, true),
        new("table_name", StringType.Default, false),
        new("column_name", StringType.Default, false),
        new("key_sequence", Int32Type.Default, false),
        new("key_name", StringType.Default, true)
    }, null);

    public static readonly Schema KeyImportExportSchema = new(new List<Field>
    {
        new("pk_catalog_name", StringType.Default, true),
        new("pk_db_schema_name", StringType.Default, true),
        new("pk_table_name", StringType.Default, false),
        new("pk_column_name", StringType.Default, false),
        new("fk_catalog_name", StringType.Default, true),
        new("fk_db_schema_name", StringType.Default, true),
        new("fk_table_name", StringType.Default, false),
        new("fk_column_name", StringType.Default, false),
        new("key_sequence", Int32Type.Default, false),
        new("fk_key_name", StringType.Default, true),
        new("pk_key_name", StringType.Default, true),
        new("update_rule", UInt8Type.Default, false),
        new("delete_rule", UInt8Type.Default, false)
    }, null);

    public static readonly Schema TypeInfoSchema = new(new List<Field>
    {
        new("type_name", StringType.Default, false),
        new("data_type", Int32Type.Default, false),
        new("column_size", Int32Type.Default, true),
        new("literal_prefix", StringType.Default, true),
        new("literal_suffix", StringType.Default, true),
        new("create_params", new ListType(new Field("item", StringType.Default, false)), true),
        new("nullable", Int32Type.Default, false),
        new("case_sensitive", BooleanType.Default, false),
        new("searchable", Int32Type.Default, false),
        new("unsigned_attribute", BooleanType.Default, true),
        new("fixed_prec_scale", BooleanType.Default, false),
        new("auto_increment", BooleanType.Default, true),
        new("local_type_name", StringType.Default, true),
        new("minimum_scale", Int32Type.Default, true),
        new("maximum_scale", Int32Type.Default, true),
        new("sql_data_type", Int32Type.Default, false),
        new("datetime_subcode", Int32Type.Default, true),
        new("num_prec_radix", Int32Type.Default, true),
        new("interval_precision", Int32Type.Default, true)
    }, null);

    public static readonly Schema SqlInfoSchema = new(new List<Field>
    {
        new("info_name", UInt32Type.Default, false)
        //TODO: once we have union serialization in Arrow Flight for .Net we should to add these fields
        // fieldList.Add(new Field("value", new UnionType(new List<Field>(), new List<byte>()), false));
        // fieldList.Add(new Field("value", new UnionType(new []
        // {
        //     new Field("string_value", StringType.Default, false),
        //     new Field("bool_value", BooleanType.Default, false),
        //     new Field("bigint_value", Int64Type.Default, false),
        //     new Field("bool_value", BooleanType.Default, false),
        //     new Field("bigint_value", Int64Type.Default, false),
        //     new Field("int32_bitmask", Int32Type.Default, false),
        //     new Field("string_list", new ListType(new Field("item", StringType.Default, false)), false),
        //     new Field("int32_to_int32_list_map", new DictionaryType(Int32Type.Default, new ListType(Int32Type.Default), false), false),
        // }, new []{(byte)ArrowTypeId.String, (byte)ArrowTypeId.Boolean, (byte)ArrowTypeId.Int64,/* (byte)3, (byte)4, (byte)5*/}, UnionMode.Dense), false));
    }, null);

    private static readonly Schema s_tableSchema = new(new List<Field>
    {
        new("catalog_name", StringType.Default, true),
        new("db_schema_name", StringType.Default, true),
        new("table_name", StringType.Default, false),
        new("table_type", StringType.Default, false)
    }, null);

    public static Schema GetTableSchema(bool includeTableSchemaField)
    {
        if (!includeTableSchemaField)
        {
            return s_tableSchema;
        }

        var fields = s_tableSchema.FieldsList.ToList();
        fields.Add(new Field("table_schema", BinaryType.Default, false));

        return new Schema(fields, s_tableSchema.Metadata);
    }

    public static IMessage? GetCommand(FlightTicket ticket)
    {
        try
        {
            return GetCommand(Any.Parser.ParseFrom(ticket.Ticket));
        }
        catch (InvalidProtocolBufferException) { } //The ticket is not a flight sql command

        return null;
    }

    public static async Task<IMessage?> GetCommand(FlightServerRecordBatchStreamReader requestStream)
    {
        return GetCommand(await requestStream.FlightDescriptor.ConfigureAwait(false));
    }

    public static IMessage? GetCommand(FlightDescriptor? request)
    {
        if (request == null) return null;
        if (request.Type == FlightDescriptorType.Command && request.ParsedAndUnpackedMessage() is { } command)
        {
            return command;
        }

        return null;
    }

    private static IMessage? GetCommand(Any command)
    {
        if (command.Is(CommandPreparedStatementQuery.Descriptor))
        {
            return command.Unpack<CommandPreparedStatementQuery>();
        }

        if (command.Is(CommandGetSqlInfo.Descriptor))
        {
            return command.Unpack<CommandGetSqlInfo>();
        }

        if (command.Is(CommandGetCatalogs.Descriptor))
        {
            return command.Unpack<CommandGetCatalogs>();
        }

        if (command.Is(CommandGetTableTypes.Descriptor))
        {
            return command.Unpack<CommandGetTableTypes>();
        }

        if (command.Is(CommandGetTables.Descriptor))
        {
            return command.Unpack<CommandGetTables>();
        }

        if (command.Is(CommandGetDbSchemas.Descriptor))
        {
            return command.Unpack<CommandGetDbSchemas>();
        }

        if (command.Is(CommandGetPrimaryKeys.Descriptor))
        {
            return command.Unpack<CommandGetPrimaryKeys>();
        }

        if (command.Is(CommandGetExportedKeys.Descriptor))
        {
            return command.Unpack<CommandGetExportedKeys>();
        }

        if (command.Is(CommandGetImportedKeys.Descriptor))
        {
            return command.Unpack<CommandGetImportedKeys>();
        }

        if (command.Is(CommandGetCrossReference.Descriptor))
        {
            return command.Unpack<CommandGetCrossReference>();
        }

        if (command.Is(CommandGetXdbcTypeInfo.Descriptor))
        {
            return command.Unpack<CommandGetXdbcTypeInfo>();
        }

        return null;
    }

    protected FlightSqlServer(ILoggerFactory? factory = null)
    {
        Logger = factory?.CreateLogger<FlightSqlServer>();
    }

    /// <summary>
    /// Lists actions supported by Flight SQL.  For Flight RPC actions support
    /// implementers should extend this method to return additional supported actions.
    /// </summary>
    public override async Task ListActions(IAsyncStreamWriter<FlightActionType> responseStream, ServerCallContext context)
    {
        foreach (var actionType in FlightSqlUtils.FlightSqlActions)
        {
            await responseStream.WriteAsync(actionType).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Attempts to execute a valid Flight SQL command.  For Flight RPC calls
    /// implementers should extend this method in order to handle RPC messages.
    /// </summary>
    public override Task<FlightInfo> GetFlightInfo(FlightDescriptor flightDescriptor, ServerCallContext context)
    {
        var sqlCommand = GetCommand(flightDescriptor);
        Logger?.LogTrace("Executing Flight SQL FlightInfo command: {DescriptorName}", sqlCommand?.Descriptor.Name);
        return sqlCommand switch
        {
            CommandStatementQuery command => GetStatementQueryFlightInfo(command, flightDescriptor, context),
            CommandPreparedStatementQuery command => GetPreparedStatementQueryFlightInfo(command, flightDescriptor, context),
            CommandGetCatalogs command => GetCatalogFlightInfo(command, flightDescriptor, context),
            CommandGetDbSchemas command => GetDbSchemaFlightInfo(command, flightDescriptor, context),
            CommandGetTables command => GetTablesFlightInfo(command, flightDescriptor, context),
            CommandGetTableTypes command => GetTableTypesFlightInfo(command, flightDescriptor, context),
            CommandGetSqlInfo command => GetSqlFlightInfo(command, flightDescriptor, context),
            CommandGetPrimaryKeys command => GetPrimaryKeysFlightInfo(command, flightDescriptor, context),
            CommandGetExportedKeys command => GetExportedKeysFlightInfo(command, flightDescriptor, context),
            CommandGetImportedKeys command => GetImportedKeysFlightInfo(command, flightDescriptor, context),
            CommandGetCrossReference command => GetCrossReferenceFlightInfo(command, flightDescriptor, context),
            CommandGetXdbcTypeInfo command => GetXdbcTypeFlightInfo(command, flightDescriptor, context),
            _ => throw new InvalidOperationException($"command type {sqlCommand?.Descriptor?.Name} not supported")
        };
    }

    /// <summary>
    /// Attempts to execute a valid Flight SQL command.  For Flight RPC calls
    /// implementers should extend this method in order to handle RPC messages.
    /// </summary>
    public override Task DoGet(FlightTicket ticket, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context)
    {
        var sqlCommand = GetCommand(ticket);
        Logger?.LogTrace("Executing Flight SQL DoGet command: {SqlCommandDescriptor}", sqlCommand?.Descriptor);
        return sqlCommand switch
        {
            CommandPreparedStatementQuery command => DoGetPreparedStatementQuery(command, responseStream, context),
            CommandGetSqlInfo command => DoGetSqlInfo(command, responseStream, context),
            CommandGetCatalogs command => DoGetCatalog(command, responseStream, context),
            CommandGetTableTypes command => DoGetTableType(command, responseStream, context),
            CommandGetTables command => DoGetTables(command, responseStream, context),
            CommandGetDbSchemas command => DoGetDbSchema(command, responseStream, context),
            CommandGetPrimaryKeys command => DoGetPrimaryKeys(command, responseStream, context),
            CommandGetExportedKeys command => DoGetExportedKeys(command, responseStream, context),
            CommandGetImportedKeys command => DoGetImportedKeys(command, responseStream, context),
            CommandGetCrossReference command => DoGetCrossReference(command, responseStream, context),
            CommandGetXdbcTypeInfo command => DoGetXbdcTypeInfo(command, responseStream, context),
            _ => throw new RpcException(new Status(StatusCode.InvalidArgument, $"DoGet command {sqlCommand?.Descriptor} is not supported."))
        };
    }

    /// <summary>
    /// Attempts to execute a valid Flight SQL command.  For Flight RPC calls
    /// implementers should extend this method in order to handle RPC messages.
    /// </summary>
    public override Task DoAction(FlightAction action, IAsyncStreamWriter<FlightResult> responseStream, ServerCallContext context)
    {
        Logger?.LogTrace("Executing Flight SQL DoAction: {ActionType}", action.Type);
        switch (action.Type)
        {
            case SqlAction.CreateRequest:
                var command = FlightSqlUtils.ParseAndUnpack<ActionCreatePreparedStatementRequest>(action.Body);
                return CreatePreparedStatement(command, action, responseStream, context);
            case SqlAction.CloseRequest:
                var closeCommand = FlightSqlUtils.ParseAndUnpack<ActionClosePreparedStatementRequest>(action.Body);
                return ClosePreparedStatement(closeCommand, action, responseStream, context);
            default:
                throw new NotImplementedException($"Action type {action.Type} not supported");
        }
    }

    /// <summary>
    /// Attempts to execute a valid Flight SQL command.  For Flight RPC calls
    /// implementers should extend this method in order to handle RPC messages.
    /// </summary>
    public override async Task DoPut(FlightServerRecordBatchStreamReader requestStream, IAsyncStreamWriter<FlightPutResult> responseStream, ServerCallContext context)
    {
        if (await GetCommand(requestStream).ConfigureAwait(false) is { } command)
        {
            await DoPutInternal(command, requestStream, responseStream, context).ConfigureAwait(false);
        }
        else
        {
            throw new NotImplementedException();
        }
    }

    private Task DoPutInternal(IMessage command, FlightServerRecordBatchStreamReader requestStream, IAsyncStreamWriter<FlightPutResult> responseStream, ServerCallContext context)
    {
        Logger?.LogTrace("Executing Flight SQL DoAction: {DescriptorName}", command.Descriptor.Name);
        return command switch
        {
            CommandStatementUpdate statementUpdate => PutStatementUpdate(statementUpdate, requestStream, responseStream, context),
            CommandPreparedStatementQuery preparedStatementQuery => PutPreparedStatementQuery(preparedStatementQuery, requestStream, responseStream, context),
            CommandPreparedStatementUpdate preparedStatementUpdate => PutPreparedStatementUpdate(preparedStatementUpdate, requestStream, responseStream, context),
            _ => throw new NotImplementedException($"Command {command.Descriptor.Name} not supported")
        };
    }

    public static bool SupportsAction(FlightAction action)
    {
        switch (action.Type)
        {
            case SqlAction.CreateRequest:
            case SqlAction.CloseRequest:
                return true;
            default:
                return false;
        }
    }


    #region FlightInfo

    protected abstract Task<FlightInfo> GetStatementQueryFlightInfo(CommandStatementQuery commandStatementQuery, FlightDescriptor flightDescriptor, ServerCallContext context);
    protected abstract Task<FlightInfo> GetPreparedStatementQueryFlightInfo(CommandPreparedStatementQuery preparedStatementQuery, FlightDescriptor flightDescriptor, ServerCallContext context);
    protected abstract Task<FlightInfo> GetCatalogFlightInfo(CommandGetCatalogs command, FlightDescriptor flightDescriptor, ServerCallContext context);
    protected abstract Task<FlightInfo> GetDbSchemaFlightInfo(CommandGetDbSchemas command, FlightDescriptor flightDescriptor, ServerCallContext context);
    protected abstract Task<FlightInfo> GetTablesFlightInfo(CommandGetTables command, FlightDescriptor flightDescriptor, ServerCallContext context);
    protected abstract Task<FlightInfo> GetTableTypesFlightInfo(CommandGetTableTypes command, FlightDescriptor flightDescriptor, ServerCallContext context);
    protected abstract Task<FlightInfo> GetSqlFlightInfo(CommandGetSqlInfo commandGetSqlInfo, FlightDescriptor flightDescriptor, ServerCallContext context);
    protected abstract Task<FlightInfo> GetPrimaryKeysFlightInfo(CommandGetPrimaryKeys command, FlightDescriptor flightDescriptor, ServerCallContext context);
    protected abstract Task<FlightInfo> GetExportedKeysFlightInfo(CommandGetExportedKeys command, FlightDescriptor flightDescriptor, ServerCallContext context);
    protected abstract Task<FlightInfo> GetImportedKeysFlightInfo(CommandGetImportedKeys command, FlightDescriptor flightDescriptor, ServerCallContext context);
    protected abstract Task<FlightInfo> GetCrossReferenceFlightInfo(CommandGetCrossReference command, FlightDescriptor flightDescriptor, ServerCallContext context);
    protected abstract Task<FlightInfo> GetXdbcTypeFlightInfo(CommandGetXdbcTypeInfo command, FlightDescriptor flightDescriptor, ServerCallContext context);

    #endregion

    #region DoGet

    protected abstract Task DoGetPreparedStatementQuery(CommandPreparedStatementQuery preparedStatementQuery, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context);
    protected abstract Task DoGetSqlInfo(CommandGetSqlInfo getSqlInfo, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context);
    protected abstract Task DoGetCatalog(CommandGetCatalogs command, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context);
    protected abstract Task DoGetTableType(CommandGetTableTypes command, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context);
    protected abstract Task DoGetTables(CommandGetTables command, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context);
    protected abstract Task DoGetPrimaryKeys(CommandGetPrimaryKeys command, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context);
    protected abstract Task DoGetDbSchema(CommandGetDbSchemas command, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context);
    protected abstract Task DoGetExportedKeys(CommandGetExportedKeys command, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context);
    protected abstract Task DoGetImportedKeys(CommandGetImportedKeys command, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context);
    protected abstract Task DoGetCrossReference(CommandGetCrossReference command, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context);
    protected abstract Task DoGetXbdcTypeInfo(CommandGetXdbcTypeInfo command, FlightServerRecordBatchStreamWriter responseStream, ServerCallContext context);

    #endregion

    #region DoAction

    protected abstract Task CreatePreparedStatement(ActionCreatePreparedStatementRequest request, FlightAction action, IAsyncStreamWriter<FlightResult> streamWriter, ServerCallContext context);
    protected abstract Task ClosePreparedStatement(ActionClosePreparedStatementRequest request, FlightAction action, IAsyncStreamWriter<FlightResult> streamWriter, ServerCallContext context);

    #endregion

    #region DoPut

    protected abstract Task PutPreparedStatementUpdate(CommandPreparedStatementUpdate command, FlightServerRecordBatchStreamReader requestStream, IAsyncStreamWriter<FlightPutResult> responseStream, ServerCallContext context);
    protected abstract Task PutStatementUpdate(CommandStatementUpdate command, FlightServerRecordBatchStreamReader requestStream, IAsyncStreamWriter<FlightPutResult> responseStream, ServerCallContext context);
    protected abstract Task PutPreparedStatementQuery(CommandPreparedStatementQuery command, FlightServerRecordBatchStreamReader requestStream, IAsyncStreamWriter<FlightPutResult> responseStream, ServerCallContext context);

    #endregion
}

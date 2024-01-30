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
using System.Buffers;
using System.Collections.Generic;
using Arrow.Flight.Protocol.Sql;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace Apache.Arrow.Flight.Sql
{
    /// <summary>
    /// Helper methods for doing common Flight Sql tasks and conversions
    /// </summary>
    public class FlightSqlUtils
    {
        public static readonly FlightActionType FlightSqlCreatePreparedStatement = new("CreatePreparedStatement",
            "Creates a reusable prepared statement resource on the server. \n" +
            "Request Message: ActionCreatePreparedStatementRequest\n" +
            "Response Message: ActionCreatePreparedStatementResult");

        public static readonly FlightActionType FlightSqlClosePreparedStatement = new("ClosePreparedStatement",
            "Closes a reusable prepared statement resource on the server. \n" +
            "Request Message: ActionClosePreparedStatementRequest\n" +
            "Response Message: N/A");

        /// <summary>
        /// List of possible actions
        /// </summary>
        public static readonly List<FlightActionType> FlightSqlActions = new()
        {
            FlightSqlCreatePreparedStatement,
            FlightSqlClosePreparedStatement
        };

        /// <summary>
        /// Helper to parse {@link com.google.protobuf.Any} objects to the specific protobuf object.
        /// </summary>
        /// <param name="source">the raw bytes source value.</param>
        /// <returns>the materialized protobuf object.</returns>
        public static Any Parse(ByteString source)
        {
            return Any.Parser.ParseFrom(source);
        }

        /// <summary>
        /// Helper to unpack {@link com.google.protobuf.Any} objects to the specific protobuf object.
        /// </summary>
        /// <param name="source">the parsed Source value.</param>
        /// <typeparam name="T">IMessage</typeparam>
        /// <returns>the materialized protobuf object.</returns>
        public static T Unpack<T>(Any source) where T : IMessage, new()
        {
            return source.Unpack<T>();
        }

        /// <summary>
        /// Helper to parse and unpack {@link com.google.protobuf.Any} objects to the specific protobuf object.
        /// </summary>
        /// <param name="source">the raw bytes source value.</param>
        /// <typeparam name="T">IMessage</typeparam>
        /// <returns>the materialized protobuf object.</returns>
        public static T ParseAndUnpack<T>(ByteString source) where T : IMessage, new()
        {
            return Unpack<T>(Parse(source));
        }
    }

    /// <summary>
    /// A set of helper functions for converting encoded commands to IMessage types
    /// </summary>
    public static class FlightSqlExtensions
    {
        private static Any ParsedCommand(this FlightDescriptor descriptor)
        {
            return FlightSqlUtils.Parse(descriptor.Command);
        }

        private static IMessage UnpackMessage(this Any command)
        {
            if (command.Is(CommandStatementQuery.Descriptor))
                return FlightSqlUtils.Unpack<CommandStatementQuery>(command);
            if (command.Is(CommandPreparedStatementQuery.Descriptor))
                return FlightSqlUtils.Unpack<CommandPreparedStatementQuery>(command);
            if (command.Is(CommandGetCatalogs.Descriptor))
                return FlightSqlUtils.Unpack<CommandGetCatalogs>(command);
            if (command.Is(CommandGetDbSchemas.Descriptor))
                return FlightSqlUtils.Unpack<CommandGetDbSchemas>(command);
            if (command.Is(CommandGetTables.Descriptor))
                return FlightSqlUtils.Unpack<CommandGetTables>(command);
            if (command.Is(CommandGetTableTypes.Descriptor))
                return FlightSqlUtils.Unpack<CommandGetTableTypes>(command);
            if (command.Is(CommandGetSqlInfo.Descriptor))
                return FlightSqlUtils.Unpack<CommandGetSqlInfo>(command);
            if (command.Is(CommandGetPrimaryKeys.Descriptor))
                return FlightSqlUtils.Unpack<CommandGetPrimaryKeys>(command);
            if (command.Is(CommandGetExportedKeys.Descriptor))
                return FlightSqlUtils.Unpack<CommandGetExportedKeys>(command);
            if (command.Is(CommandGetImportedKeys.Descriptor))
                return FlightSqlUtils.Unpack<CommandGetImportedKeys>(command);
            if (command.Is(CommandGetCrossReference.Descriptor))
                return FlightSqlUtils.Unpack<CommandGetCrossReference>(command);
            if (command.Is(CommandGetXdbcTypeInfo.Descriptor))
                return FlightSqlUtils.Unpack<CommandGetXdbcTypeInfo>(command);
            if (command.Is(TicketStatementQuery.Descriptor))
                return FlightSqlUtils.Unpack<TicketStatementQuery>(command);
            if (command.Is(TicketStatementQuery.Descriptor))
                return FlightSqlUtils.Unpack<TicketStatementQuery>(command);
            if (command.Is(CommandStatementUpdate.Descriptor))
                return FlightSqlUtils.Unpack<CommandStatementUpdate>(command);
            if (command.Is(CommandPreparedStatementUpdate.Descriptor))
                return FlightSqlUtils.Unpack<CommandPreparedStatementUpdate>(command);
            if (command.Is(CommandPreparedStatementQuery.Descriptor))
                return FlightSqlUtils.Unpack<CommandPreparedStatementQuery>(command);

            throw new ArgumentException("The defined request is invalid.");
        }

        /// <summary>
        /// Extracts a command from a FlightDescriptor
        /// </summary>
        /// <param name="descriptor"></param>
        /// <returns>An IMessage that has been parsed and unpacked</returns>
        public static IMessage? ParsedAndUnpackedMessage(this FlightDescriptor descriptor)
        {
            try
            {
                return descriptor.ParsedCommand().UnpackMessage();
            }
            catch (ArgumentException)
            {
                return null;
            }
        }

        public static ByteString Serialize(this IBufferMessage message)
        {
            int size = message.CalculateSize();
            var writer = new ArrayBufferWriter<byte>(size);
            message.WriteTo(writer);
            var schemaBytes = writer.WrittenSpan;
            return ByteString.CopyFrom(schemaBytes);
        }
    }
}

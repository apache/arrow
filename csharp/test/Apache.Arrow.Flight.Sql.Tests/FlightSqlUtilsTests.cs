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

using Arrow.Flight.Protocol.Sql;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Xunit;

namespace Apache.Arrow.Flight.Sql.Tests
{
    public class FlightSqlUtilsTests
    {
        [Fact]
        public void EnsureParseCanCorrectlyReviveTheCommand()
        {
            //Given
            var expectedCommand = new CommandStatementQuery
            {
                Query = "select * from database"
            };

            //When
            var command = FlightSqlUtils.Parse(Any.Pack(expectedCommand).ToByteString());

            //Then
            Assert.Equal(command.Unpack<CommandStatementQuery>(), expectedCommand);
        }

        [Fact]
        public void EnsureUnpackCanCreateTheCorrectObject()
        {
            //Given
            var expectedCommand = new CommandPreparedStatementQuery
            {
                PreparedStatementHandle = ByteString.Empty
            };

            //When
            var command = FlightSqlUtils.Unpack<CommandPreparedStatementQuery>(Any.Pack(expectedCommand));

            //Then
            Assert.Equal(command, expectedCommand);
        }

        [Fact]
        public void EnsureParseAndUnpackProducesTheCorrectObject()
        {
            //Given
            var expectedCommand = new CommandPreparedStatementQuery
            {
                PreparedStatementHandle = ByteString.Empty
            };

            //When
            var command = FlightSqlUtils.ParseAndUnpack<CommandPreparedStatementQuery>(Any.Pack(expectedCommand).ToByteString());

            //Then
            Assert.Equal(command, expectedCommand);
        }
    }
}

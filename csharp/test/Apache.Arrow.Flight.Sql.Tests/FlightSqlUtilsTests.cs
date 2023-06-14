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

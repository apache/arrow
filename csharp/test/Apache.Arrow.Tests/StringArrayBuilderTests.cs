using Xunit;

namespace Apache.Arrow.Tests;

public class StringArrayBuilderTests
{
    public class Set
    {
        [Theory]
        [InlineData(1, "Test5")]
        [InlineData(0, "T5")]
        [InlineData(1, "T5")]
        [InlineData(2, "T5")]
        [InlineData(2, "Test3")]
        [InlineData(1, "NewTest")]
        [InlineData(2, "NewTest")]
        [InlineData(0, "NewTest")]
        public void EnsureSetsUpdatesTheCorrectStrings(int offset, string newValue)
        {
            // Arrange
            var builder = new StringArray.Builder();
            var defaultValues = new[] { "Test",  "Test1", "Test2"};
            var expectedValues = (string[])defaultValues.Clone();
            expectedValues[offset] = newValue;
            foreach (string defaultValue in defaultValues)
            {
                builder.Append(defaultValue);
            }

            // Act
            builder.Set(offset, newValue);
            var array = builder.Build();

            // Assert
            for (int i = 0; i <expectedValues.Length; i++)
            {
                string actual = array.GetString(i);
                Assert.Equal(expectedValues[i], actual);
            }
        }
    }
}

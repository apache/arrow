# Implementation guidelines

An execution engine (or framework, or UDF executor, or storage engine, etc) can implements only a subset of the Arrow spec and/or extend it given the following constraints:

## Implementing a subset the spec
### If only producing (and not consuming) arrow vectors.
Any subset of the vector spec and the corresponding metadata can be implemented.

### If consuming and producing vectors
There is a minimal subset of vectors to be supported.
Production of a subset of vectors and their corresponding metadata is always fine.
Consumption of vectors should at least convert the unsupported input vectors to the supported subset (for example Timestamp.millis to timestamp.micros or int32 to int64)

## Extensibility
An execution engine implementor can also extend their memory representation with their own vectors internally as long as they are never exposed. Before sending data to another system expecting Arrow data these custom vectors should be converted to a type that exist in the Arrow spec.
An example of this is operating on compressed data.
These custom vectors are not exchanged externaly and there is no support for custom metadata.

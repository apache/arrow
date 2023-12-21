# Schema

(This feature was released in v1.1.0)

JSON Schema is a draft standard for describing the format of JSON data. The schema itself is also JSON data. By validating a JSON structure with JSON Schema, your code can safely access the DOM without manually checking types, or whether a key exists, etc. It can also ensure that the serialized JSON conform to a specified schema.

RapidJSON implemented a JSON Schema validator for [JSON Schema Draft v4](http://json-schema.org/documentation.html). If you are not familiar with JSON Schema, you may refer to [Understanding JSON Schema](http://spacetelescope.github.io/understanding-json-schema/).

[TOC]

# Basic Usage {#Basic}

First of all, you need to parse a JSON Schema into `Document`, and then compile the `Document` into a `SchemaDocument`.

Secondly, construct a `SchemaValidator` with the `SchemaDocument`. It is similar to a `Writer` in the sense of handling SAX events. So, you can use `document.Accept(validator)` to validate a document, and then check the validity.

~~~cpp
#include "rapidjson/schema.h"

// ...

Document sd;
if (sd.Parse(schemaJson).HasParseError()) {
    // the schema is not a valid JSON.
    // ...       
}
SchemaDocument schema(sd); // Compile a Document to SchemaDocument
// sd is no longer needed here.

Document d;
if (d.Parse(inputJson).HasParseError()) {
    // the input is not a valid JSON.
    // ...       
}

SchemaValidator validator(schema);
if (!d.Accept(validator)) {
    // Input JSON is invalid according to the schema
    // Output diagnostic information
    StringBuffer sb;
    validator.GetInvalidSchemaPointer().StringifyUriFragment(sb);
    printf("Invalid schema: %s\n", sb.GetString());
    printf("Invalid keyword: %s\n", validator.GetInvalidSchemaKeyword());
    sb.Clear();
    validator.GetInvalidDocumentPointer().StringifyUriFragment(sb);
    printf("Invalid document: %s\n", sb.GetString());
}
~~~

Some notes:

* One `SchemaDocument` can be referenced by multiple `SchemaValidator`s. It will not be modified by `SchemaValidator`s.
* A `SchemaValidator` may be reused to validate multiple documents. To run it for other documents, call `validator.Reset()` first.

# Validation during parsing/serialization {#Fused}

Unlike most JSON Schema validator implementations, RapidJSON provides a SAX-based schema validator. Therefore, you can parse a JSON from a stream while validating it on the fly. If the validator encounters a JSON value that invalidates the supplied schema, the parsing will be terminated immediately. This design is especially useful for parsing large JSON files.

## DOM parsing {#DOM}

For using DOM in parsing, `Document` needs some preparation and finalizing tasks, in addition to receiving SAX events, thus it needs some work to route the reader, validator and the document. `SchemaValidatingReader` is a helper class that doing such work.

~~~cpp
#include "rapidjson/filereadstream.h"

// ...
SchemaDocument schema(sd); // Compile a Document to SchemaDocument

// Use reader to parse the JSON
FILE* fp = fopen("big.json", "r");
FileReadStream is(fp, buffer, sizeof(buffer));

// Parse JSON from reader, validate the SAX events, and store in d.
Document d;
SchemaValidatingReader<kParseDefaultFlags, FileReadStream, UTF8<> > reader(is, schema);
d.Populate(reader);

if (!reader.GetParseResult()) {
    // Not a valid JSON
    // When reader.GetParseResult().Code() == kParseErrorTermination,
    // it may be terminated by:
    // (1) the validator found that the JSON is invalid according to schema; or
    // (2) the input stream has I/O error.

    // Check the validation result
    if (!reader.IsValid()) {
        // Input JSON is invalid according to the schema
        // Output diagnostic information
        StringBuffer sb;
        reader.GetInvalidSchemaPointer().StringifyUriFragment(sb);
        printf("Invalid schema: %s\n", sb.GetString());
        printf("Invalid keyword: %s\n", reader.GetInvalidSchemaKeyword());
        sb.Clear();
        reader.GetInvalidDocumentPointer().StringifyUriFragment(sb);
        printf("Invalid document: %s\n", sb.GetString());
    }
}
~~~

## SAX parsing {#SAX}

For using SAX in parsing, it is much simpler. If it only need to validate the JSON without further processing, it is simply:

~~~
SchemaValidator validator(schema);
Reader reader;
if (!reader.Parse(stream, validator)) {
    if (!validator.IsValid()) {
        // ...    
    }
}
~~~

This is exactly the method used in the [schemavalidator](example/schemavalidator/schemavalidator.cpp) example. The distinct advantage is low memory usage, no matter how big the JSON was (the memory usage depends on the complexity of the schema).

If you need to handle the SAX events further, then you need to use the template class `GenericSchemaValidator` to set the output handler of the validator:

~~~
MyHandler handler;
GenericSchemaValidator<SchemaDocument, MyHandler> validator(schema, handler);
Reader reader;
if (!reader.Parse(ss, validator)) {
    if (!validator.IsValid()) {
        // ...    
    }
}
~~~

## Serialization {#Serialization}

It is also possible to do validation during serializing. This can ensure the result JSON is valid according to the JSON schema.

~~~
StringBuffer sb;
Writer<StringBuffer> writer(sb);
GenericSchemaValidator<SchemaDocument, Writer<StringBuffer> > validator(s, writer);
if (!d.Accept(validator)) {
    // Some problem during Accept(), it may be validation or encoding issues.
    if (!validator.IsValid()) {
        // ...
    }
}
~~~

Of course, if your application only needs SAX-style serialization, it can simply send SAX events to `SchemaValidator` instead of `Writer`.

# Remote Schema {#Remote}

JSON Schema supports [`$ref` keyword](http://spacetelescope.github.io/understanding-json-schema/structuring.html), which is a [JSON pointer](doc/pointer.md) referencing to a local or remote schema. Local pointer is prefixed with `#`, while remote pointer is an relative or absolute URI. For example:

~~~js
{ "$ref": "definitions.json#/address" }
~~~

As `SchemaDocument` does not know how to resolve such URI, it needs a user-provided `IRemoteSchemaDocumentProvider` instance to do so.

~~~
class MyRemoteSchemaDocumentProvider : public IRemoteSchemaDocumentProvider {
public:
    virtual const SchemaDocument* GetRemoteDocument(const char* uri, SizeType length) {
        // Resolve the uri and returns a pointer to that schema.
    }
};

// ...

MyRemoteSchemaDocumentProvider provider;
SchemaDocument schema(sd, &provider);
~~~

# Conformance {#Conformance}

RapidJSON passed 262 out of 263 tests in [JSON Schema Test Suite](https://github.com/json-schema/JSON-Schema-Test-Suite) (Json Schema draft 4).

The failed test is "changed scope ref invalid" of "change resolution scope" in `refRemote.json`. It is due to that `id` schema keyword and URI combining function are not implemented.

Besides, the `format` schema keyword for string values is ignored, since it is not required by the specification.

## Regular Expression {#Regex}

The schema keyword `pattern` and `patternProperties` uses regular expression to match the required pattern.

RapidJSON implemented a simple NFA regular expression engine, which is used by default. It supports the following syntax.

|Syntax|Description|
|------|-----------|
|`ab`    | Concatenation |
|<code>a&#124;b</code>   | Alternation |
|`a?`    | Zero or one |
|`a*`    | Zero or more |
|`a+`    | One or more |
|`a{3}`  | Exactly 3 times |
|`a{3,}` | At least 3 times |
|`a{3,5}`| 3 to 5 times |
|`(ab)`  | Grouping |
|`^a`    | At the beginning |
|`a$`    | At the end |
|`.`     | Any character |
|`[abc]` | Character classes |
|`[a-c]` | Character class range |
|`[a-z0-9_]` | Character class combination |
|`[^abc]` | Negated character classes |
|`[^a-c]` | Negated character class range |
|`[\b]`   | Backspace (U+0008) |
|<code>\\&#124;</code>, `\\`, ...  | Escape characters |
|`\f` | Form feed (U+000C) |
|`\n` | Line feed (U+000A) |
|`\r` | Carriage return (U+000D) |
|`\t` | Tab (U+0009) |
|`\v` | Vertical tab (U+000B) |

For C++11 compiler, it is also possible to use the `std::regex` by defining `RAPIDJSON_SCHEMA_USE_INTERNALREGEX=0` and `RAPIDJSON_SCHEMA_USE_STDREGEX=1`. If your schemas do not need `pattern` and `patternProperties`, you can set both macros to zero to disable this feature, which will reduce some code size.

# Performance {#Performance}

Most C++ JSON libraries do not yet support JSON Schema. So we tried to evaluate the performance of RapidJSON's JSON Schema validator according to [json-schema-benchmark](https://github.com/ebdrup/json-schema-benchmark), which tests 11 JavaScript libraries running on Node.js.

That benchmark runs validations on [JSON Schema Test Suite](https://github.com/json-schema/JSON-Schema-Test-Suite), in which some test suites and tests are excluded. We made the same benchmarking procedure in [`schematest.cpp`](test/perftest/schematest.cpp).

On a Mac Book Pro (2.8 GHz Intel Core i7), the following results are collected.

|Validator|Relative speed|Number of test runs per second|
|---------|:------------:|:----------------------------:|
|RapidJSON|155%|30682|
|[`ajv`](https://github.com/epoberezkin/ajv)|100%|19770 (± 1.31%)|
|[`is-my-json-valid`](https://github.com/mafintosh/is-my-json-valid)|70%|13835 (± 2.84%)|
|[`jsen`](https://github.com/bugventure/jsen)|57.7%|11411 (± 1.27%)|
|[`schemasaurus`](https://github.com/AlexeyGrishin/schemasaurus)|26%|5145 (± 1.62%)|
|[`themis`](https://github.com/playlyfe/themis)|19.9%|3935 (± 2.69%)|
|[`z-schema`](https://github.com/zaggino/z-schema)|7%|1388 (± 0.84%)|
|[`jsck`](https://github.com/pandastrike/jsck#readme)|3.1%|606 (± 2.84%)|
|[`jsonschema`](https://github.com/tdegrunt/jsonschema#readme)|0.9%|185 (± 1.01%)|
|[`skeemas`](https://github.com/Prestaul/skeemas#readme)|0.8%|154 (± 0.79%)|
|tv4|0.5%|93 (± 0.94%)|
|[`jayschema`](https://github.com/natesilva/jayschema)|0.1%|21 (± 1.14%)|

That is, RapidJSON is about 1.5x faster than the fastest JavaScript library (ajv). And 1400x faster than the slowest one.

# Schema violation reporting {#Reporting}

(Unreleased as of 2017-09-20)

When validating an instance against a JSON Schema,
it is often desirable to report not only whether the instance is valid,
but also the ways in which it violates the schema.

The `SchemaValidator` class
collects errors encountered during validation
into a JSON `Value`.
This error object can then be accessed as `validator.GetError()`.

The structure of the error object is subject to change
in future versions of RapidJSON,
as there is no standard schema for violations.
The details below this point are provisional only.

## General provisions {#ReportingGeneral}

Validation of an instance value against a schema
produces an error value.
The error value is always an object.
An empty object `{}` indicates the instance is valid.

* The name of each member
  corresponds to the JSON Schema keyword that is violated.
* The value is either an object describing a single violation,
  or an array of such objects.

Each violation object contains two string-valued members
named `instanceRef` and `schemaRef`.
`instanceRef` contains the URI fragment serialization
of a JSON Pointer to the instance subobject
in which the violation was detected.
`schemaRef` contains the URI of the schema
and the fragment serialization of a JSON Pointer
to the subschema that was violated.

Individual violation objects can contain other keyword-specific members.
These are detailed further.

For example, validating this instance:

~~~json
{"numbers": [1, 2, "3", 4, 5]}
~~~

against this schema:

~~~json
{
  "type": "object",
  "properties": {
    "numbers": {"$ref": "numbers.schema.json"}
  }
}
~~~

where `numbers.schema.json` refers
(via a suitable `IRemoteSchemaDocumentProvider`)
to this schema:

~~~json
{
  "type": "array",
  "items": {"type": "number"}
}
~~~

produces the following error object:

~~~json
{
  "type": {
    "instanceRef": "#/numbers/2",
    "schemaRef": "numbers.schema.json#/items",
    "expected": ["number"],
    "actual": "string"
  }
}
~~~

## Validation keywords for numbers {#Numbers}

### multipleOf {#multipleof}

* `expected`: required number strictly greater than 0.
  The value of the `multipleOf` keyword specified in the schema.
* `actual`: required number.
  The instance value.

### maximum {#maximum}

* `expected`: required number.
  The value of the `maximum` keyword specified in the schema.
* `exclusiveMaximum`: optional boolean.
  This will be true if the schema specified `"exclusiveMaximum": true`,
  and will be omitted otherwise.
* `actual`: required number.
  The instance value.

### minimum {#minimum}

* `expected`: required number.
  The value of the `minimum` keyword specified in the schema.
* `exclusiveMinimum`: optional boolean.
  This will be true if the schema specified `"exclusiveMinimum": true`,
  and will be omitted otherwise.
* `actual`: required number.
  The instance value.

## Validation keywords for strings {#Strings}

### maxLength {#maxLength}

* `expected`: required number greater than or equal to 0.
  The value of the `maxLength` keyword specified in the schema.
* `actual`: required string.
  The instance value.

### minLength {#minLength}

* `expected`: required number greater than or equal to 0.
  The value of the `minLength` keyword specified in the schema.
* `actual`: required string.
  The instance value.

### pattern {#pattern}

* `actual`: required string.
  The instance value.

(The expected pattern is not reported
because the internal representation in `SchemaDocument`
does not store the pattern in original string form.)

## Validation keywords for arrays {#Arrays}

### additionalItems {#additionalItems}

This keyword is reported
when the value of `items` schema keyword is an array,
the value of `additionalItems` is `false`,
and the instance is an array
with more items than specified in the `items` array.

* `disallowed`: required integer greater than or equal to 0.
  The index of the first item that has no corresponding schema.

### maxItems and minItems {#maxItems-minItems}

* `expected`: required integer greater than or equal to 0.
  The value of `maxItems` (respectively, `minItems`)
  specified in the schema.
* `actual`: required integer greater than or equal to 0.
  Number of items in the instance array.

### uniqueItems {#uniqueItems}

* `duplicates`: required array
  whose items are integers greater than or equal to 0.
  Indices of items of the instance that are equal.

(RapidJSON only reports the first two equal items,
for performance reasons.)

## Validation keywords for objects

### maxProperties and minProperties {#maxProperties-minProperties}

* `expected`: required integer greater than or equal to 0.
  The value of `maxProperties` (respectively, `minProperties`)
  specified in the schema.
* `actual`: required integer greater than or equal to 0.
  Number of properties in the instance object.

### required {#required}

* `missing`: required array of one or more unique strings.
  The names of properties
  that are listed in the value of the `required` schema keyword
  but not present in the instance object.

### additionalProperties {#additionalProperties}

This keyword is reported
when the schema specifies `additionalProperties: false`
and the name of a property of the instance is
neither listed in the `properties` keyword
nor matches any regular expression in the `patternProperties` keyword.

* `disallowed`: required string.
  Name of the offending property of the instance.

(For performance reasons,
RapidJSON only reports the first such property encountered.)

### dependencies {#dependencies}

* `errors`: required object with one or more properties.
  Names and values of its properties are described below.

Recall that JSON Schema Draft 04 supports
*schema dependencies*,
where presence of a named *controlling* property
requires the instance object to be valid against a subschema,
and *property dependencies*,
where presence of a controlling property
requires other *dependent* properties to be also present.

For a violated schema dependency,
`errors` will contain a property
with the name of the controlling property
and its value will be the error object
produced by validating the instance object
against the dependent schema.

For a violated property dependency,
`errors` will contain a property
with the name of the controlling property
and its value will be an array of one or more unique strings
listing the missing dependent properties.

## Validation keywords for any instance type {#AnyTypes}

### enum {#enum}

This keyword has no additional properties
beyond `instanceRef` and `schemaRef`.

* The allowed values are not listed
  because `SchemaDocument` does not store them in original form.
* The violating value is not reported
  because it might be unwieldy.

If you need to report these details to your users,
you can access the necessary information
by following `instanceRef` and `schemaRef`.

### type {#type}

* `expected`: required array of one or more unique strings,
  each of which is one of the seven primitive types
  defined by the JSON Schema Draft 04 Core specification.
  Lists the types allowed by the `type` schema keyword.
* `actual`: required string, also one of seven primitive types.
  The primitive type of the instance.

### allOf, anyOf, and oneOf {#allOf-anyOf-oneOf}

* `errors`: required array of at least one object.
  There will be as many items as there are subschemas
  in the `allOf`, `anyOf` or `oneOf` schema keyword, respectively.
  Each item will be the error value
  produced by validating the instance
  against the corresponding subschema.

For `allOf`, at least one error value will be non-empty.
For `anyOf`, all error values will be non-empty.
For `oneOf`, either all error values will be non-empty,
or more than one will be empty.

### not {#not}

This keyword has no additional properties
apart from `instanceRef` and `schemaRef`.

# SAX

The term "SAX" originated from [Simple API for XML](http://en.wikipedia.org/wiki/Simple_API_for_XML). We borrowed this term for JSON parsing and generation.

In RapidJSON, `Reader` (typedef of `GenericReader<...>`) is the SAX-style parser for JSON, and `Writer` (typedef of `GenericWriter<...>`) is the SAX-style generator for JSON.

[TOC]

# Reader {#Reader}

`Reader` parses a JSON from a stream. While it reads characters from the stream, it analyzes the characters according to the syntax of JSON, and publishes events to a handler.

For example, here is a JSON.

~~~~~~~~~~js
{
    "hello": "world",
    "t": true ,
    "f": false,
    "n": null,
    "i": 123,
    "pi": 3.1416,
    "a": [1, 2, 3, 4]
}
~~~~~~~~~~

When a `Reader` parses this JSON, it publishes the following events to the handler sequentially:

~~~~~~~~~~
StartObject()
Key("hello", 5, true)
String("world", 5, true)
Key("t", 1, true)
Bool(true)
Key("f", 1, true)
Bool(false)
Key("n", 1, true)
Null()
Key("i")
Uint(123)
Key("pi")
Double(3.1416)
Key("a")
StartArray()
Uint(1)
Uint(2)
Uint(3)
Uint(4)
EndArray(4)
EndObject(7)
~~~~~~~~~~

These events can be easily matched with the JSON, but some event parameters need further explanation. Let's see the `simplereader` example which produces exactly the same output as above:

~~~~~~~~~~cpp
#include "rapidjson/reader.h"
#include <iostream>

using namespace rapidjson;
using namespace std;

struct MyHandler : public BaseReaderHandler<UTF8<>, MyHandler> {
    bool Null() { cout << "Null()" << endl; return true; }
    bool Bool(bool b) { cout << "Bool(" << boolalpha << b << ")" << endl; return true; }
    bool Int(int i) { cout << "Int(" << i << ")" << endl; return true; }
    bool Uint(unsigned u) { cout << "Uint(" << u << ")" << endl; return true; }
    bool Int64(int64_t i) { cout << "Int64(" << i << ")" << endl; return true; }
    bool Uint64(uint64_t u) { cout << "Uint64(" << u << ")" << endl; return true; }
    bool Double(double d) { cout << "Double(" << d << ")" << endl; return true; }
    bool String(const char* str, SizeType length, bool copy) { 
        cout << "String(" << str << ", " << length << ", " << boolalpha << copy << ")" << endl;
        return true;
    }
    bool StartObject() { cout << "StartObject()" << endl; return true; }
    bool Key(const char* str, SizeType length, bool copy) { 
        cout << "Key(" << str << ", " << length << ", " << boolalpha << copy << ")" << endl;
        return true;
    }
    bool EndObject(SizeType memberCount) { cout << "EndObject(" << memberCount << ")" << endl; return true; }
    bool StartArray() { cout << "StartArray()" << endl; return true; }
    bool EndArray(SizeType elementCount) { cout << "EndArray(" << elementCount << ")" << endl; return true; }
};

void main() {
    const char json[] = " { \"hello\" : \"world\", \"t\" : true , \"f\" : false, \"n\": null, \"i\":123, \"pi\": 3.1416, \"a\":[1, 2, 3, 4] } ";

    MyHandler handler;
    Reader reader;
    StringStream ss(json);
    reader.Parse(ss, handler);
}
~~~~~~~~~~

Note that RapidJSON uses templates to statically bind the `Reader` type and the handler type, instead of using classes with virtual functions. This paradigm can improve performance by inlining functions.

## Handler {#Handler}

As shown in the previous example, the user needs to implement a handler which consumes the events (via function calls) from the `Reader`. The handler must contain the following member functions.

~~~~~~~~~~cpp
class Handler {
    bool Null();
    bool Bool(bool b);
    bool Int(int i);
    bool Uint(unsigned i);
    bool Int64(int64_t i);
    bool Uint64(uint64_t i);
    bool Double(double d);
    bool RawNumber(const Ch* str, SizeType length, bool copy);
    bool String(const Ch* str, SizeType length, bool copy);
    bool StartObject();
    bool Key(const Ch* str, SizeType length, bool copy);
    bool EndObject(SizeType memberCount);
    bool StartArray();
    bool EndArray(SizeType elementCount);
};
~~~~~~~~~~

`Null()` is called when the `Reader` encounters a JSON null value.

`Bool(bool)` is called when the `Reader` encounters a JSON true or false value.

When the `Reader` encounters a JSON number, it chooses a suitable C++ type mapping. And then it calls *one* function out of `Int(int)`, `Uint(unsigned)`, `Int64(int64_t)`, `Uint64(uint64_t)` and `Double(double)`. If `kParseNumbersAsStrings` is enabled, `Reader` will always calls `RawNumber()` instead.

`String(const char* str, SizeType length, bool copy)` is called when the `Reader` encounters a string. The first parameter is pointer to the string. The second parameter is the length of the string (excluding the null terminator). Note that RapidJSON supports null character `\0` inside a string. If such situation happens, `strlen(str) < length`. The last `copy` indicates whether the handler needs to make a copy of the string. For normal parsing, `copy = true`. Only when *insitu* parsing is used, `copy = false`. And be aware that the character type depends on the target encoding, which will be explained later.

When the `Reader` encounters the beginning of an object, it calls `StartObject()`. An object in JSON is a set of name-value pairs. If the object contains members it first calls `Key()` for the name of member, and then calls functions depending on the type of the value. These calls of name-value pairs repeat until calling `EndObject(SizeType memberCount)`. Note that the `memberCount` parameter is just an aid for the handler; users who do not need this parameter may ignore it.

Arrays are similar to objects, but simpler. At the beginning of an array, the `Reader` calls `BeginArray()`. If there is elements, it calls functions according to the types of element. Similarly, in the last call `EndArray(SizeType elementCount)`, the parameter `elementCount` is just an aid for the handler.

Every handler function returns a `bool`. Normally it should return `true`. If the handler encounters an error, it can return `false` to notify the event publisher to stop further processing.

For example, when we parse a JSON with `Reader` and the handler detects that the JSON does not conform to the required schema, the handler can return `false` and let the `Reader` stop further parsing. This will place the `Reader` in an error state, with error code `kParseErrorTermination`.

## GenericReader {#GenericReader}

As mentioned before, `Reader` is a typedef of a template class `GenericReader`:

~~~~~~~~~~cpp
namespace rapidjson {

template <typename SourceEncoding, typename TargetEncoding, typename Allocator = MemoryPoolAllocator<> >
class GenericReader {
    // ...
};

typedef GenericReader<UTF8<>, UTF8<> > Reader;

} // namespace rapidjson
~~~~~~~~~~

The `Reader` uses UTF-8 as both source and target encoding. The source encoding means the encoding in the JSON stream. The target encoding means the encoding of the `str` parameter in `String()` calls. For example, to parse a UTF-8 stream and output UTF-16 string events, you can define a reader by:

~~~~~~~~~~cpp
GenericReader<UTF8<>, UTF16<> > reader;
~~~~~~~~~~

Note that, the default character type of `UTF16` is `wchar_t`. So this `reader` needs to call `String(const wchar_t*, SizeType, bool)` of the handler.

The third template parameter `Allocator` is the allocator type for internal data structure (actually a stack).

## Parsing {#SaxParsing}

The main function of `Reader` is used to parse JSON. 

~~~~~~~~~~cpp
template <unsigned parseFlags, typename InputStream, typename Handler>
bool Parse(InputStream& is, Handler& handler);

// with parseFlags = kDefaultParseFlags
template <typename InputStream, typename Handler>
bool Parse(InputStream& is, Handler& handler);
~~~~~~~~~~

If an error occurs during parsing, it will return `false`. User can also call `bool HasParseError()`, `ParseErrorCode GetParseErrorCode()` and `size_t GetErrorOffset()` to obtain the error states. In fact, `Document` uses these `Reader` functions to obtain parse errors. Please refer to [DOM](doc/dom.md) for details about parse errors.

## Token-by-Token Parsing {#TokenByTokenParsing}

Some users may wish to parse a JSON input stream a single token at a time, instead of immediately parsing an entire document without stopping. To parse JSON this way, instead of calling `Parse`, you can use the `IterativeParse` set of functions:

~~~~~~~~~~cpp
    void IterativeParseInit();
	
    template <unsigned parseFlags, typename InputStream, typename Handler>
    bool IterativeParseNext(InputStream& is, Handler& handler);

    bool IterativeParseComplete();
~~~~~~~~~~

Here is an example of iteratively parsing JSON, token by token:

~~~~~~~~~~cpp
    reader.IterativeParseInit();
    while (!reader.IterativeParseComplete()) {
        reader.IterativeParseNext<kParseDefaultFlags>(is, handler);
		// Your handler has been called once.
    }
~~~~~~~~~~

# Writer {#Writer}

`Reader` converts (parses) JSON into events. `Writer` does exactly the opposite. It converts events into JSON. 

`Writer` is very easy to use. If your application only need to converts some data into JSON, it may be a good choice to use `Writer` directly, instead of building a `Document` and then stringifying it with a `Writer`.

In `simplewriter` example, we do exactly the reverse of `simplereader`.

~~~~~~~~~~cpp
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include <iostream>

using namespace rapidjson;
using namespace std;

void main() {
    StringBuffer s;
    Writer<StringBuffer> writer(s);
    
    writer.StartObject();
    writer.Key("hello");
    writer.String("world");
    writer.Key("t");
    writer.Bool(true);
    writer.Key("f");
    writer.Bool(false);
    writer.Key("n");
    writer.Null();
    writer.Key("i");
    writer.Uint(123);
    writer.Key("pi");
    writer.Double(3.1416);
    writer.Key("a");
    writer.StartArray();
    for (unsigned i = 0; i < 4; i++)
        writer.Uint(i);
    writer.EndArray();
    writer.EndObject();

    cout << s.GetString() << endl;
}
~~~~~~~~~~

~~~~~~~~~~
{"hello":"world","t":true,"f":false,"n":null,"i":123,"pi":3.1416,"a":[0,1,2,3]}
~~~~~~~~~~

There are two `String()` and `Key()` overloads. One is the same as defined in handler concept with 3 parameters. It can handle string with null characters. Another one is the simpler version used in the above example.

Note that, the example code does not pass any parameters in `EndArray()` and `EndObject()`. An `SizeType` can be passed but it will be simply ignored by `Writer`.

You may doubt that, why not just using `sprintf()` or `std::stringstream` to build a JSON?

There are various reasons:
1. `Writer` must output a well-formed JSON. If there is incorrect event sequence (e.g. `Int()` just after `StartObject()`), it generates assertion fail in debug mode.
2. `Writer::String()` can handle string escaping (e.g. converting code point `U+000A` to `\n`) and Unicode transcoding.
3. `Writer` handles number output consistently.
4. `Writer` implements the event handler concept. It can be used to handle events from `Reader`, `Document` or other event publisher.
5. `Writer` can be optimized for different platforms.

Anyway, using `Writer` API is even simpler than generating a JSON by ad hoc methods.

## Template {#WriterTemplate}

`Writer` has a minor design difference to `Reader`. `Writer` is a template class, not a typedef. There is no `GenericWriter`. The following is the declaration.

~~~~~~~~~~cpp
namespace rapidjson {

template<typename OutputStream, typename SourceEncoding = UTF8<>, typename TargetEncoding = UTF8<>, typename Allocator = CrtAllocator<>, unsigned writeFlags = kWriteDefaultFlags>
class Writer {
public:
    Writer(OutputStream& os, Allocator* allocator = 0, size_t levelDepth = kDefaultLevelDepth)
// ...
};

} // namespace rapidjson
~~~~~~~~~~

The `OutputStream` template parameter is the type of output stream. It cannot be deduced and must be specified by user.

The `SourceEncoding` template parameter specifies the encoding to be used in `String(const Ch*, ...)`.

The `TargetEncoding` template parameter specifies the encoding in the output stream.

The `Allocator` is the type of allocator, which is used for allocating internal data structure (a stack).

The `writeFlags` are combination of the following bit-flags:

Parse flags                   | Meaning
------------------------------|-----------------------------------
`kWriteNoFlags`               | No flag is set.
`kWriteDefaultFlags`          | Default write flags. It is equal to macro `RAPIDJSON_WRITE_DEFAULT_FLAGS`, which is defined as `kWriteNoFlags`.
`kWriteValidateEncodingFlag`  | Validate encoding of JSON strings.
`kWriteNanAndInfFlag`         | Allow writing of `Infinity`, `-Infinity` and `NaN`.

Besides, the constructor of `Writer` has a `levelDepth` parameter. This parameter affects the initial memory allocated for storing information per hierarchy level.

## PrettyWriter {#PrettyWriter}

While the output of `Writer` is the most condensed JSON without white-spaces, suitable for network transfer or storage, it is not easily readable by human.

Therefore, RapidJSON provides a `PrettyWriter`, which adds indentation and line feeds in the output.

The usage of `PrettyWriter` is exactly the same as `Writer`, expect that `PrettyWriter` provides a `SetIndent(Ch indentChar, unsigned indentCharCount)` function. The default is 4 spaces.

## Completeness and Reset {#CompletenessReset}

A `Writer` can only output a single JSON, which can be any JSON type at the root. Once the singular event for root (e.g. `String()`), or the last matching `EndObject()` or `EndArray()` event, is handled, the output JSON is well-formed and complete. User can detect this state by calling `Writer::IsComplete()`.

When a JSON is complete, the `Writer` cannot accept any new events. Otherwise the output will be invalid (i.e. having more than one root). To reuse the `Writer` object, user can call `Writer::Reset(OutputStream& os)` to reset all internal states of the `Writer` with a new output stream.

# Techniques {#SaxTechniques}

## Parsing JSON to Custom Data Structure {#CustomDataStructure}

`Document`'s parsing capability is completely based on `Reader`. Actually `Document` is a handler which receives events from a reader to build a DOM during parsing.

User may uses `Reader` to build other data structures directly. This eliminates building of DOM, thus reducing memory and improving performance.

In the following `messagereader` example, `ParseMessages()` parses a JSON which should be an object with key-string pairs.

~~~~~~~~~~cpp
#include "rapidjson/reader.h"
#include "rapidjson/error/en.h"
#include <iostream>
#include <string>
#include <map>

using namespace std;
using namespace rapidjson;

typedef map<string, string> MessageMap;

struct MessageHandler
    : public BaseReaderHandler<UTF8<>, MessageHandler> {
    MessageHandler() : state_(kExpectObjectStart) {
    }

    bool StartObject() {
        switch (state_) {
        case kExpectObjectStart:
            state_ = kExpectNameOrObjectEnd;
            return true;
        default:
            return false;
        }
    }

    bool String(const char* str, SizeType length, bool) {
        switch (state_) {
        case kExpectNameOrObjectEnd:
            name_ = string(str, length);
            state_ = kExpectValue;
            return true;
        case kExpectValue:
            messages_.insert(MessageMap::value_type(name_, string(str, length)));
            state_ = kExpectNameOrObjectEnd;
            return true;
        default:
            return false;
        }
    }

    bool EndObject(SizeType) { return state_ == kExpectNameOrObjectEnd; }

    bool Default() { return false; } // All other events are invalid.

    MessageMap messages_;
    enum State {
        kExpectObjectStart,
        kExpectNameOrObjectEnd,
        kExpectValue,
    }state_;
    std::string name_;
};

void ParseMessages(const char* json, MessageMap& messages) {
    Reader reader;
    MessageHandler handler;
    StringStream ss(json);
    if (reader.Parse(ss, handler))
        messages.swap(handler.messages_);   // Only change it if success.
    else {
        ParseErrorCode e = reader.GetParseErrorCode();
        size_t o = reader.GetErrorOffset();
        cout << "Error: " << GetParseError_En(e) << endl;;
        cout << " at offset " << o << " near '" << string(json).substr(o, 10) << "...'" << endl;
    }
}

int main() {
    MessageMap messages;

    const char* json1 = "{ \"greeting\" : \"Hello!\", \"farewell\" : \"bye-bye!\" }";
    cout << json1 << endl;
    ParseMessages(json1, messages);

    for (MessageMap::const_iterator itr = messages.begin(); itr != messages.end(); ++itr)
        cout << itr->first << ": " << itr->second << endl;

    cout << endl << "Parse a JSON with invalid schema." << endl;
    const char* json2 = "{ \"greeting\" : \"Hello!\", \"farewell\" : \"bye-bye!\", \"foo\" : {} }";
    cout << json2 << endl;
    ParseMessages(json2, messages);

    return 0;
}
~~~~~~~~~~

~~~~~~~~~~
{ "greeting" : "Hello!", "farewell" : "bye-bye!" }
farewell: bye-bye!
greeting: Hello!

Parse a JSON with invalid schema.
{ "greeting" : "Hello!", "farewell" : "bye-bye!", "foo" : {} }
Error: Terminate parsing due to Handler error.
 at offset 59 near '} }...'
~~~~~~~~~~

The first JSON (`json1`) was successfully parsed into `MessageMap`. Since `MessageMap` is a `std::map`, the printing order are sorted by the key. This order is different from the JSON's order.

In the second JSON (`json2`), `foo`'s value is an empty object. As it is an object, `MessageHandler::StartObject()` will be called. However, at that moment `state_ = kExpectValue`, so that function returns `false` and cause the parsing process be terminated. The error code is `kParseErrorTermination`.

## Filtering of JSON {#Filtering}

As mentioned earlier, `Writer` can handle the events published by `Reader`. `condense` example simply set a `Writer` as handler of a `Reader`, so it can remove all white-spaces in JSON. `pretty` example uses the same relationship, but replacing `Writer` by `PrettyWriter`. So `pretty` can be used to reformat a JSON with indentation and line feed.

Actually, we can add intermediate layer(s) to filter the contents of JSON via these SAX-style API. For example, `capitalize` example capitalize all strings in a JSON.

~~~~~~~~~~cpp
#include "rapidjson/reader.h"
#include "rapidjson/writer.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/filewritestream.h"
#include "rapidjson/error/en.h"
#include <vector>
#include <cctype>

using namespace rapidjson;

template<typename OutputHandler>
struct CapitalizeFilter {
    CapitalizeFilter(OutputHandler& out) : out_(out), buffer_() {
    }

    bool Null() { return out_.Null(); }
    bool Bool(bool b) { return out_.Bool(b); }
    bool Int(int i) { return out_.Int(i); }
    bool Uint(unsigned u) { return out_.Uint(u); }
    bool Int64(int64_t i) { return out_.Int64(i); }
    bool Uint64(uint64_t u) { return out_.Uint64(u); }
    bool Double(double d) { return out_.Double(d); }
    bool RawNumber(const char* str, SizeType length, bool copy) { return out_.RawNumber(str, length, copy); }
    bool String(const char* str, SizeType length, bool) { 
        buffer_.clear();
        for (SizeType i = 0; i < length; i++)
            buffer_.push_back(std::toupper(str[i]));
        return out_.String(&buffer_.front(), length, true); // true = output handler need to copy the string
    }
    bool StartObject() { return out_.StartObject(); }
    bool Key(const char* str, SizeType length, bool copy) { return String(str, length, copy); }
    bool EndObject(SizeType memberCount) { return out_.EndObject(memberCount); }
    bool StartArray() { return out_.StartArray(); }
    bool EndArray(SizeType elementCount) { return out_.EndArray(elementCount); }

    OutputHandler& out_;
    std::vector<char> buffer_;
};

int main(int, char*[]) {
    // Prepare JSON reader and input stream.
    Reader reader;
    char readBuffer[65536];
    FileReadStream is(stdin, readBuffer, sizeof(readBuffer));

    // Prepare JSON writer and output stream.
    char writeBuffer[65536];
    FileWriteStream os(stdout, writeBuffer, sizeof(writeBuffer));
    Writer<FileWriteStream> writer(os);

    // JSON reader parse from the input stream and let writer generate the output.
    CapitalizeFilter<Writer<FileWriteStream> > filter(writer);
    if (!reader.Parse(is, filter)) {
        fprintf(stderr, "\nError(%u): %s\n", (unsigned)reader.GetErrorOffset(), GetParseError_En(reader.GetParseErrorCode()));
        return 1;
    }

    return 0;
}
~~~~~~~~~~

Note that, it is incorrect to simply capitalize the JSON as a string. For example:
~~~~~~~~~~
["Hello\nWorld"]
~~~~~~~~~~

Simply capitalizing the whole JSON would contain incorrect escape character:
~~~~~~~~~~
["HELLO\NWORLD"]
~~~~~~~~~~

The correct result by `capitalize`:
~~~~~~~~~~
["HELLO\nWORLD"]
~~~~~~~~~~

More complicated filters can be developed. However, since SAX-style API can only provide information about a single event at a time, user may need to book-keeping the contextual information (e.g. the path from root value, storage of other related values). Some processing may be easier to be implemented in DOM than SAX.

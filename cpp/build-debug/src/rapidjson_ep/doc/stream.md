# Stream

In RapidJSON, `rapidjson::Stream` is a concept for reading/writing JSON. Here we'll first show you how to use provided streams. And then see how to create a custom stream.

[TOC]

# Memory Streams {#MemoryStreams}

Memory streams store JSON in memory.

## StringStream (Input) {#StringStream}

`StringStream` is the most basic input stream. It represents a complete, read-only JSON stored in memory. It is defined in `rapidjson/rapidjson.h`.

~~~~~~~~~~cpp
#include "rapidjson/document.h" // will include "rapidjson/rapidjson.h"

using namespace rapidjson;

// ...
const char json[] = "[1, 2, 3, 4]";
StringStream s(json);

Document d;
d.ParseStream(s);
~~~~~~~~~~

Since this is very common usage, `Document::Parse(const char*)` is provided to do exactly the same as above:

~~~~~~~~~~cpp
// ...
const char json[] = "[1, 2, 3, 4]";
Document d;
d.Parse(json);
~~~~~~~~~~

Note that, `StringStream` is a typedef of `GenericStringStream<UTF8<> >`, user may use another encodings to represent the character set of the stream.

## StringBuffer (Output) {#StringBuffer}

`StringBuffer` is a simple output stream. It allocates a memory buffer for writing the whole JSON. Use `GetString()` to obtain the buffer.

~~~~~~~~~~cpp
#include "rapidjson/stringbuffer.h"
#include <rapidjson/writer.h>

StringBuffer buffer;
Writer<StringBuffer> writer(buffer);
d.Accept(writer);

const char* output = buffer.GetString();
~~~~~~~~~~

When the buffer is full, it will increases the capacity automatically. The default capacity is 256 characters (256 bytes for UTF8, 512 bytes for UTF16, etc.). User can provide an allocator and an initial capacity.

~~~~~~~~~~cpp
StringBuffer buffer1(0, 1024); // Use its allocator, initial size = 1024
StringBuffer buffer2(allocator, 1024);
~~~~~~~~~~

By default, `StringBuffer` will instantiate an internal allocator.

Similarly, `StringBuffer` is a typedef of `GenericStringBuffer<UTF8<> >`.

# File Streams {#FileStreams}

When parsing a JSON from file, you may read the whole JSON into memory and use ``StringStream`` above.

However, if the JSON is big, or memory is limited, you can use `FileReadStream`. It only read a part of JSON from file into buffer, and then let the part be parsed. If it runs out of characters in the buffer, it will read the next part from file.

## FileReadStream (Input) {#FileReadStream}

`FileReadStream` reads the file via a `FILE` pointer. And user need to provide a buffer.

~~~~~~~~~~cpp
#include "rapidjson/filereadstream.h"
#include <cstdio>

using namespace rapidjson;

FILE* fp = fopen("big.json", "rb"); // non-Windows use "r"

char readBuffer[65536];
FileReadStream is(fp, readBuffer, sizeof(readBuffer));

Document d;
d.ParseStream(is);

fclose(fp);
~~~~~~~~~~

Different from string streams, `FileReadStream` is byte stream. It does not handle encodings. If the file is not UTF-8, the byte stream can be wrapped in a `EncodedInputStream`. We will discuss more about this later in this tutorial.

Apart from reading file, user can also use `FileReadStream` to read `stdin`.

## FileWriteStream (Output) {#FileWriteStream}

`FileWriteStream` is buffered output stream. Its usage is very similar to `FileReadStream`.

~~~~~~~~~~cpp
#include "rapidjson/filewritestream.h"
#include <rapidjson/writer.h>
#include <cstdio>

using namespace rapidjson;

Document d;
d.Parse(json);
// ...

FILE* fp = fopen("output.json", "wb"); // non-Windows use "w"

char writeBuffer[65536];
FileWriteStream os(fp, writeBuffer, sizeof(writeBuffer));

Writer<FileWriteStream> writer(os);
d.Accept(writer);

fclose(fp);
~~~~~~~~~~

It can also redirect the output to `stdout`.

# iostream Wrapper {#iostreamWrapper}

Due to users' requests, RapidJSON also provides official wrappers for `std::basic_istream` and `std::basic_ostream`. However, please note that the performance will be much lower than the other streams above.

## IStreamWrapper {#IStreamWrapper}

`IStreamWrapper` wraps any class derived from `std::istream`, such as `std::istringstream`, `std::stringstream`, `std::ifstream`, `std::fstream`, into RapidJSON's input stream.

~~~cpp
#include <rapidjson/document.h>
#include <rapidjson/istreamwrapper.h>
#include <fstream>

using namespace rapidjson;
using namespace std;

ifstream ifs("test.json");
IStreamWrapper isw(ifs);

Document d;
d.ParseStream(isw);
~~~

For classes derived from `std::wistream`, use `WIStreamWrapper`.

## OStreamWrapper {#OStreamWrapper}

Similarly, `OStreamWrapper` wraps any class derived from `std::ostream`, such as `std::ostringstream`, `std::stringstream`, `std::ofstream`, `std::fstream`, into RapidJSON's input stream.

~~~cpp
#include <rapidjson/document.h>
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/writer.h>
#include <fstream>

using namespace rapidjson;
using namespace std;

Document d;
d.Parse(json);

// ...

ofstream ofs("output.json");
OStreamWrapper osw(ofs);

Writer<OStreamWrapper> writer(osw);
d.Accept(writer);
~~~

For classes derived from `std::wostream`, use `WOStreamWrapper`.

# Encoded Streams {#EncodedStreams}

Encoded streams do not contain JSON itself, but they wrap byte streams to provide basic encoding/decoding function.

As mentioned above, UTF-8 byte streams can be read directly. However, UTF-16 and UTF-32 have endian issue. To handle endian correctly, it needs to convert bytes into characters (e.g. `wchar_t` for UTF-16) while reading, and characters into bytes while writing.

Besides, it also need to handle [byte order mark (BOM)](http://en.wikipedia.org/wiki/Byte_order_mark). When reading from a byte stream, it is needed to detect or just consume the BOM if exists. When writing to a byte stream, it can optionally write BOM.

If the encoding of stream is known during compile-time, you may use `EncodedInputStream` and `EncodedOutputStream`. If the stream can be UTF-8, UTF-16LE, UTF-16BE, UTF-32LE, UTF-32BE JSON, and it is only known in runtime, you may use `AutoUTFInputStream` and `AutoUTFOutputStream`. These streams are defined in `rapidjson/encodedstream.h`.

Note that, these encoded streams can be applied to streams other than file. For example, you may have a file in memory, or a custom byte stream, be wrapped in encoded streams.

## EncodedInputStream {#EncodedInputStream}

`EncodedInputStream` has two template parameters. The first one is a `Encoding` class, such as `UTF8`, `UTF16LE`, defined in `rapidjson/encodings.h`. The second one is the class of stream to be wrapped.

~~~~~~~~~~cpp
#include "rapidjson/document.h"
#include "rapidjson/filereadstream.h"   // FileReadStream
#include "rapidjson/encodedstream.h"    // EncodedInputStream
#include <cstdio>

using namespace rapidjson;

FILE* fp = fopen("utf16le.json", "rb"); // non-Windows use "r"

char readBuffer[256];
FileReadStream bis(fp, readBuffer, sizeof(readBuffer));

EncodedInputStream<UTF16LE<>, FileReadStream> eis(bis);  // wraps bis into eis

Document d; // Document is GenericDocument<UTF8<> > 
d.ParseStream<0, UTF16LE<> >(eis);  // Parses UTF-16LE file into UTF-8 in memory

fclose(fp);
~~~~~~~~~~

## EncodedOutputStream {#EncodedOutputStream}

`EncodedOutputStream` is similar but it has a `bool putBOM` parameter in the constructor, controlling whether to write BOM into output byte stream.

~~~~~~~~~~cpp
#include "rapidjson/filewritestream.h"  // FileWriteStream
#include "rapidjson/encodedstream.h"    // EncodedOutputStream
#include <rapidjson/writer.h>
#include <cstdio>

Document d;         // Document is GenericDocument<UTF8<> > 
// ...

FILE* fp = fopen("output_utf32le.json", "wb"); // non-Windows use "w"

char writeBuffer[256];
FileWriteStream bos(fp, writeBuffer, sizeof(writeBuffer));

typedef EncodedOutputStream<UTF32LE<>, FileWriteStream> OutputStream;
OutputStream eos(bos, true);   // Write BOM

Writer<OutputStream, UTF8<>, UTF32LE<>> writer(eos);
d.Accept(writer);   // This generates UTF32-LE file from UTF-8 in memory

fclose(fp);
~~~~~~~~~~

## AutoUTFInputStream {#AutoUTFInputStream}

Sometimes an application may want to handle all supported JSON encoding. `AutoUTFInputStream` will detection encoding by BOM first. If BOM is unavailable, it will use  characteristics of valid JSON to make detection. If neither method success, it falls back to the UTF type provided in constructor.

Since the characters (code units) may be 8-bit, 16-bit or 32-bit. `AutoUTFInputStream` requires a character type which can hold at least 32-bit. We may use `unsigned`, as in the template parameter:

~~~~~~~~~~cpp
#include "rapidjson/document.h"
#include "rapidjson/filereadstream.h"   // FileReadStream
#include "rapidjson/encodedstream.h"    // AutoUTFInputStream
#include <cstdio>

using namespace rapidjson;

FILE* fp = fopen("any.json", "rb"); // non-Windows use "r"

char readBuffer[256];
FileReadStream bis(fp, readBuffer, sizeof(readBuffer));

AutoUTFInputStream<unsigned, FileReadStream> eis(bis);  // wraps bis into eis

Document d;         // Document is GenericDocument<UTF8<> > 
d.ParseStream<0, AutoUTF<unsigned> >(eis); // This parses any UTF file into UTF-8 in memory

fclose(fp);
~~~~~~~~~~

When specifying the encoding of stream, uses `AutoUTF<CharType>` as in `ParseStream()` above.

You can obtain the type of UTF via `UTFType GetType()`. And check whether a BOM is found by `HasBOM()`

## AutoUTFOutputStream {#AutoUTFOutputStream}

Similarly, to choose encoding for output during runtime, we can use `AutoUTFOutputStream`. This class is not automatic *per se*. You need to specify the UTF type and whether to write BOM in runtime.

~~~~~~~~~~cpp
using namespace rapidjson;

void WriteJSONFile(FILE* fp, UTFType type, bool putBOM, const Document& d) {
    char writeBuffer[256];
    FileWriteStream bos(fp, writeBuffer, sizeof(writeBuffer));

    typedef AutoUTFOutputStream<unsigned, FileWriteStream> OutputStream;
    OutputStream eos(bos, type, putBOM);
    
    Writer<OutputStream, UTF8<>, AutoUTF<> > writer;
    d.Accept(writer);
}
~~~~~~~~~~

`AutoUTFInputStream` and `AutoUTFOutputStream` is more convenient than `EncodedInputStream` and `EncodedOutputStream`. They just incur a little bit runtime overheads.

# Custom Stream {#CustomStream}

In addition to memory/file streams, user can create their own stream classes which fits RapidJSON's API. For example, you may create network stream, stream from compressed file, etc.

RapidJSON combines different types using templates. A class containing all required interface can be a stream. The Stream interface is defined in comments of `rapidjson/rapidjson.h`:

~~~~~~~~~~cpp
concept Stream {
    typename Ch;    //!< Character type of the stream.

    //! Read the current character from stream without moving the read cursor.
    Ch Peek() const;

    //! Read the current character from stream and moving the read cursor to next character.
    Ch Take();

    //! Get the current read cursor.
    //! \return Number of characters read from start.
    size_t Tell();

    //! Begin writing operation at the current read pointer.
    //! \return The begin writer pointer.
    Ch* PutBegin();

    //! Write a character.
    void Put(Ch c);

    //! Flush the buffer.
    void Flush();

    //! End the writing operation.
    //! \param begin The begin write pointer returned by PutBegin().
    //! \return Number of characters written.
    size_t PutEnd(Ch* begin);
}
~~~~~~~~~~

For input stream, they must implement `Peek()`, `Take()` and `Tell()`.
For output stream, they must implement `Put()` and `Flush()`. 
There are two special interface, `PutBegin()` and `PutEnd()`, which are only for *in situ* parsing. Normal streams do not implement them. However, if the interface is not needed for a particular stream, it is still need to a dummy implementation, otherwise will generate compilation error.

## Example: istream wrapper {#ExampleIStreamWrapper}

The following example is a simple wrapper of `std::istream`, which only implements 3 functions.

~~~~~~~~~~cpp
class MyIStreamWrapper {
public:
    typedef char Ch;

    MyIStreamWrapper(std::istream& is) : is_(is) {
    }

    Ch Peek() const { // 1
        int c = is_.peek();
        return c == std::char_traits<char>::eof() ? '\0' : (Ch)c;
    }

    Ch Take() { // 2
        int c = is_.get();
        return c == std::char_traits<char>::eof() ? '\0' : (Ch)c;
    }

    size_t Tell() const { return (size_t)is_.tellg(); } // 3

    Ch* PutBegin() { assert(false); return 0; }
    void Put(Ch) { assert(false); }
    void Flush() { assert(false); }
    size_t PutEnd(Ch*) { assert(false); return 0; }

private:
    MyIStreamWrapper(const MyIStreamWrapper&);
    MyIStreamWrapper& operator=(const MyIStreamWrapper&);

    std::istream& is_;
};
~~~~~~~~~~

User can use it to wrap instances of `std::stringstream`, `std::ifstream`.

~~~~~~~~~~cpp
const char* json = "[1,2,3,4]";
std::stringstream ss(json);
MyIStreamWrapper is(ss);

Document d;
d.ParseStream(is);
~~~~~~~~~~

Note that, this implementation may not be as efficient as RapidJSON's memory or file streams, due to internal overheads of the standard library.

## Example: ostream wrapper {#ExampleOStreamWrapper}

The following example is a simple wrapper of `std::istream`, which only implements 2 functions.

~~~~~~~~~~cpp
class MyOStreamWrapper {
public:
    typedef char Ch;

    MyOStreamWrapper(std::ostream& os) : os_(os) {
    }

    Ch Peek() const { assert(false); return '\0'; }
    Ch Take() { assert(false); return '\0'; }
    size_t Tell() const {  }

    Ch* PutBegin() { assert(false); return 0; }
    void Put(Ch c) { os_.put(c); }                  // 1
    void Flush() { os_.flush(); }                   // 2
    size_t PutEnd(Ch*) { assert(false); return 0; }

private:
    MyOStreamWrapper(const MyOStreamWrapper&);
    MyOStreamWrapper& operator=(const MyOStreamWrapper&);

    std::ostream& os_;
};
~~~~~~~~~~

User can use it to wrap instances of `std::stringstream`, `std::ofstream`.

~~~~~~~~~~cpp
Document d;
// ...

std::stringstream ss;
MyOStreamWrapper os(ss);

Writer<MyOStreamWrapper> writer(os);
d.Accept(writer);
~~~~~~~~~~

Note that, this implementation may not be as efficient as RapidJSON's memory or file streams, due to internal overheads of the standard library.

# Summary {#Summary}

This section describes stream classes available in RapidJSON. Memory streams are simple. File stream can reduce the memory required during JSON parsing and generation, if the JSON is stored in file system. Encoded streams converts between byte streams and character streams. Finally, user may create custom streams using a simple interface.

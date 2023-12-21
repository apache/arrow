# Encoding

According to [ECMA-404](http://www.ecma-international.org/publications/files/ECMA-ST/ECMA-404.pdf),

> (in Introduction) JSON text is a sequence of Unicode code points.

The earlier [RFC4627](http://www.ietf.org/rfc/rfc4627.txt) stated that,

> (in §3) JSON text SHALL be encoded in Unicode.  The default encoding is UTF-8.

> (in §6) JSON may be represented using UTF-8, UTF-16, or UTF-32. When JSON is written in UTF-8, JSON is 8bit compatible.  When JSON is written in UTF-16 or UTF-32, the binary content-transfer-encoding must be used.

RapidJSON supports various encodings. It can also validate the encodings of JSON, and transcoding JSON among encodings. All these features are implemented internally, without the need for external libraries (e.g. [ICU](http://site.icu-project.org/)).

[TOC]

# Unicode {#Unicode}
From [Unicode's official website](http://www.unicode.org/standard/WhatIsUnicode.html):
> Unicode provides a unique number for every character, 
> no matter what the platform,
> no matter what the program,
> no matter what the language.

Those unique numbers are called code points, which is in the range `0x0` to `0x10FFFF`.

## Unicode Transformation Format {#UTF}

There are various encodings for storing Unicode code points. These are called Unicode Transformation Format (UTF). RapidJSON supports the most commonly used UTFs, including

* UTF-8: 8-bit variable-width encoding. It maps a code point to 1–4 bytes.
* UTF-16: 16-bit variable-width encoding. It maps a code point to 1–2 16-bit code units (i.e., 2–4 bytes).
* UTF-32: 32-bit fixed-width encoding. It directly maps a code point to a single 32-bit code unit (i.e. 4 bytes).

For UTF-16 and UTF-32, the byte order (endianness) does matter. Within computer memory, they are often stored in the computer's endianness. However, when it is stored in file or transferred over network, we need to state the byte order of the byte sequence, either little-endian (LE) or big-endian (BE). 

RapidJSON provide these encodings via the structs in `rapidjson/encodings.h`:

~~~~~~~~~~cpp
namespace rapidjson {

template<typename CharType = char>
struct UTF8;

template<typename CharType = wchar_t>
struct UTF16;

template<typename CharType = wchar_t>
struct UTF16LE;

template<typename CharType = wchar_t>
struct UTF16BE;

template<typename CharType = unsigned>
struct UTF32;

template<typename CharType = unsigned>
struct UTF32LE;

template<typename CharType = unsigned>
struct UTF32BE;

} // namespace rapidjson
~~~~~~~~~~

For processing text in memory, we normally use `UTF8`, `UTF16` or `UTF32`. For processing text via I/O, we may use `UTF8`, `UTF16LE`, `UTF16BE`, `UTF32LE` or `UTF32BE`.

When using the DOM-style API, the `Encoding` template parameter in `GenericValue<Encoding>` and `GenericDocument<Encoding>` indicates the encoding to be used to represent JSON string in memory. So normally we will use `UTF8`, `UTF16` or `UTF32` for this template parameter. The choice depends on operating systems and other libraries that the application is using. For example, Windows API represents Unicode characters in UTF-16, while most Linux distributions and applications prefer UTF-8.

Example of UTF-16 DOM declaration:

~~~~~~~~~~cpp
typedef GenericDocument<UTF16<> > WDocument;
typedef GenericValue<UTF16<> > WValue;
~~~~~~~~~~

For a detail example, please check the example in [DOM's Encoding](doc/stream.md) section.

## Character Type {#CharacterType}

As shown in the declaration, each encoding has a `CharType` template parameter. Actually, it may be a little bit confusing, but each `CharType` stores a code unit, not a character (code point). As mentioned in previous section, a code point may be encoded to 1–4 code units for UTF-8.

For `UTF16(LE|BE)`, `UTF32(LE|BE)`, the `CharType` must be integer type of at least 2 and 4 bytes  respectively.

Note that C++11 introduces `char16_t` and `char32_t`, which can be used for `UTF16` and `UTF32` respectively.

## AutoUTF {#AutoUTF}

Previous encodings are statically bound in compile-time. In other words, user must know exactly which encodings will be used in the memory or streams. However, sometimes we may need to read/write files of different encodings. The encoding needed to be decided in runtime.

`AutoUTF` is an encoding designed for this purpose. It chooses which encoding to be used according to the input or output stream. Currently, it should be used with `EncodedInputStream` and `EncodedOutputStream`.

## ASCII {#ASCII}

Although the JSON standards did not mention about [ASCII](http://en.wikipedia.org/wiki/ASCII), sometimes we would like to write 7-bit ASCII JSON for applications that cannot handle UTF-8. Since any JSON can represent unicode characters in escaped sequence `\uXXXX`, JSON can always be encoded in ASCII.

Here is an example for writing a UTF-8 DOM into ASCII:

~~~~~~~~~~cpp
using namespace rapidjson;
Document d; // UTF8<>
// ...
StringBuffer buffer;
Writer<StringBuffer, Document::EncodingType, ASCII<> > writer(buffer);
d.Accept(writer);
std::cout << buffer.GetString();
~~~~~~~~~~

ASCII can be used in input stream. If the input stream contains bytes with values above 127, it will cause `kParseErrorStringInvalidEncoding` error.

ASCII *cannot* be used in memory (encoding of `Document` or target encoding of `Reader`), as it cannot represent Unicode code points.

# Validation & Transcoding {#ValidationTranscoding}

When RapidJSON parses a JSON, it can validate the input JSON, whether it is a valid sequence of a specified encoding. This option can be turned on by adding `kParseValidateEncodingFlag` in `parseFlags` template parameter.

If the input encoding and output encoding is different, `Reader` and `Writer` will automatically transcode (convert) the text. In this case, `kParseValidateEncodingFlag` is not necessary, as it must decode the input sequence. And if the sequence was unable to be decoded, it must be invalid.

## Transcoder {#Transcoder}

Although the encoding functions in RapidJSON are designed for JSON parsing/generation, user may abuse them for transcoding of non-JSON strings.

Here is an example for transcoding a string from UTF-8 to UTF-16:

~~~~~~~~~~cpp
#include "rapidjson/encodings.h"

using namespace rapidjson;

const char* s = "..."; // UTF-8 string
StringStream source(s);
GenericStringBuffer<UTF16<> > target;

bool hasError = false;
while (source.Peek() != '\0')
    if (!Transcoder<UTF8<>, UTF16<> >::Transcode(source, target)) {
        hasError = true;
        break;
    }

if (!hasError) {
    const wchar_t* t = target.GetString();
    // ...
}
~~~~~~~~~~

You may also use `AutoUTF` and the associated streams for setting source/target encoding in runtime.

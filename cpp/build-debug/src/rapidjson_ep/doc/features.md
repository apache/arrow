# Features

## General

* Cross-platform
 * Compilers: Visual Studio, gcc, clang, etc.
 * Architectures: x86, x64, ARM, etc.
 * Operating systems: Windows, Mac OS X, Linux, iOS, Android, etc.
* Easy installation
 * Header files only library. Just copy the headers to your project.
* Self-contained, minimal dependences
 * No STL, BOOST, etc.
 * Only included `<cstdio>`, `<cstdlib>`, `<cstring>`, `<inttypes.h>`, `<new>`, `<stdint.h>`. 
* Without C++ exception, RTTI
* High performance
 * Use template and inline functions to reduce function call overheads.
 * Internal optimized Grisu2 and floating point parsing implementations.
 * Optional SSE2/SSE4.2 support.

## Standard compliance

* RapidJSON should be fully RFC4627/ECMA-404 compliance.
* Support JSON Pointer (RFC6901).
* Support JSON Schema Draft v4.
* Support Unicode surrogate.
* Support null character (`"\u0000"`)
 * For example, `["Hello\u0000World"]` can be parsed and handled gracefully. There is API for getting/setting lengths of string.
* Support optional relaxed syntax.
 * Single line (`// ...`) and multiple line (`/* ... */`) comments (`kParseCommentsFlag`). 
 * Trailing commas at the end of objects and arrays (`kParseTrailingCommasFlag`).
 * `NaN`, `Inf`, `Infinity`, `-Inf` and `-Infinity` as `double` values (`kParseNanAndInfFlag`)
* [NPM compliant](http://github.com/Tencent/rapidjson/blob/master/doc/npm.md).

## Unicode

* Support UTF-8, UTF-16, UTF-32 encodings, including little endian and big endian.
 * These encodings are used in input/output streams and in-memory representation.
* Support automatic detection of encodings in input stream.
* Support transcoding between encodings internally.
 * For example, you can read a UTF-8 file and let RapidJSON transcode the JSON strings into UTF-16 in the DOM.
* Support encoding validation internally.
 * For example, you can read a UTF-8 file, and let RapidJSON check whether all JSON strings are valid UTF-8 byte sequence.
* Support custom character types.
 * By default the character types are `char` for UTF8, `wchar_t` for UTF16, `uint32_t` for UTF32.
* Support custom encodings.

## API styles

* SAX (Simple API for XML) style API
 * Similar to [SAX](http://en.wikipedia.org/wiki/Simple_API_for_XML), RapidJSON provides a event sequential access parser API (`rapidjson::GenericReader`). It also provides a generator API (`rapidjson::Writer`) which consumes the same set of events.
* DOM (Document Object Model) style API
 * Similar to [DOM](http://en.wikipedia.org/wiki/Document_Object_Model) for HTML/XML, RapidJSON can parse JSON into a DOM representation (`rapidjson::GenericDocument`), for easy manipulation, and finally stringify back to JSON if needed.
 * The DOM style API (`rapidjson::GenericDocument`) is actually implemented with SAX style API (`rapidjson::GenericReader`). SAX is faster but sometimes DOM is easier. Users can pick their choices according to scenarios.

## Parsing

* Recursive (default) and iterative parser
 * Recursive parser is faster but prone to stack overflow in extreme cases.
 * Iterative parser use custom stack to keep parsing state.
* Support *in situ* parsing.
 * Parse JSON string values in-place at the source JSON, and then the DOM points to addresses of those strings.
 * Faster than convention parsing: no allocation for strings, no copy (if string does not contain escapes), cache-friendly.
* Support 32-bit/64-bit signed/unsigned integer and `double` for JSON number type.
* Support parsing multiple JSONs in input stream (`kParseStopWhenDoneFlag`).
* Error Handling
 * Support comprehensive error code if parsing failed.
 * Support error message localization.

## DOM (Document)

* RapidJSON checks range of numerical values for conversions.
* Optimization for string literal
 * Only store pointer instead of copying
* Optimization for "short" strings
 * Store short string in `Value` internally without additional allocation.
 * For UTF-8 string: maximum 11 characters in 32-bit, 21 characters in 64-bit (13 characters in x86-64).
* Optionally support `std::string` (define `RAPIDJSON_HAS_STDSTRING=1`)

## Generation

* Support `rapidjson::PrettyWriter` for adding newlines and indentations.

## Stream

* Support `rapidjson::GenericStringBuffer` for storing the output JSON as string.
* Support `rapidjson::FileReadStream` and `rapidjson::FileWriteStream` for input/output `FILE` object.
* Support custom streams.

## Memory

* Minimize memory overheads for DOM.
 * Each JSON value occupies exactly 16/20 bytes for most 32/64-bit machines (excluding text string).
* Support fast default allocator.
 * A stack-based allocator (allocate sequentially, prohibit to free individual allocations, suitable for parsing).
 * User can provide a pre-allocated buffer. (Possible to parse a number of JSONs without any CRT allocation)
* Support standard CRT(C-runtime) allocator.
* Support custom allocators.

## Miscellaneous

* Some C++11 support (optional)
 * Rvalue reference
 * `noexcept` specifier
 * Range-based for loop

# FAQ

[TOC]

## General

1. What is RapidJSON?

   RapidJSON is a C++ library for parsing and generating JSON. You may check all [features](doc/features.md) of it.

2. Why is RapidJSON named so?

   It is inspired by [RapidXML](http://rapidxml.sourceforge.net/), which is a fast XML DOM parser.

3. Is RapidJSON similar to RapidXML?

   RapidJSON borrowed some designs of RapidXML, including *in situ* parsing, header-only library. But the two APIs are completely different. Also RapidJSON provide many features that are not in RapidXML.

4. Is RapidJSON free?

   Yes, it is free under MIT license. It can be used in commercial applications. Please check the details in [license.txt](https://github.com/Tencent/rapidjson/blob/master/license.txt).

5. Is RapidJSON small? What are its dependencies? 

   Yes. A simple executable which parses a JSON and prints its statistics is less than 30KB on Windows.

   RapidJSON depends on C++ standard library only.

6. How to install RapidJSON?

   Check [Installation section](https://miloyip.github.io/rapidjson/).

7. Can RapidJSON run on my platform?

   RapidJSON has been tested in many combinations of operating systems, compilers and CPU architecture by the community. But we cannot ensure that it can be run on your particular platform. Building and running the unit test suite will give you the answer.

8. Does RapidJSON support C++03? C++11?

   RapidJSON was firstly implemented for C++03. Later it added optional support of some C++11 features (e.g., move constructor, `noexcept`). RapidJSON shall be compatible with C++03 or C++11 compliant compilers.

9. Does RapidJSON really work in real applications?

   Yes. It is deployed in both client and server real applications. A community member reported that RapidJSON in their system parses 50 million JSONs daily.

10. How RapidJSON is tested?

   RapidJSON contains a unit test suite for automatic testing. [Travis](https://travis-ci.org/Tencent/rapidjson/)(for Linux) and [AppVeyor](https://ci.appveyor.com/project/Tencent/rapidjson/)(for Windows) will compile and run the unit test suite for all modifications. The test process also uses Valgrind (in Linux) to detect memory leaks.

11. Is RapidJSON well documented?

   RapidJSON provides user guide and API documentationn.

12. Are there alternatives?

   Yes, there are a lot alternatives. For example, [nativejson-benchmark](https://github.com/miloyip/nativejson-benchmark) has a listing of open-source C/C++ JSON libraries. [json.org](http://www.json.org/) also has a list.

## JSON

1. What is JSON?

   JSON (JavaScript Object Notation) is a lightweight data-interchange format. It uses human readable text format. More details of JSON can be referred to [RFC7159](http://www.ietf.org/rfc/rfc7159.txt) and [ECMA-404](http://www.ecma-international.org/publications/standards/Ecma-404.htm).

2. What are applications of JSON?

   JSON are commonly used in web applications for transferring structured data. It is also used as a file format for data persistence.

3. Does RapidJSON conform to the JSON standard?

   Yes. RapidJSON is fully compliance with [RFC7159](http://www.ietf.org/rfc/rfc7159.txt) and [ECMA-404](http://www.ecma-international.org/publications/standards/Ecma-404.htm). It can handle corner cases, such as supporting null character and surrogate pairs in JSON strings.

4. Does RapidJSON support relaxed syntax?

   Currently no. RapidJSON only support the strict standardized format. Support on related syntax is under discussion in this [issue](https://github.com/Tencent/rapidjson/issues/36).

## DOM and SAX

1. What is DOM style API?

   Document Object Model (DOM) is an in-memory representation of JSON for query and manipulation.

2. What is SAX style API?

   SAX is an event-driven API for parsing and generation.

3. Should I choose DOM or SAX?

   DOM is easy for query and manipulation. SAX is very fast and memory-saving but often more difficult to be applied.

4. What is *in situ* parsing?

   *in situ* parsing decodes the JSON strings directly into the input JSON. This is an optimization which can reduce memory consumption and improve performance, but the input JSON will be modified. Check [in-situ parsing](doc/dom.md) for details.

5. When does parsing generate an error?

   The parser generates an error when the input JSON contains invalid syntax, or a value can not be represented (a number is too big), or the handler of parsers terminate the parsing. Check [parse error](doc/dom.md) for details.

6. What error information is provided? 

   The error is stored in `ParseResult`, which includes the error code and offset (number of characters from the beginning of JSON). The error code can be translated into human-readable error message.

7. Why not just using `double` to represent JSON number?

   Some applications use 64-bit unsigned/signed integers. And these integers cannot be converted into `double` without loss of precision. So the parsers detects whether a JSON number is convertible to different types of integers and/or `double`.

8. How to clear-and-minimize a document or value?

   Call one of the `SetXXX()` methods - they call destructor which deallocates DOM data:

   ~~~~~~~~~~cpp
   Document d;
   ...
   d.SetObject();  // clear and minimize
   ~~~~~~~~~~

   Alternatively, use equivalent of the [C++ swap with temporary idiom](https://en.wikibooks.org/wiki/More_C%2B%2B_Idioms/Clear-and-minimize):
   ~~~~~~~~~~cpp
   Value(kObjectType).Swap(d);
   ~~~~~~~~~~
   or equivalent, but slightly longer to type:
   ~~~~~~~~~~cpp
   d.Swap(Value(kObjectType).Move()); 
   ~~~~~~~~~~

9. How to insert a document node into another document?

   Let's take the following two DOM trees represented as JSON documents:
   ~~~~~~~~~~cpp
   Document person;
   person.Parse("{\"person\":{\"name\":{\"first\":\"Adam\",\"last\":\"Thomas\"}}}");
   
   Document address;
   address.Parse("{\"address\":{\"city\":\"Moscow\",\"street\":\"Quiet\"}}");
   ~~~~~~~~~~
   Let's assume we want to merge them in such way that the whole `address` document becomes a node of the `person`:
   ~~~~~~~~~~js
   { "person": {
      "name": { "first": "Adam", "last": "Thomas" },
      "address": { "city": "Moscow", "street": "Quiet" }
      }
   }
   ~~~~~~~~~~

   The most important requirement to take care of document and value life-cycle as well as consistent memory management using the right allocator during the value transfer.
   
   Simple yet most efficient way to achieve that is to modify the `address` definition above to initialize it with allocator of the `person` document, then we just add the root member of the value:
   ~~~~~~~~~~cpp
   Document address(&person.GetAllocator());
   ...
   person["person"].AddMember("address", address["address"], person.GetAllocator());
   ~~~~~~~~~~
Alternatively, if we don't want to explicitly refer to the root value of `address` by name, we can refer to it via iterator:
   ~~~~~~~~~~cpp
   auto addressRoot = address.MemberBegin();
   person["person"].AddMember(addressRoot->name, addressRoot->value, person.GetAllocator());
   ~~~~~~~~~~
   
   Second way is to deep-clone the value from the address document:
   ~~~~~~~~~~cpp
   Value addressValue = Value(address["address"], person.GetAllocator());
   person["person"].AddMember("address", addressValue, person.GetAllocator());
   ~~~~~~~~~~

## Document/Value (DOM)

1. What is move semantics? Why?

   Instead of copy semantics, move semantics is used in `Value`. That means, when assigning a source value to a target value, the ownership of source value is moved to the target value.

   Since moving is faster than copying, this design decision forces user to aware of the copying overhead.

2. How to copy a value?

   There are two APIs: constructor with allocator, and `CopyFrom()`. See [Deep Copy Value](doc/tutorial.md) for an example.

3. Why do I need to provide the length of string?

   Since C string is null-terminated, the length of string needs to be computed via `strlen()`, with linear runtime complexity. This incurs an unnecessary overhead of many operations, if the user already knows the length of string.

   Also, RapidJSON can handle `\u0000` (null character) within a string. If a string contains null characters, `strlen()` cannot return the true length of it. In such case user must provide the length of string explicitly.

4. Why do I need to provide allocator parameter in many DOM manipulation API?

   Since the APIs are member functions of `Value`, we do not want to save an allocator pointer in every `Value`.

5. Does it convert between numerical types?

   When using `GetInt()`, `GetUint()`, ... conversion may occur. For integer-to-integer conversion, it only convert when it is safe (otherwise it will assert). However, when converting a 64-bit signed/unsigned integer to double, it will convert but be aware that it may lose precision. A number with fraction, or an integer larger than 64-bit, can only be obtained by `GetDouble()`.

## Reader/Writer (SAX)

1. Why don't we just `printf` a JSON? Why do we need a `Writer`? 

   Most importantly, `Writer` will ensure the output JSON is well-formed. Calling SAX events incorrectly (e.g. `StartObject()` pairing with `EndArray()`) will assert. Besides, `Writer` will escapes strings (e.g., `\n`). Finally, the numeric output of `printf()` may not be a valid JSON number, especially in some locale with digit delimiters. And the number-to-string conversion in `Writer` is implemented with very fast algorithms, which outperforms than `printf()` or `iostream`.

2. Can I pause the parsing process and resume it later?

   This is not directly supported in the current version due to performance consideration. However, if the execution environment supports multi-threading, user can parse a JSON in a separate thread, and pause it by blocking in the input stream.

## Unicode

1. Does it support UTF-8, UTF-16 and other format?

   Yes. It fully support UTF-8, UTF-16 (LE/BE), UTF-32 (LE/BE) and ASCII. 

2. Can it validate the encoding?

   Yes, just pass `kParseValidateEncodingFlag` to `Parse()`. If there is invalid encoding in the stream, it will generate `kParseErrorStringInvalidEncoding` error.

3. What is surrogate pair? Does RapidJSON support it?

   JSON uses UTF-16 encoding when escaping unicode character, e.g. `\u5927` representing Chinese character "big". To handle characters other than those in basic multilingual plane (BMP), UTF-16 encodes those characters with two 16-bit values, which is called UTF-16 surrogate pair. For example, the Emoji character U+1F602 can be encoded as `\uD83D\uDE02` in JSON.

   RapidJSON fully support parsing/generating UTF-16 surrogates. 

4. Can it handle `\u0000` (null character) in JSON string?

   Yes. RapidJSON fully support null character in JSON string. However, user need to be aware of it and using `GetStringLength()` and related APIs to obtain the true length of string.

5. Can I output `\uxxxx` for all non-ASCII character?

   Yes, use `ASCII<>` as output encoding template parameter in `Writer` can enforce escaping those characters.

## Stream

1. I have a big JSON file. Should I load the whole file to memory?

   User can use `FileReadStream` to read the file chunk-by-chunk. But for *in situ* parsing, the whole file must be loaded.

2. Can I parse JSON while it is streamed from network?

   Yes. User can implement a custom stream for this. Please refer to the implementation of `FileReadStream`.

3. I don't know what encoding will the JSON be. How to handle them?

   You may use `AutoUTFInputStream` which detects the encoding of input stream automatically. However, it will incur some performance overhead.

4. What is BOM? How RapidJSON handle it?

   [Byte order mark (BOM)](http://en.wikipedia.org/wiki/Byte_order_mark) sometimes reside at the beginning of file/stream to indicate the UTF encoding type of it.

   RapidJSON's `EncodedInputStream` can detect/consume BOM. `EncodedOutputStream` can optionally write a BOM. See [Encoded Streams](doc/stream.md) for example.

5. Why little/big endian is related?

   little/big endian of stream is an issue for UTF-16 and UTF-32 streams, but not UTF-8 stream.

## Performance

1. Is RapidJSON really fast?

   Yes. It may be the fastest open source JSON library. There is a [benchmark](https://github.com/miloyip/nativejson-benchmark) for evaluating performance of C/C++ JSON libraries.

2. Why is it fast?

   Many design decisions of RapidJSON is aimed at time/space performance. These may reduce user-friendliness of APIs. Besides, it also employs low-level optimizations (intrinsics, SIMD) and special algorithms (custom double-to-string, string-to-double conversions).

3. What is SIMD? How it is applied in RapidJSON?

   [SIMD](http://en.wikipedia.org/wiki/SIMD) instructions can perform parallel computation in modern CPUs. RapidJSON support Intel's SSE2/SSE4.2 and ARM's Neon to accelerate whitespace/tabspace/carriage-return/line-feed skipping. This improves performance of parsing indent formatted JSON. Define `RAPIDJSON_SSE2`, `RAPIDJSON_SSE42` or `RAPIDJSON_NEON` macro to enable this feature. However, running the executable on a machine without such instruction set support will make it crash.

4. Does it consume a lot of memory?

   The design of RapidJSON aims at reducing memory footprint.

   In the SAX API, `Reader` consumes memory proportional to maximum depth of JSON tree, plus maximum length of JSON string.

   In the DOM API, each `Value` consumes exactly 16/24 bytes for 32/64-bit architecture respectively. RapidJSON also uses a special memory allocator to minimize overhead of allocations.

5. What is the purpose of being high performance?

   Some applications need to process very large JSON files. Some server-side applications need to process huge amount of JSONs. Being high performance can improve both latency and throughput. In a broad sense, it will also save energy.

## Gossip

1. Who are the developers of RapidJSON?

   Milo Yip ([miloyip](https://github.com/miloyip)) is the original author of RapidJSON. Many contributors from the world have improved RapidJSON.  Philipp A. Hartmann ([pah](https://github.com/pah)) has implemented a lot of improvements, setting up automatic testing and also involves in a lot of discussions for the community. Don Ding ([thebusytypist](https://github.com/thebusytypist)) implemented the iterative parser. Andrii Senkovych ([jollyroger](https://github.com/jollyroger)) completed the CMake migration. Kosta ([Kosta-Github](https://github.com/Kosta-Github)) provided a very neat short-string optimization. Thank you for all other contributors and community members as well.

2. Why do you develop RapidJSON?

   It was just a hobby project initially in 2011. Milo Yip is a game programmer and he just knew about JSON at that time and would like to apply JSON in future projects. As JSON seems very simple he would like to write a header-only and fast library.

3. Why there is a long empty period of development?

   It is basically due to personal issues, such as getting new family members. Also, Milo Yip has spent a lot of spare time on translating "Game Engine Architecture" by Jason Gregory into Chinese.

4. Why did the repository move from Google Code to GitHub?

   This is the trend. And GitHub is much more powerful and convenient.

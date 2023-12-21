# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

## 1.1.0 - 2016-08-25

### Added
* Add GenericDocument ctor overload to specify JSON type (#369)
* Add FAQ (#372, #373, #374, #376)
* Add forward declaration header `fwd.h`
* Add @PlatformIO Library Registry manifest file (#400)
* Implement assignment operator for BigInteger (#404)
* Add comments support (#443)
* Adding coapp definition (#460)
* documenttest.cpp: EXPECT_THROW when checking empty allocator (470)
* GenericDocument: add implicit conversion to ParseResult (#480)
* Use <wchar.h> with C++ linkage on Windows ARM (#485)
* Detect little endian for Microsoft ARM targets 
* Check Nan/Inf when writing a double (#510)
* Add JSON Schema Implementation (#522)
* Add iostream wrapper (#530)
* Add Jsonx example for converting JSON into JSONx (a XML format) (#531)
* Add optional unresolvedTokenIndex parameter to Pointer::Get() (#532)
* Add encoding validation option for Writer/PrettyWriter (#534)
* Add Writer::SetMaxDecimalPlaces() (#536)
* Support {0, } and {0, m} in Regex (#539)
* Add Value::Get/SetFloat(), Value::IsLossLessFloat/Double() (#540)
* Add stream position check to reader unit tests (#541)
* Add Templated accessors and range-based for (#542)
* Add (Pretty)Writer::RawValue() (#543)
* Add Document::Parse(std::string), Document::Parse(const char*, size_t length) and related APIs. (#553)
* Add move constructor for GenericSchemaDocument (#554)
* Add VS2010 and VS2015 to AppVeyor CI (#555)
* Add parse-by-parts example (#556, #562)
* Support parse number as string (#564, #589)
* Add kFormatSingleLineArray for PrettyWriter (#577)
* Added optional support for trailing commas (#584)
* Added filterkey and filterkeydom examples (#615)
* Added npm docs (#639)
* Allow options for writing and parsing NaN/Infinity (#641)
* Add std::string overload to PrettyWriter::Key() when RAPIDJSON_HAS_STDSTRING is defined (#698)

### Fixed
* Fix gcc/clang/vc warnings (#350, #394, #397, #444, #447, #473, #515, #582, #589, #595, #667)
* Fix documentation (#482, #511, #550, #557, #614, #635, #660)
* Fix emscripten alignment issue (#535)
* Fix missing allocator to uses of AddMember in document (#365)
* CMake will no longer complain that the minimum CMake version is not specified (#501)
* Make it usable with old VC8 (VS2005) (#383)
* Prohibit C++11 move from Document to Value (#391)
* Try to fix incorrect 64-bit alignment (#419)
* Check return of fwrite to avoid warn_unused_result build failures (#421)
* Fix UB in GenericDocument::ParseStream (#426)
* Keep Document value unchanged on parse error (#439)
* Add missing return statement (#450)
* Fix Document::Parse(const Ch*) for transcoding (#478)
* encodings.h: fix typo in preprocessor condition (#495)
* Custom Microsoft headers are necessary only for Visual Studio 2012 and lower (#559)
* Fix memory leak for invalid regex (26e69ffde95ba4773ab06db6457b78f308716f4b)
* Fix a bug in schema minimum/maximum keywords for 64-bit integer (e7149d665941068ccf8c565e77495521331cf390)
* Fix a crash bug in regex (#605)
* Fix schema "required" keyword cannot handle duplicated keys (#609)
* Fix cmake CMP0054 warning (#612)
* Added missing include guards in istreamwrapper.h and ostreamwrapper.h (#634)
* Fix undefined behaviour (#646)
* Fix buffer overrun using PutN (#673)
* Fix rapidjson::value::Get<std::string>() may returns wrong data (#681)
* Add Flush() for all value types (#689)
* Handle malloc() fail in PoolAllocator (#691)
* Fix builds on x32 platform. #703

### Changed
* Clarify problematic JSON license (#392)
* Move Travis to container based infrastructure (#504, #558)
* Make whitespace array more compact (#513)
* Optimize Writer::WriteString() with SIMD (#544)
* x86-64 48-bit pointer optimization for GenericValue (#546)
* Define RAPIDJSON_HAS_CXX11_RVALUE_REFS directly in clang (#617)
* Make GenericSchemaDocument constructor explicit (#674)
* Optimize FindMember when use std::string (#690)

## [1.0.2] - 2015-05-14

### Added
* Add Value::XXXMember(...) overloads for std::string (#335)

### Fixed
* Include rapidjson.h for all internal/error headers.
* Parsing some numbers incorrectly in full-precision mode (`kFullPrecisionParseFlag`) (#342)
* Fix some numbers parsed incorrectly (#336)
* Fix alignment of 64bit platforms (#328)
* Fix MemoryPoolAllocator::Clear() to clear user-buffer (0691502573f1afd3341073dd24b12c3db20fbde4)

### Changed
* CMakeLists for include as a thirdparty in projects (#334, #337)
* Change Document::ParseStream() to use stack allocator for Reader (ffbe38614732af8e0b3abdc8b50071f386a4a685) 

## [1.0.1] - 2015-04-25

### Added
* Changelog following [Keep a CHANGELOG](https://github.com/olivierlacan/keep-a-changelog) suggestions.

### Fixed
* Parsing of some numbers (e.g. "1e-00011111111111") causing assertion (#314).
* Visual C++ 32-bit compilation error in `diyfp.h` (#317).

## [1.0.0] - 2015-04-22

### Added
* 100% [Coverall](https://coveralls.io/r/Tencent/rapidjson?branch=master) coverage.
* Version macros (#311)

### Fixed
* A bug in trimming long number sequence (4824f12efbf01af72b8cb6fc96fae7b097b73015).
* Double quote in unicode escape (#288).
* Negative zero roundtrip (double only) (#289).
* Standardize behavior of `memcpy()` and `malloc()` (0c5c1538dcfc7f160e5a4aa208ddf092c787be5a, #305, 0e8bbe5e3ef375e7f052f556878be0bd79e9062d).

### Removed
* Remove an invalid `Document::ParseInsitu()` API (e7f1c6dd08b522cfcf9aed58a333bd9a0c0ccbeb).

## 1.0-beta - 2015-04-8

### Added
* RFC 7159 (#101)
* Optional Iterative Parser (#76)
* Deep-copy values (#20)
* Error code and message (#27)
* ASCII Encoding (#70)
* `kParseStopWhenDoneFlag` (#83)
* `kParseFullPrecisionFlag` (881c91d696f06b7f302af6d04ec14dd08db66ceb)
* Add `Key()` to handler concept (#134)
* C++11 compatibility and support (#128)
* Optimized number-to-string and vice versa conversions (#137, #80)
* Short-String Optimization (#131)
* Local stream optimization by traits (#32)
* Travis & Appveyor Continuous Integration, with Valgrind verification (#24, #242)
* Redo all documentation (English, Simplified Chinese)

### Changed
* Copyright ownership transferred to THL A29 Limited (a Tencent company).
* Migrating from Premake to CMAKE (#192)
* Resolve all warning reports

### Removed
* Remove other JSON libraries for performance comparison (#180)

## 0.11 - 2012-11-16

## 0.1 - 2011-11-18

[Unreleased]: https://github.com/Tencent/rapidjson/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/Tencent/rapidjson/compare/v1.0.2...v1.1.0
[1.0.2]: https://github.com/Tencent/rapidjson/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/Tencent/rapidjson/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/Tencent/rapidjson/compare/v1.0-beta...v1.0.0

# 编码

根据 [ECMA-404](http://www.ecma-international.org/publications/files/ECMA-ST/ECMA-404.pdf)：

> (in Introduction) JSON text is a sequence of Unicode code points.
> 
> 翻译：JSON 文本是 Unicode 码点的序列。

较早的 [RFC4627](http://www.ietf.org/rfc/rfc4627.txt) 申明：

> (in §3) JSON text SHALL be encoded in Unicode.  The default encoding is UTF-8.
> 
> 翻译：JSON 文本应该以 Unicode 编码。缺省的编码为 UTF-8。

> (in §6) JSON may be represented using UTF-8, UTF-16, or UTF-32. When JSON is written in UTF-8, JSON is 8bit compatible.  When JSON is written in UTF-16 or UTF-32, the binary content-transfer-encoding must be used.
> 
> 翻译：JSON 可使用 UTF-8、UTF-16 或 UTF-32 表示。当 JSON 以 UTF-8 写入，该 JSON 是 8 位兼容的。当 JSON 以 UTF-16 或 UTF-32 写入，就必须使用二进制的内容传送编码。

RapidJSON 支持多种编码。它也能检查 JSON 的编码，以及在不同编码中进行转码。所有这些功能都是在内部实现，无需使用外部的程序库（如 [ICU](http://site.icu-project.org/)）。

[TOC]

# Unicode {#Unicode}
根据 [Unicode 的官方网站](http://www.unicode.org/standard/translations/t-chinese.html)：
>Unicode 给每个字符提供了一个唯一的数字，
不论是什么平台、
不论是什么程序、
不论是什么语言。

这些唯一数字称为码点（code point），其范围介乎 `0x0` 至 `0x10FFFF` 之间。

## Unicode 转换格式 {#UTF}

存储 Unicode 码点有多种编码方式。这些称为 Unicode 转换格式（Unicode Transformation Format, UTF）。RapidJSON 支持最常用的 UTF，包括：

* UTF-8：8 位可变长度编码。它把一个码点映射至 1 至 4 个字节。
* UTF-16：16 位可变长度编码。它把一个码点映射至 1 至 2 个 16 位编码单元（即 2 至 4 个字节）。
* UTF-32：32 位固定长度编码。它直接把码点映射至单个 32 位编码单元（即 4 字节）。

对于 UTF-16 及 UTF-32 来说，字节序（endianness）是有影响的。在内存中，它们通常都是以该计算机的字节序来存储。然而，当要储存在文件中或在网上传输，我们需要指明字节序列的字节序，是小端（little endian, LE）还是大端（big-endian, BE）。 

RapidJSON 通过 `rapidjson/encodings.h` 中的 struct 去提供各种编码：

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

对于在内存中的文本，我们正常会使用 `UTF8`、`UTF16` 或 `UTF32`。对于处理经过 I/O 的文本，我们可使用 `UTF8`、`UTF16LE`、`UTF16BE`、`UTF32LE` 或 `UTF32BE`。

当使用 DOM 风格的 API，`GenericValue<Encoding>` 及 `GenericDocument<Encoding>` 里的 `Encoding` 模板参数是用于指明内存中存储的 JSON 字符串使用哪种编码。因此通常我们会在此参数中使用 `UTF8`、`UTF16` 或 `UTF32`。如何选择，视乎应用软件所使用的操作系统及其他程序库。例如，Windows API 使用 UTF-16 表示 Unicode 字符，而多数的 Linux 发行版本及应用软件则更喜欢 UTF-8。

使用 UTF-16 的 DOM 声明例子：

~~~~~~~~~~cpp
typedef GenericDocument<UTF16<> > WDocument;
typedef GenericValue<UTF16<> > WValue;
~~~~~~~~~~

可以在 [DOM's Encoding](doc/stream.zh-cn.md) 一节看到更详细的使用例子。

## 字符类型 {#CharacterType}

从之前的声明中可以看到，每个编码都有一个 `CharType` 模板参数。这可能比较容易混淆，实际上，每个 `CharType` 存储一个编码单元，而不是一个字符（码点）。如之前所谈及，在 UTF-8 中一个码点可能会编码成 1 至 4 个编码单元。

对于 `UTF16(LE|BE)` 及 `UTF32(LE|BE)` 来说，`CharType` 必须分别是一个至少 2 及 4 字节的整数类型。

注意 C++11 新添了 `char16_t` 及 `char32_t` 类型，也可分别用于 `UTF16` 及 `UTF32`。

## AutoUTF {#AutoUTF}

上述所介绍的编码都是在编译期静态挷定的。换句话说，使用者必须知道内存或流之中使用了哪种编码。然而，有时候我们可能需要读写不同编码的文件，而且这些编码需要在运行时才能决定。

`AutoUTF` 是为此而设计的编码。它根据输入或输出流来选择使用哪种编码。目前它应该与 `EncodedInputStream` 及 `EncodedOutputStream` 结合使用。

## ASCII {#ASCII}

虽然 JSON 标准并未提及 [ASCII](http://en.wikipedia.org/wiki/ASCII)，有时候我们希望写入 7 位的 ASCII JSON，以供未能处理 UTF-8 的应用程序使用。由于任 JSON 都可以把 Unicode 字符表示为 `\uXXXX` 转义序列，JSON 总是可用 ASCII 来编码。

以下的例子把 UTF-8 的 DOM 写成 ASCII 的 JSON：

~~~~~~~~~~cpp
using namespace rapidjson;
Document d; // UTF8<>
// ...
StringBuffer buffer;
Writer<StringBuffer, Document::EncodingType, ASCII<> > writer(buffer);
d.Accept(writer);
std::cout << buffer.GetString();
~~~~~~~~~~

ASCII 可用于输入流。当输入流包含大于 127 的字节，就会导致 `kParseErrorStringInvalidEncoding` 错误。

ASCII * 不能 * 用于内存（`Document` 的编码，或 `Reader` 的目标编码)，因为它不能表示 Unicode 码点。

# 校验及转码 {#ValidationTranscoding}

当 RapidJSON 解析一个 JSON 时，它能校验输入 JSON，判断它是否所标明编码的合法序列。要开启此选项，请把 `kParseValidateEncodingFlag` 加入 `parseFlags` 模板参数。

若输入编码和输出编码并不相同，`Reader` 及 `Writer` 会算把文本转码。在这种情况下，并不需要 `kParseValidateEncodingFlag`，因为它必须解码输入序列。若序列不能被解码，它必然是不合法的。

## 转码器 {#Transcoder}

虽然 RapidJSON 的编码功能是为 JSON 解析／生成而设计，使用者也可以“滥用”它们来为非 JSON 字符串转码。

以下的例子把 UTF-8 字符串转码成 UTF-16：

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

你也可以用 `AutoUTF` 及对应的流来在运行时设置内源／目的之编码。

# SAX

"SAX" 此术语源于 [Simple API for XML](http://en.wikipedia.org/wiki/Simple_API_for_XML)。我们借了此术语去套用在 JSON 的解析及生成。

在 RapidJSON 中，`Reader`（`GenericReader<...>` 的 typedef）是 JSON 的 SAX 风格解析器，而 `Writer`（`GenericWriter<...>` 的 typedef）则是 JSON 的 SAX 风格生成器。

[TOC]

# Reader {#Reader}

`Reader` 从输入流解析一个 JSON。当它从流中读取字符时，它会基于 JSON 的语法去分析字符，并向处理器发送事件。

例如，以下是一个 JSON。

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

当一个 `Reader` 解析此 JSON 时，它会顺序地向处理器发送以下的事件：

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

除了一些事件参数需要再作解释，这些事件可以轻松地与 JSON 对上。我们可以看看 `simplereader` 例子怎样产生和以上完全相同的结果：

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

注意 RapidJSON 使用模板去静态挷定 `Reader` 类型及处理器的类型，而不是使用含虚函数的类。这个范式可以通过把函数内联而改善性能。

## 处理器 {#Handler}

如前例所示，使用者需要实现一个处理器（handler），用于处理来自 `Reader` 的事件（函数调用）。处理器必须包含以下的成员函数。

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

当 `Reader` 遇到 JSON null 值时会调用 `Null()`。

当 `Reader` 遇到 JSON true 或 false 值时会调用 `Bool(bool)`。

当 `Reader` 遇到 JSON number，它会选择一个合适的 C++ 类型映射，然后调用 `Int(int)`、`Uint(unsigned)`、`Int64(int64_t)`、`Uint64(uint64_t)` 及 `Double(double)` 的 * 其中之一个 *。 若开启了 `kParseNumbersAsStrings` 选项，`Reader` 便会改为调用 `RawNumber()`。

当 `Reader` 遇到 JSON string，它会调用 `String(const char* str, SizeType length, bool copy)`。第一个参数是字符串的指针。第二个参数是字符串的长度（不包含空终止符号）。注意 RapidJSON 支持字串中含有空字符 `\0`。若出现这种情况，便会有 `strlen(str) < length`。最后的 `copy` 参数表示处理器是否需要复制该字符串。在正常解析时，`copy = true`。仅当使用原位解析时，`copy = false`。此外，还要注意字符的类型与目标编码相关，我们稍后会再谈这一点。

当 `Reader` 遇到 JSON object 的开始之时，它会调用 `StartObject()`。JSON 的 object 是一个键值对（成员）的集合。若 object 包含成员，它会先为成员的名字调用 `Key()`，然后再按值的类型调用函数。它不断调用这些键值对，直至最终调用 `EndObject(SizeType memberCount)`。注意 `memberCount` 参数对处理器来说只是协助性质，使用者可能不需要此参数。

JSON array 与 object 相似，但更简单。在 array 开始时，`Reader` 会调用 `BeginArary()`。若 array 含有元素，它会按元素的类型来读用函数。相似地，最后它会调用 `EndArray(SizeType elementCount)`，其中 `elementCount` 参数对处理器来说只是协助性质。

每个处理器函数都返回一个 `bool`。正常它们应返回 `true`。若处理器遇到错误，它可以返回 `false` 去通知事件发送方停止继续处理。

例如，当我们用 `Reader` 解析一个 JSON 时，处理器检测到该 JSON 并不符合所需的 schema，那么处理器可以返回 `false`，令 `Reader` 停止之后的解析工作。而 `Reader` 会进入一个错误状态，并以 `kParseErrorTermination` 错误码标识。

## GenericReader {#GenericReader}

前面提及，`Reader` 是 `GenericReader` 模板类的 typedef：

~~~~~~~~~~cpp
namespace rapidjson {

template <typename SourceEncoding, typename TargetEncoding, typename Allocator = MemoryPoolAllocator<> >
class GenericReader {
    // ...
};

typedef GenericReader<UTF8<>, UTF8<> > Reader;

} // namespace rapidjson
~~~~~~~~~~

`Reader` 使用 UTF-8 作为来源及目标编码。来源编码是指 JSON 流的编码。目标编码是指 `String()` 的 `str` 参数所用的编码。例如，要解析一个 UTF-8 流并输出至 UTF-16 string 事件，你需要这么定义一个 reader：

~~~~~~~~~~cpp
GenericReader<UTF8<>, UTF16<> > reader;
~~~~~~~~~~

注意到 `UTF16` 的缺省类型是 `wchar_t`。因此这个 `reader` 需要调用处理器的 `String(const wchar_t*, SizeType, bool)`。

第三个模板参数 `Allocator` 是内部数据结构（实际上是一个堆栈）的分配器类型。

## 解析 {#SaxParsing}

`Reader` 的唯一功能就是解析 JSON。 

~~~~~~~~~~cpp
template <unsigned parseFlags, typename InputStream, typename Handler>
bool Parse(InputStream& is, Handler& handler);

// 使用 parseFlags = kDefaultParseFlags
template <typename InputStream, typename Handler>
bool Parse(InputStream& is, Handler& handler);
~~~~~~~~~~

若在解析中出现错误，它会返回 `false`。使用者可调用 `bool HasParseEror()`, `ParseErrorCode GetParseErrorCode()` 及 `size_t GetErrorOffset()` 获取错误状态。实际上 `Document` 使用这些 `Reader` 函数去获取解析错误。请参考 [DOM](doc/dom.zh-cn.md) 去了解有关解析错误的细节。

# Writer {#Writer}

`Reader` 把 JSON 转换（解析）成为事件。`Writer` 做完全相反的事情。它把事件转换成 JSON。

`Writer` 是非常容易使用的。若你的应用程序只需把一些数据转换成 JSON，可能直接使用 `Writer`，会比建立一个 `Document` 然后用 `Writer` 把它转换成 JSON 更加方便。

在 `simplewriter` 例子里，我们做 `simplereader` 完全相反的事情。

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

`String()` 及 `Key()` 各有两个重载。一个是如处理器 concept 般，有 3 个参数。它能处理含空字符的字符串。另一个是如上中使用的较简单版本。

注意到，例子代码中的 `EndArray()` 及 `EndObject()` 并没有参数。可以传递一个 `SizeType` 的参数，但它会被 `Writer` 忽略。

你可能会怀疑，为什么不使用 `sprintf()` 或 `std::stringstream` 去建立一个 JSON？

这有几个原因：
1. `Writer` 必然会输出一个结构良好（well-formed）的 JSON。若然有错误的事件次序（如 `Int()` 紧随 `StartObject()` 出现），它会在调试模式中产生断言失败。
2. `Writer::String()` 可处理字符串转义（如把码点 `U+000A` 转换成 `\n`）及进行 Unicode 转码。
3. `Writer` 一致地处理 number 的输出。
4. `Writer` 实现了事件处理器 concept。可用于处理来自 `Reader`、`Document` 或其他事件发生器。
5. `Writer` 可对不同平台进行优化。

无论如何，使用 `Writer` API 去生成 JSON 甚至乎比这些临时方法更简单。

## 模板 {#WriterTemplate}

`Writer` 与 `Reader` 有少许设计区别。`Writer` 是一个模板类，而不是一个 typedef。 并没有 `GenericWriter`。以下是 `Writer` 的声明。

~~~~~~~~~~cpp
namespace rapidjson {

template<typename OutputStream, typename SourceEncoding = UTF8<>, typename TargetEncoding = UTF8<>, typename Allocator = CrtAllocator<> >
class Writer {
public:
    Writer(OutputStream& os, Allocator* allocator = 0, size_t levelDepth = kDefaultLevelDepth)
// ...
};

} // namespace rapidjson
~~~~~~~~~~

`OutputStream` 模板参数是输出流的类型。它的类型不可以被自动推断，必须由使用者提供。

`SourceEncoding` 模板参数指定了 `String(const Ch*, ...)` 的编码。

`TargetEncoding` 模板参数指定输出流的编码。

`Allocator` 是分配器的类型，用于分配内部数据结构（一个堆栈）。

`writeFlags` 是以下位标志的组合：

写入位标志                     | 意义
------------------------------|-----------------------------------
`kWriteNoFlags`               | 没有任何标志。
`kWriteDefaultFlags`          | 缺省的解析选项。它等于 `RAPIDJSON_WRITE_DEFAULT_FLAGS` 宏，此宏定义为  `kWriteNoFlags`。
`kWriteValidateEncodingFlag`  | 校验 JSON 字符串的编码。
`kWriteNanAndInfFlag`         | 容许写入 `Infinity`, `-Infinity` 及 `NaN`。

此外，`Writer` 的构造函数有一 `levelDepth` 参数。存储每层阶信息的初始内存分配量受此参数影响。

## PrettyWriter {#PrettyWriter}

`Writer` 所输出的是没有空格字符的最紧凑 JSON，适合网络传输或储存，但不适合人类阅读。

因此，RapidJSON 提供了一个 `PrettyWriter`，它在输出中加入缩进及换行。

`PrettyWriter` 的用法与 `Writer` 几乎一样，不同之处是 `PrettyWriter` 提供了一个 `SetIndent(Ch indentChar, unsigned indentCharCount)` 函数。缺省的缩进是 4 个空格。

## 完整性及重置 {#CompletenessReset}

一个 `Writer` 只可输出单个 JSON，其根节点可以是任何 JSON 类型。当处理完单个根节点事件（如 `String()`），或匹配的最后 `EndObject()` 或 `EndArray()` 事件，输出的 JSON 是结构完整（well-formed）及完整的。使用者可调用 `Writer::IsComplete()` 去检测完整性。

当 JSON 完整时，`Writer` 不能再接受新的事件。不然其输出便会是不合法的（例如有超过一个根节点）。为了重新利用 `Writer` 对象，使用者可调用 `Writer::Reset(OutputStream& os)` 去重置其所有内部状态及设置新的输出流。

# 技巧 {#SaxTechniques}

## 解析 JSON 至自定义结构 {#CustomDataStructure}

`Document` 的解析功能完全依靠 `Reader`。实际上 `Document` 是一个处理器，在解析 JSON 时接收事件去建立一个 DOM。

使用者可以直接使用 `Reader` 去建立其他数据结构。这消除了建立 DOM 的步骤，从而减少了内存开销并改善性能。

在以下的 `messagereader` 例子中，`ParseMessages()` 解析一个 JSON，该 JSON 应该是一个含键值对的 object。

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

第一个 JSON（`json1`）被成功地解析至 `MessageMap`。由于 `MessageMap` 是一个 `std::map`，打印次序按键值排序。此次序与 JSON 中的次序不同。

在第二个 JSON（`json2`）中，`foo` 的值是一个空 object。由于它是一个 object，`MessageHandler::StartObject()` 会被调用。然而，在 `state_ = kExpectValue` 的情况下，该函数会返回 `false`，并导致解析过程终止。错误代码是 `kParseErrorTermination`。

## 过滤 JSON {#Filtering}

如前面提及过，`Writer` 可处理 `Reader` 发出的事件。`example/condense/condense.cpp` 例子简单地设置 `Writer` 作为一个 `Reader` 的处理器，因此它能移除 JSON 中的所有空白字符。`example/pretty/pretty.cpp` 例子使用同样的关系，只是以 `PrettyWriter` 取代 `Writer`。因此 `pretty` 能够重新格式化 JSON，加入缩进及换行。

实际上，我们可以使用 SAX 风格 API 去加入（多个）中间层去过滤 JSON 的内容。例如 `capitalize` 例子可以把所有 JSON string 改为大写。

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

注意到，不可简单地把 JSON 当作字符串去改为大写。例如：
~~~~~~~~~~
["Hello\nWorld"]
~~~~~~~~~~

简单地把整个 JSON 转为大写的话会产生错误的转义符：
~~~~~~~~~~
["HELLO\NWORLD"]
~~~~~~~~~~

而 `capitalize` 就会产生正确的结果：
~~~~~~~~~~
["HELLO\nWORLD"]
~~~~~~~~~~

我们还可以开发更复杂的过滤器。然而，由于 SAX 风格 API 在某一时间点只能提供单一事件的信息，使用者需要自行记录一些上下文信息（例如从根节点起的路径、储存其他相关值）。对于处理某些情况，用 DOM 会比 SAX 更容易实现。


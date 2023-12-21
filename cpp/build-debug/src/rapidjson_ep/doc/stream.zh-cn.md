# 流

在 RapidJSON 中，`rapidjson::Stream` 是用於读写 JSON 的概念（概念是指 C++ 的 concept）。在这里我们先介绍如何使用 RapidJSON 提供的各种流。然后再看看如何自行定义流。

[TOC]

# 内存流 {#MemoryStreams}

内存流把 JSON 存储在内存之中。

## StringStream（输入）{#StringStream}

`StringStream` 是最基本的输入流，它表示一个完整的、只读的、存储于内存的 JSON。它在 `rapidjson/rapidjson.h` 中定义。

~~~~~~~~~~cpp
#include "rapidjson/document.h" // 会包含 "rapidjson/rapidjson.h"

using namespace rapidjson;

// ...
const char json[] = "[1, 2, 3, 4]";
StringStream s(json);

Document d;
d.ParseStream(s);
~~~~~~~~~~

由于这是非常常用的用法，RapidJSON 提供 `Document::Parse(const char*)` 去做完全相同的事情：

~~~~~~~~~~cpp
// ...
const char json[] = "[1, 2, 3, 4]";
Document d;
d.Parse(json);
~~~~~~~~~~

需要注意，`StringStream` 是 `GenericStringStream<UTF8<> >` 的 typedef，使用者可用其他编码类去代表流所使用的字符集。

## StringBuffer（输出）{#StringBuffer}

`StringBuffer` 是一个简单的输出流。它分配一个内存缓冲区，供写入整个 JSON。可使用 `GetString()` 来获取该缓冲区。

~~~~~~~~~~cpp
#include "rapidjson/stringbuffer.h"
#include <rapidjson/writer.h>

StringBuffer buffer;
Writer<StringBuffer> writer(buffer);
d.Accept(writer);

const char* output = buffer.GetString();
~~~~~~~~~~

当缓冲区满溢，它将自动增加容量。缺省容量是 256 个字符（UTF8 是 256 字节，UTF16 是 512 字节等）。使用者能自行提供分配器及初始容量。

~~~~~~~~~~cpp
StringBuffer buffer1(0, 1024); // 使用它的分配器，初始大小 = 1024
StringBuffer buffer2(allocator, 1024);
~~~~~~~~~~

如无设置分配器，`StringBuffer` 会自行实例化一个内部分配器。

相似地，`StringBuffer` 是 `GenericStringBuffer<UTF8<> >` 的 typedef。

# 文件流 {#FileStreams}

当要从文件解析一个 JSON，你可以把整个 JSON 读入内存并使用上述的 `StringStream`。

然而，若 JSON 很大，或是内存有限，你可以改用 `FileReadStream`。它只会从文件读取一部分至缓冲区，然后让那部分被解析。若缓冲区的字符都被读完，它会再从文件读取下一部分。

## FileReadStream（输入） {#FileReadStream}

`FileReadStream` 通过 `FILE` 指针读取文件。使用者需要提供一个缓冲区。

~~~~~~~~~~cpp
#include "rapidjson/filereadstream.h"
#include <cstdio>

using namespace rapidjson;

FILE* fp = fopen("big.json", "rb"); // 非 Windows 平台使用 "r"

char readBuffer[65536];
FileReadStream is(fp, readBuffer, sizeof(readBuffer));

Document d;
d.ParseStream(is);

fclose(fp);
~~~~~~~~~~

与 `StringStreams` 不一样，`FileReadStream` 是一个字节流。它不处理编码。若文件并非 UTF-8 编码，可以把字节流用 `EncodedInputStream` 包装。我们很快会讨论这个问题。

除了读取文件，使用者也可以使用 `FileReadStream` 来读取 `stdin`。

## FileWriteStream（输出）{#FileWriteStream}

`FileWriteStream` 是一个含缓冲功能的输出流。它的用法与 `FileReadStream` 非常相似。

~~~~~~~~~~cpp
#include "rapidjson/filewritestream.h"
#include <rapidjson/writer.h>
#include <cstdio>

using namespace rapidjson;

Document d;
d.Parse(json);
// ...

FILE* fp = fopen("output.json", "wb"); // 非 Windows 平台使用 "w"

char writeBuffer[65536];
FileWriteStream os(fp, writeBuffer, sizeof(writeBuffer));

Writer<FileWriteStream> writer(os);
d.Accept(writer);

fclose(fp);
~~~~~~~~~~

它也可以把输出导向 `stdout`。

# iostream 包装类 {#iostreamWrapper}

基于用户的要求，RapidJSON 提供了正式的 `std::basic_istream` 和 `std::basic_ostream` 包装类。然而，请注意其性能会大大低于以上的其他流。

## IStreamWrapper {#IStreamWrapper}

`IStreamWrapper` 把任何继承自 `std::istream` 的类（如 `std::istringstream`、`std::stringstream`、`std::ifstream`、`std::fstream`）包装成 RapidJSON 的输入流。

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

对于继承自 `std::wistream` 的类，则使用 `WIStreamWrapper`。

## OStreamWrapper {#OStreamWrapper}

相似地，`OStreamWrapper` 把任何继承自 `std::ostream` 的类（如 `std::ostringstream`、`std::stringstream`、`std::ofstream`、`std::fstream`）包装成 RapidJSON 的输出流。

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

对于继承自 `std::wistream` 的类，则使用 `WIStreamWrapper`。

# 编码流 {#EncodedStreams}

编码流（encoded streams）本身不存储 JSON，它们是通过包装字节流来提供基本的编码／解码功能。

如上所述，我们可以直接读入 UTF-8 字节流。然而，UTF-16 及 UTF-32 有字节序（endian）问题。要正确地处理字节序，需要在读取时把字节转换成字符（如对 UTF-16 使用 `wchar_t`），以及在写入时把字符转换为字节。

除此以外，我们也需要处理 [字节顺序标记（byte order mark, BOM）](http://en.wikipedia.org/wiki/Byte_order_mark)。当从一个字节流读取时，需要检测 BOM，或者仅仅是把存在的 BOM 消去。当把 JSON 写入字节流时，也可选择写入 BOM。

若一个流的编码在编译期已知，你可使用 `EncodedInputStream` 及 `EncodedOutputStream`。若一个流可能存储 UTF-8、UTF-16LE、UTF-16BE、UTF-32LE、UTF-32BE 的 JSON，并且编码只能在运行时得知，你便可以使用 `AutoUTFInputStream` 及 `AutoUTFOutputStream`。这些流定义在 `rapidjson/encodedstream.h`。

注意到，这些编码流可以施于文件以外的流。例如，你可以用编码流包装内存中的文件或自定义的字节流。

## EncodedInputStream {#EncodedInputStream}

`EncodedInputStream` 含两个模板参数。第一个是 `Encoding` 类型，例如定义于 `rapidjson/encodings.h` 的 `UTF8`、`UTF16LE`。第二个参数是被包装的流的类型。

~~~~~~~~~~cpp
#include "rapidjson/document.h"
#include "rapidjson/filereadstream.h"   // FileReadStream
#include "rapidjson/encodedstream.h"    // EncodedInputStream
#include <cstdio>

using namespace rapidjson;

FILE* fp = fopen("utf16le.json", "rb"); // 非 Windows 平台使用 "r"

char readBuffer[256];
FileReadStream bis(fp, readBuffer, sizeof(readBuffer));

EncodedInputStream<UTF16LE<>, FileReadStream> eis(bis);  // 用 eis 包装 bis

Document d; // Document 为 GenericDocument<UTF8<> > 
d.ParseStream<0, UTF16LE<> >(eis);  // 把 UTF-16LE 文件解析至内存中的 UTF-8

fclose(fp);
~~~~~~~~~~

## EncodedOutputStream {#EncodedOutputStream}

`EncodedOutputStream` 也是相似的，但它的构造函数有一个 `bool putBOM` 参数，用于控制是否在输出字节流写入 BOM。

~~~~~~~~~~cpp
#include "rapidjson/filewritestream.h"  // FileWriteStream
#include "rapidjson/encodedstream.h"    // EncodedOutputStream
#include <rapidjson/writer.h>
#include <cstdio>

Document d;         // Document 为 GenericDocument<UTF8<> > 
// ...

FILE* fp = fopen("output_utf32le.json", "wb"); // 非 Windows 平台使用 "w"

char writeBuffer[256];
FileWriteStream bos(fp, writeBuffer, sizeof(writeBuffer));

typedef EncodedOutputStream<UTF32LE<>, FileWriteStream> OutputStream;
OutputStream eos(bos, true);   // 写入 BOM

Writer<OutputStream, UTF8<>, UTF32LE<>> writer(eos);
d.Accept(writer);   // 这里从内存的 UTF-8 生成 UTF32-LE 文件

fclose(fp);
~~~~~~~~~~

## AutoUTFInputStream {#AutoUTFInputStream}

有时候，应用软件可能需要㲃理所有可支持的 JSON 编码。`AutoUTFInputStream` 会先使用 BOM 来检测编码。若 BOM 不存在，它便会使用合法 JSON 的特性来检测。若两种方法都失败，它就会倒退至构造函数提供的 UTF 类型。

由于字符（编码单元／code unit）可能是 8 位、16 位或 32 位，`AutoUTFInputStream` 需要一个能至少储存 32 位的字符类型。我们可以使用 `unsigned` 作为模板参数：

~~~~~~~~~~cpp
#include "rapidjson/document.h"
#include "rapidjson/filereadstream.h"   // FileReadStream
#include "rapidjson/encodedstream.h"    // AutoUTFInputStream
#include <cstdio>

using namespace rapidjson;

FILE* fp = fopen("any.json", "rb"); // 非 Windows 平台使用 "r"

char readBuffer[256];
FileReadStream bis(fp, readBuffer, sizeof(readBuffer));

AutoUTFInputStream<unsigned, FileReadStream> eis(bis);  // 用 eis 包装 bis

Document d;         // Document 为 GenericDocument<UTF8<> > 
d.ParseStream<0, AutoUTF<unsigned> >(eis); // 把任何 UTF 编码的文件解析至内存中的 UTF-8

fclose(fp);
~~~~~~~~~~

当要指定流的编码，可使用上面例子中 `ParseStream()` 的参数 `AutoUTF<CharType>`。

你可以使用 `UTFType GetType()` 去获取 UTF 类型，并且用 `HasBOM()` 检测输入流是否含有 BOM。

## AutoUTFOutputStream {#AutoUTFOutputStream}

相似地，要在运行时选择输出的编码，我们可使用 `AutoUTFOutputStream`。这个类本身并非「自动」。你需要在运行时指定 UTF 类型，以及是否写入 BOM。

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

`AutoUTFInputStream`／`AutoUTFOutputStream` 是比 `EncodedInputStream`／`EncodedOutputStream` 方便。但前者会产生一点运行期额外开销。

# 自定义流 {#CustomStream}

除了内存／文件流，使用者可创建自行定义适配 RapidJSON API 的流类。例如，你可以创建网络流、从压缩文件读取的流等等。

RapidJSON 利用模板结合不同的类型。只要一个类包含所有所需的接口，就可以作为一个流。流的接合定义在 `rapidjson/rapidjson.h` 的注释里：

~~~~~~~~~~cpp
concept Stream {
    typename Ch;    //!< 流的字符类型

    //! 从流读取当前字符，不移动读取指针（read cursor）
    Ch Peek() const;

    //! 从流读取当前字符，移动读取指针至下一字符。
    Ch Take();

    //! 获取读取指针。
    //! \return 从开始以来所读过的字符数量。
    size_t Tell();

    //! 从当前读取指针开始写入操作。
    //! \return 返回开始写入的指针。
    Ch* PutBegin();

    //! 写入一个字符。
    void Put(Ch c);

    //! 清空缓冲区。
    void Flush();

    //! 完成写作操作。
    //! \param begin PutBegin() 返回的开始写入指针。
    //! \return 已写入的字符数量。
    size_t PutEnd(Ch* begin);
}
~~~~~~~~~~

输入流必须实现 `Peek()`、`Take()` 及 `Tell()`。
输出流必须实现 `Put()` 及 `Flush()`。
`PutBegin()` 及 `PutEnd()` 是特殊的接口，仅用于原位（*in situ*）解析。一般的流不需实现它们。然而，即使接口不需用于某些流，仍然需要提供空实现，否则会产生编译错误。

## 例子：istream 的包装类 {#ExampleIStreamWrapper}

以下的简单例子是 `std::istream` 的包装类，它只需现 3 个函数。

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

使用者能用它来包装 `std::stringstream`、`std::ifstream` 的实例。

~~~~~~~~~~cpp
const char* json = "[1,2,3,4]";
std::stringstream ss(json);
MyIStreamWrapper is(ss);

Document d;
d.ParseStream(is);
~~~~~~~~~~

但要注意，由于标准库的内部开销问，此实现的性能可能不如 RapidJSON 的内存／文件流。

## 例子：ostream 的包装类 {#ExampleOStreamWrapper}

以下的例子是 `std::istream` 的包装类，它只需实现 2 个函数。

~~~~~~~~~~cpp
class MyOStreamWrapper {
public:
    typedef char Ch;

    OStreamWrapper(std::ostream& os) : os_(os) {
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

使用者能用它来包装 `std::stringstream`、`std::ofstream` 的实例。

~~~~~~~~~~cpp
Document d;
// ...

std::stringstream ss;
MyOStreamWrapper os(ss);

Writer<MyOStreamWrapper> writer(os);
d.Accept(writer);
~~~~~~~~~~

但要注意，由于标准库的内部开销问，此实现的性能可能不如 RapidJSON 的内存／文件流。

# 总结 {#Summary}

本节描述了 RapidJSON 提供的各种流的类。内存流很简单。若 JSON 存储在文件中，文件流可减少 JSON 解析及生成所需的内存量。编码流在字节流和字符流之间作转换。最后，使用者可使用一个简单接口创建自定义的流。

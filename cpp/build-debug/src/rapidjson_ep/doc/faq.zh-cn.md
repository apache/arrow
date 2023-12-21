# 常见问题

[TOC]

## 一般问题

1. RapidJSON 是什么？

   RapidJSON 是一个 C++ 库，用于解析及生成 JSON。读者可参考它的所有 [特点](doc/features.zh-cn.md)。

2. 为什么称作 RapidJSON？

   它的灵感来自于 [RapidXML](http://rapidxml.sourceforge.net/)，RapidXML 是一个高速的 XML DOM 解析器。

3. RapidJSON 与 RapidXML 相似么？

   RapidJSON 借镜了 RapidXML 的一些设计, 包括原位（*in situ*）解析、只有头文件的库。但两者的 API 是完全不同的。此外 RapidJSON 也提供许多 RapidXML 没有的特点。

4. RapidJSON 是免费的么？

   是的，它在 MIT 协议下免费。它可用于商业软件。详情请参看 [license.txt](https://github.com/Tencent/rapidjson/blob/master/license.txt)。

5. RapidJSON 很小么？它有何依赖？

   是的。在 Windows 上，一个解析 JSON 并打印出统计的可执行文件少于 30KB。

   RapidJSON 仅依赖于 C++ 标准库。

6. 怎样安装 RapidJSON？

   见 [安装一节](../readme.zh-cn.md#安装)。

7. RapidJSON 能否运行于我的平台？

   社区已在多个操作系统／编译器／CPU 架构的组合上测试 RapidJSON。但我们无法确保它能运行于你特定的平台上。只需要生成及执行单元测试便能获取答案。

8. RapidJSON 支持 C++03 么？C++11 呢？

   RapidJSON 开始时在 C++03 上实现。后来加入了可选的 C++11 特性支持（如转移构造函数、`noexcept`）。RapidJSON 应该兼容所有遵从 C++03 或 C++11 的编译器。

9. RapidJSON 是否真的用于实际应用？

   是的。它被配置于前台及后台的真实应用中。一个社区成员说 RapidJSON 在他们的系统中每日解析 5 千万个 JSON。

10. RapidJSON 是如何被测试的？

   RapidJSON 包含一组单元测试去执行自动测试。[Travis](https://travis-ci.org/Tencent/rapidjson/)（供 Linux 平台）及 [AppVeyor](https://ci.appveyor.com/project/Tencent/rapidjson/)（供 Windows 平台）会对所有修改进行编译及执行单元测试。在 Linux 下还会使用 Valgrind 去检测内存泄漏。

11. RapidJSON 是否有完整的文档？

   RapidJSON 提供了使用手册及 API 说明文档。

12. 有没有其他替代品？

   有许多替代品。例如 [nativejson-benchmark](https://github.com/miloyip/nativejson-benchmark) 列出了一些开源的 C/C++ JSON 库。[json.org](http://www.json.org/) 也有一个列表。

## JSON

1. 什么是 JSON？

   JSON (JavaScript Object Notation) 是一个轻量的数据交换格式。它使用人类可读的文本格式。更多关于 JSON 的细节可考 [RFC7159](http://www.ietf.org/rfc/rfc7159.txt) 及 [ECMA-404](http://www.ecma-international.org/publications/standards/Ecma-404.htm)。

2. JSON 有什么应用场合？

   JSON 常用于网页应用程序，以传送结构化数据。它也可作为文件格式用于数据持久化。

3. RapidJSON 是否符合 JSON 标准？

   是。RapidJSON 完全符合 [RFC7159](http://www.ietf.org/rfc/rfc7159.txt) 及 [ECMA-404](http://www.ecma-international.org/publications/standards/Ecma-404.htm)。它能处理一些特殊情况，例如支持 JSON 字符串中含有空字符及代理对（surrogate pair）。

4. RapidJSON 是否支持宽松的语法？

   目前不支持。RapidJSON 只支持严格的标准格式。宽松语法可以在这个 [issue](https://github.com/Tencent/rapidjson/issues/36) 中进行讨论。

## DOM 与 SAX

1. 什么是 DOM 风格 API？

   Document Object Model（DOM）是一个储存于内存的 JSON 表示方式，用于查询及修改 JSON。

2. 什么是 SAX 风格 API?

   SAX 是一个事件驱动的 API，用于解析及生成 JSON。

3. 我应用 DOM 还是 SAX？

   DOM 易于查询及修改。SAX 则是非常快及省内存的，但通常较难使用。

4. 什么是原位（*in situ*）解析？

   原位解析会把 JSON 字符串直接解码至输入的 JSON 中。这是一个优化，可减少内存消耗及提升性能，但输入的 JSON 会被更改。进一步细节请参考 [原位解析](doc/dom.zh-cn.md) 。

5. 什么时候会产生解析错误？

   当输入的 JSON 包含非法语法，或不能表示一个值（如 Number 太大），或解析器的处理器中断解析过程，解析器都会产生一个错误。详情请参考 [解析错误](doc/dom.zh-cn.md)。

6. 有什么错误信息？

   错误信息存储在 `ParseResult`，它包含错误代号及偏移值（从 JSON 开始至错误处的字符数目）。可以把错误代号翻译为人类可读的错误讯息。

7. 为何不只使用 `double` 去表示 JSON number？

   一些应用需要使用 64 位无号／有号整数。这些整数不能无损地转换成 `double`。因此解析器会检测一个 JSON number 是否能转换至各种整数类型及 `double`。

8. 如何清空并最小化 `document` 或 `value` 的容量？

   调用 `SetXXX()` 方法 - 这些方法会调用析构函数，并重建空的 Object 或 Array:

   ~~~~~~~~~~cpp
   Document d;
   ...
   d.SetObject();  // clear and minimize
   ~~~~~~~~~~

   另外，也可以参考在 [C++ swap with temporary idiom](https://en.wikibooks.org/wiki/More_C%2B%2B_Idioms/Clear-and-minimize) 中的一种等价的方法:
   ~~~~~~~~~~cpp
   Value(kObjectType).Swap(d);
   ~~~~~~~~~~
   或者，使用这个稍微长一点的代码也能完成同样的事情:
   ~~~~~~~~~~cpp
   d.Swap(Value(kObjectType).Move()); 
   ~~~~~~~~~~

9. 如何将一个 `document` 节点插入到另一个 `document` 中？

   比如有以下两个 document(DOM):
   ~~~~~~~~~~cpp
   Document person;
   person.Parse("{\"person\":{\"name\":{\"first\":\"Adam\",\"last\":\"Thomas\"}}}");
   
   Document address;
   address.Parse("{\"address\":{\"city\":\"Moscow\",\"street\":\"Quiet\"}}");
   ~~~~~~~~~~
   假设我们希望将整个 `address` 插入到 `person` 中，作为其的一个子节点:
   ~~~~~~~~~~js
   { "person": {
      "name": { "first": "Adam", "last": "Thomas" },
      "address": { "city": "Moscow", "street": "Quiet" }
      }
   }
   ~~~~~~~~~~

   在插入节点的过程中需要注意 `document` 和 `value` 的生命周期并且正确地使用 allocator 进行内存分配和管理。

   一个简单有效的方法就是修改上述 `address` 变量的定义，让其使用 `person` 的 allocator 初始化，然后将其添加到根节点。

   ~~~~~~~~~~cpp
   Documnet address(&person.GetAllocator());
   ...
   person["person"].AddMember("address", address["address"], person.GetAllocator());
   ~~~~~~~~~~
   当然，如果你不想通过显式地写出 `address` 的 key 来得到其值，可以使用迭代器来实现:
   ~~~~~~~~~~cpp
   auto addressRoot = address.MemberBegin();
   person["person"].AddMember(addressRoot->name, addressRoot->value, person.GetAllocator());
   ~~~~~~~~~~
   
   此外，还可以通过深拷贝 address document 来实现:
   ~~~~~~~~~~cpp
   Value addressValue = Value(address["address"], person.GetAllocator());
   person["person"].AddMember("address", addressValue, person.GetAllocator());
   ~~~~~~~~~~

## Document/Value (DOM)

1. 什么是转移语义？为什么？

   `Value` 不用复制语义，而使用了转移语义。这是指，当把来源值赋值于目标值时，来源值的所有权会转移至目标值。

   由于转移快于复制，此设计决定强迫使用者注意到复制的消耗。

2. 怎样去复制一个值？

   有两个 API 可用：含 allocator 的构造函数，以及 `CopyFrom()`。可参考 [深复制 Value](doc/tutorial.zh-cn.md) 里的用例。

3. 为什么我需要提供字符串的长度？

   由于 C 字符串是空字符结尾的，需要使用 `strlen()` 去计算其长度，这是线性复杂度的操作。若使用者已知字符串的长度，对很多操作来说会造成不必要的消耗。

   此外，RapidJSON 可处理含有 `\u0000`（空字符）的字符串。若一个字符串含有空字符，`strlen()` 便不能返回真正的字符串长度。在这种情况下使用者必须明确地提供字符串长度。

4. 为什么在许多 DOM 操作 API 中要提供分配器作为参数？

   由于这些 API 是 `Value` 的成员函数，我们不希望为每个 `Value` 储存一个分配器指针。

5. 它会转换各种数值类型么？

   当使用 `GetInt()`、`GetUint()` 等 API 时，可能会发生转换。对于整数至整数转换，仅当保证转换安全才会转换（否则会断言失败）。然而，当把一个 64 位有号／无号整数转换至 double 时，它会转换，但有可能会损失精度。含有小数的数字、或大于 64 位的整数，都只能使用 `GetDouble()` 获取其值。

## Reader/Writer (SAX)

1. 为什么不仅仅用 `printf` 输出一个 JSON？为什么需要 `Writer`？

   最重要的是，`Writer` 能确保输出的 JSON 是格式正确的。错误地调用 SAX 事件（如 `StartObject()` 错配 `EndArray()`）会造成断言失败。此外，`Writer` 会把字符串进行转义（如 `\n`）。最后，`printf()` 的数值输出可能并不是一个合法的 JSON number，特别是某些 locale 会有数字分隔符。而且 `Writer` 的数值字符串转换是使用非常快的算法来实现的，胜过 `printf()` 及 `iostream`。

2. 我能否暂停解析过程，并在稍后继续？

   基于性能考虑，目前版本并不直接支持此功能。然而，若执行环境支持多线程，使用者可以在另一线程解析 JSON，并通过阻塞输入流去暂停。

## Unicode

1. 它是否支持 UTF-8、UTF-16 及其他格式？

   是。它完全支持 UTF-8、UTF-16（大端／小端）、UTF-32（大端／小端）及 ASCII。

2. 它能否检测编码的合法性？

   能。只需把 `kParseValidateEncodingFlag` 参考传给 `Parse()`。若发现在输入流中有非法的编码，它就会产生 `kParseErrorStringInvalidEncoding` 错误。

3. 什么是代理对（surrogate pair)？RapidJSON 是否支持？

   JSON 使用 UTF-16 编码去转义 Unicode 字符，例如 `\u5927` 表示中文字“大”。要处理基本多文种平面（basic multilingual plane，BMP）以外的字符时，UTF-16 会把那些字符编码成两个 16 位值，这称为 UTF-16 代理对。例如，绘文字字符 U+1F602 在 JSON 中可被编码成 `\uD83D\uDE02`。

   RapidJSON 完全支持解析及生成 UTF-16 代理对。 

4. 它能否处理 JSON 字符串中的 `\u0000`（空字符）？

   能。RapidJSON 完全支持 JSON 字符串中的空字符。然而，使用者需要注意到这件事，并使用 `GetStringLength()` 及相关 API 去取得字符串真正长度。

5. 能否对所有非 ASCII 字符输出成 `\uxxxx` 形式？

   可以。只要在 `Writer` 中使用 `ASCII<>` 作为输出编码参数，就可以强逼转义那些字符。

## 流

1. 我有一个很大的 JSON 文件。我应否把它整个载入内存中？

   使用者可使用 `FileReadStream` 去逐块读入文件。但若使用于原位解析，必须载入整个文件。

2. 我能否解析一个从网络上串流进来的 JSON？

   可以。使用者可根据 `FileReadStream` 的实现，去实现一个自定义的流。

3. 我不知道一些 JSON 将会使用哪种编码。怎样处理它们？

   你可以使用 `AutoUTFInputStream`，它能自动检测输入流的编码。然而，它会带来一些性能开销。

4. 什么是 BOM？RapidJSON 怎样处理它？

   [字节顺序标记（byte order mark, BOM）](http://en.wikipedia.org/wiki/Byte_order_mark) 有时会出现于文件／流的开始，以表示其 UTF 编码类型。

   RapidJSON 的 `EncodedInputStream` 可检测／跳过 BOM。`EncodedOutputStream` 可选择是否写入 BOM。可参考 [编码流](doc/stream.zh-cn.md) 中的例子。

5. 为什么会涉及大端／小端？

   流的大端／小端是 UTF-16 及 UTF-32 流要处理的问题，而 UTF-8 不需要处理。

## 性能

1. RapidJSON 是否真的快？

   是。它可能是最快的开源 JSON 库。有一个 [评测](https://github.com/miloyip/nativejson-benchmark) 评估 C/C++ JSON 库的性能。

2. 为什么它会快？

   RapidJSON 的许多设计是针对时间／空间性能来设计的，这些决定可能会影响 API 的易用性。此外，它也使用了许多底层优化（内部函数／intrinsic、SIMD）及特别的算法（自定义的 double 至字符串转换、字符串至 double 的转换）。

3. 什是是 SIMD？它如何用于 RapidJSON？

   [SIMD](http://en.wikipedia.org/wiki/SIMD) 指令可以在现代 CPU 中执行并行运算。RapidJSON 支持使用 Intel 的 SSE2/SSE4.2 和 ARM 的 Neon 来加速对空白符、制表符、回车符和换行符的过滤处理。在解析含缩进的 JSON 时，这能提升性能。只要定义名为 `RAPIDJSON_SSE2` ，`RAPIDJSON_SSE42` 或 `RAPIDJSON_NEON` 的宏，就能启动这个功能。然而，若在不支持这些指令集的机器上执行这些可执行文件，会导致崩溃。

4. 它会消耗许多内存么？

   RapidJSON 的设计目标是减低内存占用。

   在 SAX API 中，`Reader` 消耗的内存与 JSON 树深度加上最长 JSON 字符成正比。

   在 DOM API 中，每个 `Value` 在 32/64 位架构下分别消耗 16/24 字节。RapidJSON 也使用一个特殊的内存分配器去减少分配的额外开销。

5. 高性能的意义何在？

   有些应用程序需要处理非常大的 JSON 文件。而有些后台应用程序需要处理大量的 JSON。达到高性能同时改善延时及吞吐量。更广义来说，这也可以节省能源。

## 八卦

1. 谁是 RapidJSON 的开发者？

   叶劲峰（Milo Yip，[miloyip](https://github.com/miloyip)）是 RapidJSON 的原作者。全世界许多贡献者一直在改善 RapidJSON。Philipp A. Hartmann（[pah](https://github.com/pah)）实现了许多改进，也设置了自动化测试，而且还参与许多社区讨论。丁欧南（Don Ding，[thebusytypist](https://github.com/thebusytypist)）实现了迭代式解析器。Andrii Senkovych（[jollyroger](https://github.com/jollyroger)）完成了向 CMake 的迁移。Kosta（[Kosta-Github](https://github.com/Kosta-Github)）提供了一个非常灵巧的短字符串优化。也需要感谢其他献者及社区成员。

2. 为何你要开发 RapidJSON？

   在 2011 年开始这项目时，它只是一个兴趣项目。Milo Yip 是一个游戏程序员，他在那时候认识到 JSON 并希望在未来的项目中使用。由于 JSON 好像很简单，他希望写一个快速的仅有头文件的程序库。

3. 为什么开发中段有一段长期空档？

   主要是个人因素，例如加入新家庭成员。另外，Milo Yip 也花了许多业余时间去翻译 Jason Gregory 的《Game Engine Architecture》至中文版《游戏引擎架构》。

4. 为什么这个项目从 Google Code 搬到 GitHub？

   这是大势所趋，而且 GitHub 更为强大及方便。

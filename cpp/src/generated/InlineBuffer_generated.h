// automatically generated by the FlatBuffers compiler, do not modify


#ifndef FLATBUFFERS_GENERATED_INLINEBUFFER_ORG_APACHE_ARROW_COMPUTEIR_FLATBUF_H_
#define FLATBUFFERS_GENERATED_INLINEBUFFER_ORG_APACHE_ARROW_COMPUTEIR_FLATBUF_H_

#include "flatbuffers/flatbuffers.h"

namespace org {
namespace apache {
namespace arrow {
namespace computeir {
namespace flatbuf {

struct Int8Buffer;
struct Int8BufferBuilder;

struct Int16Buffer;
struct Int16BufferBuilder;

struct Int32Buffer;
struct Int32BufferBuilder;

struct Int64Buffer;
struct Int64BufferBuilder;

struct UInt8Buffer;
struct UInt8BufferBuilder;

struct UInt16Buffer;
struct UInt16BufferBuilder;

struct UInt32Buffer;
struct UInt32BufferBuilder;

struct UInt64Buffer;
struct UInt64BufferBuilder;

struct Float32Buffer;
struct Float32BufferBuilder;

struct Float64Buffer;
struct Float64BufferBuilder;

struct TableBuffer;
struct TableBufferBuilder;

struct InlineBuffer;
struct InlineBufferBuilder;

enum class InlineBufferImpl : uint8_t {
  NONE = 0,
  Int8Buffer = 1,
  Int16Buffer = 2,
  Int32Buffer = 3,
  Int64Buffer = 4,
  UInt8Buffer = 5,
  UInt16Buffer = 6,
  UInt32Buffer = 7,
  UInt64Buffer = 8,
  Float32Buffer = 9,
  Float64Buffer = 10,
  TableBuffer = 11,
  MIN = NONE,
  MAX = TableBuffer
};

inline const InlineBufferImpl (&EnumValuesInlineBufferImpl())[12] {
  static const InlineBufferImpl values[] = {
    InlineBufferImpl::NONE,
    InlineBufferImpl::Int8Buffer,
    InlineBufferImpl::Int16Buffer,
    InlineBufferImpl::Int32Buffer,
    InlineBufferImpl::Int64Buffer,
    InlineBufferImpl::UInt8Buffer,
    InlineBufferImpl::UInt16Buffer,
    InlineBufferImpl::UInt32Buffer,
    InlineBufferImpl::UInt64Buffer,
    InlineBufferImpl::Float32Buffer,
    InlineBufferImpl::Float64Buffer,
    InlineBufferImpl::TableBuffer
  };
  return values;
}

inline const char * const *EnumNamesInlineBufferImpl() {
  static const char * const names[13] = {
    "NONE",
    "Int8Buffer",
    "Int16Buffer",
    "Int32Buffer",
    "Int64Buffer",
    "UInt8Buffer",
    "UInt16Buffer",
    "UInt32Buffer",
    "UInt64Buffer",
    "Float32Buffer",
    "Float64Buffer",
    "TableBuffer",
    nullptr
  };
  return names;
}

inline const char *EnumNameInlineBufferImpl(InlineBufferImpl e) {
  if (flatbuffers::IsOutRange(e, InlineBufferImpl::NONE, InlineBufferImpl::TableBuffer)) return "";
  const size_t index = static_cast<size_t>(e);
  return EnumNamesInlineBufferImpl()[index];
}

template<typename T> struct InlineBufferImplTraits {
  static const InlineBufferImpl enum_value = InlineBufferImpl::NONE;
};

template<> struct InlineBufferImplTraits<org::apache::arrow::computeir::flatbuf::Int8Buffer> {
  static const InlineBufferImpl enum_value = InlineBufferImpl::Int8Buffer;
};

template<> struct InlineBufferImplTraits<org::apache::arrow::computeir::flatbuf::Int16Buffer> {
  static const InlineBufferImpl enum_value = InlineBufferImpl::Int16Buffer;
};

template<> struct InlineBufferImplTraits<org::apache::arrow::computeir::flatbuf::Int32Buffer> {
  static const InlineBufferImpl enum_value = InlineBufferImpl::Int32Buffer;
};

template<> struct InlineBufferImplTraits<org::apache::arrow::computeir::flatbuf::Int64Buffer> {
  static const InlineBufferImpl enum_value = InlineBufferImpl::Int64Buffer;
};

template<> struct InlineBufferImplTraits<org::apache::arrow::computeir::flatbuf::UInt8Buffer> {
  static const InlineBufferImpl enum_value = InlineBufferImpl::UInt8Buffer;
};

template<> struct InlineBufferImplTraits<org::apache::arrow::computeir::flatbuf::UInt16Buffer> {
  static const InlineBufferImpl enum_value = InlineBufferImpl::UInt16Buffer;
};

template<> struct InlineBufferImplTraits<org::apache::arrow::computeir::flatbuf::UInt32Buffer> {
  static const InlineBufferImpl enum_value = InlineBufferImpl::UInt32Buffer;
};

template<> struct InlineBufferImplTraits<org::apache::arrow::computeir::flatbuf::UInt64Buffer> {
  static const InlineBufferImpl enum_value = InlineBufferImpl::UInt64Buffer;
};

template<> struct InlineBufferImplTraits<org::apache::arrow::computeir::flatbuf::Float32Buffer> {
  static const InlineBufferImpl enum_value = InlineBufferImpl::Float32Buffer;
};

template<> struct InlineBufferImplTraits<org::apache::arrow::computeir::flatbuf::Float64Buffer> {
  static const InlineBufferImpl enum_value = InlineBufferImpl::Float64Buffer;
};

template<> struct InlineBufferImplTraits<org::apache::arrow::computeir::flatbuf::TableBuffer> {
  static const InlineBufferImpl enum_value = InlineBufferImpl::TableBuffer;
};

bool VerifyInlineBufferImpl(flatbuffers::Verifier &verifier, const void *obj, InlineBufferImpl type);
bool VerifyInlineBufferImplVector(flatbuffers::Verifier &verifier, const flatbuffers::Vector<flatbuffers::Offset<void>> *values, const flatbuffers::Vector<uint8_t> *types);

struct Int8Buffer FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  typedef Int8BufferBuilder Builder;
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_ITEMS = 4
  };
  const flatbuffers::Vector<uint8_t> *items() const {
    return GetPointer<const flatbuffers::Vector<uint8_t> *>(VT_ITEMS);
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyOffsetRequired(verifier, VT_ITEMS) &&
           verifier.VerifyVector(items()) &&
           verifier.EndTable();
  }
};

struct Int8BufferBuilder {
  typedef Int8Buffer Table;
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_items(flatbuffers::Offset<flatbuffers::Vector<uint8_t>> items) {
    fbb_.AddOffset(Int8Buffer::VT_ITEMS, items);
  }
  explicit Int8BufferBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  Int8BufferBuilder &operator=(const Int8BufferBuilder &);
  flatbuffers::Offset<Int8Buffer> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<Int8Buffer>(end);
    fbb_.Required(o, Int8Buffer::VT_ITEMS);
    return o;
  }
};

inline flatbuffers::Offset<Int8Buffer> CreateInt8Buffer(
    flatbuffers::FlatBufferBuilder &_fbb,
    flatbuffers::Offset<flatbuffers::Vector<uint8_t>> items = 0) {
  Int8BufferBuilder builder_(_fbb);
  builder_.add_items(items);
  return builder_.Finish();
}

inline flatbuffers::Offset<Int8Buffer> CreateInt8BufferDirect(
    flatbuffers::FlatBufferBuilder &_fbb,
    const std::vector<uint8_t> *items = nullptr) {
  auto items__ = items ? _fbb.CreateVector<uint8_t>(*items) : 0;
  return org::apache::arrow::computeir::flatbuf::CreateInt8Buffer(
      _fbb,
      items__);
}

struct Int16Buffer FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  typedef Int16BufferBuilder Builder;
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_ITEMS = 4
  };
  const flatbuffers::Vector<uint16_t> *items() const {
    return GetPointer<const flatbuffers::Vector<uint16_t> *>(VT_ITEMS);
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyOffsetRequired(verifier, VT_ITEMS) &&
           verifier.VerifyVector(items()) &&
           verifier.EndTable();
  }
};

struct Int16BufferBuilder {
  typedef Int16Buffer Table;
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_items(flatbuffers::Offset<flatbuffers::Vector<uint16_t>> items) {
    fbb_.AddOffset(Int16Buffer::VT_ITEMS, items);
  }
  explicit Int16BufferBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  Int16BufferBuilder &operator=(const Int16BufferBuilder &);
  flatbuffers::Offset<Int16Buffer> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<Int16Buffer>(end);
    fbb_.Required(o, Int16Buffer::VT_ITEMS);
    return o;
  }
};

inline flatbuffers::Offset<Int16Buffer> CreateInt16Buffer(
    flatbuffers::FlatBufferBuilder &_fbb,
    flatbuffers::Offset<flatbuffers::Vector<uint16_t>> items = 0) {
  Int16BufferBuilder builder_(_fbb);
  builder_.add_items(items);
  return builder_.Finish();
}

inline flatbuffers::Offset<Int16Buffer> CreateInt16BufferDirect(
    flatbuffers::FlatBufferBuilder &_fbb,
    const std::vector<uint16_t> *items = nullptr) {
  auto items__ = items ? _fbb.CreateVector<uint16_t>(*items) : 0;
  return org::apache::arrow::computeir::flatbuf::CreateInt16Buffer(
      _fbb,
      items__);
}

struct Int32Buffer FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  typedef Int32BufferBuilder Builder;
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_ITEMS = 4
  };
  const flatbuffers::Vector<uint32_t> *items() const {
    return GetPointer<const flatbuffers::Vector<uint32_t> *>(VT_ITEMS);
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyOffsetRequired(verifier, VT_ITEMS) &&
           verifier.VerifyVector(items()) &&
           verifier.EndTable();
  }
};

struct Int32BufferBuilder {
  typedef Int32Buffer Table;
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_items(flatbuffers::Offset<flatbuffers::Vector<uint32_t>> items) {
    fbb_.AddOffset(Int32Buffer::VT_ITEMS, items);
  }
  explicit Int32BufferBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  Int32BufferBuilder &operator=(const Int32BufferBuilder &);
  flatbuffers::Offset<Int32Buffer> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<Int32Buffer>(end);
    fbb_.Required(o, Int32Buffer::VT_ITEMS);
    return o;
  }
};

inline flatbuffers::Offset<Int32Buffer> CreateInt32Buffer(
    flatbuffers::FlatBufferBuilder &_fbb,
    flatbuffers::Offset<flatbuffers::Vector<uint32_t>> items = 0) {
  Int32BufferBuilder builder_(_fbb);
  builder_.add_items(items);
  return builder_.Finish();
}

inline flatbuffers::Offset<Int32Buffer> CreateInt32BufferDirect(
    flatbuffers::FlatBufferBuilder &_fbb,
    const std::vector<uint32_t> *items = nullptr) {
  auto items__ = items ? _fbb.CreateVector<uint32_t>(*items) : 0;
  return org::apache::arrow::computeir::flatbuf::CreateInt32Buffer(
      _fbb,
      items__);
}

struct Int64Buffer FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  typedef Int64BufferBuilder Builder;
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_ITEMS = 4
  };
  const flatbuffers::Vector<uint64_t> *items() const {
    return GetPointer<const flatbuffers::Vector<uint64_t> *>(VT_ITEMS);
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyOffsetRequired(verifier, VT_ITEMS) &&
           verifier.VerifyVector(items()) &&
           verifier.EndTable();
  }
};

struct Int64BufferBuilder {
  typedef Int64Buffer Table;
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_items(flatbuffers::Offset<flatbuffers::Vector<uint64_t>> items) {
    fbb_.AddOffset(Int64Buffer::VT_ITEMS, items);
  }
  explicit Int64BufferBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  Int64BufferBuilder &operator=(const Int64BufferBuilder &);
  flatbuffers::Offset<Int64Buffer> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<Int64Buffer>(end);
    fbb_.Required(o, Int64Buffer::VT_ITEMS);
    return o;
  }
};

inline flatbuffers::Offset<Int64Buffer> CreateInt64Buffer(
    flatbuffers::FlatBufferBuilder &_fbb,
    flatbuffers::Offset<flatbuffers::Vector<uint64_t>> items = 0) {
  Int64BufferBuilder builder_(_fbb);
  builder_.add_items(items);
  return builder_.Finish();
}

inline flatbuffers::Offset<Int64Buffer> CreateInt64BufferDirect(
    flatbuffers::FlatBufferBuilder &_fbb,
    const std::vector<uint64_t> *items = nullptr) {
  auto items__ = items ? _fbb.CreateVector<uint64_t>(*items) : 0;
  return org::apache::arrow::computeir::flatbuf::CreateInt64Buffer(
      _fbb,
      items__);
}

struct UInt8Buffer FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  typedef UInt8BufferBuilder Builder;
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_ITEMS = 4
  };
  const flatbuffers::Vector<uint8_t> *items() const {
    return GetPointer<const flatbuffers::Vector<uint8_t> *>(VT_ITEMS);
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyOffsetRequired(verifier, VT_ITEMS) &&
           verifier.VerifyVector(items()) &&
           verifier.EndTable();
  }
};

struct UInt8BufferBuilder {
  typedef UInt8Buffer Table;
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_items(flatbuffers::Offset<flatbuffers::Vector<uint8_t>> items) {
    fbb_.AddOffset(UInt8Buffer::VT_ITEMS, items);
  }
  explicit UInt8BufferBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  UInt8BufferBuilder &operator=(const UInt8BufferBuilder &);
  flatbuffers::Offset<UInt8Buffer> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<UInt8Buffer>(end);
    fbb_.Required(o, UInt8Buffer::VT_ITEMS);
    return o;
  }
};

inline flatbuffers::Offset<UInt8Buffer> CreateUInt8Buffer(
    flatbuffers::FlatBufferBuilder &_fbb,
    flatbuffers::Offset<flatbuffers::Vector<uint8_t>> items = 0) {
  UInt8BufferBuilder builder_(_fbb);
  builder_.add_items(items);
  return builder_.Finish();
}

inline flatbuffers::Offset<UInt8Buffer> CreateUInt8BufferDirect(
    flatbuffers::FlatBufferBuilder &_fbb,
    const std::vector<uint8_t> *items = nullptr) {
  auto items__ = items ? _fbb.CreateVector<uint8_t>(*items) : 0;
  return org::apache::arrow::computeir::flatbuf::CreateUInt8Buffer(
      _fbb,
      items__);
}

struct UInt16Buffer FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  typedef UInt16BufferBuilder Builder;
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_ITEMS = 4
  };
  const flatbuffers::Vector<uint16_t> *items() const {
    return GetPointer<const flatbuffers::Vector<uint16_t> *>(VT_ITEMS);
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyOffsetRequired(verifier, VT_ITEMS) &&
           verifier.VerifyVector(items()) &&
           verifier.EndTable();
  }
};

struct UInt16BufferBuilder {
  typedef UInt16Buffer Table;
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_items(flatbuffers::Offset<flatbuffers::Vector<uint16_t>> items) {
    fbb_.AddOffset(UInt16Buffer::VT_ITEMS, items);
  }
  explicit UInt16BufferBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  UInt16BufferBuilder &operator=(const UInt16BufferBuilder &);
  flatbuffers::Offset<UInt16Buffer> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<UInt16Buffer>(end);
    fbb_.Required(o, UInt16Buffer::VT_ITEMS);
    return o;
  }
};

inline flatbuffers::Offset<UInt16Buffer> CreateUInt16Buffer(
    flatbuffers::FlatBufferBuilder &_fbb,
    flatbuffers::Offset<flatbuffers::Vector<uint16_t>> items = 0) {
  UInt16BufferBuilder builder_(_fbb);
  builder_.add_items(items);
  return builder_.Finish();
}

inline flatbuffers::Offset<UInt16Buffer> CreateUInt16BufferDirect(
    flatbuffers::FlatBufferBuilder &_fbb,
    const std::vector<uint16_t> *items = nullptr) {
  auto items__ = items ? _fbb.CreateVector<uint16_t>(*items) : 0;
  return org::apache::arrow::computeir::flatbuf::CreateUInt16Buffer(
      _fbb,
      items__);
}

struct UInt32Buffer FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  typedef UInt32BufferBuilder Builder;
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_ITEMS = 4
  };
  const flatbuffers::Vector<uint32_t> *items() const {
    return GetPointer<const flatbuffers::Vector<uint32_t> *>(VT_ITEMS);
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyOffsetRequired(verifier, VT_ITEMS) &&
           verifier.VerifyVector(items()) &&
           verifier.EndTable();
  }
};

struct UInt32BufferBuilder {
  typedef UInt32Buffer Table;
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_items(flatbuffers::Offset<flatbuffers::Vector<uint32_t>> items) {
    fbb_.AddOffset(UInt32Buffer::VT_ITEMS, items);
  }
  explicit UInt32BufferBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  UInt32BufferBuilder &operator=(const UInt32BufferBuilder &);
  flatbuffers::Offset<UInt32Buffer> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<UInt32Buffer>(end);
    fbb_.Required(o, UInt32Buffer::VT_ITEMS);
    return o;
  }
};

inline flatbuffers::Offset<UInt32Buffer> CreateUInt32Buffer(
    flatbuffers::FlatBufferBuilder &_fbb,
    flatbuffers::Offset<flatbuffers::Vector<uint32_t>> items = 0) {
  UInt32BufferBuilder builder_(_fbb);
  builder_.add_items(items);
  return builder_.Finish();
}

inline flatbuffers::Offset<UInt32Buffer> CreateUInt32BufferDirect(
    flatbuffers::FlatBufferBuilder &_fbb,
    const std::vector<uint32_t> *items = nullptr) {
  auto items__ = items ? _fbb.CreateVector<uint32_t>(*items) : 0;
  return org::apache::arrow::computeir::flatbuf::CreateUInt32Buffer(
      _fbb,
      items__);
}

struct UInt64Buffer FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  typedef UInt64BufferBuilder Builder;
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_ITEMS = 4
  };
  const flatbuffers::Vector<uint64_t> *items() const {
    return GetPointer<const flatbuffers::Vector<uint64_t> *>(VT_ITEMS);
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyOffsetRequired(verifier, VT_ITEMS) &&
           verifier.VerifyVector(items()) &&
           verifier.EndTable();
  }
};

struct UInt64BufferBuilder {
  typedef UInt64Buffer Table;
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_items(flatbuffers::Offset<flatbuffers::Vector<uint64_t>> items) {
    fbb_.AddOffset(UInt64Buffer::VT_ITEMS, items);
  }
  explicit UInt64BufferBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  UInt64BufferBuilder &operator=(const UInt64BufferBuilder &);
  flatbuffers::Offset<UInt64Buffer> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<UInt64Buffer>(end);
    fbb_.Required(o, UInt64Buffer::VT_ITEMS);
    return o;
  }
};

inline flatbuffers::Offset<UInt64Buffer> CreateUInt64Buffer(
    flatbuffers::FlatBufferBuilder &_fbb,
    flatbuffers::Offset<flatbuffers::Vector<uint64_t>> items = 0) {
  UInt64BufferBuilder builder_(_fbb);
  builder_.add_items(items);
  return builder_.Finish();
}

inline flatbuffers::Offset<UInt64Buffer> CreateUInt64BufferDirect(
    flatbuffers::FlatBufferBuilder &_fbb,
    const std::vector<uint64_t> *items = nullptr) {
  auto items__ = items ? _fbb.CreateVector<uint64_t>(*items) : 0;
  return org::apache::arrow::computeir::flatbuf::CreateUInt64Buffer(
      _fbb,
      items__);
}

struct Float32Buffer FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  typedef Float32BufferBuilder Builder;
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_ITEMS = 4
  };
  const flatbuffers::Vector<float> *items() const {
    return GetPointer<const flatbuffers::Vector<float> *>(VT_ITEMS);
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyOffsetRequired(verifier, VT_ITEMS) &&
           verifier.VerifyVector(items()) &&
           verifier.EndTable();
  }
};

struct Float32BufferBuilder {
  typedef Float32Buffer Table;
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_items(flatbuffers::Offset<flatbuffers::Vector<float>> items) {
    fbb_.AddOffset(Float32Buffer::VT_ITEMS, items);
  }
  explicit Float32BufferBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  Float32BufferBuilder &operator=(const Float32BufferBuilder &);
  flatbuffers::Offset<Float32Buffer> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<Float32Buffer>(end);
    fbb_.Required(o, Float32Buffer::VT_ITEMS);
    return o;
  }
};

inline flatbuffers::Offset<Float32Buffer> CreateFloat32Buffer(
    flatbuffers::FlatBufferBuilder &_fbb,
    flatbuffers::Offset<flatbuffers::Vector<float>> items = 0) {
  Float32BufferBuilder builder_(_fbb);
  builder_.add_items(items);
  return builder_.Finish();
}

inline flatbuffers::Offset<Float32Buffer> CreateFloat32BufferDirect(
    flatbuffers::FlatBufferBuilder &_fbb,
    const std::vector<float> *items = nullptr) {
  auto items__ = items ? _fbb.CreateVector<float>(*items) : 0;
  return org::apache::arrow::computeir::flatbuf::CreateFloat32Buffer(
      _fbb,
      items__);
}

struct Float64Buffer FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  typedef Float64BufferBuilder Builder;
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_ITEMS = 4
  };
  const flatbuffers::Vector<double> *items() const {
    return GetPointer<const flatbuffers::Vector<double> *>(VT_ITEMS);
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyOffsetRequired(verifier, VT_ITEMS) &&
           verifier.VerifyVector(items()) &&
           verifier.EndTable();
  }
};

struct Float64BufferBuilder {
  typedef Float64Buffer Table;
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_items(flatbuffers::Offset<flatbuffers::Vector<double>> items) {
    fbb_.AddOffset(Float64Buffer::VT_ITEMS, items);
  }
  explicit Float64BufferBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  Float64BufferBuilder &operator=(const Float64BufferBuilder &);
  flatbuffers::Offset<Float64Buffer> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<Float64Buffer>(end);
    fbb_.Required(o, Float64Buffer::VT_ITEMS);
    return o;
  }
};

inline flatbuffers::Offset<Float64Buffer> CreateFloat64Buffer(
    flatbuffers::FlatBufferBuilder &_fbb,
    flatbuffers::Offset<flatbuffers::Vector<double>> items = 0) {
  Float64BufferBuilder builder_(_fbb);
  builder_.add_items(items);
  return builder_.Finish();
}

inline flatbuffers::Offset<Float64Buffer> CreateFloat64BufferDirect(
    flatbuffers::FlatBufferBuilder &_fbb,
    const std::vector<double> *items = nullptr) {
  auto items__ = items ? _fbb.CreateVector<double>(*items) : 0;
  return org::apache::arrow::computeir::flatbuf::CreateFloat64Buffer(
      _fbb,
      items__);
}

struct TableBuffer FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  typedef TableBufferBuilder Builder;
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_ITEMS = 4
  };
  const flatbuffers::Vector<uint8_t> *items() const {
    return GetPointer<const flatbuffers::Vector<uint8_t> *>(VT_ITEMS);
  }
  const org::apache::arrow::computeir::flatbuf::InlineBuffer *items_nested_root() const {
    return flatbuffers::GetRoot<org::apache::arrow::computeir::flatbuf::InlineBuffer>(items()->Data());
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyOffsetRequired(verifier, VT_ITEMS) &&
           verifier.VerifyVector(items()) &&
           verifier.EndTable();
  }
};

struct TableBufferBuilder {
  typedef TableBuffer Table;
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_items(flatbuffers::Offset<flatbuffers::Vector<uint8_t>> items) {
    fbb_.AddOffset(TableBuffer::VT_ITEMS, items);
  }
  explicit TableBufferBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  TableBufferBuilder &operator=(const TableBufferBuilder &);
  flatbuffers::Offset<TableBuffer> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<TableBuffer>(end);
    fbb_.Required(o, TableBuffer::VT_ITEMS);
    return o;
  }
};

inline flatbuffers::Offset<TableBuffer> CreateTableBuffer(
    flatbuffers::FlatBufferBuilder &_fbb,
    flatbuffers::Offset<flatbuffers::Vector<uint8_t>> items = 0) {
  TableBufferBuilder builder_(_fbb);
  builder_.add_items(items);
  return builder_.Finish();
}

inline flatbuffers::Offset<TableBuffer> CreateTableBufferDirect(
    flatbuffers::FlatBufferBuilder &_fbb,
    const std::vector<uint8_t> *items = nullptr) {
  auto items__ = items ? _fbb.CreateVector<uint8_t>(*items) : 0;
  return org::apache::arrow::computeir::flatbuf::CreateTableBuffer(
      _fbb,
      items__);
}

/// An inline replacement for org.apache.arrow.Buffer because that
/// requires a sidecar block of bytes into which offsets can point.
/// A union of buffers of each primitive type is provided to avoid
/// the need for reinterpret_cast, std::mem::transmute, ...
/// The final member of the union is a bytes buffer aligned suitably
/// to hold any flatbuffer Table.
struct InlineBuffer FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  typedef InlineBufferBuilder Builder;
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_IMPL_TYPE = 4,
    VT_IMPL = 6
  };
  org::apache::arrow::computeir::flatbuf::InlineBufferImpl impl_type() const {
    return static_cast<org::apache::arrow::computeir::flatbuf::InlineBufferImpl>(GetField<uint8_t>(VT_IMPL_TYPE, 0));
  }
  const void *impl() const {
    return GetPointer<const void *>(VT_IMPL);
  }
  template<typename T> const T *impl_as() const;
  const org::apache::arrow::computeir::flatbuf::Int8Buffer *impl_as_Int8Buffer() const {
    return impl_type() == org::apache::arrow::computeir::flatbuf::InlineBufferImpl::Int8Buffer ? static_cast<const org::apache::arrow::computeir::flatbuf::Int8Buffer *>(impl()) : nullptr;
  }
  const org::apache::arrow::computeir::flatbuf::Int16Buffer *impl_as_Int16Buffer() const {
    return impl_type() == org::apache::arrow::computeir::flatbuf::InlineBufferImpl::Int16Buffer ? static_cast<const org::apache::arrow::computeir::flatbuf::Int16Buffer *>(impl()) : nullptr;
  }
  const org::apache::arrow::computeir::flatbuf::Int32Buffer *impl_as_Int32Buffer() const {
    return impl_type() == org::apache::arrow::computeir::flatbuf::InlineBufferImpl::Int32Buffer ? static_cast<const org::apache::arrow::computeir::flatbuf::Int32Buffer *>(impl()) : nullptr;
  }
  const org::apache::arrow::computeir::flatbuf::Int64Buffer *impl_as_Int64Buffer() const {
    return impl_type() == org::apache::arrow::computeir::flatbuf::InlineBufferImpl::Int64Buffer ? static_cast<const org::apache::arrow::computeir::flatbuf::Int64Buffer *>(impl()) : nullptr;
  }
  const org::apache::arrow::computeir::flatbuf::UInt8Buffer *impl_as_UInt8Buffer() const {
    return impl_type() == org::apache::arrow::computeir::flatbuf::InlineBufferImpl::UInt8Buffer ? static_cast<const org::apache::arrow::computeir::flatbuf::UInt8Buffer *>(impl()) : nullptr;
  }
  const org::apache::arrow::computeir::flatbuf::UInt16Buffer *impl_as_UInt16Buffer() const {
    return impl_type() == org::apache::arrow::computeir::flatbuf::InlineBufferImpl::UInt16Buffer ? static_cast<const org::apache::arrow::computeir::flatbuf::UInt16Buffer *>(impl()) : nullptr;
  }
  const org::apache::arrow::computeir::flatbuf::UInt32Buffer *impl_as_UInt32Buffer() const {
    return impl_type() == org::apache::arrow::computeir::flatbuf::InlineBufferImpl::UInt32Buffer ? static_cast<const org::apache::arrow::computeir::flatbuf::UInt32Buffer *>(impl()) : nullptr;
  }
  const org::apache::arrow::computeir::flatbuf::UInt64Buffer *impl_as_UInt64Buffer() const {
    return impl_type() == org::apache::arrow::computeir::flatbuf::InlineBufferImpl::UInt64Buffer ? static_cast<const org::apache::arrow::computeir::flatbuf::UInt64Buffer *>(impl()) : nullptr;
  }
  const org::apache::arrow::computeir::flatbuf::Float32Buffer *impl_as_Float32Buffer() const {
    return impl_type() == org::apache::arrow::computeir::flatbuf::InlineBufferImpl::Float32Buffer ? static_cast<const org::apache::arrow::computeir::flatbuf::Float32Buffer *>(impl()) : nullptr;
  }
  const org::apache::arrow::computeir::flatbuf::Float64Buffer *impl_as_Float64Buffer() const {
    return impl_type() == org::apache::arrow::computeir::flatbuf::InlineBufferImpl::Float64Buffer ? static_cast<const org::apache::arrow::computeir::flatbuf::Float64Buffer *>(impl()) : nullptr;
  }
  const org::apache::arrow::computeir::flatbuf::TableBuffer *impl_as_TableBuffer() const {
    return impl_type() == org::apache::arrow::computeir::flatbuf::InlineBufferImpl::TableBuffer ? static_cast<const org::apache::arrow::computeir::flatbuf::TableBuffer *>(impl()) : nullptr;
  }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyField<uint8_t>(verifier, VT_IMPL_TYPE) &&
           VerifyOffsetRequired(verifier, VT_IMPL) &&
           VerifyInlineBufferImpl(verifier, impl(), impl_type()) &&
           verifier.EndTable();
  }
};

template<> inline const org::apache::arrow::computeir::flatbuf::Int8Buffer *InlineBuffer::impl_as<org::apache::arrow::computeir::flatbuf::Int8Buffer>() const {
  return impl_as_Int8Buffer();
}

template<> inline const org::apache::arrow::computeir::flatbuf::Int16Buffer *InlineBuffer::impl_as<org::apache::arrow::computeir::flatbuf::Int16Buffer>() const {
  return impl_as_Int16Buffer();
}

template<> inline const org::apache::arrow::computeir::flatbuf::Int32Buffer *InlineBuffer::impl_as<org::apache::arrow::computeir::flatbuf::Int32Buffer>() const {
  return impl_as_Int32Buffer();
}

template<> inline const org::apache::arrow::computeir::flatbuf::Int64Buffer *InlineBuffer::impl_as<org::apache::arrow::computeir::flatbuf::Int64Buffer>() const {
  return impl_as_Int64Buffer();
}

template<> inline const org::apache::arrow::computeir::flatbuf::UInt8Buffer *InlineBuffer::impl_as<org::apache::arrow::computeir::flatbuf::UInt8Buffer>() const {
  return impl_as_UInt8Buffer();
}

template<> inline const org::apache::arrow::computeir::flatbuf::UInt16Buffer *InlineBuffer::impl_as<org::apache::arrow::computeir::flatbuf::UInt16Buffer>() const {
  return impl_as_UInt16Buffer();
}

template<> inline const org::apache::arrow::computeir::flatbuf::UInt32Buffer *InlineBuffer::impl_as<org::apache::arrow::computeir::flatbuf::UInt32Buffer>() const {
  return impl_as_UInt32Buffer();
}

template<> inline const org::apache::arrow::computeir::flatbuf::UInt64Buffer *InlineBuffer::impl_as<org::apache::arrow::computeir::flatbuf::UInt64Buffer>() const {
  return impl_as_UInt64Buffer();
}

template<> inline const org::apache::arrow::computeir::flatbuf::Float32Buffer *InlineBuffer::impl_as<org::apache::arrow::computeir::flatbuf::Float32Buffer>() const {
  return impl_as_Float32Buffer();
}

template<> inline const org::apache::arrow::computeir::flatbuf::Float64Buffer *InlineBuffer::impl_as<org::apache::arrow::computeir::flatbuf::Float64Buffer>() const {
  return impl_as_Float64Buffer();
}

template<> inline const org::apache::arrow::computeir::flatbuf::TableBuffer *InlineBuffer::impl_as<org::apache::arrow::computeir::flatbuf::TableBuffer>() const {
  return impl_as_TableBuffer();
}

struct InlineBufferBuilder {
  typedef InlineBuffer Table;
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_impl_type(org::apache::arrow::computeir::flatbuf::InlineBufferImpl impl_type) {
    fbb_.AddElement<uint8_t>(InlineBuffer::VT_IMPL_TYPE, static_cast<uint8_t>(impl_type), 0);
  }
  void add_impl(flatbuffers::Offset<void> impl) {
    fbb_.AddOffset(InlineBuffer::VT_IMPL, impl);
  }
  explicit InlineBufferBuilder(flatbuffers::FlatBufferBuilder &_fbb)
        : fbb_(_fbb) {
    start_ = fbb_.StartTable();
  }
  InlineBufferBuilder &operator=(const InlineBufferBuilder &);
  flatbuffers::Offset<InlineBuffer> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = flatbuffers::Offset<InlineBuffer>(end);
    fbb_.Required(o, InlineBuffer::VT_IMPL);
    return o;
  }
};

inline flatbuffers::Offset<InlineBuffer> CreateInlineBuffer(
    flatbuffers::FlatBufferBuilder &_fbb,
    org::apache::arrow::computeir::flatbuf::InlineBufferImpl impl_type = org::apache::arrow::computeir::flatbuf::InlineBufferImpl::NONE,
    flatbuffers::Offset<void> impl = 0) {
  InlineBufferBuilder builder_(_fbb);
  builder_.add_impl(impl);
  builder_.add_impl_type(impl_type);
  return builder_.Finish();
}

inline bool VerifyInlineBufferImpl(flatbuffers::Verifier &verifier, const void *obj, InlineBufferImpl type) {
  switch (type) {
    case InlineBufferImpl::NONE: {
      return true;
    }
    case InlineBufferImpl::Int8Buffer: {
      auto ptr = reinterpret_cast<const org::apache::arrow::computeir::flatbuf::Int8Buffer *>(obj);
      return verifier.VerifyTable(ptr);
    }
    case InlineBufferImpl::Int16Buffer: {
      auto ptr = reinterpret_cast<const org::apache::arrow::computeir::flatbuf::Int16Buffer *>(obj);
      return verifier.VerifyTable(ptr);
    }
    case InlineBufferImpl::Int32Buffer: {
      auto ptr = reinterpret_cast<const org::apache::arrow::computeir::flatbuf::Int32Buffer *>(obj);
      return verifier.VerifyTable(ptr);
    }
    case InlineBufferImpl::Int64Buffer: {
      auto ptr = reinterpret_cast<const org::apache::arrow::computeir::flatbuf::Int64Buffer *>(obj);
      return verifier.VerifyTable(ptr);
    }
    case InlineBufferImpl::UInt8Buffer: {
      auto ptr = reinterpret_cast<const org::apache::arrow::computeir::flatbuf::UInt8Buffer *>(obj);
      return verifier.VerifyTable(ptr);
    }
    case InlineBufferImpl::UInt16Buffer: {
      auto ptr = reinterpret_cast<const org::apache::arrow::computeir::flatbuf::UInt16Buffer *>(obj);
      return verifier.VerifyTable(ptr);
    }
    case InlineBufferImpl::UInt32Buffer: {
      auto ptr = reinterpret_cast<const org::apache::arrow::computeir::flatbuf::UInt32Buffer *>(obj);
      return verifier.VerifyTable(ptr);
    }
    case InlineBufferImpl::UInt64Buffer: {
      auto ptr = reinterpret_cast<const org::apache::arrow::computeir::flatbuf::UInt64Buffer *>(obj);
      return verifier.VerifyTable(ptr);
    }
    case InlineBufferImpl::Float32Buffer: {
      auto ptr = reinterpret_cast<const org::apache::arrow::computeir::flatbuf::Float32Buffer *>(obj);
      return verifier.VerifyTable(ptr);
    }
    case InlineBufferImpl::Float64Buffer: {
      auto ptr = reinterpret_cast<const org::apache::arrow::computeir::flatbuf::Float64Buffer *>(obj);
      return verifier.VerifyTable(ptr);
    }
    case InlineBufferImpl::TableBuffer: {
      auto ptr = reinterpret_cast<const org::apache::arrow::computeir::flatbuf::TableBuffer *>(obj);
      return verifier.VerifyTable(ptr);
    }
    default: return true;
  }
}

inline bool VerifyInlineBufferImplVector(flatbuffers::Verifier &verifier, const flatbuffers::Vector<flatbuffers::Offset<void>> *values, const flatbuffers::Vector<uint8_t> *types) {
  if (!values || !types) return !values && !types;
  if (values->size() != types->size()) return false;
  for (flatbuffers::uoffset_t i = 0; i < values->size(); ++i) {
    if (!VerifyInlineBufferImpl(
        verifier,  values->Get(i), types->GetEnum<InlineBufferImpl>(i))) {
      return false;
    }
  }
  return true;
}

}  // namespace flatbuf
}  // namespace computeir
}  // namespace arrow
}  // namespace apache
}  // namespace org

#endif  // FLATBUFFERS_GENERATED_INLINEBUFFER_ORG_APACHE_ARROW_COMPUTEIR_FLATBUF_H_

#include <Rcpp.h>
#include "rrrow_types.h"

using namespace Rcpp ;
using namespace arrow ;

// Buffer for R data, is immutable so we can use the pointer directly.
// Does R's gc compact memory and move objects after allocation? I don't think
// so?
class RBuffer : public Buffer {
public:
  RBuffer(SEXP data) : Buffer(nullptr, 0) {
    is_mutable_ = false;
    data_ = reinterpret_cast<const uint8_t*>(INTEGER(data));
    size_ = Rf_length(data) * sizeof(int);
    capacity_ = size_;
  }
  ~RBuffer() {}
};

std::shared_ptr<Buffer> init_null_bitmap(IntegerVector array) {
  int64_t null_bytes = BitUtil::BytesForBits(array.size());

  auto null_bitmap = std::make_shared<PoolBuffer>(default_memory_pool());
  null_bitmap->Resize(null_bytes);

  auto null_bitmap_data = null_bitmap->mutable_data();
  memset(null_bitmap_data, 0, static_cast<size_t>(null_bytes));
  return null_bitmap;
}

std::shared_ptr<Buffer> values_to_bitmap(IntegerVector array) {
  auto bitmap = init_null_bitmap(array);
  auto bitmap_data = bitmap->mutable_data();

  for (int i = 0; i < array.size(); ++i) {
    if (array.at(i) != NA_INTEGER) {
      BitUtil::SetBit(bitmap_data, i);
    }
  }

  return bitmap;
}
// [[Rcpp::export]]
array_ptr array(IntegerVector input) {
  auto buffer = std::make_shared<RBuffer>(input);

  // The first buffer is the null bitmap, which we are ignoring for now
  BufferVector buffers = {values_to_bitmap(input), buffer};

  auto data = std::make_shared<ArrayData>(int32(), input.length(),
    std::move(buffers), 0, 0);
  auto array = MakeArray(data) ;

  return array_ptr( new std::shared_ptr<Array>(array.get()) ) ;
}

// [[Rcpp::export]]
CharacterVector array_string(array_ptr const& array) {
  return (*array)->ToString();
}

// [[Rcpp::export]]
IntegerVector as_r_int(array_ptr const& array) {
  std::shared_ptr<Int32Array> int32_array =
    std::static_pointer_cast<Int32Array>(*array);

  int n = (*array)->length();

  IntegerVector out(n);
  memcpy(INTEGER(out), int32_array->raw_values(), n * sizeof(int));

  return out;
}

#ifndef ARROW_CAST_UTIL_H
#define ARROW_CAST_UTIL_H

#include <iostream>
#include <type_traits>

#include "arrow/status.h"
#include "arrow/buffer.h"

using arrow::ResizableBuffer;

namespace arrow{
namespace internal{
    template <typename InType , typename OutType>
    struct can_copy_ptr{
        static constexpr bool value = std::is_convertible<InType , OutType>::value;
    };
    template <typename InType , typename OutType , typename std::enable_if<can_copy_ptr<InType ,OutType>::value>::type* = nullptr>
    Status ConvertPhysicalType(const InType * in_ptr , int64_t /*length*/ , const OutType ** out_ptr){
        *out_ptr = reinterpret_cast<const OutType*>(in_ptr);
        return Status::OK();
    }

    template <typename InType , typename OutType , typename std::enable_if<not can_copy_ptr<InType ,OutType>::value>::type* = nullptr>
    Status ConvertPhysicalType(const InType * in_ptr , int64_t length , const OutType ** out_ptr ){
        ResizableBuffer buf(nullptr , 0 );
        RETURN_NOT_OK(buf.Resize(length * sizeof(OutType)));
        OutType * mutable_out_ptr = reinterpret_cast<OutType*>(buf.mutable_data());
        std::copy(in_ptr , in_ptr + length ,  mutable_out_ptr);
        *out_ptr = mutable_out_ptr;
        return Status::OK();
    }
}   // namespace internal
}   // namespace arrow

#endif //cast-util.h

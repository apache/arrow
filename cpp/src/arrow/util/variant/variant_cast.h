// Copyright (c) MapBox
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification,
// are permitted provided that the following conditions are met:
//
// - Redistributions of source code must retain the above copyright notice, this
//   list of conditions and the following disclaimer.
// - Redistributions in binary form must reproduce the above copyright notice, this
//   list of conditions and the following disclaimer in the documentation and/or
//   other materials provided with the distribution.
// - Neither the name "MapBox" nor the names of its contributors may be
//   used to endorse or promote products derived from this software without
//   specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
// ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
// ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#ifndef ARROW_UTIL_VARIANT_CAST_H
#define ARROW_UTIL_VARIANT_CAST_H

#include <type_traits>

namespace arrow {
namespace util {

namespace detail {

template <class T>
class static_caster
{
public:
    template <class V>
    T& operator()(V& v) const
    {
        return static_cast<T&>(v);
    }
};

template <class T>
class dynamic_caster
{
public:
    using result_type = T&;
    template <class V>
    T& operator()(V& v, typename std::enable_if<!std::is_polymorphic<V>::value>::type* = nullptr) const
    {
        throw std::bad_cast();
    }
    template <class V>
    T& operator()(V& v, typename std::enable_if<std::is_polymorphic<V>::value>::type* = nullptr) const
    {
        return dynamic_cast<T&>(v);
    }
};

template <class T>
class dynamic_caster<T*>
{
public:
    using result_type = T*;
    template <class V>
    T* operator()(V& v, typename std::enable_if<!std::is_polymorphic<V>::value>::type* = nullptr) const
    {
        return nullptr;
    }
    template <class V>
    T* operator()(V& v, typename std::enable_if<std::is_polymorphic<V>::value>::type* = nullptr) const
    {
        return dynamic_cast<T*>(&v);
    }
};
}

template <class T, class V>
typename detail::dynamic_caster<T>::result_type
dynamic_variant_cast(V& v)
{
    return arrow::util::apply_visitor(detail::dynamic_caster<T>(), v);
}

template <class T, class V>
typename detail::dynamic_caster<const T>::result_type
dynamic_variant_cast(const V& v)
{
    return arrow::util::apply_visitor(detail::dynamic_caster<const T>(), v);
}

template <class T, class V>
T& static_variant_cast(V& v)
{
    return arrow::util::apply_visitor(detail::static_caster<T>(), v);
}

template <class T, class V>
const T& static_variant_cast(const V& v)
{
    return arrow::util::apply_visitor(detail::static_caster<const T>(), v);
}

}  // namespace util
}  // namespace arrow

#endif // ARROW_UTIL_VARIANT_CAST_H

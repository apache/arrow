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

#ifndef ARROW_UTIL_VARIANT_OPTIONAL_H
#define ARROW_UTIL_VARIANT_OPTIONAL_H

#pragma message("This implementation of optional is deprecated. See https://github.com/mapbox/variant/issues/64.")

#include <type_traits>
#include <utility>

#include <arrow/util/variant.h>

namespace arrow {
namespace util {

template <typename T>
class optional
{
    static_assert(!std::is_reference<T>::value, "optional doesn't support references");

    struct none_type
    {
    };

    variant<none_type, T> variant_;

public:
    optional() = default;

    optional(optional const& rhs)
    {
        if (this != &rhs)
        { // protect against invalid self-assignment
            variant_ = rhs.variant_;
        }
    }

    optional(T const& v) { variant_ = v; }

    explicit operator bool() const noexcept { return variant_.template is<T>(); }

    T const& get() const { return variant_.template get<T>(); }
    T& get() { return variant_.template get<T>(); }

    T const& operator*() const { return this->get(); }
    T operator*() { return this->get(); }

    optional& operator=(T const& v)
    {
        variant_ = v;
        return *this;
    }

    optional& operator=(optional const& rhs)
    {
        if (this != &rhs)
        {
            variant_ = rhs.variant_;
        }
        return *this;
    }

    template <typename... Args>
    void emplace(Args&&... args)
    {
        variant_ = T{std::forward<Args>(args)...};
    }

    void reset() { variant_ = none_type{}; }

}; // class optional

} // namespace util
} // namespace arrow

#endif // ARROW_UTIL_VARIANT_OPTIONAL_H

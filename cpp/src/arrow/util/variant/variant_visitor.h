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

#ifndef ARROW_UTIL_VARIANT_VISITOR_HPP
#define ARROW_UTIL_VARIANT_VISITOR_HPP

#include <utility>

namespace arrow {
namespace util {

template <typename... Fns>
struct visitor;

template <typename Fn>
struct visitor<Fn> : Fn
{
    using Fn::operator();

    template<typename T>
    visitor(T&& fn) : Fn(std::forward<T>(fn)) {}
};

template <typename Fn, typename... Fns>
struct visitor<Fn, Fns...> : Fn, visitor<Fns...>
{
    using Fn::operator();
    using visitor<Fns...>::operator();

    template<typename T, typename... Ts>
    visitor(T&& fn, Ts&&... fns)
        : Fn(std::forward<T>(fn))
        , visitor<Fns...>(std::forward<Ts>(fns)...) {}
};

template <typename... Fns>
visitor<typename std::decay<Fns>::type...> make_visitor(Fns&&... fns)
{
    return visitor<typename std::decay<Fns>::type...>
        (std::forward<Fns>(fns)...);
}

} // namespace util
} // namespace arrow

#endif // ARROW_UTIL_VARIANT_VISITOR_HPP

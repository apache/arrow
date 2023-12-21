//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_SRC_LOCALE_MO_LAMBDA_HPP_INCLUDED
#define BOOST_SRC_LOCALE_MO_LAMBDA_HPP_INCLUDED

#include <boost/locale/config.hpp>
#include <memory>

namespace boost { namespace locale { namespace gnu_gettext { namespace lambda {

    struct plural {
        virtual int operator()(int n) const = 0;
        virtual plural* clone() const = 0;
        virtual ~plural() = default;
    };

    typedef std::shared_ptr<plural> plural_ptr;

    plural_ptr compile(const char* c_expression);

}}}} // namespace boost::locale::gnu_gettext::lambda

#endif

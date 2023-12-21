//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#define BOOST_LOCALE_SOURCE
#include <boost/locale/date_time.hpp>
#include <boost/locale/formatting.hpp>
#include "boost/locale/shared/ios_prop.hpp"
#include <algorithm>
#include <typeinfo>

namespace boost { namespace locale {

    ios_info::string_set::string_set() : type(0), size(0), ptr(0) {}
    ios_info::string_set::~string_set()
    {
        delete[] ptr;
    }
    ios_info::string_set::string_set(const string_set& other)
    {
        if(other.ptr != 0) {
            ptr = new char[other.size];
            size = other.size;
            type = other.type;
            memcpy(ptr, other.ptr, size);
        } else {
            ptr = 0;
            size = 0;
            type = 0;
        }
    }

    void ios_info::string_set::swap(string_set& other)
    {
        std::swap(type, other.type);
        std::swap(size, other.size);
        std::swap(ptr, other.ptr);
    }

    ios_info::string_set& ios_info::string_set::operator=(string_set other)
    {
        swap(other);
        return *this;
    }

    ios_info::ios_info() : flags_(0), domain_id_(0), time_zone_(time_zone::global()) {}

    ios_info::~ios_info() = default;

    ios_info::ios_info(const ios_info& other) = default;
    ios_info& ios_info::operator=(const ios_info& other) = default;

    void ios_info::display_flags(uint64_t f)
    {
        flags_ = (flags_ & ~uint64_t(flags::display_flags_mask)) | f;
    }
    uint64_t ios_info::display_flags() const
    {
        return flags_ & flags::display_flags_mask;
    }

    void ios_info::currency_flags(uint64_t f)
    {
        flags_ = (flags_ & ~uint64_t(flags::currency_flags_mask)) | f;
    }
    uint64_t ios_info::currency_flags() const
    {
        return flags_ & flags::currency_flags_mask;
    }

    void ios_info::date_flags(uint64_t f)
    {
        flags_ = (flags_ & ~uint64_t(flags::date_flags_mask)) | f;
    }
    uint64_t ios_info::date_flags() const
    {
        return flags_ & flags::date_flags_mask;
    }

    void ios_info::time_flags(uint64_t f)
    {
        flags_ = (flags_ & ~uint64_t(flags::time_flags_mask)) | f;
    }
    uint64_t ios_info::time_flags() const
    {
        return flags_ & flags::time_flags_mask;
    }

    void ios_info::domain_id(int id)
    {
        domain_id_ = id;
    }
    int ios_info::domain_id() const
    {
        return domain_id_;
    }

    void ios_info::time_zone(const std::string& tz)
    {
        time_zone_ = tz;
    }
    std::string ios_info::time_zone() const
    {
        return time_zone_;
    }

    const ios_info::string_set& ios_info::date_time_pattern_set() const
    {
        return datetime_;
    }

    ios_info::string_set& ios_info::date_time_pattern_set()
    {
        return datetime_;
    }

    ios_info& ios_info::get(std::ios_base& ios)
    {
        return impl::ios_prop<ios_info>::get(ios);
    }

    void ios_info::on_imbue() {}

    namespace {
        struct initializer {
            initializer() { impl::ios_prop<ios_info>::global_init(); }
        } initializer_instance;
    } // namespace

}} // namespace boost::locale

//
// Copyright (c) 2009-2011 Artyom Beilis (Tonkikh)
//
// Distributed under the Boost Software License, Version 1.0.
// https://www.boost.org/LICENSE_1_0.txt

#define BOOST_LOCALE_SOURCE
#include "boost/locale/posix/codecvt.hpp"
#include <boost/locale/encoding.hpp>
#include <boost/locale/hold_ptr.hpp>
#include <boost/locale/util.hpp>
#include <algorithm>
#include <cerrno>
#include <stdexcept>
#include <vector>

#include "boost/locale/encoding/conv.hpp"
#include "boost/locale/posix/all_generator.hpp"
#ifdef BOOST_LOCALE_WITH_ICONV
#    include "boost/locale/util/iconv.hpp"
#endif

namespace boost { namespace locale { namespace impl_posix {

#ifdef BOOST_LOCALE_WITH_ICONV
    class mb2_iconv_converter : public util::base_converter {
    public:
        mb2_iconv_converter(const std::string& encoding) :
            encoding_(encoding), to_utf_((iconv_t)(-1)), from_utf_((iconv_t)(-1))
        {
            iconv_t d = (iconv_t)(-1);
            std::vector<uint32_t> first_byte_table;
            try {
                d = iconv_open(utf32_encoding(), encoding.c_str());
                if(d == (iconv_t)(-1)) {
                    throw std::runtime_error("Unsupported encoding" + encoding);
                }
                for(unsigned c = 0; c < 256; c++) {
                    const char ibuf[2] = {char(c), 0};
                    size_t insize = sizeof(ibuf);
                    uint32_t obuf[2] = {illegal, illegal};
                    size_t outsize = sizeof(obuf);
                    // Basic single codepoint conversion
                    call_iconv(d, ibuf, &insize, reinterpret_cast<char*>(obuf), &outsize);
                    if(insize == 0 && outsize == 0 && obuf[1] == 0) {
                        first_byte_table.push_back(obuf[0]);
                        continue;
                    }

                    // Test if this is illegal first byte or incomplete
                    insize = 1;
                    outsize = sizeof(obuf);
                    call_iconv(d, nullptr, nullptr, nullptr, nullptr);
                    size_t res = call_iconv(d, ibuf, &insize, reinterpret_cast<char*>(obuf), &outsize);

                    // Now if this single byte starts a sequence we add incomplete
                    // to know to ask that we need two bytes, otherwise it may only be
                    // illegal

                    uint32_t point;
                    if(res == (size_t)(-1) && errno == EINVAL)
                        point = incomplete;
                    else
                        point = illegal;
                    first_byte_table.push_back(point);
                }
            } catch(...) {
                if(d != (iconv_t)(-1))
                    iconv_close(d);
                throw;
            }
            iconv_close(d);
            first_byte_table_.reset(new std::vector<uint32_t>());
            first_byte_table_->swap(first_byte_table);
        }

        mb2_iconv_converter(const mb2_iconv_converter& other) :
            first_byte_table_(other.first_byte_table_), encoding_(other.encoding_), to_utf_((iconv_t)(-1)),
            from_utf_((iconv_t)(-1))
        {}

        ~mb2_iconv_converter()
        {
            if(to_utf_ != (iconv_t)(-1))
                iconv_close(to_utf_);
            if(from_utf_ != (iconv_t)(-1))
                iconv_close(from_utf_);
        }

        bool is_thread_safe() const override { return false; }

        mb2_iconv_converter* clone() const override { return new mb2_iconv_converter(*this); }

        uint32_t to_unicode(const char*& begin, const char* end) override
        {
            if(begin == end)
                return incomplete;

            unsigned char seq0 = *begin;
            uint32_t index = (*first_byte_table_)[seq0];
            if(index == illegal)
                return illegal;
            if(index != incomplete) {
                begin++;
                return index;
            } else if(begin + 1 == end)
                return incomplete;

            open(to_utf_, utf32_encoding(), encoding_.c_str());

            // maybe illegal or may be double byte

            const char inseq[3] = {static_cast<char>(seq0), begin[1], 0};
            size_t insize = sizeof(inseq);
            uint32_t result[2] = {illegal, illegal};
            size_t outsize = sizeof(result);
            call_iconv(to_utf_, inseq, &insize, reinterpret_cast<char*>(result), &outsize);
            if(outsize == 0 && insize == 0 && result[1] == 0) {
                begin += 2;
                return result[0];
            }
            return illegal;
        }

        uint32_t from_unicode(uint32_t cp, char* begin, const char* end) override
        {
            if(cp == 0) {
                if(begin != end) {
                    *begin = 0;
                    return 1;
                } else {
                    return incomplete;
                }
            }

            open(from_utf_, encoding_.c_str(), utf32_encoding());

            const uint32_t inbuf[2] = {cp, 0};
            size_t insize = sizeof(inbuf);
            char outseq[3] = {0};
            size_t outsize = 3;

            call_iconv(from_utf_, reinterpret_cast<const char*>(inbuf), &insize, outseq, &outsize);

            if(insize != 0 || outsize > 1)
                return illegal;
            size_t len = 2 - outsize;
            size_t reminder = end - begin;
            if(reminder < len)
                return incomplete;
            for(unsigned i = 0; i < len; i++)
                *begin++ = outseq[i];
            return len;
        }

        void open(iconv_t& d, const char* to, const char* from)
        {
            if(d != (iconv_t)(-1))
                return;
            d = iconv_open(to, from);
        }

        static const char* utf32_encoding()
        {
            union {
                char one;
                uint32_t value;
            } test;
            test.value = 1;
            if(test.one == 1)
                return "UTF-32LE";
            else
                return "UTF-32BE";
        }

        int max_len() const override { return 2; }

    private:
        std::shared_ptr<std::vector<uint32_t>> first_byte_table_;
        std::string encoding_;
        iconv_t to_utf_;
        iconv_t from_utf_;
    };

    std::unique_ptr<util::base_converter> create_iconv_converter(const std::string& encoding)
    {
        try {
            return std::unique_ptr<util::base_converter>(new mb2_iconv_converter(encoding));
        } catch(const std::exception& e) {
            return nullptr;
        }
    }

#else // no iconv
    std::unique_ptr<util::base_converter> create_iconv_converter(const std::string& /*encoding*/)
    {
        return nullptr;
    }
#endif

    std::locale create_codecvt(const std::locale& in, const std::string& encoding, char_facet_t type)
    {
        if(conv::impl::normalize_encoding(encoding.c_str()) == "utf8")
            return util::create_utf8_codecvt(in, type);

        try {
            return util::create_simple_codecvt(in, encoding, type);
        } catch(const conv::invalid_charset_error&) {
            return util::create_codecvt(in, create_iconv_converter(encoding), type);
        }
    }

}}} // namespace boost::locale::impl_posix

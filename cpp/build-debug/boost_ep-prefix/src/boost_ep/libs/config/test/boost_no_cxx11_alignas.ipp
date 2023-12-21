//  (C) Copyright Andrey Semashev 2013, 2020

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX11_ALIGNAS
//  TITLE:         C++11 alignas keyword.
//  DESCRIPTION:   The compiler does not support the C++11 alignment specification with alignas keyword.

namespace boost_no_cxx11_alignas {

template< typename T >
struct alignment_of
{
    static const unsigned int value = sizeof(T);
};

struct alignas(16) my_data1
{
    char data[10];
};

struct alignas(double) my_data2
{
    char data[16];
};

template< typename T >
struct alignas(alignment_of< T >::value) my_data3
{
    char data[16];
};

my_data1 dummy1[2];
my_data2 dummy2;
my_data3< int > dummy3;
alignas(16) char dummy4[10];
alignas(double) char dummy5[32];
alignas(alignment_of< int >::value) char dummy6[32];

int test()
{
    // TODO: Test that the data is actually aligned on platforms with uintptr_t
    return 0;
}

}

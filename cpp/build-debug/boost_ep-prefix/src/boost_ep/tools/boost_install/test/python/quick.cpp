
// Copyright 2017, 2018 Peter Dimov.
//
// Distributed under the Boost Software License, Version 1.0.
//
// See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt

#if defined(_WIN32)
# define BOOST_PYTHON_STATIC_LIB
#endif
#include <boost/python.hpp>
#include <boost/core/lightweight_test.hpp>

namespace python = boost::python;

int main()
{
    Py_Initialize();

    python::dict env;
    python::object result = python::exec( "number = 42", env, env );

    BOOST_TEST_EQ( python::extract<int>( env["number"] ), 42 );

    return boost::report_errors();
}

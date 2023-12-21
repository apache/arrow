
// Copyright 2018 Peter Dimov.
//
// Distributed under the Boost Software License, Version 1.0.
//
// See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt

#include <boost/wave.hpp>
#include <boost/wave/cpplexer/cpp_lex_token.hpp>
#include <boost/wave/cpplexer/cpp_lex_iterator.hpp>
#include <iostream>

int main()
{
    std::string input(
        "#if 0\n"
        "6.28\n"
        "#else\n"
        "3.14\n"
        "#endif\n"
    );

    try
    {
        typedef boost::wave::cpplexer::lex_token<> token_type;
        typedef boost::wave::cpplexer::lex_iterator<token_type> lex_iterator_type;
        typedef boost::wave::context<std::string::iterator, lex_iterator_type> context_type;

        context_type ctx( input.begin(), input.end(), "input.cpp" );

        for( context_type::iterator_type first = ctx.begin(), last = ctx.end(); first != last; ++first )
        {
            std::cout << first->get_value();
        }

        return 0;
    }
    catch( boost::wave::cpp_exception const & x )
    {
        std::cerr << x.file_name() << "(" << x.line_no() << "): " << x.description() << std::endl;
        return 1;
    }
    catch( std::exception const & x )
    {
        std::cerr << "Exception: " << x.what() << std::endl;
        return 2;
    }
}

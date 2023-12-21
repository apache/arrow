//  Copyright 2017 Peter Dimov.
//
//  Distributed under the Boost Software License, Version 1.0.
//
//  See accompanying file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt

#include <boost/config/pragma_message.hpp>

BOOST_PRAGMA_MESSAGE("first message")

#define MSG2 "second message"
BOOST_PRAGMA_MESSAGE(MSG2)

#include <boost/config.hpp> // BOOST_STRINGIZE

#define MSG3 third message
BOOST_PRAGMA_MESSAGE(BOOST_STRINGIZE(MSG3))

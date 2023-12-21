# Copyright 2021-2022 Andrey Semashev
#
# Distributed under the Boost Software License, Version 1.0.
# See accompanying file LICENSE_1_0.txt or copy at https://www.boost.org/LICENSE_1_0.txt

cmake_minimum_required(VERSION 3.5)
project(BoostLog VERSION "${BOOST_SUPERPROJECT_VERSION}" LANGUAGES CXX)

include(CheckCXXSourceCompiles)

set(BOOST_LOG_NO_THREADS OFF CACHE BOOL "Disable multithreading support in Boost.Log")
set(BOOST_LOG_WITHOUT_CHAR OFF CACHE BOOL "Disable support for narrow character logging in Boost.Log")
set(BOOST_LOG_WITHOUT_WCHAR_T OFF CACHE BOOL "Disable support for wide character logging in Boost.Log")
set(BOOST_LOG_WITHOUT_DEFAULT_FACTORIES OFF CACHE BOOL "Disable default factories for filters and formatters in Boost.Log")
set(BOOST_LOG_WITHOUT_SETTINGS_PARSERS OFF CACHE BOOL "Disable settings, filter and formatter parsers in Boost.Log")
if (WIN32)
    set(BOOST_LOG_NO_QUERY_PERFORMANCE_COUNTER OFF CACHE BOOL "Disable using QueryPerformanceCounter API on Windows in Boost.Log")
    set(BOOST_LOG_WITHOUT_DEBUG_OUTPUT OFF CACHE BOOL "Disable support for debugger output on Windows in Boost.Log")
    set(BOOST_LOG_WITHOUT_EVENT_LOG OFF CACHE BOOL "Disable support for event log on Windows in Boost.Log")
endif()
set(BOOST_LOG_WITHOUT_IPC OFF CACHE BOOL "Disable support for inter-process communication in Boost.Log")
set(BOOST_LOG_WITHOUT_SYSLOG OFF CACHE BOOL "Disable support for syslog API in Boost.Log")
set(BOOST_LOG_USE_NATIVE_SYSLOG OFF CACHE BOOL "Force-enable using native syslog API in Boost.Log")
set(BOOST_LOG_USE_COMPILER_TLS OFF CACHE BOOL "Enable using compiler-specific intrinsics for thread-local storage in Boost.Log")

set(BOOST_LOG_REGEX_BACKENDS "std::regex" "Boost.Regex" "Boost.Xpressive")
set(BOOST_LOG_USE_REGEX_BACKEND "Boost.Regex" CACHE STRING "Regular expressions backend to use in Boost.Log")
set_property(CACHE BOOST_LOG_USE_REGEX_BACKEND PROPERTY STRINGS ${BOOST_LOG_REGEX_BACKENDS})
if (NOT BOOST_LOG_USE_REGEX_BACKEND IN_LIST BOOST_LOG_REGEX_BACKENDS)
    message(FATAL_ERROR "BOOST_LOG_USE_REGEX_BACKEND must be one of: ${BOOST_LOG_REGEX_BACKENDS}")
endif()

if (NOT BOOST_LOG_NO_THREADS)
    set(THREADS_PREFER_PTHREAD_FLAG ON)
    find_package(Threads REQUIRED)
endif()

# Note: We can't use the Boost::library targets in the configure checks as they may not yet be included
# by the superproject when this CMakeLists.txt is included. We also don't want to hardcode include paths
# of the needed libraries and their dependencies, recursively, as this is too fragile and requires maintenance.
# Instead, we collect include paths of all libraries and use them in the configure checks. This works faster
# if there is a unified Boost include tree in the filesystem (i.e. if `b2 headers` was run or we're in the
# official monolithic Boost distribution tree).
include(cmake/BoostLibraryIncludes.cmake)

set(CMAKE_REQUIRED_INCLUDES ${BOOST_LIBRARY_INCLUDES})

check_cxx_source_compiles("#include <${CMAKE_CURRENT_SOURCE_DIR}/../config/checks/architecture/x86.cpp>\nint main() {}" BOOST_LOG_TARGET_X86)

set(CMAKE_REQUIRED_DEFINITIONS "-DBOOST_ALL_NO_LIB")
check_cxx_source_compiles("#include <${CMAKE_CURRENT_SOURCE_DIR}/config/atomic-int32/atomic_int32.cpp>" BOOST_LOG_HAS_LOCK_FREE_ATOMIC_INT32)
unset(CMAKE_REQUIRED_DEFINITIONS)

if (BOOST_LOG_TARGET_X86)
    if (CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
        if (CMAKE_SIZEOF_VOID_P EQUAL 4)
            set(boost_log_ssse3_cflags "/arch:SSE2")
            set(boost_log_avx2_cflags "/arch:AVX")
        endif()
    elseif (CMAKE_CXX_COMPILER_ID STREQUAL "Intel")
        if (WIN32)
            set(boost_log_ssse3_cflags "/QxSSSE3")
            set(boost_log_avx2_cflags "/arch:CORE-AVX2")
        else()
            set(boost_log_ssse3_cflags "-xSSSE3")
            set(boost_log_avx2_cflags "-xCORE-AVX2 -fabi-version=0")
        endif()
    else()
        set(boost_log_ssse3_cflags "-msse -msse2 -msse3 -mssse3")
        set(boost_log_avx2_cflags "-mavx -mavx2")
        if (NOT CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
            string(APPEND boost_log_avx2_cflags " -fabi-version=0")
        endif()
    endif()

    set(CMAKE_REQUIRED_FLAGS "${boost_log_ssse3_cflags}")
    check_cxx_source_compiles("#include <${CMAKE_CURRENT_SOURCE_DIR}/config/x86-ext/ssse3.cpp>" BOOST_LOG_COMPILER_HAS_SSSE3)
    unset(CMAKE_REQUIRED_FLAGS)
    set(CMAKE_REQUIRED_FLAGS "${boost_log_avx2_cflags}")
    check_cxx_source_compiles("#include <${CMAKE_CURRENT_SOURCE_DIR}/config/x86-ext/avx2.cpp>" BOOST_LOG_COMPILER_HAS_AVX2)
    unset(CMAKE_REQUIRED_FLAGS)
endif()

if (NOT BOOST_LOG_WITHOUT_SYSLOG AND NOT BOOST_LOG_USE_NATIVE_SYSLOG)
    check_cxx_source_compiles("#include <${CMAKE_CURRENT_SOURCE_DIR}/config/native-syslog/native_syslog.cpp>" BOOST_LOG_HAS_NATIVE_SYSLOG)
endif()

if (CMAKE_SYSTEM_NAME STREQUAL "SunOS")
    # Solaris headers are broken and cannot be included in C++03 when _XOPEN_SOURCE=600. At the same time, they cannot be included with _XOPEN_SOURCE=500 in C++11 and later.
    # This is because the system headers check the C language version and error out if the version does not match. We have to test if we can request _XOPEN_SOURCE=600.
    check_cxx_source_compiles("#include <${CMAKE_CURRENT_SOURCE_DIR}/config/xopen-source-600/xopen_source_600.cpp>" BOOST_LOG_HAS_XOPEN_SOURCE_600)
endif()

unset(CMAKE_REQUIRED_INCLUDES)

set(boost_log_common_public_defines
    # NOTE:
    # We deactivate autolinking, because cmake based builds don't need it
    # and we don't implement name mangling for the library file anyway.
    # Ususally the parent CMakeLists.txt file should already have globally defined BOOST_ALL_NO_LIB
    BOOST_LOG_NO_LIB
)

set(boost_log_common_private_defines
    __STDC_CONSTANT_MACROS
    BOOST_SPIRIT_USE_PHOENIX_V3=1
    BOOST_THREAD_DONT_USE_CHRONO=1 # Don't introduce false dependency on Boost.Chrono
)

set(boost_log_private_defines
    BOOST_LOG_BUILDING_THE_LIB
)

set(boost_log_setup_private_defines
    BOOST_LOG_SETUP_BUILDING_THE_LIB
)

if (BUILD_SHARED_LIBS)
    list(APPEND boost_log_common_public_defines BOOST_LOG_DYN_LINK)
    list(APPEND boost_log_private_defines BOOST_LOG_DLL)
    list(APPEND boost_log_setup_private_defines BOOST_LOG_SETUP_DLL)
else()
    list(APPEND boost_log_common_public_defines BOOST_LOG_STATIC_LINK)
endif()

set(boost_log_sources
    src/alignment_gap_between.hpp
    src/attribute_name.cpp
    src/attribute_set_impl.hpp
    src/attribute_set.cpp
    src/attribute_value_set.cpp
    src/bit_tools.hpp
    src/code_conversion.cpp
    src/stateless_allocator.hpp
    src/unique_ptr.hpp
    src/core.cpp
    src/record_ostream.cpp
    src/severity_level.cpp
    src/global_logger_storage.cpp
    src/named_scope.cpp
    src/process_name.cpp
    src/process_id.cpp
    src/thread_id.cpp
    src/id_formatting.hpp
    src/murmur3.hpp
    src/timer.cpp
    src/exceptions.cpp
    src/default_attribute_names.cpp
    src/default_sink.hpp
    src/default_sink.cpp
    src/text_ostream_backend.cpp
    src/text_file_backend.cpp
    src/text_multifile_backend.cpp
    src/thread_specific.cpp
    src/once_block.cpp
    src/timestamp.cpp
    src/threadsafe_queue.cpp
    src/event.cpp
    src/trivial.cpp
    src/spirit_encoding.hpp
    src/spirit_encoding.cpp
    src/format_parser.cpp
    src/date_time_format_parser.cpp
    src/named_scope_format_parser.cpp
    src/permissions.cpp
    src/dump.cpp
)

if (BOOST_LOG_COMPILER_HAS_SSSE3)
    set(boost_log_sources_ssse3 src/dump_ssse3.cpp)
    set_source_files_properties(${boost_log_sources_ssse3} PROPERTIES COMPILE_FLAGS "${boost_log_ssse3_cflags}")
    list(APPEND boost_log_private_defines BOOST_LOG_USE_SSSE3)
endif()
if (BOOST_LOG_COMPILER_HAS_AVX2)
    set(boost_log_sources_avx2 src/dump_avx2.cpp)
    set_source_files_properties(${boost_log_sources_avx2} PROPERTIES COMPILE_FLAGS "${boost_log_avx2_cflags}")
    list(APPEND boost_log_private_defines BOOST_LOG_USE_AVX2)
endif()

if (NOT BOOST_LOG_WITHOUT_SYSLOG)
    list(APPEND boost_log_sources src/syslog_backend.cpp)
    if (BOOST_LOG_USE_NATIVE_SYSLOG OR BOOST_LOG_HAS_NATIVE_SYSLOG)
        list(APPEND boost_log_common_private_defines BOOST_LOG_USE_NATIVE_SYSLOG)
    endif()
endif()

if (WIN32)
    list(APPEND boost_log_sources
        src/windows/light_rw_mutex.cpp
        src/windows/is_debugger_present.cpp
    )
    list(APPEND boost_log_common_private_defines
        # Disable warnings about using 'insecure' standard C functions.
        # These affect MSVC C/C++ library headers, which are used by various compilers.
        _SCL_SECURE_NO_WARNINGS
        _SCL_SECURE_NO_DEPRECATE
        _CRT_SECURE_NO_WARNINGS
        _CRT_SECURE_NO_DEPRECATE
    )

    if (NOT BOOST_LOG_WITHOUT_DEBUG_OUTPUT)
        list(APPEND boost_log_sources src/windows/debug_output_backend.cpp)
    endif()

    if (NOT BOOST_LOG_WITHOUT_IPC)
        list(APPEND boost_log_sources
            src/windows/auto_handle.hpp
            src/windows/object_name.cpp
            src/windows/utf_code_conversion.hpp
            src/windows/mapped_shared_memory.hpp
            src/windows/mapped_shared_memory.cpp
            src/windows/ipc_sync_wrappers.hpp
            src/windows/ipc_sync_wrappers.cpp
            src/windows/ipc_reliable_message_queue.cpp
        )
        list(APPEND boost_log_private_libs
            secur32
        )
    endif()

    if (NOT BOOST_LOG_WITHOUT_EVENT_LOG)
        # Find message compiler (mc)
        if (NOT CMAKE_MC_COMPILER AND DEFINED ENV{MC} AND EXISTS "$ENV{MC}" AND NOT IS_DIRECTORY "$ENV{MC}")
            set(CMAKE_MC_COMPILER "$ENV{MC}")
        endif()
        if (NOT CMAKE_MC_COMPILER)
            if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
                set(mc_executable "windmc.exe")
            else()
                set(mc_executable "mc.exe")
            endif()
            if (CMAKE_RC_COMPILER)
                # Use resource compiler directory as a hint. CMake will initialize CMAKE_RC_COMPILER
                # automatically based on environment variables, and message compiler typically resides
                # in the same directory in the Windows SDK installation. Reusing resource compiler
                # directory also provides user's choice of Windows SDK version and host and target
                # architecture.
                get_filename_component(rc_compiler_path "${CMAKE_RC_COMPILER}" DIRECTORY)
                list(APPEND mc_search_hints "${rc_compiler_path}")
            endif()
            if (CMAKE_CXX_COMPILER)
                # Message compiler can be located next to the compiler, e.g. on MinGW installations.
                get_filename_component(cxx_compiler_path "${CMAKE_CXX_COMPILER}" DIRECTORY)
                list(APPEND mc_search_hints "${cxx_compiler_path}")
            endif()
            message(DEBUG "Boost.Log: ${mc_executable} search hints: ${mc_search_hints}")
            find_program(CMAKE_MC_COMPILER "${mc_executable}" HINTS "${mc_search_hints}")
            if (CMAKE_MC_COMPILER STREQUAL "CMAKE_MC_COMPILER-NOTFOUND")
                message(STATUS "Boost.Log: Message compiler ${mc_executable} not found, event log support will be disabled.")
                unset(CMAKE_MC_COMPILER)
            else()
                message(STATUS "Boost.Log: Message compiler found: ${CMAKE_MC_COMPILER}")
            endif()
        endif()

        if (CMAKE_MC_COMPILER)
            add_custom_command(
                OUTPUT
                    "${CMAKE_CURRENT_BINARY_DIR}/src/windows/simple_event_log.h"
                    "${CMAKE_CURRENT_BINARY_DIR}/src/windows/simple_event_log.rc"
                COMMAND "${CMAKE_MC_COMPILER}"
                ARGS
                    -h "${CMAKE_CURRENT_BINARY_DIR}/src/windows"
                    -r "${CMAKE_CURRENT_BINARY_DIR}/src/windows"
                    "${CMAKE_CURRENT_SOURCE_DIR}/src/windows/simple_event_log.mc"
                MAIN_DEPENDENCY
                    "${CMAKE_CURRENT_SOURCE_DIR}/src/windows/simple_event_log.mc"
                COMMENT
                    "Building src/windows/simple_event_log.mc"
                VERBATIM
            )
            set_source_files_properties("${CMAKE_CURRENT_BINARY_DIR}/src/windows/simple_event_log.h" PROPERTIES GENERATED TRUE)
            set_source_files_properties("${CMAKE_CURRENT_BINARY_DIR}/src/windows/simple_event_log.rc" PROPERTIES GENERATED TRUE)

            list(APPEND boost_log_sources
                "${CMAKE_CURRENT_BINARY_DIR}/src/windows/simple_event_log.h"
                "${CMAKE_CURRENT_BINARY_DIR}/src/windows/simple_event_log.rc"
                src/windows/event_log_registry.hpp
                src/windows/event_log_backend.cpp
            )
            list(APPEND boost_log_private_libs
                psapi
            )
        else()
            set(BOOST_LOG_WITHOUT_EVENT_LOG ON)
        endif()
    endif()
else()
    if (NOT BOOST_LOG_WITHOUT_IPC)
        list(APPEND boost_log_sources
            src/posix/object_name.cpp
            src/posix/ipc_sync_wrappers.hpp
            src/posix/ipc_reliable_message_queue.cpp
        )
    endif()
endif()

if (CYGWIN)
    # Boost.Interprocess does not compile on Cygwin: https://github.com/boostorg/interprocess/issues/76
    set(BOOST_LOG_WITHOUT_IPC ON)
    list(APPEND boost_log_common_private_defines
        __USE_W32_SOCKETS
        _XOPEN_SOURCE=600
    )
endif()

if (WIN32 OR CYGWIN)
    list(APPEND boost_log_common_private_defines
        BOOST_USE_WINDOWS_H
        WIN32_LEAN_AND_MEAN
        NOMINMAX
        SECURITY_WIN32
    )
    list(APPEND boost_log_private_libs
        ws2_32
        mswsock
        advapi32
    )
endif()

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    list(APPEND boost_log_common_private_defines _XOPEN_SOURCE=600)
    list(APPEND boost_log_private_libs rt)
endif()

if (CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
    list(APPEND boost_log_private_libs rt)
endif()

if (CMAKE_SYSTEM_NAME STREQUAL "HP-UX")
    list(APPEND boost_log_common_private_defines _XOPEN_SOURCE=600)
    list(APPEND boost_log_private_libs ipv6)
endif()

if (CMAKE_SYSTEM_NAME STREQUAL "SunOS")
    if (BOOST_LOG_HAS_XOPEN_SOURCE_600)
        list(APPEND boost_log_common_private_defines _XOPEN_SOURCE=600)
    else()
        list(APPEND boost_log_common_private_defines _XOPEN_SOURCE=500)
    endif()
    list(APPEND boost_log_common_private_defines __EXTENSIONS__)
    list(APPEND boost_log_private_libs
        socket
        nsl
    )
endif()

if (BOOST_LOG_USE_REGEX_BACKEND STREQUAL "Boost.Regex")
    set(boost_log_regex_backend_private_libs Boost::regex)
    list(APPEND boost_log_common_private_defines BOOST_LOG_USE_BOOST_REGEX)
elseif (BOOST_LOG_USE_REGEX_BACKEND STREQUAL "Boost.Xpressive")
    set(boost_log_regex_backend_private_libs Boost::xpressive)
    list(APPEND boost_log_common_private_defines BOOST_LOG_USE_BOOST_XPRESSIVE)
else()
    list(APPEND boost_log_common_private_defines BOOST_LOG_USE_STD_REGEX)
endif()


if (BOOST_LOG_NO_THREADS)
    list(APPEND boost_log_common_public_defines BOOST_LOG_NO_THREADS)
endif()
if (BOOST_LOG_WITHOUT_CHAR)
    list(APPEND boost_log_common_public_defines BOOST_LOG_WITHOUT_CHAR)
endif()
if (BOOST_LOG_WITHOUT_WCHAR_T)
    list(APPEND boost_log_common_public_defines BOOST_LOG_WITHOUT_WCHAR_T)
endif()
if (BOOST_LOG_WITHOUT_DEFAULT_FACTORIES)
    list(APPEND boost_log_setup_private_defines BOOST_LOG_WITHOUT_DEFAULT_FACTORIES)
endif()
if (BOOST_LOG_WITHOUT_SETTINGS_PARSERS)
    list(APPEND boost_log_setup_private_defines BOOST_LOG_WITHOUT_SETTINGS_PARSERS)
endif()
if (BOOST_LOG_WITHOUT_SYSLOG)
    list(APPEND boost_log_common_private_defines BOOST_LOG_WITHOUT_SYSLOG)
endif()
if (BOOST_LOG_WITHOUT_IPC)
    list(APPEND boost_log_common_private_defines BOOST_LOG_WITHOUT_IPC)
endif()
if (WIN32)
    if (BOOST_LOG_NO_QUERY_PERFORMANCE_COUNTER)
        list(APPEND boost_log_common_private_defines BOOST_LOG_NO_QUERY_PERFORMANCE_COUNTER)
    endif()
    if (BOOST_LOG_WITHOUT_DEBUG_OUTPUT)
        list(APPEND boost_log_common_private_defines BOOST_LOG_WITHOUT_DEBUG_OUTPUT)
    endif()
    if (BOOST_LOG_WITHOUT_EVENT_LOG)
        list(APPEND boost_log_common_private_defines BOOST_LOG_WITHOUT_EVENT_LOG)
    endif()
endif()


if (CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    list(APPEND boost_log_common_private_cxxflags
        /bigobj
        /wd4503 # decorated name length exceeded, name was truncated
        /wd4456 # declaration of 'A' hides previous local declaration
        /wd4459 # declaration of 'A' hides global declaration
        /wd4003 # not enough actual parameters for macro 'X' - caused by BOOST_PP_IS_EMPTY and BOOST_PP_IS_BEGIN_PARENS which are used by Fusion
    )
endif()

if (CMAKE_CXX_COMPILER_ID STREQUAL "Intel")
    # Disable Intel warnings:
    # warning #177: function "X" was declared but never referenced
    # warning #780: using-declaration ignored -- it refers to the current namespace
    # warning #2196: routine is both "inline" and "noinline"
    # remark #1782: #pragma once is obsolete. Use #ifndef guard instead.
    # remark #193: zero used for undefined preprocessing identifier "X"
    # remark #304: access control not specified ("public" by default)
    # remark #981: operands are evaluated in unspecified order
    # remark #1418: external function definition with no prior declaration
    # Mostly comes from Boost.Phoenix: warning #411: class "X" defines no constructor to initialize the following: reference member "Y"...
    # warning #734: "X" (declared at line N of "file.hpp"), required for copy that was eliminated, is inaccessible
    # warning #279: controlling expression is constant
    if (WIN32)
        list(APPEND boost_log_common_private_cxxflags
            "/Qwd177,780,2196,1782,193,304,981,1418,411,734,279"
        )
    else()
        list(APPEND boost_log_common_private_cxxflags
            "-wd177,780,2196,1782,193,304,981,1418,411,734,279"
        )
    endif()
endif()

if ((WIN32 OR CYGWIN) AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    list(APPEND boost_log_common_private_linkflags
        -Wl,--enable-auto-import
    )
endif()


add_library(boost_log
    ${boost_log_sources}
    ${boost_log_sources_ssse3}
    ${boost_log_sources_avx2}
)
if (BOOST_SUPERPROJECT_VERSION)
    set_target_properties(boost_log PROPERTIES VERSION "${BOOST_SUPERPROJECT_VERSION}")
endif()
set(boost_log_install_targets boost_log)
add_library(Boost::log ALIAS boost_log)

target_include_directories(boost_log
    PUBLIC
        include
    PRIVATE
        src
        "${CMAKE_CURRENT_BINARY_DIR}/src"
)

target_link_libraries(boost_log
    PUBLIC
        Boost::array
        Boost::assert
        Boost::config
        Boost::container
        Boost::core
        Boost::date_time
        Boost::filesystem
        Boost::function_types
        Boost::fusion
        Boost::intrusive
        Boost::move
        Boost::mpl
        Boost::parameter
        Boost::phoenix
        Boost::predef
        Boost::preprocessor
        Boost::proto
        Boost::range
        Boost::smart_ptr
        Boost::static_assert
        Boost::system
        Boost::throw_exception
        Boost::type_index
        Boost::type_traits
        Boost::utility
        Boost::winapi

    PRIVATE
        Boost::align
        Boost::asio
        Boost::bind
        Boost::exception
        Boost::interprocess
        Boost::lexical_cast
        Boost::optional
        Boost::random
        Boost::spirit
        ${boost_log_regex_backend_private_libs}
)

if (NOT BOOST_LOG_NO_THREADS)
    target_link_libraries(boost_log
        PUBLIC
            Boost::atomic
            Boost::thread
    )
endif()

target_compile_definitions(boost_log
    PUBLIC
        ${boost_log_common_public_defines}
        ${boost_log_public_defines}
    PRIVATE
        ${boost_log_common_private_defines}
        ${boost_log_private_defines}
)

target_compile_options(boost_log
    PRIVATE
        ${boost_log_common_private_cxxflags}
)

target_link_libraries(boost_log
    PUBLIC
        ${boost_log_public_libs}
    PRIVATE
        ${boost_log_private_libs}
        ${boost_log_common_private_linkflags}
)

# An alias target for Boost::log but with all optional dependencies (i.e. for support headers)
add_library(boost_log_with_support INTERFACE)
add_library(Boost::log_with_support ALIAS boost_log_with_support)

target_link_libraries(boost_log_with_support
    INTERFACE
        Boost::log

        Boost::exception
        Boost::regex
        Boost::spirit
        Boost::xpressive
)


set(boost_log_setup_public_deps
    Boost::log

    Boost::assert
    Boost::config
    Boost::core
    Boost::iterator
    Boost::lexical_cast
    Boost::move
    Boost::optional
    Boost::parameter
    Boost::phoenix
    Boost::preprocessor
    Boost::property_tree
    Boost::smart_ptr
    Boost::type_traits
)

if (NOT BOOST_LOG_WITHOUT_SETTINGS_PARSERS)
    set(boost_log_setup_sources
        src/setup/parser_utils.hpp
        src/setup/parser_utils.cpp
        src/setup/init_from_stream.cpp
        src/setup/init_from_settings.cpp
        src/setup/settings_parser.cpp
        src/setup/filter_parser.cpp
        src/setup/formatter_parser.cpp
    )

    if (NOT BOOST_LOG_WITHOUT_DEFAULT_FACTORIES)
        list(APPEND boost_log_setup_sources
            src/setup/default_filter_factory.hpp
            src/setup/default_filter_factory.cpp
            src/setup/matches_relation_factory.cpp
            src/setup/default_formatter_factory.hpp
            src/setup/default_formatter_factory.cpp
        )
    endif()

    add_library(boost_log_setup ${boost_log_setup_sources})
    if (BOOST_SUPERPROJECT_VERSION)
        set_target_properties(boost_log_setup PROPERTIES VERSION "${BOOST_SUPERPROJECT_VERSION}")
    endif()
    list(APPEND boost_log_install_targets boost_log_setup)

    target_include_directories(boost_log_setup
        PUBLIC
            include
        PRIVATE
            src
    )

    target_link_libraries(boost_log_setup
        PUBLIC
            ${boost_log_setup_public_deps}

        PRIVATE
            Boost::asio
            Boost::bind
            Boost::date_time
            Boost::exception
            Boost::filesystem
            Boost::io
            Boost::spirit
            Boost::throw_exception
            Boost::utility
    )

    if (NOT BOOST_LOG_WITHOUT_DEFAULT_FACTORIES)
        target_link_libraries(boost_log_setup
            PRIVATE
                Boost::fusion
                Boost::mpl
                ${boost_log_regex_backend_private_libs}
        )
    endif()

    target_compile_definitions(boost_log_setup
        PUBLIC
            ${boost_log_common_public_defines}
        PRIVATE
            ${boost_log_common_private_defines}
            ${boost_log_setup_private_defines}
    )

    target_compile_options(boost_log_setup
        PRIVATE
            ${boost_log_common_private_cxxflags}
    )

    target_link_libraries(boost_log_setup
        PRIVATE
            ${boost_log_common_private_linkflags}
    )
else()
    add_library(boost_log_setup INTERFACE)
    target_include_directories(boost_log_setup INTERFACE include)
    target_compile_definitions(boost_log_setup INTERFACE ${boost_log_common_public_defines})
    target_link_libraries(boost_log_setup INTERFACE ${boost_log_setup_public_deps})
endif()

add_library(Boost::log_setup ALIAS boost_log_setup)

if (BOOST_SUPERPROJECT_VERSION AND NOT CMAKE_VERSION VERSION_LESS 3.13)
    boost_install(TARGETS ${boost_log_install_targets} VERSION "${BOOST_SUPERPROJECT_VERSION}" HEADER_DIRECTORY include)
endif()


cmake_minimum_required(VERSION 3.15)

set(command "/opt/homebrew/Cellar/cmake/3.28.0/bin/cmake;-DCMAKE_C_COMPILER=/Library/Developer/CommandLineTools/usr/bin/cc;-DCMAKE_CXX_COMPILER=/Library/Developer/CommandLineTools/usr/bin/c++;-DCMAKE_AR=/Library/Developer/CommandLineTools/usr/bin/ar;-DCMAKE_RANLIB=/Library/Developer/CommandLineTools/usr/bin/ranlib;-DBUILD_SHARED_LIBS=OFF;-DBUILD_STATIC_LIBS=ON;-DBUILD_TESTING=OFF;-DCMAKE_BUILD_TYPE=DEBUG;-DCMAKE_CXX_FLAGS= -Qunused-arguments -fcolor-diagnostics -fPIC;-DCMAKE_CXX_FLAGS_DEBUG=-g -Werror -O0 -ggdb   -Wno-error;-DCMAKE_CXX_FLAGS_MISIZEREL=-Os -DNDEBUG ;-DCMAKE_CXX_FLAGS_RELEASE=-O3 -DNDEBUG -O2  ;-DCMAKE_CXX_FLAGS_RELWITHDEBINFO=-O2 -g -DNDEBUG -ggdb  ;-DCMAKE_CXX_STANDARD=17;-DCMAKE_C_FLAGS= -Qunused-arguments -fPIC;-DCMAKE_C_FLAGS_DEBUG=-g -Werror -O0 -ggdb   -Wno-error;-DCMAKE_C_FLAGS_MISIZEREL=-Os -DNDEBUG ;-DCMAKE_C_FLAGS_RELEASE=-O3 -DNDEBUG -O2  ;-DCMAKE_C_FLAGS_RELWITHDEBINFO=-O2 -g -DNDEBUG -ggdb  ;-DCMAKE_EXPORT_NO_PACKAGE_REGISTRY=;-DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=;-DCMAKE_INSTALL_LIBDIR=lib;-DCMAKE_OSX_SYSROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX14.2.sdk;-DCMAKE_VERBOSE_MAKEFILE=FALSE;-DCMAKE_INSTALL_PREFIX=/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/utf8proc_ep-install;-GUnix Makefiles;-S;/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/utf8proc_ep-prefix/src/utf8proc_ep;-B;/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/utf8proc_ep-prefix/src/utf8proc_ep-build")
set(log_merged "")
set(log_output_on_failure "1")
set(stdout_log "/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/utf8proc_ep-prefix/src/utf8proc_ep-stamp/utf8proc_ep-configure-out.log")
set(stderr_log "/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/utf8proc_ep-prefix/src/utf8proc_ep-stamp/utf8proc_ep-configure-err.log")
execute_process(
  COMMAND ${command}
  RESULT_VARIABLE result
  OUTPUT_FILE "${stdout_log}"
  ERROR_FILE "${stderr_log}"
)
macro(read_up_to_max_size log_file output_var)
  file(SIZE ${log_file} determined_size)
  set(max_size 10240)
  if (determined_size GREATER max_size)
    math(EXPR seek_position "${determined_size} - ${max_size}")
    file(READ ${log_file} ${output_var} OFFSET ${seek_position})
    set(${output_var} "...skipping to end...\n${${output_var}}")
  else()
    file(READ ${log_file} ${output_var})
  endif()
endmacro()
if(result)
  set(msg "Command failed: ${result}\n")
  foreach(arg IN LISTS command)
    set(msg "${msg} '${arg}'")
  endforeach()
  if (${log_merged})
    set(msg "${msg}\nSee also\n  ${stderr_log}")
  else()
    set(msg "${msg}\nSee also\n  /Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/utf8proc_ep-prefix/src/utf8proc_ep-stamp/utf8proc_ep-configure-*.log")
  endif()
  if (${log_output_on_failure})
    message(SEND_ERROR "${msg}")
    if (${log_merged})
      read_up_to_max_size("${stderr_log}" error_log_contents)
      message(STATUS "Log output is:\n${error_log_contents}")
    else()
      read_up_to_max_size("${stdout_log}" out_log_contents)
      read_up_to_max_size("${stderr_log}" err_log_contents)
      message(STATUS "stdout output is:\n${out_log_contents}")
      message(STATUS "stderr output is:\n${err_log_contents}")
    endif()
    message(FATAL_ERROR "Stopping after outputting logs.")
  else()
    message(FATAL_ERROR "${msg}")
  endif()
else()
  if(NOT "Unix Makefiles" MATCHES "Ninja")
    set(msg "utf8proc_ep configure command succeeded.  See also /Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/utf8proc_ep-prefix/src/utf8proc_ep-stamp/utf8proc_ep-configure-*.log")
    message(STATUS "${msg}")
  endif()
endif()

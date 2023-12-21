set(command "/opt/homebrew/Cellar/cmake/3.28.0/bin/cmake;-P;/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/jemalloc_ep-prefix/src/jemalloc_ep-stamp/download-jemalloc_ep.cmake")

execute_process(COMMAND ${command} RESULT_VARIABLE result)
if(result)
  set(msg "Command failed (${result}):\n")
  foreach(arg IN LISTS command)
    set(msg "${msg} '${arg}'")
  endforeach()
  message(FATAL_ERROR "${msg}")
endif()
set(command "/opt/homebrew/Cellar/cmake/3.28.0/bin/cmake;-P;/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/jemalloc_ep-prefix/src/jemalloc_ep-stamp/verify-jemalloc_ep.cmake")

execute_process(COMMAND ${command} RESULT_VARIABLE result)
if(result)
  set(msg "Command failed (${result}):\n")
  foreach(arg IN LISTS command)
    set(msg "${msg} '${arg}'")
  endforeach()
  message(FATAL_ERROR "${msg}")
endif()
set(command "/opt/homebrew/Cellar/cmake/3.28.0/bin/cmake;-P;/Users/simon/Desktop/arrow/arrowSF/cpp/build-debug/jemalloc_ep-prefix/src/jemalloc_ep-stamp/extract-jemalloc_ep.cmake")

execute_process(COMMAND ${command} RESULT_VARIABLE result)
if(result)
  set(msg "Command failed (${result}):\n")
  foreach(arg IN LISTS command)
    set(msg "${msg} '${arg}'")
  endforeach()
  message(FATAL_ERROR "${msg}")
endif()

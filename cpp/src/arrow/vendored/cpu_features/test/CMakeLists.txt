#
# libraries for tests
#

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF) # prefer use of -std11 instead of -gnustd11

include_directories(../include)
add_definitions(-DCPU_FEATURES_TEST)

##------------------------------------------------------------------------------
add_library(string_view ../src/string_view.c)
##------------------------------------------------------------------------------
add_library(filesystem_for_testing filesystem_for_testing.cc)
target_compile_definitions(filesystem_for_testing PUBLIC CPU_FEATURES_MOCK_FILESYSTEM)
##------------------------------------------------------------------------------
add_library(hwcaps_for_testing hwcaps_for_testing.cc)
target_compile_definitions(hwcaps_for_testing PUBLIC CPU_FEATURES_MOCK_GET_ELF_HWCAP_FROM_GETAUXVAL)
target_link_libraries(hwcaps_for_testing filesystem_for_testing)
##------------------------------------------------------------------------------
add_library(stack_line_reader ../src/stack_line_reader.c)
target_compile_definitions(stack_line_reader PUBLIC STACK_LINE_READER_BUFFER_SIZE=1024)
target_link_libraries(stack_line_reader string_view)
##------------------------------------------------------------------------------
add_library(stack_line_reader_for_test ../src/stack_line_reader.c)
target_compile_definitions(stack_line_reader_for_test PUBLIC STACK_LINE_READER_BUFFER_SIZE=16)
target_link_libraries(stack_line_reader_for_test string_view filesystem_for_testing)
##------------------------------------------------------------------------------
add_library(all_libraries ../src/stack_line_reader.c ../src/unix_features_aggregator.c)
target_link_libraries(all_libraries hwcaps_for_testing stack_line_reader string_view)

#
# tests
#
link_libraries(gtest gmock_main)

## bit_utils_test
add_executable(bit_utils_test bit_utils_test.cc)
target_link_libraries(bit_utils_test)
add_test(NAME bit_utils_test COMMAND bit_utils_test)
##------------------------------------------------------------------------------
## string_view_test
add_executable(string_view_test string_view_test.cc ../src/string_view.c)
target_link_libraries(string_view_test string_view)
add_test(NAME string_view_test COMMAND string_view_test)
##------------------------------------------------------------------------------
## stack_line_reader_test
add_executable(stack_line_reader_test stack_line_reader_test.cc)
target_link_libraries(stack_line_reader_test stack_line_reader_for_test)
add_test(NAME stack_line_reader_test COMMAND stack_line_reader_test)
##------------------------------------------------------------------------------
## unix_features_aggregator_test
add_executable(unix_features_aggregator_test unix_features_aggregator_test.cc)
target_link_libraries(unix_features_aggregator_test all_libraries)
add_test(NAME unix_features_aggregator_test COMMAND unix_features_aggregator_test)
##------------------------------------------------------------------------------
## cpuinfo_x86_test
if(PROCESSOR_IS_X86)
  add_executable(cpuinfo_x86_test cpuinfo_x86_test.cc ../src/cpuinfo_x86.c)
  target_compile_definitions(cpuinfo_x86_test PUBLIC CPU_FEATURES_MOCK_CPUID_X86)
  target_link_libraries(cpuinfo_x86_test all_libraries)
  add_test(NAME cpuinfo_x86_test COMMAND cpuinfo_x86_test)
endif()
##------------------------------------------------------------------------------
## cpuinfo_arm_test
if(PROCESSOR_IS_ARM)
  add_executable(cpuinfo_arm_test cpuinfo_arm_test.cc ../src/cpuinfo_arm.c)
  target_link_libraries(cpuinfo_arm_test all_libraries)
  add_test(NAME cpuinfo_arm_test COMMAND cpuinfo_arm_test)
endif()
##------------------------------------------------------------------------------
## cpuinfo_aarch64_test
if(PROCESSOR_IS_AARCH64)
  add_executable(cpuinfo_aarch64_test cpuinfo_aarch64_test.cc ../src/cpuinfo_aarch64.c)
  target_link_libraries(cpuinfo_aarch64_test all_libraries)
  add_test(NAME cpuinfo_aarch64_test COMMAND cpuinfo_aarch64_test)
endif()
##------------------------------------------------------------------------------
## cpuinfo_mips_test
if(PROCESSOR_IS_MIPS)
  add_executable(cpuinfo_mips_test cpuinfo_mips_test.cc  ../src/cpuinfo_mips.c)
  target_link_libraries(cpuinfo_mips_test all_libraries)
  add_test(NAME cpuinfo_mips_test COMMAND cpuinfo_mips_test)
endif()
##------------------------------------------------------------------------------
## cpuinfo_ppc_test
if(PROCESSOR_IS_POWER)
  add_executable(cpuinfo_ppc_test cpuinfo_ppc_test.cc  ../src/cpuinfo_ppc.c)
  target_link_libraries(cpuinfo_ppc_test all_libraries)
  add_test(NAME cpuinfo_ppc_test COMMAND cpuinfo_ppc_test)
endif()

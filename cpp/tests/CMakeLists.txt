# Add test source files
set(TEST_SOURCES
    test_file_json.cc
)

# Create test executable
add_executable(ArrowJsonTests ${TEST_SOURCES})

# Link Google Test and your project libraries
target_link_libraries(ArrowJsonTests gtest gtest_main arrow_dataset arrow_io)

# Add tests
add_test(NAME ArrowJsonTests COMMAND ArrowJsonTests)
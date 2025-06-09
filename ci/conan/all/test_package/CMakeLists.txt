cmake_minimum_required(VERSION 3.15)
project(test_package LANGUAGES CXX)

find_package(Arrow REQUIRED CONFIG)

add_executable(${PROJECT_NAME} test_package.cpp)

if (TARGET Arrow::arrow_shared)
    target_link_libraries(${PROJECT_NAME} PRIVATE Arrow::arrow_shared)
else()
    target_link_libraries(${PROJECT_NAME} PRIVATE Arrow::arrow_static)
endif()

if (${Arrow_VERSION} VERSION_LESS "10.0.0")
    target_compile_features(${PROJECT_NAME} PRIVATE cxx_std_11)
else()
    target_compile_features(${PROJECT_NAME} PRIVATE cxx_std_17)
endif()

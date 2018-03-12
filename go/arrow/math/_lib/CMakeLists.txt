cmake_minimum_required(VERSION 3.6)

project(math-func)
set(CMAKE_C_STANDARD 99)

add_library(memory STATIC float64.c int64.c uint64.c)



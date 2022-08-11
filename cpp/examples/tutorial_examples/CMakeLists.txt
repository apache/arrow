project(MyExample)
set(CMAKE_CXX_STANDARD 11)
set(BUILD_WARNING_LEVEL CHECKIN)
find_package(Arrow REQUIRED)
get_filename_component(ARROW_CONFIG_PATH ${Arrow_CONFIG} DIRECTORY)
find_package(Parquet REQUIRED HINTS ${ARROW_CONFIG_PATH})
find_package(ArrowDataset REQUIRED HINTS ${ARROW_CONFIG_PATH})

add_executable(arrow_example arrow_example.cc)
target_link_libraries(arrow_example PRIVATE arrow_shared parquet arrow_dataset_shared)

add_executable(file_access_example file_access_example.cc)
target_link_libraries(file_access_example PRIVATE arrow_shared parquet
                                                  arrow_dataset_shared)

add_executable(compute_example compute_example.cc)
target_link_libraries(compute_example PRIVATE arrow_shared parquet arrow_dataset_shared)

add_executable(dataset_example dataset_example.cc)
target_link_libraries(dataset_example PRIVATE arrow_shared parquet arrow_dataset_shared)

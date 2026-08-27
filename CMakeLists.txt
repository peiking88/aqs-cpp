cmake_minimum_required(VERSION 3.14)
project(aqs-cpp VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

find_package(Threads REQUIRED)

# 核心库：AQS 框架 + 三大同步原语（可重入锁 / 信号量 / 倒计时门闩）
add_library(aqs src/aqs.cpp)
target_include_directories(aqs PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_compile_options(aqs PRIVATE -Wall -Wextra)
target_link_libraries(aqs PUBLIC Threads::Threads)

enable_testing()
add_executable(test_aqs tests/test_aqs.cpp)
target_compile_options(test_aqs PRIVATE -Wall -Wextra)
target_link_libraries(test_aqs PRIVATE aqs)
add_test(NAME aqs_all COMMAND test_aqs)

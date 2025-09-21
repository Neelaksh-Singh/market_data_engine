# market_data_engine/CMakeLists.txt
cmake_minimum_required(VERSION 3.24) # Matches Databento's example and allows modern features
project(MarketDataEngine LANGUAGES CXX)

# Set C++ standard to C++17 or later for modern features and performance
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Prefer the Clang compiler if available, otherwise use GCC.
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(STATUS "Using Clang compiler: ${CMAKE_CXX_COMPILER}")
else()
    message(STATUS "Using GCC compiler: ${CMAKE_CXX_COMPILER}")
    # Set more aggressive warnings and optimizations
    add_compile_options(-Wall -Wextra -Wpedantic -Wconversion -O3 -march=native)
endif()

find_package(Threads REQUIRED)

# Use FetchContent to include the Databento C++ library
include(FetchContent)

# 1. Declare and make available ASIO (a header-only dependency of Databento)
FetchContent_Declare(asio
  GIT_REPOSITORY https://github.com/chriskohlhoff/asio.git
  GIT_TAG asio-1-28-0 
  CONFIGURE_COMMAND "" 
  BUILD_COMMAND ""     
)

FetchContent_GetProperties(asio)
if(NOT asio_POPULATED)
  FetchContent_Populate(asio)
  # Creates an interface target for ASIO
  add_library(asio INTERFACE)
  target_include_directories(asio INTERFACE ${asio_SOURCE_DIR}/asio/include)
  # ASIO requires the Threads library on some platforms
  target_link_libraries(asio INTERFACE Threads::Threads)
endif()

# 2. Declare and make available the main Databento library
FetchContent_Declare(databento
  GIT_REPOSITORY https://github.com/databento/databento-cpp
  GIT_TAG main 
)

FetchContent_MakeAvailable(databento) # This handles populating and adding the databento target

# Set up the main executable
add_executable(market_data_engine
    src/main.cpp
    src/DatabentoHandler.cpp
)

# Tells the compiler where to find our header files.
target_include_directories(market_data_engine
    PRIVATE
        include
)

# Link the required libraries to our executable.
target_link_libraries(market_data_engine
    PRIVATE
        databento::databento # Links the fetched Databento client
        Threads::Threads     # Links the pthread library
        asio                 # Links our internally created ASIO target
)
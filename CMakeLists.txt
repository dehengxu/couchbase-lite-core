#[[
LiteCore CMake Project

This is the CMake project for building the Couchbase LiteCore project.  This is the core of the Couchbase Lite library.
It makes use of a lot of source and sublibraries and some cannot be used on their own.  Here are a description of the
targets that result from this project:

BLIPStatic - The BLIP communication library for crafting messages that can be sent over a provided connection
C4Tests - A test runner that runs tests based on the shared library
CppTests - A test runner that runs test based on the static library
FleeceStatic - The Fleece serialization library for saving data to a binary format
LiteCoreStatic - The static LiteCore library
LiteCore - The shared LiteCore library
LiteCoreWebSocket - WebSocket implementation with C4Socket API to plug into LiteCore
LiteCoreREST - A simple library used for enabling testing of the shared library (not used in production)
mbedcrypto - The cryptography suite from mbedTLS (https://tls.mbed.org/)
SQLite3_UnicodeSN - The snowball tokenizer library for SQLite
Support - A library of shared functionality that is used in some or all of the other targets

This project is built for the following platforms at Couchbase:
- Windows 10
- UWP
- macOS 10.13
- CentOS 7 (clang / libc++)
- Android API 19+

Platform logic is largely separated into the cmake/platform_*.cmake files.  Platforms are conglomerated together as follows
- platform_base
    - platform_unix
        - platform_apple
        - platform_linux
            - platform_linux_desktop
            - platform_android
    - platform_win
        - platform_win_desktop
        - platform_win_store
]]#

cmake_minimum_required (VERSION 3.9)
cmake_policy(VERSION 3.9)

# Mac/apple setup -- must appear before the first "project()" line"
set(CMAKE_OSX_DEPLOYMENT_TARGET "10.12")
if(NOT DEFINED CMAKE_OSX_SYSROOT)
    # Tells Mac builds to use the current SDK's headers & libs, not what's in the OS.
    set(CMAKE_OSX_SYSROOT macosx)
endif()

project (LiteCore)

### BUILD SETTINGS:

set(COMPILE_FLAGS   "${COMPILE_FLAGS}   -Wall -Werror")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 11)
set_property(DIRECTORY APPEND PROPERTY COMPILE_DEFINITIONS
    $<$<CONFIG:Debug>:DEBUG>
)

option(CODE_COVERAGE_ENABLED "Set whether or not code coverage information should be generated" OFF)
option(BUILD_ENTERPRISE "Set whether or not to build enterprise edition" OFF)
option(LITECORE_DISABLE_ICU "Disables ICU linking" OFF)
option(DISABLE_LTO_BUILD "Disable build with Link-time optimization" OFF)

add_definitions(
    -DCMAKE                  # Let the source know this is a CMAKE build
    -D__STDC_FORMAT_MACROS   # Enables printf format macros for variable sized types (e.g. size_t)
)

if(BUILD_ENTERPRISE)
    add_definitions(
        -DCOUCHBASE_ENTERPRISE      # Tells LiteCore it's an EE build
    )
endif()

if(MSVC)
    add_definitions(-DWIN32_LEAN_AND_MEAN -D_WIN32_WINNT=0x0A00)
    if(WINDOWS_STORE)
        include("${PROJECT_SOURCE_DIR}/cmake/platform_win_store.cmake")
    else()
        include("${PROJECT_SOURCE_DIR}/cmake/platform_win_desktop.cmake")
    endif()
elseif(APPLE)
    include("${PROJECT_SOURCE_DIR}/cmake/platform_apple.cmake")
elseif(ANDROID)
    include("${PROJECT_SOURCE_DIR}/cmake/platform_android.cmake")
elseif("${CMAKE_SYSTEM_NAME}" STREQUAL "Linux")
    include("${PROJECT_SOURCE_DIR}/cmake/platform_linux_desktop.cmake")
else()
    message(FATAL_ERROR "Unable to determine a supported platform from ${CMAKE_SYSTEM_NAME}")
endif(MSVC)

if(CMAKE_COMPILER_IS_GNUCC)
    # Suppress an annoying note about GCC 7 ABI changes, and linker errors about the Fleece C API
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-psabi -Wno-odr")
endif()

if(CODE_COVERAGE_ENABLED)
  if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug" AND "${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage -O0")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --coverage -O0")
  elseif(NOT "${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
    message(WARN " Code coverage not supported for non-debug builds")
  else()
    message(WARN " Code coverage only supported on Clang")
  endif()
endif()

setup_globals()
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${LITECORE_C_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${LITECORE_CXX_FLAGS}")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${LITECORE_SHARED_LINKER_FLAGS}")
set(CMAKE_STATIC_LINKER_FLAGS "${CMAKE_STATIC_LINKER_FLAGS} ${LITECORE_STATIC_LINKER_FLAGS}")
set(SQLITE3_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/vendor/SQLiteCpp/sqlite3) # For SQLite3_UnicodeSN

add_subdirectory(REST                       EXCLUDE_FROM_ALL)
add_subdirectory(vendor/fleece              EXCLUDE_FROM_ALL)
add_subdirectory(vendor/sqlite3-unicodesn   EXCLUDE_FROM_ALL)
add_subdirectory(vendor/BLIP-Cpp            EXCLUDE_FROM_ALL)
add_subdirectory(vendor/mbedtls             EXCLUDE_FROM_ALL)

# Generate file repo_version.h containing Git repo information, and add it to #include path:
set(GENERATED_HEADERS_DIR "${CMAKE_BINARY_DIR}/generated_headers")
file(MAKE_DIRECTORY "${GENERATED_HEADERS_DIR}")
if (UNIX)
    execute_process(COMMAND /bin/bash "${PROJECT_SOURCE_DIR}/build_cmake/scripts/get_repo_version.sh"
                                      "${GENERATED_HEADERS_DIR}/repo_version.h"
                                      WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}")
else()
    execute_process(COMMAND powershell "${PROJECT_SOURCE_DIR}/build_cmake/scripts/get_repo_version.ps1"
                                      "${GENERATED_HEADERS_DIR}/repo_version.h"
                                      WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}")
endif()
include_directories(${GENERATED_HEADERS_DIR})

### SUPPORT LIBRARY:

set_support_source(RESULT SUPPORT_SRC)
add_library(
    Support STATIC
    ${SUPPORT_SRC}
)

target_compile_definitions(
    Support PRIVATE
    -DNOMINMAX
)

target_include_directories(
    Support PRIVATE
    vendor/fleece/API
    vendor/fleece/Fleece/Support
    LiteCore/BlobStore
    LiteCore/Database
    LiteCore/Support
    LiteCore/Storage
    LiteCore/Query
    LiteCore/RevID
    Crypto
    Network
    vendor/mbedtls/include
    vendor/mbedtls/crypto/include
    C
    C/include
    vendor/BLIP-Cpp/include/blip_cpp
    vendor/BLIP-Cpp/src/util
    vendor/BLIP-Cpp/src/websocket
    vendor/SQLiteCpp/sqlite3
    vendor/SQLiteCpp/include
)
setup_support_build()

### WebSocket LIBRARY:

set(
    LC_WEBSOCKET_SRC
    Networking/HTTPTypes.cc
    Networking/HTTPLogic.cc
    Networking/Poller.cc
    Networking/TCPSocket.cc
    Networking/BuiltInWebSocket.cc
    Networking/TLSContext.cc
    REST/netUtils.cc
    vendor/sockpp/src/acceptor.cpp
    vendor/sockpp/src/connector.cpp
    vendor/sockpp/src/datagram_socket.cpp
    vendor/sockpp/src/exception.cpp
    vendor/sockpp/src/inet_address.cpp
    vendor/sockpp/src/inet6_address.cpp
    vendor/sockpp/src/mbedtls_context.cpp
    vendor/sockpp/src/socket.cpp
    vendor/sockpp/src/stream_socket.cpp
)

add_library(LiteCoreWebSocket STATIC ${LC_WEBSOCKET_SRC})
target_include_directories(
    LiteCoreWebSocket PRIVATE
    C
    C/include
    Crypto
    LiteCore/Support
    Networking
    Replicator
    REST
    vendor/BLIP-Cpp/include/blip_cpp
    vendor/BLIP-Cpp/src/util
    vendor/fleece/Fleece/Support
    vendor/fleece/API
    vendor/sockpp/include
    vendor/mbedtls/include
    vendor/mbedtls/crypto/include
)

### LITECORE LIBRARY:

set_litecore_source(RESULT ALL_SRC_FILES)
add_library(LiteCoreStatic STATIC ${ALL_SRC_FILES})
target_compile_definitions(
    LiteCoreStatic PRIVATE
    -DLITECORE_IMPL
    -DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1  # Default to NORMAL sync mode on SQLite (safe with WAL mode)
    -DSQLITE_LIKE_DOESNT_MATCH_BLOBS    # Optimize SQLite "like" queries
    -DSQLITE_OMIT_SHARED_CACHE          # Allows SQLite to discard shared cache, eliminating some branches
    -DSQLITE_OMIT_DECLTYPE              # Disable returning the declared column type from query (reduces memory usage)
    -DSQLITE_OMIT_DATETIME_FUNCS        # Don't compile SQLite date functions
    -DSQLITE_ENABLE_EXPLAIN_COMMENTS    # Add comment text to the result of sqlite3_explain
    -DSQLITE_DISABLE_FTS3_UNICODE       # Disable FTS3 unicode61 tokenizer (not used in LiteCore)
    -DSQLITE_ENABLE_MEMORY_MANAGEMENT   # Enable sqlite3_release_memory to release unused memory faster
    -DSQLITE_ENABLE_STAT4               # Enable enhanced query planning
    -DSQLITE_HAVE_ISNAN                 # Use system provided isnan()
    -DHAVE_LOCALTIME_R                  # Use localtime_r instead of localtime
    -DHAVE_USLEEP                       # Allow millisecond precision sleep
    -DHAVE_UTIME                        # Use utime() instead of utimes()
    -DSQLITE_OMIT_LOAD_EXTENSION        # Disable extensions (not needed for LiteCore)
    -DSQLITE_ENABLE_FTS4                # Build FTS versions 3 and 4
    -DSQLITE_ENABLE_FTS3_PARENTHESIS    # Allow AND and NOT support in FTS parser
    -DSQLITE_ENABLE_FTS3_TOKENIZER      # Allow LiteCore to define a tokenizer
    -DSQLITE_DQS=0                      # Disallow double-quoted strings (only identifiers)
    
)

if(BUILD_ENTERPRISE)
    target_compile_definitions(
        LiteCoreStatic PRIVATE
        -DSQLITE_HAS_CODEC              # Enables SQLite encryption extension (SEE)
    )
endif()

target_include_directories(
    LiteCoreStatic PRIVATE
    vendor/fleece/API
    vendor/fleece/Fleece/Core
    vendor/fleece/Fleece/Mutable
    vendor/fleece/Fleece/Support
    LiteCore/BlobStore
    LiteCore/Database
    LiteCore/Query
    LiteCore/Query/N1QL_Parser
    LiteCore/RevTrees
    LiteCore/Storage
    LiteCore/Support
    C
    C/include 
    Crypto
    Networking
    Replicator
    REST
    vendor/BLIP-Cpp/include/blip_cpp
    vendor/BLIP-Cpp/src/util
    vendor/SQLiteCpp/sqlite3
    vendor/SQLiteCpp/include
    vendor/sqlite3-unicodesn
    vendor/mbedtls/include
    vendor/mbedtls/crypto/include
    vendor/sockpp/include
)

if(WIN32)
    add_library(LiteCore SHARED MSVC/SQLiteTempDirectory.cc) #No-op for non-UWP
    target_include_directories(
        LiteCore PRIVATE
        vendor/SQLiteCpp/sqlite3
    )
else()
    if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/empty.cpp")
        file(WRITE empty.cpp)
    endif()
    add_library(LiteCore SHARED empty.cpp)
endif()

# Library flags defined in platform_linux
set(
    LITECORE_LIBRARIES_PRIVATE
    ${WHOLE_LIBRARY_FLAG}
    LiteCoreStatic
    FleeceStatic
    ${NO_WHOLE_LIBRARY_FLAG}
    Support
    SQLite3_UnicodeSN
    BLIPStatic
    mbedcrypto
    mbedtls
    mbedx509
)

target_include_directories(
    LiteCore INTERFACE
    C/include
)
target_link_libraries(LiteCore PRIVATE ${LITECORE_LIBRARIES_PRIVATE})
setup_litecore_build()

install (
    TARGETS LiteCore
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    OPTIONAL
)

### TESTS:

add_subdirectory(LiteCore/tests)
add_subdirectory(C/tests)

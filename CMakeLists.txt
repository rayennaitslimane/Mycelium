cmake_minimum_required(VERSION 3.23)
project(myc LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(CTest)

find_package(isa-l REQUIRED)
find_package(libpqxx REQUIRED)
find_package(botan REQUIRED)
find_package(lmdb REQUIRED)
find_package(msgpack REQUIRED)
find_package(httplib REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(Threads REQUIRED)

add_library(myc
    core/ErasureCodec.cpp
    core/Crypto.cpp
    core/MetadataStore.cpp
    core/MetricsCollector.cpp
    core/StorageNode.cpp
    core/StorageNodeServer.cpp
    core/ShardTransport.cpp
    core/StorageClient.cpp
    core/StorageClientServer.cpp
)

target_include_directories(myc PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(myc PUBLIC
    isa-l::isa-l
    libpqxx::pqxx
    botan::botan
    lmdb::lmdb
    msgpack-cxx
    httplib::httplib
    nlohmann_json::nlohmann_json
    Threads::Threads
)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)

    add_executable(myc_unit_tests
        tests/ErasureCodecTest.cpp
        tests/CryptoTest.cpp
    )

    target_link_libraries(myc_unit_tests PRIVATE
        myc
        GTest::gtest_main
    )

    add_test(NAME myc_unit_tests COMMAND myc_unit_tests)

    add_executable(myc_integration_tests
        tests/MetadataStoreTest.cpp
        tests/StorageNodeTest.cpp
        tests/StorageNodeServerTest.cpp
        tests/StorageClientTest.cpp
        tests/StorageClientServerTest.cpp
    )

    target_link_libraries(myc_integration_tests
        PRIVATE
            myc
            GTest::gtest_main
    )

    add_test(
        NAME myc_integration_tests
        COMMAND myc_integration_tests
    )
endif()

add_executable(storage_node cmd/StorageNodeMain.cpp)
target_link_libraries(storage_node PRIVATE myc)

add_executable(storage_client cmd/StorageClientMain.cpp)
target_link_libraries(storage_client PRIVATE myc)

add_executable(benchmark cmd/BenchmarkMain.cpp)
target_link_libraries(benchmark PRIVATE myc)

add_executable(metadata_benchmark cmd/MetadataBenchmarkMain.cpp)
target_link_libraries(metadata_benchmark PRIVATE myc)

install(TARGETS myc storage_node storage_client
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
)
#cmake_minimum_required(VERSION 3.6)
#enable_language(CXX)
#enable_language(C)
#enable_language(ASM)
#set(CMAKE_C_COMPILER "gcc")
#set(CMAKE_CXX_COMPILER "g++")
#set(CMAKE_C_STANDARD 11)
#if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 5.1)
#    set(CMAKE_CXX_STANDARD 14)
#else ()
#    set(CMAKE_CXX_STANDARD 11)
#endif ()
#project(Microbench)

include_directories("./")
include_directories("../")
include_directories("../../")
include_directories("../../")
include_directories("../../src")
include_directories("../../src/levelhash")
include_directories("../../src/faster")
include_directories("../../src/faster/misc")
include_directories("../../src/faster/io")
include_directories("../../src/faster/cc")
include_directories("../../src/faster/core")
include_directories("../../src/faster/memory")
include_directories("../../src/faster/api")

set(FH_HYBRID_LIBS
        ../../src/faster/misc/address.cc
        ../../src/faster/misc/lss_allocator.cc
        ../../src/faster/misc/thread.cc
        )
if (CMAKE_SYSTEM_NAME MATCHES "Linux")
    set(FH_HYBRID_LIBS
            ../../src/faster/io/file_linux.cc
            ${FH_HYBRID_LIBS})
    set(NUMA -lnuma)
else ()
    set(FH_HYBRID_LIBS
            ../../src/faster/io/file_windows.cc
            ${FH_HYBRID_LIBS})
endif ()

set(FH_HYBRID_TEST
        FH_HybridTest.cpp)

message("subproject" ${CMAKE_CURRENT_SOURCE_DIR})
message("faster lib" ${FH_LIBS})

set(LH_HYBRID_LIBS
        ../../src/levelhash/hash.c
        ../../src/levelhash/level_hashing.c
        )

set(LH_HYBRID_TEST
        LH_HybridTest.cpp)

set(FH_HYBRID_TESTS_EXES ${FH_HYBRID_TEST})
foreach (sourcefile ${FH_HYBRID_TESTS_EXES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile} ${FH_HYBRID_LIBS})
    set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
            PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
            OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
            )
    target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} gtest ${PTHREAD} ${LTBB} ${AIO} ${UUID})
endforeach (sourcefile ${FH_HYBRID_TESTS_EXES})

set(LH_HYBRID_TESTS_EXES ${LH_HYBRID_TEST})
foreach (sourcefile ${LH_HYBRID_TESTS_EXES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile} ${LH_HYBRID_LIBS})
    set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
            PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
            OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
            )
    target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${PTHREAD})
endforeach (sourcefile ${LH_HYBRID_TESTS_EXES})

#include(ExternalProject)
if ((CMAKE_SYSTEM_NAME MATCHES "Linux" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 5.1) OR CMAKE_SYSTEM_NAME MATCHES "Darwin")
    option(WITH_JUNCTION "Test on junction" ON)
    if (WITH_JUNCTION)
        option(WITH_JUNCTION "build with junction" ON)
    endif ()
    option(WITH_LIBCUCKOO "Test on libcuckoo" ON)
    if (WITH_CUCKOO)
        option(WITH_LIBCUCKOO "build with libcukoo" ON)
    endif ()
    option(WITH_FOLLY "Test on folly" ON)
    if (WITH_FOLLY)
        option(WITH_FOLLY "build with folly" ON)
    endif ()
endif ()

if (WITH_JUNCTION)
    #[[set(JUNCTION_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/junction")
    message(STATUS "BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
    message(STATUS "JUNCTION_PREFIX: ${JUNCTION_PREFIX}")
    ExternalProject_Add(junction-deps
            PREFIX "${JUNCTION_PREFIX}"
            GIT_REPOSITORY https://github.com/preshing/junction
            UPDATE_COMMAND git clone https://github.com/preshing/turf.git && rm -rf ../turf && mv turf ..
            GIT_TAG master
            UPDATE_COMMAND ""
            INSTALL_COMMAND ""
            BUILD_COMMAND $(MAKE) -j8
            CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
            LOG_DOWNLOAD ON
            LOG_CONFIGURE ON
            LOG_BUILD ON
            )]]

    include_directories(
            ${JUNCTION_PREFIX}/src/junction-deps
            ${JUNCTION_PREFIX}/src/turf
            ${JUNCTION_PREFIX}/src/junction-deps-build/include
            ${JUNCTION_PREFIX}/src/junction-deps-build/turf/include
    )

    link_directories(${JUNCTION_PREFIX}/src/junction-deps-build/)
    link_directories(${JUNCTION_PREFIX}/src/junction-deps-build/turf)

    set(JUNCTION_TEST
            JH_HybridTest.cpp)

    foreach (sourcefile ${JUNCTION_TEST})
        get_filename_component(exename ${sourcefile} NAME_WE)
        add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile})
        set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
                PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
                EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
                EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
                OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
                )
        add_dependencies(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} junction-deps)
        target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} gtest ${PTHREAD} ${ATOMIC} -ljunction -lturf)
    endforeach (sourcefile ${JUNCTION_TEST})
endif ()

if (WITH_LIBCUCKOO)
    #[[set(LIBCUCKOO_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/libcuckoo")
    message(STATUS "BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
    message(STATUS "LIBCUCKOO_PREFIX: ${LIBCUCKOO_PREFIX}")
    ExternalProject_Add(libcuckoo
            PREFIX "${LIBCUCKOO_PREFIX}"
            GIT_REPOSITORY https://github.com/efficient/libcuckoo
            GIT_TAG master
            UPDATE_COMMAND ""
            INSTALL_COMMAND ""
            BUILD_COMMAND $(MAKE) -j8
            CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} cmake -DCMAKE_INSTALL_PREFIX=../install -DBUILD_EXAMPLES=1 -DBUILD_TESTS=1
            LOG_DOWNLOAD ON
            LOG_CONFIGURE ON
            LOG_BUILD ON
            )]]

    include_directories(
            ${LIBCUCKOO_PREFIX}/src/libcuckoo
    )

    link_directories(${LIBCUCKOO_PREFIX}/src/libcukoo-build/)

    set(CUKOO_TEST
            CH_HybridTest.cpp
            CH_HybridNumaTest.cpp)

    foreach (sourcefile ${CUKOO_TEST})
        get_filename_component(exename ${sourcefile} NAME_WE)
        add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile})
        set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
                PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
                EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
                EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
                OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
                )
        add_dependencies(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} libcuckoo)
        target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} gtest ${PTHREAD} ${ATOMIC} ${NUMA})
    endforeach (sourcefile ${CUKOO_TEST})
endif ()

option(WITH_AWLMAP "processing awlmap" ON)
if (WITH_AWLMAP)
    #[[set(AWLMAP_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/awlmap")
    message(STATUS "BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
    message(STATUS "AWLMAP_PREFIX: ${AWLMAP_PREFIX}")
    message(STATUS "DISABLE_FAST_TABLE: ${DISABLE_FAST_TABLE}")
    ExternalProject_Add(awlmap
            PREFIX "${AWLMAP_PREFIX}"
            GIT_REPOSITORY https://github.com/huangjiahua/concurrent_hash_map
            GIT_TAG master
            UPDATE_COMMAND ""
            INSTALL_COMMAND ""
            BUILD_COMMAND $(MAKE) -j8
            CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DDISABLE_FAST_TABLE=${DISABLE_FAST_TABLE} cmake -DCMAKE_INSTALL_PREFIX=../install -DBUILD_EXAMPLES=1 -DBUILD_TESTS=1
            LOG_DOWNLOAD ON
            LOG_CONFIGURE ON
            LOG_BUILD ON
            )]]

    include_directories(
            ${AWLMAP_PREFIX}/src/awlmap
            ${AWLMAP_PREFIX}/src/awlmap/hash_map
            ${AWLMAP_PREFIX}/src/awlmap/heavyhitter
            ${AWLMAP_PREFIX}/src/awlmap/my_haz_ptr
            ${AWLMAP_PREFIX}/src/awlmap/my_haz_ptr/haz_ptr
    )

    link_directories(${AWLMAP_PREFIX}/src/awlmap-build/)

    set(AWLMAP_LIBS
            ${AWLMAP_PREFIX}/src/awlmap/hash_map/concurrent_hash_map.cpp
            ${AWLMAP_PREFIX}/src/awlmap/hash_map/thread.cpp
            ${AWLMAP_PREFIX}/src/awlmap/my_haz_ptr/haz_ptr/haz_ptr.cpp
            )

    set(AWLMAP_TEST
            AH_HybridTest.cpp
            AH_HybridNumaTest.cpp)

    foreach (sourcefile ${AWLMAP_TEST})
        get_filename_component(exename ${sourcefile} NAME_WE)
        add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile} ${AWLMAP_LIBS})
        set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
                PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
                EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
                EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
                OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
                )
        add_dependencies(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} awlmap)
        target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} gtest ${PTHREAD} ${ATOMIC} ${NUMA})
        if (${DISABLE_FAST_TABLE})
            target_compile_definitions(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} PUBLIC DISABLE_FAST_TABLE)
        endif ()
        if (${ENABLE_CACHE_DATA_NODE})
            target_compile_definitions(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} PUBLIC ENABLE_CACHE_DATA_NODE)
        endif ()
    endforeach (sourcefile ${AWLMAP_TEST})
endif ()

if (WITH_FOLLY)
    #[[set(FOLLY_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/folly")
    message(STATUS "BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
    message(STATUS "FOLLY_PREFIX: ${FOLLY_PREFIX}")
    ExternalProject_Add(folly
            PREFIX "${FOLLY_PREFIX}"
            GIT_REPOSITORY https://github.com/facebook/folly
            GIT_TAG master
            UPDATE_COMMAND ""
            INSTALL_COMMAND ""
            BUILD_COMMAND $(MAKE) -j8
            CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
            LOG_DOWNLOAD ON
            LOG_CONFIGURE ON
            LOG_BUILD ON
            )]]

    include_directories(
            ${FOLLY_PREFIX}/src/folly
            ${FOLLY_PREFIX}/src/folly-build
    )

    link_directories(${FOLLY_PREFIX}/src/folly-build/)

    set(FOLLY_TEST
            OH_HybridTest.cpp)

    foreach (sourcefile ${FOLLY_TEST})
        get_filename_component(exename ${sourcefile} NAME_WE)
        add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile})
        set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
                PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
                EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
                EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
                OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
                )
        add_dependencies(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} folly)
        target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} -lboost_regex -lfolly -lgflags -lglog -ldl gtest -ldouble-conversion ${PTHREAD} ${ATOMIC})
    endforeach (sourcefile ${FOLLY_TEST})
endif ()
cmake_minimum_required(VERSION 3.6)
project(hashcomp)
enable_language(CXX)
enable_language(C)
enable_language(ASM)
set(CMAKE_C_COMPILER "gcc")
set(CMAKE_CXX_COMPILER "g++")
set(CMAKE_C_STANDARD 11)
if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 5.1)
    set(CMAKE_CXX_STANDARD 14)
else ()
    set(CMAKE_CXX_STANDARD 11)
endif ()

if (CMAKE_SYSTEM_NAME MATCHES "Darwin")
    enable_testing()
    find_package(gtest REQUIRED)
    include_directories(${GTEST_INCLUDE_DIRS})
    link_directories(/usr/local/lib)
else ()
    include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/third-party/gtest-1.7.0/fused-src)
    include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/third-party/gtest-1.7.0/fused-src/gtest)
    add_subdirectory(third-party/gtest-1.7.0/fused-src/gtest)
    enable_testing()
endif ()

add_subdirectory(stream)

include_directories("./")
include_directories("./src")
include_directories("./src/bench")
include_directories("./src/levelhash")
include_directories("./src/faster")
include_directories("src/faster/misc")
include_directories("src/faster/io")
include_directories("src/faster/cc")
include_directories("src/faster/core")
include_directories("src/faster/memory")
include_directories("src/faster/api")
include_directories("src/mhash")
include_directories("src/mhash/util")
include_directories("src/mhash/epoch")
include_directories("src/mhash/hazard")
include_directories("src/mhash/hazard/epoch")
include_directories("src/mhash/hazard/brown")
include_directories("src/xhash")
include_directories("utils")

if (CMAKE_SYSTEM_NAME MATCHES "Darwin")
    find_package(Boost 1.50.0 REQUIRED COMPONENTS date_time filesystem iostreams)
    include_directories(${Boost_INCLUDE_DIR})
    link_directories(${Boost_LIBRARY_DIR})
    link_directories(/usr/local/lib)
endif ()

if (NOT CMAKE_SYSTEM_NAME MATCHES "CYGWIN")
    find_package(tbb 1.0 REQUIRED COMPONENTS tbbmalloc tbb tbb_preview)
    if (NOT TBB_FIND_COMPONENTS EQUAL 0)
        message("-- Found TBB at: ${TBB_DIR}")
        message("-- ${TBB_FIND_COMPONENTS} found at ${TBB_ROOT}")
        set(LTBB "-L${TBB_DIR}/../../ -ltbb")
    endif ()
endif ()

if (CMAKE_SYSTEM_NAME MATCHES "Linux")
    set(PTHREAD -lpthread)
    set(ATOMIC -latomic)
    set(MLIB -lm)
    set(DL -ldl)
    set(AIO -laio)
    set(UUID -luuid)
    set(NUMA -lnuma)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wimplicit-function-declaration")
    option(WITH_NUMA "enforcing numa test" ON)
    if (WITH_NUMA)
        set(NUMA_EXES
                tests/NUMA_Unit/NUMA_dummy_test.cpp)
    endif ()
else ()
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wincompatible-pointer-types -Wint-conversion")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wimplicit-function-declaration -Wdiscarded-qualifiers")
endif ()

set(XH_LIBS
        src/xhash/global.cpp
        src/xhash/helper.cpp
        #src/xhash/table.cpp
        src/xhash/mem_alloc.cpp
        src/xhash/index_hash.cpp)

set(XT_LIBS
        src/xhash/global.cpp
        src/xhash/helper.cpp
        #src/xhash/table.cpp
        src/xhash/mem_alloc.cpp
        src/xhash/index_btree.cpp)

set(LH_LIBS
        src/levelhash/hash.c
        src/levelhash/level_hashing.c
        )

set(FH_LIBS
        src/faster/misc/address.cc
        src/faster/misc/lss_allocator.cc
        src/faster/misc/thread.cc
        )
if (CMAKE_SYSTEM_NAME MATCHES "Linux")
    set(FH_LIBS
            src/faster/io/file_linux.cc
            ${FH_LIBS})
else ()
    set(FH_LIBS
            src/faster/io/file_windows.cc
            ${FH_LIBS})
endif ()

set(MH_LIBS
        src/mhash/epoch/faster/lss_allocator.cc
        src/mhash/epoch/faster/thread.cc)

option(WITH_XH_TEST "build with xhtests" ON)
if (WITH_XH_TEST)
    set(XH_TEST
            utils/XH_UnitTest.cpp)
endif ()

option(WITH_XT_TEST "build with xttests" ON)
if (WITH_XT_TEST)
    set(XT_TEST
            utils/XT_UnitTest.cpp)
endif ()

option(WITH_LH_TESTS "build with lhests" ON)
if (WITH_LH_TESTS)
    set(LH_TEST
            tests/LH_MicroTest.cpp)
endif ()

option(WITH_FH_TESTS "build with fast hash tests" ON)
if (WITH_FH_TESTS)
    set(FH_TEST
            tests/FH_Unit/faster_function_test.cpp
            tests/FH_Unit/in_memory_test.cpp
            tests/FH_Unit/faster_struct_test.cpp
            tests/FH_InsertDeleteUpsertTest.cpp
            tests/FH_RoundInsertDeleteTest.cpp
            tests/FH_MicroDeleteTest.cpp
            tests/FH_MicroGrowingTest.cpp
            tests/FH_MicroCTest.cpp
            tests/FH_MicroTest.cpp
            tests/FH_YCSB.cpp
            tests/FH_StableRUTest.cpp
            utils/FH_UnitTest.cpp
            utils/EP_UnitTest.cpp)
endif ()

option(WITH_MH_TESTS "build with multi-level hash tests " ON)
if (WITH_MH_TESTS)
    set(MH_TEST
            tests/MH_Unit/MH_dummy_test.cpp
            tests/MH_Unit/MH_MaskRWPtr_test.cpp
            tests/MH_Efficiency/AtomicMaskByteTest.cpp
            tests/MH_Efficiency/AlignasEffectsOnAtom.cpp
            tests/MH_Efficiency/HazardDummyTest.cpp
            tests/MH_Efficiency/EpochUsageTest.cpp
            tests/MH_Efficiency/EpochBzUsageTest.cpp
            tests/MH_Efficiency/EpochTBUsageTest.cpp
            tests/MH_Efficiency/EpochBenchmark.cpp
            utils/MH_UnitTest.cpp
            tests/MH_MicroTest.cpp
            tests/MH_YCSB.cpp)
endif ()

option(WITH_UTILS "build with utils" ON)
if (WITH_UTILS)
    if (HASHFUNC)
        if (${HASHFUNC} EQUAL 0)
            add_definitions(-DHASHFUNC=0)
        elseif (${HASHFUNC} EQUAL 1)
            add_definitions(-DHASHFUNC=1)
        elseif (${HASHFUNC} EQUAL 2)
            add_definitions(-DHASHFUNC=2)
        elseif (${HASHFUNC} EQUAL 3)
            add_definitions(-DHASHFUNC=3)
        elseif (${HASHFUNC} EQUAL 4)
            add_definitions(-DHASHFUNC=4)
        else ()
            add_definitions(-DHASHFUNC=5)
        endif ()
    endif ()
    set(HASHLIBS
            src/mhash/util/City.cpp
            src/mhash/util/MurmurHash1.cpp
            src/mhash/util/MurmurHash2.cpp
            src/mhash/util/MurmurHash3.cpp)
    set(UTILS
            utils/DistributionTool.cpp
            utils/MicroDistTool.cpp
            utils/UH_UnitTest.cpp
            utils/UH_HashTest.cpp)
endif ()

option(WITH_STRUCT "build with structs" ON)
if (WITH_STRUCT)
    set(STRUCT_TEST
            utils/DummyTest.cpp
            utils/Struct_UnionTest.cpp
            utils/PTR_UnitTest.cpp
            utils/Load_UnitTest.cpp
            utils/CacheSteppingTest.cpp
            utils/CacheSteppingAtomicTest.cpp)
endif ()

find_package(Threads)

set(STRUCT_TESTS_EXES ${STRUCT_TEST})
foreach (sourcefile ${STRUCT_TESTS_EXES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile})
    set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
            PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
            OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
            )
    target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${PTHREAD})
endforeach (sourcefile ${STRUCT_TESTS_EXES})

set(XH_TESTS_EXES ${XH_TEST})
foreach (sourcefile ${XH_TESTS_EXES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile} ${XH_LIBS})
    set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
            PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
            OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
            )
    target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${PTHREAD})
endforeach (sourcefile ${XH_TESTS_EXES})

set(XT_TESTS_EXES ${XT_TEST})
foreach (sourcefile ${XT_TESTS_EXES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile} ${XT_LIBS})
    set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
            PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
            OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
            )
    target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${PTHREAD})
endforeach (sourcefile ${XT_TESTS_EXES})

set(LH_TESTS_EXES ${LH_TEST})
foreach (sourcefile ${LH_TESTS_EXES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile} ${LH_LIBS})
    set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
            PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
            OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
            )
    target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${PTHREAD})
endforeach (sourcefile ${LH_TESTS_EXES})

set(FH_TESTS_EXES ${FH_TEST})
foreach (sourcefile ${FH_TESTS_EXES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile} ${FH_LIBS})
    set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
            PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
            OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
            )
    target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} gtest ${PTHREAD} ${LTBB} ${AIO} ${UUID})
endforeach (sourcefile ${FH_TESTS_EXES})

set(MF_TESTS_EXES ${MH_TEST})
foreach (sourcefile ${MF_TESTS_EXES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile} ${MH_LIBS} ${HASHLIBS})
    set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
            PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
            OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
            )
    target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} gtest ${PTHREAD} ${ATOMIC})
endforeach (sourcefile ${MF_TESTS_EXES})

set(UTILS_EXES ${UTILS})
foreach (sourcefile ${UTILS_EXES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile} ${HASHLIBS})
    set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
            PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
            OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
            )
    target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${PTHREAD})
endforeach (sourcefile ${UTILS_EXES})

include(ExternalProject)
if ((CMAKE_SYSTEM_NAME MATCHES "Linux" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 5.1) OR CMAKE_SYSTEM_NAME MATCHES "Darwin")
    if (WITH_JUNCTION)
        option(WITH_JUNCTION "build with junction" ON)
    endif ()
    if (WITH_CUCKOO)
        option(WITH_LIBCUCKOO "build with libcukoo" ON)
    endif ()
    if (WITH_FOLLY)
        option(WITH_FOLLY "build with folly" ON)
    endif ()
    option(WITH_ZT_TEST "build with xttests" ON)
endif ()

if (WITH_NUMA)
    set(NUMA_TESTS_EXES ${NUMA_EXES})
    foreach (sourcefile ${NUMA_TESTS_EXES})
        get_filename_component(exename ${sourcefile} NAME_WE)
        add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile})
        set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
                PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
                EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
                EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
                OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
                )
        target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} gtest ${PTHREAD} ${NUMA})
    endforeach (sourcefile ${NUMA_TESTS_EXES})
endif ()

if (WITH_ZT_TEST)
    include_directories("src/bztree")
    set(PMEM_BACKEND "PMDK" CACHE STRING "Persistent memory backend type")
    string(TOUPPER ${PMEM_BACKEND} PMEM_BACKEND)
    set(GOOGLE_FRAMEWORK "OFF")

    if (${PMEM_BACKEND} STREQUAL "PMDK")
        add_definitions(-DPMEM)
        add_definitions(-DPMDK)
        message(STATUS "Persistence support: PMDK")
    elseif (${PMEM_BACKEND} STREQUAL "EMU")
        add_definitions(-DPMEM)
        add_definitions(-DPMEMEMU)
        message(STATUS "Persistence support: emulation")
    elseif (${PMEM_BACKEND} STREQUAL "VOLATILE")
        message(STATUS "Persistence support: off")
    else ()
        message(FATAL_ERROR "Unsupported persistent memory backend: ${PMEM_BACKEND}")
    endif ()

    ##################### PMwCAS #########################
    add_definitions(-DDESC_CAP=16)
    set(PMWCAS_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/pmwcas")
    message(STATUS "BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
    message(STATUS "PMWCAS_PREFIX: ${PMWCAS_PREFIX}")
    ExternalProject_Add(PMWCAS
            PREFIX "${PMWCAS_PREFIX}"
            GIT_REPOSITORY https://github.com/HaoPatrick/pmwcas.git
            GIT_TAG master
            UPDATE_COMMAND ""
            INSTALL_COMMAND ""
            BUILD_COMMAND $(MAKE)
            CMAKE_ARGS -DPMEM_BACKEND=${PMEM_BACKEND} -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DDESC_CAP=16 -DBUILD_APPS=${BUILD_TESTS} -DGOOGLE_FRAMEWORK=${GOOGLE_FRAMEWORK}
            LOG_DOWNLOAD ON
            LOG_CONFIGURE ON
            LOG_BUILD ON
            )

    # PMwCAS and PMDK paths
    include_directories(
            ${PMWCAS_PREFIX}/src/PMWCAS
            ${PMWCAS_PREFIX}/src/PMWCAS/src/
            ${PMWCAS_PREFIX}/src/PMWCAS/include
            ${PMWCAS_PREFIX}/src/PMWCAS-build/pmdk/src/PMDK/src/include
    )

    link_directories(${PMWCAS_PREFIX}/src/PMWCAS-build/)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")
    if (CMAKE_SYSTEM_NAME MATCHES "Linux")
        set(LINK_FLAGS "-lnuma -lpthread -pthread -lrt")
        if (${PMEM_BACKEND} STREQUAL "PMDK")
            if (CMAKE_SYSTEM_NAME MATCHES "Linux")
                set(LINK_FLAGS "${LINK_FLAGS} -lpmemobj")
            endif ()
            if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
                link_directories(${PMWCAS_PREFIX}/src/PMWCAS-build/pmdk/src/PMDK/src/debug)
            else ()
                link_directories(${PMWCAS_PREFIX}/src/PMWCAS-build/pmdk/src/PMDK/src/nondebug)
            endif ()
        endif ()
    else ()
        set(LINK_FLAGS "-lpthread -pthread")
    endif ()
    link_libraries(${LINK_FLAGS})

    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")

    if (${CMAKE_BUILD_TYPE} STREQUAL "Profile")
        add_compile_definitions("PROFILE")
        link_libraries("-lprofiler")
    elseif (${CMAKE_BUILD_TYPE} STREQUAL "Release")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Ofast -march=native")
    endif ()

    set(ZT_LIBS
            src/bztree/bztree.cpp)

    set(ZT_TEST
            utils/ZT_UnitTest.cpp)

    set(PMWCAS_LIBS "-lpmwcas -lrt -lnuma")

    set(ZT_TESTS_EXES ${ZT_TEST})
    set(MAX_FREEZE_RETRY 1 CACHE STRING "MAX retry on frozen node")
    set(ENABLE_MERGE 0 CACHE STRING "MAX retry on frozen node")
    message(STATUS "ENABLE_MERGE: " ${ENABLE_MERGE})
    foreach (sourcefile ${ZT_TESTS_EXES})
        get_filename_component(exename ${sourcefile} NAME_WE)
        add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile} ${ZT_LIBS})
        target_compile_definitions(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} PRIVATE ENABLE_MERGE=${ENABLE_MERGE})
        target_compile_definitions(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} PRIVATE MAX_FREEZE_RETRY=${MAX_FREEZE_RETRY})
        add_dependencies(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} PMWCAS)
        set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
                PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
                EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
                EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
                OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
                )
        target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${DL} ${PTHREAD} ${PMWCAS_LIBS})
    endforeach (sourcefile ${ZT_TESTS_EXES})
endif ()

if (WITH_JUNCTION)
    set(JUNCTION_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/junction")
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
            )

    include_directories(
            ${JUNCTION_PREFIX}/src/junction-deps
            ${JUNCTION_PREFIX}/src/turf
            ${JUNCTION_PREFIX}/src/junction-deps-build/include
            ${JUNCTION_PREFIX}/src/junction-deps-build/turf/include
    )

    link_directories(${JUNCTION_PREFIX}/src/junction-deps-build/)
    link_directories(${JUNCTION_PREFIX}/src/junction-deps-build/turf)

    set(JUNCTION_TEST
            tests/CF2_Unit/Junction_Usage.cpp
            tests/JH_MicroTest.cpp)

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
    set(LIBCUCKOO_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/libcuckoo")
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
            )

    include_directories(
            ${LIBCUCKOO_PREFIX}/src/libcuckoo
    )

    link_directories(${LIBCUCKOO_PREFIX}/src/libcukoo-build/)

    set(CUKOO_TEST
            tests/CF2_Unit/CUCKOO_Usage.cpp
            tests/CH_MicroTest.cpp)

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
        target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} gtest ${PTHREAD} ${ATOMIC})
    endforeach (sourcefile ${CUKOO_TEST})
endif ()

option(WITH_AWLMAP "processing awlmap" ON)
if (WITH_AWLMAP)
    set(AWLMAP_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/awlmap")
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
            )

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
            tests/AWL_Unit/awlmap_Usage.cpp
            tests/AH_MicroTest.cpp
            tests/AA_MicroTest.cpp)

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
        target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} gtest ${PTHREAD} ${ATOMIC})
        if (${DISABLE_FAST_TABLE})
            target_compile_definitions(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} PUBLIC DISABLE_FAST_TABLE)
        endif ()
    endforeach (sourcefile ${AWLMAP_TEST})
endif ()

if (WITH_FOLLY)
    set(FOLLY_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/folly")
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
            )

    include_directories(
            ${FOLLY_PREFIX}/src/folly
            ${FOLLY_PREFIX}/src/folly-build
    )

    link_directories(${FOLLY_PREFIX}/src/folly-build/)

    set(FOLLY_TEST
            tests/CF2_Unit/Folly_Usage.cpp
            tests/OH_MicroTest.cpp)

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
        target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} -lfolly -lgflags -lglog -ldl gtest -ldouble-conversion ${PTHREAD} ${ATOMIC})
    endforeach (sourcefile ${FOLLY_TEST})
endif ()

option(WITH_QUARTZ "processing quartz" ON)
if (WITH_QUARTZ)
    set(QUARTZ_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/quartz")
    message(STATUS "BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
    message(STATUS "QUARTZ_PREFIX: ${QUARTZ_PREFIX}")
    ExternalProject_Add(quartz
            PREFIX "${QUARTZ_PREFIX}"
            GIT_REPOSITORY https://github.com/whupro2017/quartz
            GIT_TAG master
            UPDATE_COMMAND ""
            INSTALL_COMMAND ""
            BUILD_COMMAND $(MAKE) -j8
            CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} #-DCMAKE_C_COMPILER=gcc -DPROGRAM_LANGUAGE=C
            LOG_DOWNLOAD ON
            LOG_CONFIGURE ON
            LOG_BUILD ON
            )

    link_directories(${QUARTZ_PREFIX}/src/quartz-build/src/lib)

    add_custom_command(
            TARGET quartz POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy
            ${QUARTZ_PREFIX}/src/quartz/nvmemul.ini
            ${CMAKE_CURRENT_BINARY_DIR}/nvmemul.ini)

    set(QUARTZ_TEST
            tests/PM_Unit/Quartz_Usage.cpp)

    foreach (sourcefile ${QUARTZ_TEST})
        get_filename_component(exename ${sourcefile} NAME_WE)
        add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile})
        set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
                PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
                EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
                EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
                OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
                )
        target_include_directories(
                ${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} PUBLIC
                ${QUARTZ_PREFIX}/src/quartz/src/lib
                ${QUARTZ_PREFIX}/src/quartz-build
        )
        add_dependencies(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} quartz)
        target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} nvmemul pthread)
    endforeach (sourcefile ${QUARTZ_TEST})
endif ()
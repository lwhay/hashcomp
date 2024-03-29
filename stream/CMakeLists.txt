cmake_minimum_required(VERSION 3.6)
#project(stream)
#set(CMAKE_C_COMPILER "gcc")
#set(CMAKE_CXX_COMPILER "g++")
#set(CMAKE_C_STANDARD 11)
#if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 5.1)
#    set(CMAKE_CXX_STANDARD 14)
#else ()
#    set(CMAKE_CXX_STANDARD 11)
#endif ()
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DPRINT_TRACE=1")
message(${CMAKE_CURRENT_BINARY_DIR})

include_directories(src/cfrequent)
include_directories(src/heavyhitter)
include_directories(src/quantile)
include_directories(src/replacealgo)
include_directories(src/concisereplacement)
include_directories(src/utils)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../../src/bench)

if (CMAKE_SYSTEM_NAME MATCHES "Darwin")
    enable_testing()
    find_package(gtest REQUIRED)
    include_directories(${GTEST_INCLUDE_DIRS})
    link_directories(/usr/local/lib)
else ()
    #[[include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/../../third-party/gtest-1.7.0/fused-src)
    include_directories(SYSTEM ${PROJECT_SOURCE_DIR}../../third-party/gtest-1.7.0/fused-src/gtest)
    add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/../../third-party/gtest-1.7.0/fused-src/gtest build)]]
endif ()

if ((CMAKE_SYSTEM_NAME MATCHES "Linux") OR (CMAKE_SYSTEM_NAME MATCHES "Darwin"))
    set(PTHREAD -lpthread)
    if (CMAKE_SYSTEM_NAME MATCHES "Linux")
        set(AIO /usr/lib64/libaio.so -lrt)
    endif ()
    find_package(Boost 1.50.0 REQUIRED COMPONENTS program_options date_time filesystem iostreams)
    include_directories(${Boost_INCLUDE_DIR})
    #if ((${Boost_VERSION} VERSION_GREATER 1.70.0) AND (CMAKE_SYSTEM_NAME MATCHES "Linux"))
    set(BOOST_PSEUDO "-lboost_program_options -lboost_date_time -lboost_iostreams")
    #endif ()
else ()
    message("Windows cygwin")
    set(CMAKE_C_COMPILER "gcc")
    set(CMAKE_CXX_COMPILER "g++")
    set(BOOST_ROOT D:/cygwin64)
    set(Boost_INCLUDE_DIR ${BOOST_ROOT}/usr/include)
    set(Boost_LIBRARY_DIR ${BOOST_ROOT}/lib)
    find_package(Boost 1.66.0 REQUIRED COMPONENTS program_options date_time filesystem iostreams)
    include_directories(${Boost_INCLUDE_DIR})
    link_directories(${Boost_LIBRARY_DIR})
endif ()

set(CFREQUENT_LIBS
        src/cfrequent/prng.cc
        src/cfrequent/rand48.cc
        ../src/mhash/util/City.cpp)

set(STREAM_EFFICIENCYTEST
        tests/GK_EfficiencyTest.cpp
        tests/ZipfianVerification.cpp
        tests/QuantilePointGenerator.cpp
        tests/ClickStreamStatistics.cpp
        tests/MCWorkloadStatistics.cpp
        tests/ClickStreamGenerator.cpp
        tests/SerialHeaveyHitterTest.cpp
        tests/cfrequent/QuantileVerification.cpp
        tests/heavyhitter/WOGeneralSSEfficiency.cpp
        tests/heavyhitter/ClickStreamHashEfficiency.cpp
        tests/heavyhitter/ParallelGLSSEfficiency.cpp
        tests/heavyhitter/GeneralLazySS_MappingTest.cpp
        tests/replacealgo/ReplacementAlgorithmTest.cpp
        tests/replacealgo/PriorityQueueTest.cpp
        tests/replacealgo/MergeGeneralAltorithmTest.cpp
        tests/replacealgo/ARCAlgorithmTest.cpp
        tests/replacealgo/RemovingHotspotTest.cpp
        tests/replacealgo/HotspotDiffHitTest.cpp
        tests/replacealgo/ConciseTest.cpp
        tests/replacealgo/MaxDeltaTest.cpp
        tests/replacealgo/CodingNumericTest.cpp
        tests/replacealgo/RemovingPrimitiveTest.cpp
        tests/replacealgo/InterArrivalTest.cpp)

foreach (sourcefile ${STREAM_EFFICIENCYTEST})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile} ${CFREQUENT_LIBS})
    set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
            PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
            OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
            )
    target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${BOOST_PSEUDO} ${Boost_LIBRARIES} ${PTHREAD})
endforeach (sourcefile ${STREAM_EFFICIENCYTEST})

set(STREAM_UNITTEST
        tests/heavyhitter/Frequency_UnitTest.cpp
        tests/quantile/GK_UnitTest.cpp
        tests/heavyhitter/OPTHH_UnitTest.cpp
        tests/heavyhitter/GeneralLazySS_UnitTest.cpp)

foreach (sourcefile ${STREAM_UNITTEST})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile} ${CFREQUENT_LIBS})
    set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
            PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
            OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
            )
    target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} gtest ${PTHREAD})
endforeach (sourcefile ${STREAM_TEST})

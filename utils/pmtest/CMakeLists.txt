include_directories("./basis")

set(PM_LIBS
        # basis/alias.c
        basis/nvram.c)

set(PM_TEST
        basis/pmalloc.cpp
        basis/pnvram.c
        basis/test.c)


find_package(LIBPMEMOBJ++ REQUIRED)
link_directories(${LIBPMEMOBJ++_LIBRARY_DIRS})
message(${LIBPMEMOBJ++_LIBRARIES})


set(PM_TEST_EXES ${PM_TEST})
foreach (sourcefile ${PM_TEST_EXES})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile} ${PM_LIBS})
    set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
            PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
            EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
            OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
            )
    target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${LIBPMEMOBJ++_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT}) # gtest ${PTHREAD} ${AIO} ${UUID} ${NUMA})
endforeach (sourcefile ${PM_TEST_EXES})
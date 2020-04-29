add_executable(numatest ${SUM_STORE_HEADERS} numa-test.c)
target_link_libraries(numatest -lpthread -lnuma -fopenmp)
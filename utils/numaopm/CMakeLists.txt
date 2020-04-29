add_executable(numatest numa-test.c)
target_link_libraries(numatest -lpthread -lnuma -fopenmp)
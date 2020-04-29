add_executable(numatest numa-test.cpp)
target_link_libraries(numatest -lpthread -lnuma -fopenmp)
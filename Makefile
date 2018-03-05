CC = g++
# main.cpp
MAIN     = main.cpp #city/city-test.cc

# include resources path
INC_PATH = include

# g++ flags
CC_FLAG  = -std=c++11 -O3 -lpthread -m64 -mrtm

# test resources
TEST_SRC = ./test/test_hashtable.cc ./test/test_hikv.cc ./test/test_bptree.cc ./test/data_set.cc

# google city hash resources
CITY_SRC = ./city/city.cc

#
CC_SRC = ./src/halgorithm.cc ./src/hhash.cc ./src/hikv.cc ./src/hthread.cc

# libraries
TBB_LIB  = ./third-party/tbb -ltbb
JEMALLOC_LIB = ./third-party/jemalloc -ljemalloc
# JEMALLOC_LIB = ./third-party/jemalloc-4.2.1/lib -ljemalloc
# output file
OUTPUT   = run

# -L $(TBB_LIB) -L $(JEMALLOC_LIB) 
all:
	$(CC) $(CC_FLAG) $(MAIN) $(CC_SRC) $(TEST_SRC) $(CITY_SRC) -L $(TBB_LIB) -L $(JEMALLOC_LIB)  -I $(INC_PATH) -o $(OUTPUT)

git:
	git add include/*.h include/bptree/*.h 
	git add *.cc src/*.cc test/*.cc city/*.cc benchmarks/*.cc
	git add Makefile

path:
	export LD_LIBRARY_PATH=./third-party/tbb:./third-party/jemalloc/
	export CPATH=./include/tbb
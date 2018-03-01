#include "htest.h"
#include "htype.h"
#include "hikv.h"
#include "hhash.h"
#include <iostream>

using namespace hikv;
using namespace std;

// 1 test type
// |- 0 test hikv read from file
// |- 1 test hikv random
// |- 2 test hashtable read from file
// |- 3 test hashtable random
// |- 4 test bplus tree read from file
// |- 5 test bplus tree random
int opt_type;

// 2 input file or kv count
char *data_in;
int num_kv;

// 3~4
int num_server_threads;
int num_backend_threads;

// 5~6
int max_key_length;
int max_value_length;

// ./run 0 benchmarks/data_100M.in 1 1 16 128
// ./run 1 5000000 8 1 16 128

bool parse_parameter(int argc, char *argv[])
{
    if(argc < 2) {
        return false;
    }

    opt_type = atol(argv[1]);

    if(opt_type & 1) {      
        // random read (1, 3, 5)
        data_in = NULL;
        num_kv = atol(argv[2]);
    } else {   
        // read from file (0, 2, 4)
        data_in = argv[2];
        num_kv = 0;
    }

    // test hikv
    if(opt_type == 0 || opt_type == 1) 
    {
        num_server_threads = atol(argv[3]);
        num_backend_threads = atol(argv[4]);

        max_key_length = atol(argv[5]);
        max_value_length = atol(argv[6]);

    } else if (opt_type == 2 || opt_type == 3) 
    {
        // test hashtable
        num_server_threads = atol(argv[3]);
        
        max_key_length = atol(argv[4]);
        max_value_length = atol(argv[5]);

    } else if (opt_type == 4 || opt_type == 5) 
    {
        // test bptree
        num_server_threads = atol(argv[3]);
        max_key_length = atol(argv[4]);
        max_value_length = atol(argv[5]);
    }
    return true;
}

void hash_table_test() 
{
    TestHashTableFactory *factory = new TestHashTableFactory(data_in, num_kv, num_server_threads, \
            max_key_length, max_value_length);
    factory->multiple_thread_test();
}

void hikv_test() 
{
    TestHiKVFactory *factory = new TestHiKVFactory(data_in, num_kv, max_key_length, max_value_length, \
                    num_server_threads, num_backend_threads);
    factory->multiple_thread_test();
}

void bplus_tree_test() 
{
    TestBpTreeFactory *factory = new TestBpTreeFactory(data_in, num_kv, max_key_length, \
                    max_value_length, num_server_threads);
    factory->multiple_thread_test();
}

int main(int argc, char *argv[]) 
{
    if(parse_parameter(argc, argv)) 
    {
        if(opt_type == 0 || opt_type == 1) 
        {
            hikv_test();
        } else if(opt_type == 2 || opt_type == 3) 
        {
            hash_table_test();
        } else if(opt_type == 4 || opt_type == 5) 
        {
            bplus_tree_test();
        }
        return 0;
    } else {
        return 1;
    }
    return 0;
}

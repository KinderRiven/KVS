#include "htest.h"
#include "hhash.h"
#include "htype.h"
#include "hstatus.h"
#include <fstream>
#include <iostream>
#include <cstdio>
#include <unistd.h>
#include <sys/time.h>

using namespace std;
using namespace hikv;

// Function to use single thread test 
void TestHashTableFactory::single_thread_test() {
    
    int opt, opt_count = 0;
    double time_count = 0;
    struct timeval begin_time, end_time;
    pid_t pid;
    char key[1024], value[1024];
    Status status;
    
    HashTable *ht = new HashTable(num_partitions, num_buckets); 
    ifstream in(data_in);
    
    // Show pid message.
    pid = getpid();
    cout << "running data in : " << data_in << endl;
    cout << "pid : " << pid << endl;
    
    while(in >> opt) {
        in >> key >> value;
        Slice s_key = Slice(key), s_value = Slice(value);
        opt_count++;
        gettimeofday(&begin_time, NULL);
        switch(opt) {
            case HASH_PUT:
                status = ht->Put(s_key, s_value);
                break;
            case HASH_GET:
                ht->Get(s_key, s_value);
                break;
            case HASH_DEL:
                ht->Delete(s_key);
                break;
            default:
                cout << "Illegal Parameters!" << endl;
                break;
        }
        gettimeofday(&end_time, NULL);
        assert(status.ok() == 1);
        double exe_time = (double)(1000000.0 * ( end_time.tv_sec - begin_time.tv_sec ) + \
                    end_time.tv_usec - begin_time.tv_usec) / 1000000.0;
        time_count += exe_time;
    }
    // Show result.
    printf("opt count : %d, time_count : %.5f s, iops : %.5f\n", \
            opt_count, time_count, (double) opt_count / time_count);
    delete(ht);
    in.close();
}
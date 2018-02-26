#include "htest.h"
#include <vector>
#include <string>

using namespace std;
using namespace hikv;

vector<int> vec_opt[MAX_TEST_THREAD];
vector<string> vec_key[MAX_TEST_THREAD];
vector<string> vec_value[MAX_TEST_THREAD];

static void* thread_run(void *args_) 
{    
    struct test_thread_args *args = (struct test_thread_args *)args_;
    int tid = args->thread_id;
    int opt_count = vec_opt[tid].size();
    Status status;
    HiKV *hikv = args->hikv;
    struct timeval begin_time, end_time;    // count time
    double time_count = 0;
    
    for(int i = 0; i < opt_count; i++) {
        Slice s_key = Slice(vec_key[tid][i]);
        Slice s_value = Slice(vec_value[tid][i]);
        gettimeofday(&begin_time, NULL);
        switch(vec_opt[tid][i]) {
            case TEST_PUT:
                status = hikv->Put(s_key, s_value);
                break;
            default:
                cout << "Illegal Parameters!" << endl;
                break;
        }
        assert(status.is_ok() == 1);
        gettimeofday(&end_time, NULL);
        double exe_time = (double)(1000000.0 * ( end_time.tv_sec - begin_time.tv_sec ) + \
                    end_time.tv_usec - begin_time.tv_usec) / 1000000.0;
        time_count += exe_time;
    }
    printf("[Thread %d] opt count : %d, time_count : %.5f s, iops : %.5f\n", \
            tid, opt_count, time_count, (double) opt_count / time_count);
    pthread_exit(0);
}

// Function to read data into vector.
static int read_data(const char *data_in, int num_threads) 
{
    int opt, count = 0;
    int all = 0;
    string key, value;
    ifstream in(data_in);
    while(in >> opt) 
    {
        in >> key >> value;
        vec_opt[count].push_back(opt);
        vec_key[count].push_back(key);
        vec_value[count].push_back(value);
        count = (count + 1) % num_threads;
        all++;
    }
    in.close();
    return all;
}

// Function to struct.
TestHiKVFactory::TestHiKVFactory(const char *data_in, \
                    int num_ht_partitions, \
                    int num_ht_buckets, \
                    uint32_t num_server_threads, \
                    uint32_t num_backend_threads, \
                    uint32_t num_backend_queues)
{
    this->data_in = data_in;
    this->num_ht_partitions = num_ht_partitions;
    this->num_ht_buckets = num_ht_buckets;
    this->num_server_threads = num_server_threads;
    this->num_backend_queues = num_backend_queues;
    this->num_backend_threads = num_backend_threads;
}

// Function to use single thread test
void TestHiKVFactory::multiple_thread_test() 
{
    double time_count = 0;
    int opt_count = 0;
    struct timeval begin_time, end_time;    // count time
    hikv = new HiKV(128, 1024, this->num_ht_partitions, this->num_ht_buckets, \
                this->num_backend_threads, this->num_backend_queues);
    // Read data
    opt_count = read_data(data_in, num_server_threads);
    // Create Put thread
    gettimeofday(&begin_time, NULL);
    for(int i = 0; i < num_server_threads; i++) 
    {
        struct test_thread_args *args = new test_thread_args();
        args->thread_id = i;
        args->hikv = hikv;
        pthread_create(thread_id + i, NULL, thread_run, args);
    }
    // Join thread.
    for(int i = 0; i < num_server_threads; i++) 
    {
        pthread_join(thread_id[i], NULL);
    }
    gettimeofday(&end_time, NULL);
    time_count = (double)(1000000.0 * ( end_time.tv_sec - begin_time.tv_sec ) + \
                    end_time.tv_usec - begin_time.tv_usec) / 1000000.0;
    printf("[Result] opt count : %d, time_count : %.5f s, iops : %.5f\n", \
            opt_count, time_count, (double) opt_count / time_count);
    // Confire
    for(int i = 0; i < num_server_threads; i++) 
    {
        for(int j = 0; j < vec_key[i].size(); j++) 
        {
            Slice key = Slice(vec_key[i][j]);
            Slice value;
            Status status = hikv->Get(key, value);
            assert(status.is_ok() == true);
        }
    }
}

void TestHiKVFactory::single_thread_test() 
{    
    int opt, opt_count = 0;
    double time_count = 0;
    struct timeval begin_time, end_time;    // count time
    pid_t pid;
    string key, value;
    Status status;
    vector<string> vec_key, vec_value;
    
    hikv = new HiKV(128, 1024, this->num_ht_partitions, this->num_ht_buckets, \
                this->num_backend_threads, this->num_backend_queues);
    ifstream in(data_in);
    
    // Show Message.
    pid = getpid();
    printf("[HashTable test is running]\n");
    printf("[Input filename] : %s\n[PID] : %d\n", data_in, pid);
    
    while(in >> opt) 
    {
        in >> key >> value;
        vec_key.push_back(key);
        vec_value.push_back(value);
        Slice s_key = Slice(key), s_value = Slice(value);
        opt_count++;
        gettimeofday(&begin_time, NULL);
        switch(opt) 
        {
            case TEST_PUT:
                status = hikv->Put(s_key, s_value);
                break;
            default:
                cout << "Illegal Parameters!" << endl;
                break;
        }
        gettimeofday(&end_time, NULL);
        assert(status.is_ok() == 1);
        //Execute time (second).
        double exe_time = (double)(1000000.0 * ( end_time.tv_sec - begin_time.tv_sec ) + \
                    end_time.tv_usec - begin_time.tv_usec) / 1000000.0;
        time_count += exe_time;
    }
    // Show result.
    printf("[Result] opt count : %d, time_count : %.5f s, iops : %.5f\n", \
            opt_count, time_count, (double) opt_count / time_count);
    // Verification
    for(int i = 0; i < vec_key.size(); i++) 
    {
        Slice key = Slice(vec_key[i]);
        Slice value;
        Status status = hikv->Get(key, value);
        assert(status.is_ok() == true);
    }
    in.close();
}
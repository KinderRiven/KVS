#include "htest.h"
#include <vector>
using namespace hikv;
using namespace std;

double bp_vec_iops[MAX_TEST_THREAD];
vector<int> bp_vec_opt[MAX_TEST_THREAD];
vector<string>  bp_vec_key[MAX_TEST_THREAD];
vector<uint64_t>  bp_vec_value[MAX_TEST_THREAD];

TestBpTreeFactory::TestBpTreeFactory(const char *data_in, int num_kvs, \
                    int max_key_length, int max_value_length, \
                    int num_threads)
{
    this->data_in = NULL;
    this->num_kvs = num_kvs;
    this->max_key_length = max_key_length;
    this->max_value_length = max_value_length;
    this->num_threads = num_threads;
}

// Function to read data into vector.
static int read_data(const char *data_in, int num_kvs, \
                int max_key_length, int max_value_length, int num_threads) 
{
    int opt, count = 0;
    int all = 0;
    string key;
    uint64_t value;
    if(data_in != NULL) 
    {
        ifstream in(data_in);
        while(in >> opt) 
        {
            in >> key >> value;
            bp_vec_opt[count].push_back(opt);
            bp_vec_key[count].push_back(key);
            bp_vec_value[count].push_back(value);
            count = (count + 1) % num_threads;
            all++;
        }
        in.close();
    } 
    else if(num_kvs > 0) 
    {
        static const char alphabet[] =
    			"abcdefghijklmnopqrstuvwxyz"
    			"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    			"0123456789";
        random_device rd;
        mt19937 gen(rd());
        uniform_int_distribution<> range1( max_key_length, max_key_length);
        uniform_int_distribution<> range2( max_value_length / 5, max_value_length );
        uniform_int_distribution<> range3( 0, sizeof(alphabet) / sizeof(*alphabet) - 2 );
        for(int i = 0; i < num_kvs; i++) 
        {
            int key_length = range1(gen);
            int value_length = range2(gen);
            string s_key = "";
            uint64_t u64_value = (uint64_t)i;
            
            for(int i = 0; i < key_length; i++) {
                s_key += alphabet[range3(gen)];
            }
            bp_vec_opt[count].push_back(1);
            bp_vec_key[count].push_back(s_key);
            bp_vec_value[count].push_back(u64_value);
            count = (count + 1) % num_threads;
            all++;
        }
    }
    return all;
}

static void* thread_run(void *args_) 
{    
    struct test_thread_args *args = (struct test_thread_args *)args_;
    int tid = args->thread_id;
    int opt_count = bp_vec_opt[tid].size();
    BpTree *bp = args->bp;
    struct timeval begin_time, end_time;    // count time
    double time_count = 0;
    
    for(int i = 0; i < opt_count; i++) {
        KEY key = KEY(bp_vec_key[tid][i]);
        DATA data = DATA(bp_vec_value[tid][i]);
        bool res;
        gettimeofday(&begin_time, NULL);
        switch(bp_vec_opt[tid][i]) {
            case TEST_PUT:
                res = bp->Put(key, data);
                break;
            default:
                cout << "Illegal Parameters!" << endl;
                break;
        }
        assert(res == true);
        gettimeofday(&end_time, NULL);
        double exe_time = (double)(1000000.0 * ( end_time.tv_sec - begin_time.tv_sec ) + \
                    end_time.tv_usec - begin_time.tv_usec) / 1000000.0;
        time_count += exe_time;
    }
    printf("[Thread %d] opt count : %d, time_count : %.5f s, iops : %.5f\n", \
            tid, opt_count, time_count, (double) opt_count / time_count);
    bp_vec_iops[tid] = (double) opt_count / time_count;
    pthread_exit(0);
}

void TestBpTreeFactory::multiple_thread_test()
{
    double time_count = 0;
    int opt_count = 0;
    struct timeval begin_time, end_time;    // count time
    bp = new BpTree();
    
    // Read data
    printf("[INFO] Waiting Data\n");
    opt_count = read_data(data_in, num_kvs, max_key_length, max_value_length, num_threads);
    
    // Create Put thread
    gettimeofday(&begin_time, NULL);
    for(int i = 0; i < num_threads; i++) 
    {
        struct test_thread_args *args = new test_thread_args();
        args->thread_id = i;
        args->bp = bp;
        pthread_create(thread_id + i, NULL, thread_run, args);
    }

    // Join thread.
    printf("[INFO] Waiting thread exit\n");
    for(int i = 0; i < num_threads; i++) 
    {
        pthread_join(thread_id[i], NULL);
    }
    gettimeofday(&end_time, NULL);
    time_count = (double)(1000000.0 * ( end_time.tv_sec - begin_time.tv_sec ) + \
                    end_time.tv_usec - begin_time.tv_usec) / 1000000.0;
    
    // show result
    double sum_iops = 0;
    for(int i = 0; i < num_threads; i++) {
        sum_iops += bp_vec_iops[i];
    }
    printf("[Result] opt count : %d, time_count : %.5f s, iops : %.5f\n", \
            opt_count, time_count, sum_iops);
    // confire
    Config config;
    for(int i = 0; i < num_threads; i++) 
    {
        int vec_size = bp_vec_key[i].size();
        for(int j = 0; j < vec_size; j++) 
        {

        }
    }
}
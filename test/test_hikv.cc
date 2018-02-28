#include "htest.h"
#include <vector>
#include <string>

using namespace std;
using namespace hikv;

// opt type
static vector<int> vec_opt[MAX_TEST_THREAD];
static vector<string> vec_key[MAX_TEST_THREAD];
static vector<string> vec_value[MAX_TEST_THREAD];
static double vec_iops[MAX_TEST_THREAD];
static vector<Status> vec_status[MAX_TEST_THREAD];

static void* thread_run(void *args_) 
{    
    struct test_thread_args *args = (struct test_thread_args *)args_;
    int tid = args->thread_id;
    int opt_count = vec_opt[tid].size();
    Status status;
    HiKV *hikv = args->hikv;
    struct timeval begin_time, end_time;    // count time
    double time_count = 0;
    
    for(int i = 0; i < opt_count; i++) 
    {
        Slice s_key = Slice(vec_key[tid][i]);
        Slice s_value = Slice(vec_value[tid][i]);
        Config config = Config(tid);
        gettimeofday(&begin_time, NULL);
        switch(vec_opt[tid][i]) 
        {
            case TEST_PUT:
                status = hikv->Put(s_key, s_value, config);
                break;
            default:
                cout << "Illegal Parameters!" << endl;
                break;
        }
        assert(status.is_ok() == 1);
        gettimeofday(&end_time, NULL);
// collect rdtsc time
#if ((defined COLLECT_RDTSC) || (defined COLLECT_TIME))
    vec_status[tid].push_back(status);
#endif
        double exe_time = (double)(1000000.0 * ( end_time.tv_sec - begin_time.tv_sec ) + \
                    end_time.tv_usec - begin_time.tv_usec) / 1000000.0;
        time_count += exe_time;
    }
    printf("[Thread %d] opt count : %d, time_count : %.5f s, iops : %.5f\n", \
            tid, opt_count, time_count, (double) opt_count / time_count);
    vec_iops[tid] = (double) opt_count / time_count;
    pthread_exit(0);
}

// Function to read data into vector.
static int read_data(const char *data_in, int num_kvs, \
                int max_key_length, int max_value_length, int num_threads) 
{
    int opt, count = 0;
    int all = 0;
    string key, value;
    if(data_in != NULL) 
    {
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
            string s_key = "", s_value = "";
            
            for(int i = 0; i < key_length; i++) {
                s_key += alphabet[range3(gen)];
            }
            for(int i = 0; i < value_length; i++) {
                s_value += alphabet[range3(gen)];
            }
            vec_opt[count].push_back(1);
            vec_key[count].push_back(s_key);
            vec_value[count].push_back(s_value);
            count = (count + 1) % num_threads;
            all++;
        }
    }
    return all;
}

// Function to struct.
TestHiKVFactory::TestHiKVFactory(const char *data_in, int num_kvs, \
                    int max_key_length, int max_value_length, \
                    uint32_t num_server_threads, \
                    uint32_t num_backend_threads)
{
    this->num_kvs = num_kvs;
    this->data_in = data_in;
    this->max_key_length = max_key_length;
    this->max_value_length = max_value_length;
    this->num_server_threads = num_server_threads;
    this->num_backend_threads = num_backend_threads;
    this->num_ht_partitions =  NUM_HT_PARTITIONS;
}

// Function to use single thread test
void TestHiKVFactory::multiple_thread_test() 
{
    double time_count = 0;
    int opt_count = 0;
    struct timeval begin_time, end_time;    // count time
    // Read data
    printf("[INFO] Waiting Data\n");
    opt_count = read_data(data_in, num_kvs, max_key_length, max_value_length, num_server_threads);
    
    // calculate bucket count
    this->num_ht_buckets = opt_count * 2 / (this->num_ht_partitions * NUM_ITEMS_PER_BUCKET);
    hikv = new HiKV(128, 1024, this->num_ht_partitions, this->num_ht_buckets, \
                this->num_server_threads, \
                this->num_backend_threads);
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
    printf("[INFO] Waiting thread exit\n");
    for(int i = 0; i < num_server_threads; i++) 
    {
        pthread_join(thread_id[i], NULL);
    }
    gettimeofday(&end_time, NULL);
    time_count = (double)(1000000.0 * ( end_time.tv_sec - begin_time.tv_sec ) + \
                    end_time.tv_usec - begin_time.tv_usec) / 1000000.0;
    
    // show result
    double sum_iops = 0;
    for(int i = 0; i < num_server_threads; i++) {
        sum_iops += vec_iops[i];
    }
    printf("[Result] opt count : %d, time_count : %.5f s, iops : %.5f\n", \
            opt_count, time_count, sum_iops);

    // confire
    printf("[INFO] HashTable Verification\n");
    Config config;
    for(int i = 0; i < num_server_threads; i++) 
    {
        int vec_size = vec_key[i].size();
        for(int j = 0; j < vec_size; j++) 
        {
            Slice key = Slice(vec_key[i][j]);
            Slice value;
            Status status = hikv->Get(key, value, config);
            assert(status.is_ok() == true);
        }
    }
#ifdef BACKEND_THREAD
    printf("[INFO] BplusTree Verification\n"); 
    BpTree *bp = hikv->get_bp();
    for(int i = 0; i < num_server_threads; i++) 
    {
        int vec_size = vec_key[i].size();
        for(int j = 0; j < vec_size; j++) 
        {
            KEY key = KEY(vec_key[i][j]);
            DATA data;
            bool res = bp->Get(key, data);
            assert(res == true);
        }
    }
    printf("[END]\n");
#endif

#if ((defined COLLECT_RDTSC) || (defined COLLECT_TIME))
    int r_st = 8;               // parameter we use
    int op_count[16] = {0};     // all operator count
    uint64_t op_sum[16] = {0};  // all operator cost
    int over_count[16] = {0};   // over operator count
    uint64_t over_sum[16] = {0}; 
    int queue_collision_times = 0;
    int lock_collision_times = 0;
    // [0] lock    [1] find exist [2] find empty [3] put item 
    // [4] persist [5] unlock     [6] return     [7] add worker
    double limit_t[16] = {0};
    uint64_t filter = 0xffffffff;
    for(int i = 0; i < num_server_threads; i++)
    {
        int vec_size = vec_status[i].size();
        for(int j = 0; j < vec_size; j++) 
        {
            Status st = vec_status[i][j];
            queue_collision_times += st.get_queue_collision();
            // if (st.get_queue_collision()) {
            //     st.print_rdt(0, 16, 0);
            // }
            lock_collision_times += st.get_lock_collision();
            // if (st.get_lock_collision()) {
            //     st.print_rdt(0, 16, 0);
            // }
            // st.print_rdt(0, 16, 0);
            for(int k = 0; k < r_st; k++) 
            {
                uint64_t rdt = st.get_rdt(k);
                if(rdt < filter) {
                    op_count[k]++;
                    op_sum[k] += rdt;
                }
            }
        }
    }
    // calculate limit[]
    for(int i = 0; i < r_st; i++) 
    {
        limit_t[i] = (double)op_sum[i] / op_count[i] * 5.0;
    }
    // Here to calculate average
    for(int i = 0; i < num_server_threads; i++)
    {
        int vec_size = vec_status[i].size();
        for(int j = 0; j < vec_size; j++)
        {
            Status st = vec_status[i][j];
            for(int k = 0; k < r_st; k++) 
            {
                uint64_t rdt = st.get_rdt(k);
                if((double)rdt >= limit_t[k]) {
                    over_count[k]++;
                    over_sum[k] += (uint64_t) rdt;
                }
            }
        }
    }
    // Here to print result
    printf("[queue collision count] : %d [lock collision count] : %d\n", \
        queue_collision_times, lock_collision_times);
    for(int i = 0; i < r_st; i++) 
    {
        printf("[%02d]", i);
        printf("[count : %5d cost : %10llu avg : %8.2f]", \
            op_count[i], op_sum[i], (double)op_sum[i] / op_count[i]);
        if(over_count[i] == 0) {
            over_count[i] = 1;
        }
        printf("[count : %5d cost : %10llu avg : %8.2f]\n", \
            over_count[i], over_sum[i], (double)over_sum[i] / over_count[i]);
    }
    for(int i = 0; i < r_st; i++)
    {
        printf("[%02d][%8.2f][count : %4.2lf%% (%-8d/%8d) cost : %4.2lf%%]\n", \
            i, limit_t[i], (double)over_count[i]/op_count[i] * 100.0, \
            over_count[i], op_count[i], (double)over_sum[i]/op_sum[i] * 100.0);
    }
#endif
}

void TestHiKVFactory::single_thread_test() 
{    
    
}
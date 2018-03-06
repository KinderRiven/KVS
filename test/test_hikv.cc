#include "htest.h"
#include <vector>
#include <string>

using namespace std;
using namespace hikv;

static void* thread_run(void *args_) 
{    
    struct test_thread_args *args = (struct test_thread_args *)args_;
    struct timeval begin_time, end_time;    // count time
    
    Status status;
    HiKV *hikv = (HiKV*)args->object;
    TestHiKVFactory *factory = (TestHiKVFactory*)args->factory;
    TestDataSet *data_set = factory->data_set;
    
    int tid = args->thread_id;
    int opt_count = data_set->vec_opt[tid].size();
    double time_count = 0;
    
    for(int i = 0; i < opt_count; i++)
    {
        Slice s_key = Slice(data_set->vec_key[tid][i]);
        Slice s_value = Slice(data_set->vec_value[tid][i]);
        Config config = Config(tid);
        gettimeofday(&begin_time, NULL);
        switch(data_set->vec_opt[tid][i]) 
        {
            case TEST_PUT:
                status = hikv->Put(s_key, s_value, config);
                break;
            case TEST_GET:
                status = hikv->Get(s_key, s_value, config);
                break;
            case TEST_DEL:
                status = hikv->Delete(s_key, config);
                break;
            default:
                cout << "[ERROR] Illegal Parameters!" << endl;
                break;
        }
        assert(status.is_ok() == 1);
        gettimeofday(&end_time, NULL);

// collect rdtsc time
#if ((defined COLLECT_RDTSC) || (defined COLLECT_TIME))
    factory->vec_status[tid].push_back(status);
#endif

        // add exe time to time sum.
        double exe_time = (double)(1000000.0 * ( end_time.tv_sec - begin_time.tv_sec ) + \
                    end_time.tv_usec - begin_time.tv_usec) / 1000000.0;
        time_count += exe_time;
    }
    printf("[Thread %2d] Opt Count : %8d, Time Count : %8.5f s, IOPS : %8.5f\n", \
            tid, opt_count, time_count, (double) opt_count / time_count);
    factory->arr_iops[tid] = (double) opt_count / time_count;
    pthread_exit(0);
}

// Function to struct.
TestHiKVFactory::TestHiKVFactory(HiKV *hikv, TestDataSet *data_set, \
                    int num_server_threads, int num_backend_threads, \
                    int key_length, int value_length)
{
    this->hikv = hikv;
    this->data_set = data_set;
    this->key_length = key_length;
    this->value_length = value_length;
    this->num_server_threads = num_server_threads;
    this->num_backend_threads = num_backend_threads;
    this->num_ht_partitions =  NUM_HT_PARTITIONS;
}

void TestHiKVFactory::thread_init()
{
    for(int i = 0; i < num_server_threads; i++) 
    {
        struct test_thread_args *args = new test_thread_args();
        args->thread_id = i;
        args->object = (void *)hikv;
        args->factory = (void *)this;
        pthread_create(thread_id + i, NULL, thread_run, args);
    }

    // Join thread.
    printf("[RUNNING] Waiting thread to exit...\n");
    for(int i = 0; i < num_server_threads; i++) 
    {
        pthread_join(thread_id[i], NULL);
    }
}

void TestHiKVFactory::verify_hashtable()
{
    printf("[RUNNING] Validating HashTable...\n");
    Config config;
    for(int i = 0; i < num_server_threads; i++) 
    {
        int vec_size = data_set->vec_key[i].size();
        for(int j = 0; j < vec_size; j++) 
        {
            Slice key = Slice(data_set->vec_key[i][j]);
            Slice value;
            Status status = hikv->Get(key, value, config);
            assert(value == data_set->vec_value[i][j]);
            assert(status.is_ok() == true);
        }
    }
    printf("[OK] HashTable status OK!\n");
}

void TestHiKVFactory::verify_bptree()
{
    printf("[RUNNING] Validating BpTree...\n"); 
    HBpTree *bp = hikv->get_bp();
    for(int i = 0; i < num_server_threads; i++) 
    {
        int vec_size = data_set->vec_key[i].size();
        for(int j = 0; j < vec_size; j++) 
        {
            KEY key = KEY(data_set->vec_key[i][j]);
            DATA data;
            
            bool res = bp->Get(key, data);
            struct hash_table_item *kv_item = (struct hash_table_item *)data.value_addr;
            size_t key_length = HIKV_KEY_LENGTH(kv_item->vec_length);
            size_t value_length = HIKV_VALUE_LENGTH(kv_item->vec_length);
            
            Slice h_key = Slice(data_set->vec_key[i][j]);
            Slice h_value = Slice(data_set->vec_value[i][j]);
            Slice b_key = Slice((const char*)kv_item->data, key_length);
            Slice b_value = Slice((const char*)kv_item->data + (NVMKV_ROUNDUP8(key_length)), value_length);
            
            assert(h_key == b_key);
            assert(h_value == b_value);
            assert(res == true);
        }
    }
    printf("[OK] BpTree status OK!\n");
}

void TestHiKVFactory::data_analysis()
{
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
            queue_collision_times += st.queue_collision;
            lock_collision_times += st.hashtable_collision;
            for(int k = 0; k < r_st; k++) 
            {
                uint64_t rdt = st.get_rdt(k);
                if(rdt < filter) 
                {
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
                if ( (double)rdt >= limit_t[k] ) {
                    over_count[k]++;
                    over_sum[k] += (uint64_t) rdt;
                }
            }
        }
    }
    // Here to print result
    printf("[Queue Collision Count] : %d [Lock Collision Count] : %d\n", \
        queue_collision_times, lock_collision_times);
    for(int i = 0; i < r_st; i++)
    {
        printf("[%02d]", i);
        printf("[Count : %5d cost : %10llu AVG : %8.2f]", \
            op_count[i], op_sum[i], (double)op_sum[i] / op_count[i]);
        if(over_count[i] == 0) 
        {
            over_count[i] = 1;
        }
        printf("[Count : %5d cost : %10llu AVG : %8.2f]\n", \
            over_count[i], over_sum[i], (double)over_sum[i] / over_count[i]);
    }
    for(int i = 0; i < r_st; i++)
    {
        printf("[%02d][%8.2f][Count : %4.2lf%% (%-8d/%8d) Cost : %4.2lf%%]\n", \
            i, limit_t[i], (double)over_count[i]/op_count[i] * 100.0, \
            over_count[i], op_count[i], (double)over_sum[i]/op_sum[i] * 100.0);
    }
}

// Function to use single thread test
void TestHiKVFactory::multiple_thread_test() 
{
    double time_count = 0;
    double sum_iops = 0;
    int opt_count = data_set->data_count;
    struct timeval begin_time, end_time;

    // If hikv is null we need to create a new hikv to run our test.
    if(hikv == NULL) {
        // calculate bucket count.
        this->num_ht_buckets = opt_count * 2 / (this->num_ht_partitions * NUM_ITEMS_PER_BUCKET);
        hikv = new HiKV(128, 1024, this->num_ht_partitions, this->num_ht_buckets, \
                    this->num_server_threads, \
                    this->num_backend_threads);
    }

    // Create run thread and wait threads to exit,
    // here we count the time use gettimeofday.
    gettimeofday(&begin_time, NULL);
    
    // create run thread
    thread_init();

    gettimeofday(&end_time, NULL);

    // Show hikv result (iops and runtime).
    // Here we show the sum iops of all threads,
    time_count = (double)(1000000.0 * ( end_time.tv_sec - begin_time.tv_sec ) + \
                    end_time.tv_usec - begin_time.tv_usec) / 1000000.0;
    
    for(int i = 0; i < num_server_threads; i++) {
        sum_iops += this->arr_iops[i];
    }
    printf("[Result] Opt Count : %d, Time Count : %.5f s, iops : %.5f\n", \
            opt_count, time_count, sum_iops);

    // In order to ensure the correctness of the data, we have to verify hashtable.
    verify_hashtable();

    // In order to ensure the correctness of the data, we have to verify bplus tree.
#ifdef BACKEND_THREAD
    verify_bptree();
#endif

    // If you need more accurate statistics, we need to run this function.
#if ((defined COLLECT_RDTSC) || (defined COLLECT_TIME))
    data_analysis();
#endif
}
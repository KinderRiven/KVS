#include "htest.h"
#include <vector>
using namespace std;
using namespace hikv;


// opt type
static vector<int> vec_opt[MAX_TEST_THREAD];
static vector<string> vec_key[MAX_TEST_THREAD];
static vector<string> vec_value[MAX_TEST_THREAD];
static double vec_iops[MAX_TEST_THREAD];
static vector<Status> vec_status[MAX_TEST_THREAD];


TestHashTableFactory::TestHashTableFactory(const char *data_in, int num_kvs, int num_threads, \
                    int max_key_length, int max_value_length)
{
    this->num_kvs = num_kvs;
    this->data_in = data_in;
    this->max_key_length = max_key_length;
    this->max_value_length = max_value_length;
    this->num_threads = num_threads;
    this->num_ht_partitions =  NUM_HT_PARTITIONS;
}

static void* thread_run(void *args_) 
{    
    struct test_thread_args *args = (struct test_thread_args *)args_;
    int tid = args->thread_id;
    int opt_count = vec_opt[tid].size();
    Status status;
    HashTable *ht = args->ht;
    struct timeval begin_time, end_time;    // count time
    double time_count = 0;
    
    for(int i = 0; i < opt_count; i++) 
    {
        Slice s_key = Slice(vec_key[tid][i]);
        Slice s_value = Slice(vec_value[tid][i]);
        gettimeofday(&begin_time, NULL);
        switch(vec_opt[tid][i]) 
        {
            case TEST_PUT:
                status = ht->Put(s_key, s_value);
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
        // add exe time to time sum.
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

void TestHashTableFactory::multiple_thread_test()
{
    double time_count = 0;
    int opt_count = 0;
    struct timeval begin_time, end_time;    // count time

    // Read data
    printf("[INFO] Waiting Data\n");
    opt_count = read_data(data_in, num_kvs, max_key_length, max_value_length, num_threads);
    
    this->num_ht_buckets = opt_count * 2 / (this->num_ht_partitions * NUM_ITEMS_PER_BUCKET);
    ht = new HashTable(this->num_ht_partitions, this->num_ht_buckets);
    
    // Create Put thread
    gettimeofday(&begin_time, NULL);
    for(int i = 0; i < num_threads; i++) 
    {
        struct test_thread_args *args = new test_thread_args();
        args->thread_id = i;
        args->ht = ht;
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
        sum_iops += vec_iops[i];
    }
    printf("[Result] opt count : %d, time_count : %.5f s, iops : %.5f\n", \
            opt_count, time_count, sum_iops);
}


// Function to use single thread test 
void TestHashTableFactory::single_thread_test() 
{

}
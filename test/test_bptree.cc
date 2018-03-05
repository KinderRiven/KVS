#include "htest.h"
#include <vector>
using namespace hikv;
using namespace std;

TestBpTreeFactory::TestBpTreeFactory(HBpTree *bp, TestDataSet *data_set, \
                    int num_threads, \
                    int key_length, int value_length)
{
    this->bp = bp;
    this->data_set = data_set;
    this->num_threads = num_threads;
    this->key_length = key_length;
    this->value_length = value_length;
}


static void* thread_run(void *args_) 
{    
    struct test_thread_args *args = (struct test_thread_args *)args_;
    struct timeval begin_time, end_time;    // count time
 
    HBpTree *bp = (HBpTree*)args->object;
    TestBpTreeFactory *factory = (TestBpTreeFactory*)args->factory;
    TestDataSet *data_set = factory->data_set;

    int tid = args->thread_id;
    int opt_count = data_set->vec_opt[tid].size();
    double time_count = 0;
   
    for(int i = 0; i < opt_count; i++) {
        KEY key = KEY(data_set->vec_key[tid][i]);
        DATA data;
        bool res;
        gettimeofday(&begin_time, NULL);
        switch(data_set->vec_opt[tid][i]) {
            case TEST_PUT:
                res = bp->Put(key, data);
                break;
            default:
                cout << "[ERROR] Illegal Parameters!" << endl;
                break;
        }
        assert(res == true);
        gettimeofday(&end_time, NULL);
        double exe_time = (double)(1000000.0 * ( end_time.tv_sec - begin_time.tv_sec ) + \
                    end_time.tv_usec - begin_time.tv_usec) / 1000000.0;
        time_count += exe_time;
    }
    printf("[Thread %d] Opt Count : %d, Time Count : %.5f s, IOPS : %.5f\n", \
            tid, opt_count, time_count, (double) opt_count / time_count);
    factory->arr_iops[tid] = (double) opt_count / time_count;
    pthread_exit(0);
}

void TestBpTreeFactory::thread_init()
{
    for(int i = 0; i < num_threads; i++) 
    {
        struct test_thread_args *args = new test_thread_args();
        args->thread_id = i;
        args->object = (void *)bp;
        args->factory = (void *)this;
        pthread_create(thread_id + i, NULL, thread_run, args);
    }
    // Join thread.
    printf("[RUNNING] Waiting thread to exit...\n");
    for(int i = 0; i < num_threads; i++) 
    {
        pthread_join(thread_id[i], NULL);
    }
}

void TestBpTreeFactory::multiple_thread_test()
{
    double time_count = 0;
    double sum_iops = 0;
    int opt_count = data_set->data_count;
    struct timeval begin_time, end_time;    // count time

    if (bp == NULL) {
        bp = new HBpTree();
    }
    
    // Create Put thread
    gettimeofday(&begin_time, NULL);
    thread_init();
    gettimeofday(&end_time, NULL);
    
    // show result
    time_count = (double)(1000000.0 * ( end_time.tv_sec - begin_time.tv_sec ) + \
                    end_time.tv_usec - begin_time.tv_usec) / 1000000.0;
    
    // show result
    for(int i = 0; i < num_threads; i++) {
        sum_iops += this->arr_iops[i];
    }
    printf("[Result] Opt Count : %d, Time Count : %.5f s, IOPS : %.5f\n", \
            opt_count, time_count, sum_iops);
}
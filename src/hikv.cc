#include "hikv.h"
using namespace hikv;

HiKV::HiKV(size_t max_key_length, size_t max_value_length, \
                    uint32_t num_server_threads, \
                    uint32_t num_server_queues, \
                    uint32_t num_backend_threads, \
                    uint32_t num_backend_queues) {
    // Here to init hash table.
    hash_table = new HashTable(16, 128);

    // Here to init server threadpool.
    server_thread_pool =  \
        new ThreadPool(TYPE_THREAD_POOL_HASH, num_server_threads, \
                        num_server_queues, 1024);
    server_thread_pool->init();  

    // Here to init backend threadpool.
    backend_thread_pool = \
        new ThreadPool(TYPE_THREAD_POOL_BTREE, num_backend_threads, \
                        num_backend_queues, 1024);
    backend_thread_pool->init();
}

static int queue_alloactor() {
    return 0;
}

// Function to put KV.
Status HiKV::Put(Slice &key, Slice &value) {
    Status status;
    status.set_running(true);
    int qid = queue_alloactor();
    server_thread_pool->add_worker(qid, this->hash_table, key, value, &status);
    while(status.is_running() == true) {}
    return status;
}
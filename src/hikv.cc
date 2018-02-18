#include "hikv.h"
using namespace hikv;
using namespace std;

HiKV::HiKV(size_t max_key_length, size_t max_value_length, \
                    uint32_t num_backend_threads, \
                    uint32_t num_backend_queues) {
    // Here to init hash table.
    hash_table = new HashTable(16, 512);

    // Here to init backend threadpool.
    backend_thread_pool = \
        new ThreadPool(TYPE_THREAD_POOL_BTREE, num_backend_threads, \
                        num_backend_queues, 1024);
    backend_thread_pool->init();
    
    this->num_backend_queues = num_backend_queues;
}

HiKV::~HiKV() {
    delete(hash_table);
    delete(backend_thread_pool);
}

Status HiKV::Get(Slice &key, Slice &value) {
    Status status;
    status = hash_table->Get(key, value);
    return status;
}

// Function to put KV.
Status HiKV::Put(Slice &key, Slice &value) {
    Status status;
    status = hash_table->Put(key, value);
    // backend_thread_pool->add_worker(TYPE_WORKER_PUT, 0, NULL, key, value);
    return status;
}
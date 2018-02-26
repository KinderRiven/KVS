#include "hikv.h"
using namespace hikv;
using namespace std;

HiKV::HiKV(size_t max_key_length, size_t max_value_length, \
                    uint32_t num_ht_partitions, \
                    uint32_t num_ht_buckets, \
                    uint32_t num_backend_threads, \
                    uint32_t num_backend_queues) 
{
    // Here to init hash table.
    this->num_ht_partitions = num_ht_partitions;
    this->num_ht_buckets = num_ht_buckets;
    hash_table = new HashTable(this->num_ht_partitions, this->num_ht_buckets);

    // Here to init backend threadpool.
    backend_thread_pool = \
        new ThreadPool(TYPE_THREAD_POOL_BTREE, num_backend_threads, \
                        num_backend_queues, 1024);
    backend_thread_pool->init();
    this->num_backend_queues = num_backend_queues;
    this->num_backend_threads = num_backend_threads;

    // Print Message.
    this->Print();
}

// Function to print hikv message.
void HiKV::Print()
{
    cout << "[HashTable]" << endl;
    cout << "num partitions per ht:" <<  this->num_ht_partitions << " ";
    cout << "num buckets per part: " << this->num_ht_buckets << " ";
    cout << "count items : " << this->num_ht_partitions * this->num_ht_buckets * NUM_ITEMS_PER_BUCKET << endl;
    cout << "[ThreadPool]" << endl;
    cout << "num backend queues : " << this->num_backend_queues << " ";
    cout << "num backend threads : " << this->num_backend_threads << endl;
}

HiKV::~HiKV() 
{
    delete(hash_table);
    delete(backend_thread_pool);
}

Status HiKV::Get(Slice &key, Slice &value) 
{
    Status status;
    status = hash_table->Get(key, value);
    return status;
}

// Function to put KV.
Status HiKV::Put(Slice &key, Slice &value) 
{
    Status status;
    status = hash_table->Put(key, value);
    // TODO backend thread.
    //backend_thread_pool->add_worker(TYPE_WORKER_PUT, 0, NULL, key, value);
    return status;
}
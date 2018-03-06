#include "hikv.h"
using namespace hikv;
using namespace std;
HiKV::HiKV(size_t max_key_length, size_t max_value_length, \
                    uint32_t num_ht_partitions, \
                    uint32_t num_ht_buckets, \
                    uint32_t num_server_threads, \
                    uint32_t num_backend_threads) 
{
    // Here to init hash table.
    this->num_ht_partitions = num_ht_partitions;
    this->num_ht_buckets = num_ht_buckets;
    hash_table = new HashTable(this->num_ht_partitions, this->num_ht_buckets);
    
    // Here to init bplus tree.
    bp_tree = new HBpTree();
    
    // Here to init backend threadpool.
    this->num_server_threads = num_server_threads;
    this->num_backend_queues = num_server_threads;
    this->num_backend_threads = num_backend_threads;
    backend_thread_pool = new ThreadPool(TYPE_THREAD_POOL_BTREE, this->num_server_threads, \
                        this->num_backend_threads, this->num_backend_queues, MAX_QUEUE_SIZE);
    backend_thread_pool->init();

    // Print Message.
    this->Print();
}

// Function to print hikv message.
void HiKV::Print()
{
    this->hash_table->Print();
    this->backend_thread_pool->Print();
}

HiKV::~HiKV() 
{
    delete(hash_table);
    delete(backend_thread_pool);
}

// Public function to convert Slice to Key
static void conver_slice_to_key(Slice &s, KEY &k)
{
    k.key_length = s.size();
    memcpy((void *)k.key, (void *)s.data(), s.size());
}

// Public function to get value from hashtable index
Status HiKV::Get(Slice &key, Slice &value, Config &config) 
{
    Status status;
    status = hash_table->Get(key, value);
    return status;
}

// Public function to Delete Key-value
Status HiKV::Delete(Slice &key, Config &config)
{
    Status status;
    status = hash_table->Delete(key);

    if(status.is_ok()) 
    {
        KEY bp_key;
        conver_slice_to_key(key, bp_key);
        bp_tree->Delete(bp_key);
    }
    return status;
}

Status HiKV::Scan(Slice &lower, Slice &upper, std::vector<Slice> &values)
{
    Status status;
    return status;
}


// Public function to PUT Key-value.
Status HiKV::Put(Slice &key, Slice &value, Config &config) 
{
    Status status;
#ifdef COLLECT_RDTSC
    uint64_t st_start, st_end;
#endif
    // [6] hashtable put operator 
#ifdef COLLECT_RDTSC
    st_start = rdtsc();
#endif
    // Here to put kv into hashtable
    status = hash_table->Put(key, value);
#ifdef COLLECT_RDTSC
    st_end = rdtsc();
    status.append_rdt(st_end - st_start);
#endif

// [7] collect add worker
#ifdef COLLECT_RDTSC
    st_start = rdtsc();
#endif
    KEY bp_key;
    DATA bp_data = DATA(status.v_addr);
    conver_slice_to_key(key, bp_key);
#ifdef ASYC_MODE
    bp_tree->Put(bp_key, bp_data);
#else
    #ifdef BACKEND_THREAD
        // Here to add worker into queue
        bool success = true;
        #ifdef DYNAMIC_QUEUE
            success = backend_thread_pool->add_worker_into_group(TYPE_WORKER_PUT, config.get_tid(), \
                (void *)bp_tree, bp_key, bp_data, status);
        #else
            success = backend_thread_pool->add_worker_into_queue(TYPE_WORKER_PUT, config.get_tid(), \
                (void *)bp_tree, bp_key, bp_data, status);
        #endif
        #ifdef QUEUE_OPTIMIZATION
            if (!success) {
                bp_tree->Put(bp_key, bp_data);
            }
        #endif
    #endif
#endif

#ifdef COLLECT_RDTSC
    st_end = rdtsc();
    status.append_rdt(st_end - st_start);
#endif

    return status;
}


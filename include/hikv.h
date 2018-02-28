/*
 *  Description :
 *  Copyright (C) 2018 Institute of Computing Technology, CAS
 *  HiVK.
 *  Author : KinderRiven <hanshukai@ict.ac.cn>
 *  Date : 2018/2/4
 */
#ifndef HIKV_H_
#define HIKV_H_
#include "hhash.h"
#include "config.h"
#include "bptree/hbptree.h"
#include "hthread.h"
#include <vector>
namespace hikv 
{
    class HiKV 
    {
        struct hikv_thread_args 
        {
            int thread_id;
            HiKV *hikv;
        };
        public:
            HiKV(size_t max_key_length, size_t max_value_length, \
                    uint32_t num_ht_partitions, \
                    uint32_t num_ht_buckets, \
                    uint32_t num_server_threads, \
                    uint32_t num_backend_threads);     // threadpool
            ~HiKV();
            // Single processing
            Status Put(Slice &key, Slice &value, Config &config);
            Status Get(Slice &key, Slice &value, Config &config);
            Status Delete(Slice &key, Slice &value, Config &config);
            // Batch processing
            void Print();
            // Just for test
            BpTree *get_bp() { return bp_tree; }
        private:
            size_t max_key_length;
            size_t max_value_length;
            // bpTree
            BpTree *bp_tree;
            // hashtable
            uint32_t num_ht_partitions;
            uint32_t num_ht_buckets;
            HashTable *hash_table;
            // threadpool
            uint32_t num_server_threads;
            uint32_t num_backend_threads;
            uint32_t num_backend_queues;
            ThreadPool *backend_thread_pool;
        private:
    };
}
#endif
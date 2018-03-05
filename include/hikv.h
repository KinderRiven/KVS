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
#include "hbptree.h"
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
            Status Put(Slice &key, Slice &value, Config &config);
            Status Get(Slice &key, Slice &value, Config &config);
            Status Delete(Slice &key, Config &config);
            void Print();
        public:
            HBpTree *get_bp() { return bp_tree; }
        private:
            // other var
            size_t max_key_length;
            size_t max_value_length;
            
            // bptree var
            HBpTree *bp_tree;
            
            // hashtable  var
            uint32_t num_ht_partitions;
            uint32_t num_ht_buckets;
            HashTable *hash_table;
            
            // threadpool var
            uint32_t num_server_threads;
            uint32_t num_backend_threads;
            uint32_t num_backend_queues;
            ThreadPool *backend_thread_pool;
        private:
    };
}
#endif
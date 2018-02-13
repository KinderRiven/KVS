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
#include "hthread.h"
namespace hikv 
{
    class HiKV 
    {
        public:
            HiKV(size_t max_key_length, size_t max_value_length, \
                    uint32_t num_server_threads, \
                    uint32_t num_server_queues, \
                    uint32_t num_backend_threads, \
                    uint32_t num_backend_queues);     // threadpool
            ~HiKV();
            Status Put(Slice &key, Slice &value);
            Status Get(Slice &key, Slice &value);
            Status Delete(Slice &key, Slice &value);
        private:
            size_t max_key_length;
            size_t max_value_length;
            // TODO B+Tree
            HashTable *hash_table;
            ThreadPool *server_thread_pool;
            ThreadPool *backend_thread_pool;
            uint32_t num_server_threads;
            uint32_t num_backend_threads;
    };
}
#endif
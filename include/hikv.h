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
namespace hikv 
{
    class HiKV 
    {
        public:
            HiKV(size_t key_length, size_t value_length);     // threadpool
            ~HiKV();
        private:
            size_t max_key_length;
            size_t max_value_length;
            // TODO B+Tree
            HashTable *hash_table;
            // TODO ThreadPool
    };
}
#endif
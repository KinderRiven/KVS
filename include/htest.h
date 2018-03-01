/*
 *  Description :
 *  Copyright (C) 2018 Institute of Computing Technology, CAS
 *  TestFactory.
 *  Author : KinderRiven <hanshukai@ict.ac.cn>
 *  Date : 2018/2/3
 */

#ifndef HTEST_H_
#define HTEST_H_

#include "htype.h"
#include "hhash.h"
#include "hikv.h"
#include "bptree/hbptree.h"
#include "hstatus.h"

#include <fstream>
#include <iostream>
#include <cstdio>
#include <unistd.h>
#include <sys/time.h>
#include <pthread.h>
#include <string>
#include <cstdlib>
#include <limits>
#include <random>
#include <algorithm>
#include <cstring>

#define MAX_TEST_THREAD 36

namespace hikv 
{
    #ifndef TEST_PUT
        #define TEST_PUT 1
        #define TEST_GET 2
        #define TEST_DEL 3
    #endif
    struct test_thread_args 
    {
        int thread_id;
        HiKV *hikv;
        BpTree *bp;
        HashTable *ht;
    };
    class TestHashTableFactory 
    {
        public:
            TestHashTableFactory(const char *data_in, int num_kvs, int num_threads, \
                    int max_key_length, int max_value_length);
            void single_thread_test();
            void multiple_thread_test();
        private:
            pthread_t thread_id[MAX_TEST_THREAD];
            HashTable *ht;
            const char *data_in;   
            int num_kvs;
            int num_ht_partitions;
            int num_ht_buckets;
            int max_key_length;
            int max_value_length;
            uint32_t num_threads;
    };
    class TestBpTreeFactory
    {
        public:
            TestBpTreeFactory(const char *data_in, int num_kvs, \
                    int max_key_length, int max_value_length, \
                    int num_threads);
            void single_thread_test();
            void multiple_thread_test();
        private:
            pthread_t thread_id[MAX_TEST_THREAD];
            BpTree *bp;
            const char *data_in;
            int num_kvs;
            int max_key_length;
            int max_value_length;
            int num_threads;
    };
    class TestHiKVFactory 
    {
        public:
            TestHiKVFactory(const char *data_in, int num_kvs, \
                    int max_key_length, int max_value_length, \
                    // threadpool
                    uint32_t num_server_threads, \
                    uint32_t num_backend_threads);
            void single_thread_test();
            void multiple_thread_test();
        private:
            pthread_t thread_id[MAX_TEST_THREAD];
            HiKV *hikv;
            const char *data_in;   
            int num_kvs;
            int num_ht_partitions;
            int num_ht_buckets;
            int max_key_length;
            int max_value_length;
            uint32_t num_server_threads;
            uint32_t num_backend_threads;        
    };
}

#endif //NVM_KV_TEST_H

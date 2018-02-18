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
#include "hstatus.h"

#include <fstream>
#include <iostream>
#include <cstdio>
#include <unistd.h>
#include <sys/time.h>
#include <pthread.h>

#define MAX_TEST_THREAD 36

namespace hikv {
    #ifndef TEST_PUT
        #define TEST_PUT 1
        #define TEST_GET 2
        #define TEST_DEL 3
    #endif
    struct test_thread_args {
        int thread_id;
        HiKV *hikv;
    };
    class TestHashTableFactory {
        public:
            TestHashTableFactory(const char *data_in, int num_partitions, int num_buckets) \
                : data_in(data_in), num_partitions(num_partitions), num_buckets(num_buckets) {};
            void single_thread_test();
            void multiple_thread_test();
        private:
            const char *data_in;
            int num_partitions;
            int num_buckets;
    };
    class TestHiKVFactory {
        public:
            TestHiKVFactory(const char *data_in, uint32_t num_server_threads, \
                    uint32_t num_backend_threads, \
                    uint32_t num_backend_queues) \
                    : data_in(data_in), 
                    num_server_threads(num_server_threads), \
                    num_backend_threads(num_backend_threads), \
                    num_backend_queues(num_backend_queues) {};
            void single_thread_test();
            void multiple_thread_test();
        private:
            pthread_t thread_id[MAX_TEST_THREAD];
            const char *data_in;
            HiKV *hikv;
            uint32_t num_server_threads;
            uint32_t num_backend_threads;
            uint32_t num_backend_queues;
            
    };
}

#endif //NVM_KV_TEST_H

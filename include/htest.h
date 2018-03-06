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
#include "hbptree.h"
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
    
    // Class : TestDataSet
    // This class is used to store all the test data,
    // once the data is generated, we can always use it through this class.
    class TestDataSet 
    {
        public:
            int data_count;
            vector<int> vec_opt[MAX_TEST_THREAD];       // save operator type
            vector<string> vec_key[MAX_TEST_THREAD];    // save key
            vector<string> vec_value[MAX_TEST_THREAD];  // save value
            vector<uint64_t> vec_int[MAX_TEST_THREAD];  // save int value
        public:
            TestDataSet();
            void init();
            void read_data(const char *file_name, int num_vector);
            void generate_data(int num_kvs, int key_length, int value_length, int num_vector);
            
    };
    struct test_thread_args 
    {
        int thread_id;
        void *object;   // hikv, hashtable, bplus tree
        void *factory;  // testhikvfactory
    };
    
    // Class : TestHashTableFactory 
    // Used to detect hash table throughput.
    class TestHashTableFactory 
    {
        public:
            TestHashTableFactory(HashTable *ht, TestDataSet *data_set, int num_threads, \
                    int key_length, int value_length);
            void single_thread_test();
            void multiple_thread_test();
        public:
            HashTable *ht;  
            TestDataSet *data_set;
            vector<Status> vec_status[MAX_NUM_THREADS];
            double arr_iops[MAX_NUM_THREADS];
        private:
            void thread_init();
            void verify_hashtable();
            void data_analysis();
        private:
            pthread_t thread_id[MAX_TEST_THREAD];
            int num_threads;
            int num_ht_partitions;
            int num_ht_buckets;
            int key_length;
            int value_length;
    };

    // Class : TestBpTreeFactory 
    // Used to detect bplus tree throughput.
    class TestBpTreeFactory
    {
        public:
            TestBpTreeFactory(HBpTree *bp_tree, TestDataSet *data_set, \
                    int num_threads, \
                    int key_length, int value_length);
            void single_thread_test();
            void multiple_thread_test();
        public:
            HBpTree *bp;
            TestDataSet *data_set;
            vector<Status> vec_status[MAX_NUM_THREADS];
            double arr_iops[MAX_NUM_THREADS];
        private:
            private:
            void thread_init();
            void verify_bptree();
            void data_analysis();
        private:
            pthread_t thread_id[MAX_TEST_THREAD];
            int num_threads;
            int key_length;
            int value_length;
    };

    // Class : TestBpTreeFactory 
    // Used to detect hikv throughput, which also attached 
    // a more accurate parameter extraction.
    class TestHiKVFactory 
    {
        public:
            TestHiKVFactory(HiKV *hikv, TestDataSet *data_set, \
                    int num_server_threads, int num_backend_threads, \
                    int key_length, int value_length);
            void single_thread_test();
            void multiple_thread_test();   
        public:
            HiKV *hikv;
            TestDataSet *data_set;
            vector<Status> vec_status[MAX_NUM_THREADS];
            double arr_iops[MAX_NUM_THREADS];
        private:
            void thread_init();
            void verify_hashtable();
            void verify_bptree();
            void data_analysis();
        private:
            pthread_t thread_id[MAX_TEST_THREAD];
            int num_ht_partitions;
            int num_ht_buckets;
            int key_length;
            int value_length;
            int num_server_threads;
            int num_backend_threads; 
    };
}

#endif //NVM_KV_TEST_H

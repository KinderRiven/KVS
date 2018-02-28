/*
 *  Description :
 *  Copyright (C) 2018 Institute of Computing Technology, CAS
 *  Thread.
 *  Author : KinderRiven <hanshukai@ict.ac.cn>
 *  Date : 2018/2/4 
 */
#ifndef HTHREAD_H_
#define HTHREAD_H_

//#define _GNU_SOURCE
#include "htype.h"
#include "hhash.h"
#include "bptree/hbptree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

// threads
#define MAX_NUM_THREADS 64
#define DEFAULT_NUM_THREADS 1

// queue
#define MAX_NUM_QUEUES 128
#define DEFAULT_NUM_QUEUES 1

// queue size
#define MAX_QUEUE_SIZE 81920
#define DEFAULT_QUEUE_SIZE 8192

#define TYPE_THREAD_POOL_BTREE 1

#define TYPE_WORKER_PUT 1
#define TYPE_WORKER_GET 2
#define TYPE_WORKER_DEL 3

#define COLLISION_FAIL_TIMES

namespace hikv
{
    class ThreadPool;
    // Bplus tree worker struct
    struct bplus_tree_worker 
    {
        BpTree *bp;
        uint8_t type;
        KEY key;
        DATA data;
    };
    // HashTable worker struct
    struct hash_table_worker 
    {
        HashTable *hash_table;
        Slice key, value;
        uint8_t type;
    };
    // Thread arg
    struct thread_args 
    {
        uint32_t start_qid;
        uint32_t end_qid;
        uint32_t queues_size;
        bool one_queue_one_consumer;
        ThreadPool *tp;
    };
    // Thread request queue
    struct request_queue 
    {
        uint32_t size;      // queue size
        uint32_t weight;    // queue weight for dispenser
        volatile uint32_t write_index;
        volatile uint32_t max_write_index;
        volatile uint32_t read_index;
        volatile uint32_t max_read_index;
        uint8_t  data[0];    // ptr
    };
    //class threadpool
    class ThreadPool 
    {
        private:
            struct request_queue *queues[MAX_NUM_QUEUES];
            pthread_t pthread_id[MAX_NUM_THREADS];
            uint32_t num_producers;
            uint32_t num_consumers;
            uint32_t num_queues;
            uint32_t queue_size;
            uint8_t type;
            bool one_queue_one_consumer;
            bool one_queue_one_producer;
            bool init_queue();
            bool init_thread();
        public:
            ThreadPool();
            ThreadPool(uint8_t type, uint32_t num_producers, uint32_t num_consumers, \
                uint32_t num_queues, uint32_t queue_size);
            ~ThreadPool();
            bool init();
            uint8_t get_type() { return type; }
            bool add_worker(uint8_t opt, uint32_t qid, void *object, \
                        KEY &key, DATA &data, Status &status);
            struct request_queue *get_queue(int qid);
            void Print();
    };
}

#endif
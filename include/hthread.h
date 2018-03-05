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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "hbptree.h"
#include <pthread.h>
#include "config.h"
#include <sched.h>
#include <unistd.h>

namespace hikv 
{
    class ThreadPool;
    
    // Bplus tree worker struct
    struct bplus_tree_worker  
    {
        HBpTree *bp;
        uint8_t type;
        KEY key;
        DATA data;
    };
    
    // HashTable worker struct
    struct hash_table_worker 
    {
    };
    
    // Thread arg
    struct thread_args 
    {
        uint32_t thread_id;
        uint32_t start_qid;
        uint32_t end_qid;
        uint32_t queues_size;
        uint32_t num_producers;
        bool one_queue_one_consumer;
        ThreadPool *tp;
    };
    
    // Thread queue group
    struct queue_group
    {
        uint32_t num_queues;
        uint32_t per_queue_size;
        volatile uint32_t write_index;
        volatile uint32_t max_write_index;
        volatile uint32_t read_index;
        volatile uint32_t max_read_index;
        void *queues[MAX_QUEUES_PER_GROUP];
    };
    
    // Thread request queue
    struct request_queue 
    {
        uint32_t size;          // queue size
        volatile uint32_t write_index;
        volatile uint32_t max_write_index;
        volatile uint32_t read_index;
        volatile uint32_t max_read_index;
        uint8_t  data[0];       // ptr
    };
    
    //class threadpool
    class ThreadPool 
    {
        private:
            // threadpool var
            uint8_t type;
            uint32_t num_producers;
            uint32_t num_consumers;
            pthread_t pthread_id[MAX_NUM_THREADS];
            bool one_queue_one_consumer;
            bool one_queue_one_producer;
            // queue var
            struct request_queue *queues[MAX_NUM_QUEUES];
            uint32_t num_queues;
            uint32_t queue_size;
            // queue group var
            uint32_t num_queue_groups;
            uint32_t per_queue_size;
            struct queue_group *queue_groups[MAX_NUM_QUEUES_GROUP];
        private:
            bool init_queue();
            bool init_thread();
        public:
            // struct function
            ThreadPool();
            ThreadPool(uint8_t type, uint32_t num_producers, uint32_t num_consumers, \
                uint32_t num_queues, uint32_t queue_size);
            ~ThreadPool();
            // initlization ThreadPool.
            bool init();
            // get function
            uint8_t get_type() { return type; }
            struct request_queue *get_queue(int qid);
            struct queue_group *get_queue_group(int gid);
            // print message.
            void Print();
            // add task into one queue.
            bool add_worker_into_queue(uint8_t opt, uint32_t qid, void *object, \
                        KEY &key, DATA &data, Status &status); 
            // add task into one queue group.
            bool add_worker_into_group(uint8_t opt, uint32_t qid, void *object, \
                        KEY &key, DATA &data, Status &status);
    };
}

#endif
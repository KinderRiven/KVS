/*
 *  Description :
 *  Copyright (C) 2018 Institute of Computing Technology, CAS
 *  Configure file.
 *  Author : KinderRiven <hanshukai@ict.ac.cn>
 *  Date : 2018/2/3
 */
#ifndef CONFIG_H_
#define CONFIG_H_

///////////////////////////////////////////////

// HIKV Parameter
#define MAX_KEY_LENGTH      16
#define MAX_VALUE_LENGTH    256
#define PERSIST_DATA

// queue optimiazation
// #define QUEUE_OPTIMIZATION

// #define ASYC_MODE

///////////////////////////////////////////////

// ThreadPool Parameter
#define BACKEND_THREAD

// [thread]
#define MAX_NUM_THREADS     64
#define DEFAULT_NUM_THREADS 1

// [queue]
#define MAX_NUM_QUEUES      128
#define DEFAULT_NUM_QUEUES  1
#define MAX_QUEUE_SIZE      8192
#define DEFAULT_QUEUE_SIZE  8192

// [queue group]
#define INITIAL_NUM_QUEUE_PER_GROUP 2
#define MAX_NUM_QUEUES_GROUP 128
#define MAX_QUEUES_PER_GROUP 128

// [task work]
#define TYPE_THREAD_POOL_BTREE 1
#define TYPE_WORKER_PUT 1
#define TYPE_WORKER_GET 2
#define TYPE_WORKER_DEL 3

// whether or not think about thread affinity
// #define THREAD_CPU_BIND

// using queue group
// #define DYNAMIC_QUEUE

// if [define queue optimization] & [collision > fail times]
// server thread get false from add_worker,
// and then server thread do it by self.
#define COLLISION_FAIL_TIMES 16

///////////////////////////////////////////////

// HashTable Parrmeter.
#define NUM_HT_PARTITIONS 32

// the max partitions of per hashtable.
#define MAX_PARTITIONS_PER_HT 4096

// the count of items of per bucker.
#define NUM_ITEMS_PER_BUCKET (15)

///////////////////////////////////////////////

// we can collect clock by rdtsc()
// #define COLLECT_RDTSC

// we can collect time by timeofday()
// #define COLLECT_TIME

// define COLLECT_QUEUE
// collect queue collision
#define COLLECT_QUEUE

// define COLLECT_LOCK
// collect lock collision 
#define COLLECT_LOCK

#endif
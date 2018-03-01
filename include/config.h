/*
 *  Description :
 *  Copyright (C) 2018 Institute of Computing Technology, CAS
 *  Configure file.
 *  Author : KinderRiven <hanshukai@ict.ac.cn>
 *  Date : 2018/2/3
 */
#ifndef CONFIG_H_
#define CONFIG_H_

#define MAX_KEY_LENGTH 128
#define MAX_VALUE_LENGTH 256

// define COLLECT_RDTSC
// We can collect clock by rdtsc()
#define COLLECT_RDTSC

// define COLLECT_TIME
// We can collect time by timeofday()
#define COLLECT_TIME

// define BACKEND_THREAD
// add task into queue
#define BACKEND_THREAD

// define COLLECT_QUEUE
// collect queue collision
#define COLLECT_QUEUE

// define COLLECT_LOCK
// collect lock collision 
#define COLLECT_LOCK

#define NUM_HT_PARTITIONS 32

// using queue optimiazation
#define QUEUE_OPTIMIZATION

// #define THREAD_CPU_BIND

#endif
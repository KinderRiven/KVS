/*
 *  Description :
 *  Copyright (C) 2018 Institute of Computing Technology, CAS
 *  Thread.
 *  Author : KinderRiven <hanshukai@ict.ac.cn>
 *  Date : 2018/2/4 
 */
#ifndef HTHREAD_H_
#define HTHREAD_H_
#include <pthread.h>
namespace hikv
{
    //class threadpool
    class ThreadPool 
    {
        // The work struct
        typedef struct work {
            // argc : the number of parameter | argv : parameter
            void process(int argc, char *argv[]);
        } work_t;
    };
}

#endif
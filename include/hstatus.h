/*
 *  Description :
 *  Copyright (C) 2018 Institute of Computing Technology, CAS
 *  For status.
 *  Author : KinderRiven <hanshukai@ict.ac.cn>
 *  Date : 2018/2/5
 */
#ifndef HSTATUS_H_
#define HSTATUS_H_

#include <utility>
#include <string>
#include <cstring>
#include <assert.h>
#include <stdlib.h>
#include <iostream>
namespace hikv
{
    #define STATUS_OK (1 << 0)
    #define STATUS_RUNNING (1 << 1)

    // Class : Status
    // Used to return the results of the call, and in the 
    // necessary use of the operation of the tracking process 
    // to obtain the implementation of some of the parameters.
    class Status 
    {
        public:
            Status() 
            {
                ok = running = false;
                rdt_count = 0;
                queue_collision = hashtable_collision = 0;
            }
            ~Status(){}
        public:
            uint64_t hash64;            // save key hash value
            uint64_t v_addr;            // save kv_item addr
            bool queue_collision;       // judge whether queue block
            bool hashtable_collision;   // judge whether hashtable block
        
        public:
            void set_ok(bool ok) { this->ok = ok; }
            bool is_ok() { return ok; }
            void set_running(bool running) { this->running = running; }
            bool is_running() { return running; }
        
        private:
            volatile bool ok;       // opterator result
            volatile bool running;  // runnning is true, else false
        
        public:
            // printf rdt[0:]
            void print_rdt(int start, int end, int range)
            {
                int s = s < 0 ? 0 : start;
                int e = end < (rdt_count - 1) ? end : (rdt_count - 1);
                bool flag = false;
                for(int i = s; i <= e; i++) {
                    if(rdt[i] >= range) {
                        printf("[%d] : %-5llu ", i, rdt[i]);
                        flag = true;
                    }
                }
                if (flag) { printf("\n"); }
            }
            // append rdtsc reslut into rdt[]
            void append_rdt(uint64_t rd) { rdt[rdt_count++] = rd; }
            // get rdt[i]
            uint64_t get_rdt(int i)  { return rdt[i]; }
        private:
            uint64_t rdt[16];       // record rdtsc() return
            uint8_t rdt_count;      // rdt_count
    };
}

#endif
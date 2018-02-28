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
    #define MESSAGE_LENGTH 128
    class Status 
    {
        public:
            Status()
            { 
                ok = running = false;
                rdt_count = 0;
                queue_collision = lock_collision = 0;
                msg = (char *)malloc(MESSAGE_LENGTH);
            }

            ~Status()
            {
            }

            void set_ok(bool ok) { this->ok = ok; }
            bool is_ok() { return ok; }

            void set_running(bool running) 
            {
                this->running = running;
            }

            bool is_running() { return running; }

            void set_msg(const char *msg) 
            {
                memcpy((void *)this->msg, (void *)msg, strlen(msg));
            }

            friend std::ostream& operator << (std::ostream &output, Status &s) 
            {
                if(s.ok) {
                    output << "[OK]:TRUE ";
                } else {
                    output << "[OK]:FALSE ";
                }
                output << "[MSG]:"<< s.msg;
                return output;
            }

            // printf rdt[0:]
            void print_rdt(int start, int end, int range)
            {
                int s = s < 0 ? 0 : start;
                int e = end < (rdt_count - 1) ? end : (rdt_count - 1);
                bool flag = false;
                for(int i = s; i <= e; i++) 
                {
                    if(rdt[i] >= range) 
                    {
                        printf("[%d] : %-5llu ", i, rdt[i]);
                        flag = true;
                    }
                }
                if(flag) { printf("\n"); }
            }

            // append rdtsc reslut into rdt[]
            void append_rdt(uint64_t rd) { rdt[rdt_count++] = rd; }

            // get rdt[i]
            uint64_t get_rdt(int i)  { return rdt[i]; }

            void set_queue_collision(bool yes) { this->queue_collision = yes; }
            bool get_queue_collision() { return this->queue_collision; }

            void set_lock_collision(bool yes) { this->lock_collision = yes; }
            bool get_lock_collision() { return this->lock_collision; }
        private:
            char *msg;
            volatile bool ok;       // opterator result
            volatile bool running;  // is runnning is true, else false
            uint64_t rdt[16];       // record rdtsc() return
            uint8_t rdt_count;
            bool queue_collision;
            bool lock_collision;
    };
}

#endif
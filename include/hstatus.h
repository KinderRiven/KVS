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
                ok = 0;
                running = 0; 
                msg = (char *)malloc(MESSAGE_LENGTH);
            }
            ~Status(){}

            void set_ok(bool ok_) {
                ok = ok_;
            }

            bool is_ok() { 
                return ok;
            }

            void set_running(bool running_) {
                running = running_;
            }

            bool is_running() {
                return running;
            }

            void set_msg(const char *msg_) {
                memcpy((void *)msg, (void *)msg_, strlen(msg_));
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
        private:
            volatile bool ok;
            volatile bool running;
            char *msg;
    };
}

#endif
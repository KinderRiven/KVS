/*
 *  Description :
 *  Copyright (C) 2018 Institute of Computing Technology, CAS
 *  For status.
 *  Author : KinderRiven <hanshukai@ict.ac.cn>
 *  Date : 2018/2/5
 */
#ifndef HSTATUS_H_
#define HSTATUS_H_

namespace hikv
{
    #define STATUS_OK (1 << 0)
    #define STATUS_RUNNING (1 << 1)
    class Status 
    {
        public:
            Status(){ 
                ok = 0;
                running = 0; 
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
        private:
            bool ok;
            bool running;
    };
}

#endif
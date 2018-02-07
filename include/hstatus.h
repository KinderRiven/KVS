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
    class Status 
    {
        public:
            Status(){}
            Status(bool ok) : _ok(ok){}
            ~Status(){}
            bool ok() {
                return _ok;
            }
            void set_ok(bool ok) {
                _ok = ok;
            }
        private:
            bool _ok;
    };
}

#endif
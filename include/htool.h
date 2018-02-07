/*
 *  Description :
 *  Copyright (C) 2018 Institute of Computing Technology, CAS
 *  For memory manager.
 *  Author : KinderRiven <hanshukai@ict.ac.cn>
 *  Date : 2018/2/4
 */
#ifndef HTOOLS_H_
#define HTOOLS_H_

#include <cstring>
#define HIKV_ROUNDUP8(x) (((x) + 7UL) & (~7UL))

namespace hikv 
{
    class Tool 
    {
        public:
            static bool hmemcmp(const uint8_t *dest, const uint8_t *src, size_t length) 
            {
                return memcmp(dest, src, length) == 0;
            }

            static void hmemcpy(uint8_t *dest, const uint8_t *src, size_t length) 
            {
                memcpy((void *)dest, (const void *)src, length);
            }
    };
}

#endif
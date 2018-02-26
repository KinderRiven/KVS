/*
 *  Description :
 *  Copyright (C) 2018 Institute of Computing Technology, CAS
 *  Some Algorithm:
 *      - Hash (Google City Hash)
 *  Author : KinderRiven <hanshukai@ict.ac.cn>
 *  Date : 2018/2/4
 */
#ifndef HALGORITHM_H_
#define HALGORITHM_H_
#include "city.h"
#include "citycrc.h"
namespace hikv 
{
    class Algorithm 
    {
        public:
            //Function for hash64 interface     
            static uint64_t hash64(const char *key, size_t key_len) {
                return CityHash64(key, key_len);
            }

            //Function for hash32 interface
            static uint32_t hash32(const char *key, size_t key_len) {
                return CityHash32(key, key_len);
            }

            //Function for hash16 interface
            static uint16_t hash16(const char *key, size_t key_len) {
                return (uint16_t)(CityHash32(key, key_len) >> 16);
            }
    };
}
#endif
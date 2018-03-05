/*
 *  Description :
 *  Copyright (C) 2018 Institute of Computing Technology, CAS
 *  Utils.
 *  Author : KinderRiven <hanshukai@ict.ac.cn>
 *  Date : 2018/2/4
 */

#ifndef UTIL_H_
#define UTIL_H_

#include <cstring>
#include <htype.h>

#define NVM_WRITE_LATENCY 600
// We need to emulate longer write latency of NVM
// #define IN_MEMORY_KV
#define HASHTABLE_PERSISTENCY

// Used to align
#define NVMKV_ROUNDUP8(x) (((x) + 7UL) & (~7UL))
#define HIKV_ROUNDUP8(x)  (((x) + 7UL) & (~7UL))

// CPU barrier.
#define memory_barrier() asm volatile("" ::: "memory")

#define sfence() asm volatile("sfence" ::: "memory")

static inline void clflush(volatile void *__p) 
{
	asm volatile("clflush (%0)" :: "r"(__p));
}

static inline uint64_t rdtsc (void) 
{
    unsigned l, u;
    __asm__ __volatile__("rdtsc" : "=a" (l), "=d" (u));
    return ((uint64_t)u << 32) | l;
}

static void persist_data(void *p, size_t length)
{
//if in_memory_kv, only need to emulate longer write latency of NVM.
#ifdef IN_MEMORY_KV
	double factor = 1 + ((double)length) / 64 * 2 / 16;
	uint64_t cpu_ticks = NVM_WRITE_LATENCY * 2.1 * factor;
	uint64_t start = rdtsc();
	while (rdtsc() < start + cpu_ticks) {}
// if persist kv store, need to flush cache, add longer write latency, and guarantee ordering.
#else 
	size_t flush_count = length / 64;
	size_t i = 0;
	for (i = 0; i <= flush_count; i++)
		clflush((uint8_t *)p + i * 64);

	double factor = 1 + ((double)length) / 64 * 2 / 16;
	uint64_t cpu_ticks = NVM_WRITE_LATENCY * 2.1 * factor;
	uint64_t begin = rdtsc();
	while (rdtsc() < begin + cpu_ticks) {}
	sfence();		
#endif
}

static void memcpy8(uint8_t *dest, const uint8_t *src, size_t length)
{
    length = NVMKV_ROUNDUP8(length);
    switch (length >> 3)
    {
        case 0:
            break;
        case 1:
            *(uint64_t *)(dest + 0) = *(const uint64_t *)(src + 0);
            break;			
        case 2:
            *(uint64_t *)(dest + 8) = *(const uint64_t *)(src + 8);
            *(uint64_t *)(dest + 0) = *(const uint64_t *)(src + 0);
            break;
        case 3:
            *(uint64_t *)(dest + 16) = *(const uint64_t *)(src + 16);
            *(uint64_t *)(dest + 8) = *(const uint64_t *)(src + 8);
            *(uint64_t *)(dest + 0) = *(const uint64_t *)(src + 0);
            break;
        case 4:
            *(uint64_t *)(dest + 24) = *(const uint64_t *)(src + 24);
            *(uint64_t *)(dest + 16) = *(const uint64_t *)(src + 16);
            *(uint64_t *)(dest + 8) = *(const uint64_t *)(src + 8);
            *(uint64_t *)(dest + 0) = *(const uint64_t *)(src + 0);
            break;			
        default:
            memcpy(dest, src, length);
            break;
    }
}

static bool mehcached_memcmp8(const uint8_t *dest, const uint8_t *src, size_t length)
{
    length = NVMKV_ROUNDUP8(length);
    switch (length >> 3)
    {
        case 0:
            return true;
        case 1:
            if (*(const uint64_t *)(dest + 0) != *(const uint64_t *)(src + 0))
                return false;
            return true;
        case 2:
            if (*(const uint64_t *)(dest + 0) != *(const uint64_t *)(src + 0))
                return false;
            if (*(const uint64_t *)(dest + 8) != *(const uint64_t *)(src + 8))
                return false;
            return true;
        case 3:
            if (*(const uint64_t *)(dest + 0) != *(const uint64_t *)(src + 0))
                return false;
            if (*(const uint64_t *)(dest + 8) != *(const uint64_t *)(src + 8))
                return false;
            if (*(const uint64_t *)(dest + 16) != *(const uint64_t *)(src + 16))
                return false;
            return true;
        case 4:
            if (*(const uint64_t *)(dest + 0) != *(const uint64_t *)(src + 0))
                return false;
            if (*(const uint64_t *)(dest + 8) != *(const uint64_t *)(src + 8))
                return false;
            if (*(const uint64_t *)(dest + 16) != *(const uint64_t *)(src + 16))
                return false;
            if (*(const uint64_t *)(dest + 24) != *(const uint64_t *)(src + 24))
                return false;
            return true;
        default:
            return memcmp(dest, src, length) == 0;
    }
}

static int key_memcmp8(const uint8_t *key1, const uint8_t *key2, size_t key1_length, size_t key2_length)
{
    key1_length = NVMKV_ROUNDUP8(key1_length);
    key2_length = NVMKV_ROUNDUP8(key2_length);
	size_t i = 0;
	size_t offset1 = key1_length >> 3;
	size_t offset2 = key2_length >> 3;
	if (offset1 == 0 || offset2 == 0)
		return 0;
	if (key1_length > key2_length)
		return 1;
	if (key1_length < key2_length)
		return -1;
	for (i=0; i < offset1; i++)
	{
		if (*(const uint64_t *)(key1 + i*8) > *(const uint64_t *)(key2 + i*8))
			return 1;
		if (*(const uint64_t *)(key1 + i*8) < *(const uint64_t *)(key2 + i*8))
			return -1;
	}
	return 0;
}
#endif
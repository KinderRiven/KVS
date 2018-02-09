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
#endif
/*
 *  Description :
 *  Copyright (C) 2018 Institute of Computing Technology, CAS
 *  For hash algorithm.
 *  Author : KinderRiven <hanshukai@ict.ac.cn>
 *  Date : 2018/2/4
 */
#ifndef HHASH_H_
#define HHASH_H_
#include "config.h"
#include "htype.h"
#include "hstatus.h"

// Hight 8 bits is key length.
#define HIKV_KEY_LENGTH(vec_length) ((vec_length >> 24))
// Low 24 bits is value length.
#define HIKV_VALUE_MASK  (((uint32_t)1 << 24) - 1)
#define HIKV_VALUE_LENGTH(kv_length_vec) ((kv_length_vec) & HIKV_VALUE_MASK)
// Combine key-value length.
#define HIKV_VEC_KV_LENGTH(key_length, value_length) \
    (((uint32_t)(key_length) << 24) | (uint32_t)(value_length))

// the count of items of per bucker.
#define NUM_ITEMS_PER_BUCKET 128
// the max partitions of per hashtable.
#define MAX_PARTITIONS_PER_HT 16

        
namespace hikv 
{
    // struct hash table's item
    typedef struct hash_table_item {
        uint32_t vec_length;      // key_size:[0~7], value_size:[8~31]
        uint8_t data[0];
    } hash_table_item_t;

    // struct bucket's item
    typedef struct bucket_item {
        uint64_t signature;
        uint64_t addr;
    } bucket_item_t;

    // struct hash table's bucket (Each bucket has some items)
    typedef struct partition_bucket {
        uint32_t version;
        struct partition_bucket *extra_bucket;
        struct bucket_item items[NUM_ITEMS_PER_BUCKET];
    } partition_bucket_t;
    
    // struct hash partition (Each partition has some buckets)
    typedef struct hash_table_partition {
        struct partition_bucket *buckets;       // buckets
        uint32_t num_buckets;                   // count buckets
    } hash_table_partition_t;

    // class hash table (Each hash-table has some partition)
    class HashTable 
    {
        public:
            HashTable(uint32_t num_partitions, uint32_t num_buckets);
            ~HashTable();
            Status Get(Slice &key, Slice &value);
            Status Put(Slice &key, Slice &value);
            Status Delete(Slice &key);
        private:
            uint16_t calc_partition_index(const char *key, size_t key_size);
            uint16_t calc_bucket_index(const char *key, size_t key_size);
            struct hash_table_partition *partitions[MAX_PARTITIONS_PER_HT];
            uint32_t num_partitions;
            uint32_t num_buckets;
    };
}
#endif
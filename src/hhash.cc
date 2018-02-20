#include "hhash.h"
#include "halgorithm.h"
#include "util.h"
#include "htool.h"
#include <iostream>
using namespace hikv;

// Function to compare keys.
// [return] : If equal return false, else return false.
static bool compare2keys(void *key1, uint32_t key1_len, void *key2, uint32_t key2_len) {
    return key1_len == key2_len && !memcmp(key1, key2, key1_len);
}

// Funcation to malloc a hashtable partition.
// [return] : the ptr of partition.
static struct hash_table_partition* malloc_one_partition(uint32_t num_buckets) {
    //calculate size of total buckets
    uint32_t total_buckets_size = \
            sizeof(struct partition_bucket) * ( num_buckets );
    
    //malloc one partition
    struct hash_table_partition* p_partition = \
            ( hash_table_partition* )malloc( sizeof( struct hash_table_partition ) );
    assert(p_partition != NULL);
    
    //malloc some buckets
    p_partition->buckets = ( struct partition_bucket* )malloc( total_buckets_size );
    assert(p_partition->buckets != NULL);
    return p_partition;
}

// Funcation to malloc a hashtable partition.
// [return] : the ptr of partition.
static bool free_one_partition(struct hash_table_partition* partition) {
    if(partition == NULL)
        return false;
    free(partition->buckets);
    return true;
}

// Function to lock bucket.
// bucket : which to be locked.
static void lock_bucket(struct partition_bucket *bucket) {
    while(true) {
        // Modify the lowest bit to 
        uint32_t v = *(volatile uint32_t *)&bucket->version & ~1U;
        uint32_t new_v = v | 1U;
        // compare & swap
        if (__sync_bool_compare_and_swap((volatile uint32_t *)&bucket->version, v, new_v))
			break;
    }
}

// Function to unlock bucket.
// bucket : which to be unlocked.
static void unlock_bucket(struct partition_bucket *bucket) {
    memory_barrier();
    // if value is zero mean unlock before but now has to unlock
    assert( ( *(volatile uint32_t *)&bucket->version & 1U ) == 1U );
    ( *(volatile uint32_t *)&bucket->version )++;
}

// Function to read a bucket version
// bucket : read version from it
static uint32_t read_begin_version(struct partition_bucket *bucket) {
    while(true) {
        uint32_t v = *(volatile uint32_t *)&bucket->version;
		memory_barrier();
        // if is not be locked.
        if ((v & 1U) != 0U)
			continue;
		return v;
    }
}

// Function to read a bucket version
static uint32_t read_end_version(struct partition_bucket *bucket) {
    memory_barrier();
    // if is not be locked.
    uint32_t v = *(volatile uint32_t *)&bucket->version;
	return v;
}

// Function to get item from bucket.
static uint32_t find_item_index(const char *key, size_t key_len, \
                                struct partition_bucket **bucket) {
    // Hash64 to compare with bucket signature.
    uint64_t _hash64 = Algorithm::hash64(key, key_len);
    struct partition_bucket *current_bucket = *bucket;
    while(true) {
        for(int i = 0; i < NUM_ITEMS_PER_BUCKET; i++) {
            if(current_bucket->items[i].signature != _hash64)
                continue;
            // Find a same signature.
            struct hash_table_item* item = (struct hash_table_item*)current_bucket->items[i].addr;
            // Compare two keys.
            if(!compare2keys( (void *)key, key_len, (void *)item->data, HIKV_KEY_LENGTH(item->vec_length) ))
                continue;
            *bucket = current_bucket;
            return i;
        }
        if( current_bucket-> extra_bucket == NULL )
            break;
        current_bucket = current_bucket->extra_bucket;
    }
    // 404 No Found.
    return NUM_ITEMS_PER_BUCKET;
}

// Function to get empty item from bucket
// bucket : to locate key-value 
static uint32_t find_empty_item_index(struct partition_bucket **bucket) {
    
    struct partition_bucket *current_bucket = *bucket;
    while(true) {
        for(int i = 0; i < NUM_ITEMS_PER_BUCKET; i++) {
            if(current_bucket->items[i].signature == 0) {
                *bucket = current_bucket;
                return i;
            }
        }
        if( current_bucket-> extra_bucket == NULL )
            break;
        current_bucket = current_bucket->extra_bucket;
    }
    return NUM_ITEMS_PER_BUCKET;
}

// Funcation to set item in bucket, and put key-value.
// [return] : if ok return true, else return false.
static bool put_bucket_item(const char *key,  size_t key_len, \
                        const char *value, size_t value_len, \
                        struct partition_bucket *bucket, uint32_t item_index) 
{
    uint64_t signature = Algorithm::hash64(key, key_len);
    bucket->items[item_index].signature = signature;
    uint32_t malloc_size = sizeof(struct hash_table_item) + (key_len + value_len);
    struct hash_table_item* addr = (struct hash_table_item*) malloc ( malloc_size );
    bucket->items[item_index].addr = (uint64_t) addr;
    
    // Copy data to the hashtable item.
    addr->vec_length = HIKV_VEC_KV_LENGTH(key_len, value_len);   
    memcpy((void *)addr->data, (void *) key, key_len);
    memcpy((void *)(addr->data + key_len), (void *) value, value_len);
    return true;
}

// Funcation to update key-value.
// [return] : if ok return true, else return false.
static bool update_bucket_item(const char *key, size_t key_len, \
                            const char *value, size_t value_len, \
                            struct partition_bucket *bucket, uint32_t item_index) 
{
    // Free the old hashtable item.
    struct hash_table_item *addr = (struct hash_table_item*) bucket->items[item_index].addr;
    free(addr);

    // Malloc a new hashtable item.
    uint32_t malloc_size = sizeof(struct hash_table_item) + (key_len + value_len);
    addr = (struct hash_table_item*) malloc ( malloc_size );
    addr->vec_length = HIKV_VEC_KV_LENGTH(key_len, value_len);
    memcpy((void *)addr->data, (void *) key, key_len);
    memcpy((void *)(addr->data + key_len), (void *) value, value_len);

    // Update the bucket.
    bucket->items[item_index].addr = (uint64_t)addr;
    return true;
}

// Funcation to update key-value.
// [return] : if ok return true, else return false.
static bool delete_bucket_item(struct partition_bucket *bucket, uint32_t item_index)
{
    // Free the old hashtable item.
    struct hash_table_item *addr = (struct hash_table_item*) bucket->items[item_index].addr;
    free(addr);
    // Clear the item of bucket.
    bucket->items[item_index].signature = 0;
    bucket->items[item_index].addr = 0;
    return true;
}

// Structure Function.
HashTable::HashTable(uint32_t num_partitions, uint32_t num_buckets) {
    // Ensure num_partions <= define max
    assert(num_partitions <= MAX_PARTITIONS_PER_HT);
    this->num_partitions = num_partitions;
    this->num_buckets = num_buckets;
    // malloc partitions
    for(int i = 0; i < num_partitions; i++) {
        this->partitions[i] = malloc_one_partition(num_buckets);
    }
}

// Destruct Function.
HashTable::~HashTable() {
    for(int i = 0; i < num_partitions; i++) {
        free_one_partition(partitions[i]);
        free(partitions[i]);
    }
}

// Function to calculate partition index
uint16_t HashTable::calc_partition_index(const char *key, size_t key_len) {
    return Algorithm::hash16(key, key_len) % num_partitions;
}

// Function to calculate bucket index
uint16_t HashTable::calc_bucket_index(const char *key, size_t key_len) {
    return Algorithm::hash32(key, key_len) % num_buckets;
}

// Put key-value into hashtable
// When we put kv in a bucket, we need to lock the bucket
Status HashTable::Put(Slice &s_key, Slice &s_value) { 
    Status status;
    // Here to calculate partition index.
    const char *key = s_key.data();
    size_t key_len = s_key.size();
    const char *value = s_value.data();
    size_t value_len = s_value.size();
    
    // Calculate the hash index of partition and bucket.
    uint16_t partition_index = calc_partition_index(key, key_len);
    uint32_t bucket_index = calc_bucket_index(key, key_len);

    // Partition can not be null.
    assert(partitions[partition_index] != NULL);

    // Here to calculate bucket index.
    struct partition_bucket *bucket = partitions[partition_index]->buckets + bucket_index;
    struct partition_bucket *tmp_bucket = bucket;
    
    // Lock bucket prepare to put key-value
    lock_bucket(bucket);
    uint32_t item_index = find_item_index(key, key_len, &tmp_bucket);
    
    bool res;
    if (item_index != NUM_ITEMS_PER_BUCKET) {
        // Find so we need to update.
        res = update_bucket_item(key, key_len, value, value_len, tmp_bucket, item_index);
        bucket = tmp_bucket;
        status.set_msg("Found and Update.");
    } else {
        item_index = find_empty_item_index(&bucket);
        if(item_index != NUM_ITEMS_PER_BUCKET) {
            res = put_bucket_item(key, key_len, value, value_len, bucket, item_index);
            status.set_msg("Not Found and Put.");
        } else {
            // TODO no space operator.
            res = false;
            status.set_msg("Error No Space");
        }
    }
    status.set_ok(res);
    // TODO Here to clflsh data in nvm to persist data.
    persist_data((void *)bucket, 16);
    // Unlock bucket.
    unlock_bucket(bucket);
    return status;
}

// Get key-value into hashtable
// Don't have to lock buclet, we only to use compare & swap to
// ensure the bucket didn't change during we search. ^_^
Status HashTable::Get(Slice &s_key, Slice &s_value) {
    Status status;
    const char *key = s_key.data();
    size_t key_len = s_key.size();
    uint16_t partition_index = calc_partition_index(key, key_len);
    // Partition can not be null.
    assert(partitions[partition_index] != NULL);
    
    // Here to calculate bucket index.
    uint32_t bucket_index = calc_bucket_index(key, key_len);
    struct partition_bucket *bucket = partitions[partition_index]->buckets + bucket_index;
    while(true)
    {
        uint32_t begin_version = read_begin_version(bucket);
        uint32_t item_index = find_item_index(key, key_len, &bucket);
        if(item_index == NUM_ITEMS_PER_BUCKET) {
            // If this item have been modified while reading.
            if(begin_version != read_end_version(bucket)) {
                continue;
            }
            // 404 No Found.
            status.set_msg("404 Not Found.");
            status.set_ok(false);
            break;
        }
        struct bucket_item bk_item = bucket->items[item_index];
        struct hash_table_item* ht_item = (struct hash_table_item*) bk_item.addr;
        uint32_t vec_length = ht_item->vec_length;
        uint32_t key_length = HIKV_KEY_LENGTH(vec_length);
        uint32_t value_length = HIKV_VALUE_LENGTH(vec_length);
        s_value.set((char *)ht_item->data + key_length, value_length);
        status.set_ok(true);
        status.set_msg("500 Found.");
        break;
    }
    return status;
}

// Delete key-value into hashtable
// We need to lock the bucket during we delete.
Status HashTable::Delete(Slice &s_key) {
    Status status;
    // Here to calculate partition index.
    const char *key = s_key.data();
    size_t key_len = s_key.size();

    // Calculate the hash index of partition and bucket.
    uint16_t partition_index = calc_partition_index(key, key_len);
    uint32_t bucket_index = calc_bucket_index(key, key_len);
    
    // Partition can not be null.
    assert(partitions[partition_index] != NULL);

    // Here to calculate bucket index.
    struct partition_bucket *bucket = partitions[partition_index]->buckets + bucket_index;
   
    // Lock bucket prepare to put key-value
    lock_bucket(bucket);
    uint32_t item_index = find_item_index(key, key_len, &bucket);

    if (item_index != NUM_ITEMS_PER_BUCKET) {
        bool res = delete_bucket_item(bucket, item_index);
        status.set_ok(res);
    } else {
        // 404 No Found.
        status.set_ok(false);
    }
    // Persist data
    persist_data((void *)bucket, 16);
    // Unlock bucket.
    unlock_bucket(bucket);
    return status; 
}
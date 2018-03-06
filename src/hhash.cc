#include "hhash.h"
#include "halgorithm.h"
#include "util.h"
#include "htool.h"
#include <iostream>
using namespace hikv;

// Function to compare keys.
// [return] : If equal return false, else return false.
static bool compare2keys(uint8_t *key1, uint32_t len1, uint8_t *key2, uint32_t len2) 
{
    return len1 == len2 && !memcmp(key1, key2, len1);
}

// Funcation to malloc a hashtable partition.
// [return] : the ptr of partition.
static struct hash_table_partition* malloc_one_partition(uint32_t num_buckets) 
{
    // calculate size of total buckets
    uint32_t total_buckets_size = sizeof(struct partition_bucket) * ( num_buckets );
    
    // malloc one partition
    struct hash_table_partition* p_partition = (struct hash_table_partition*) malloc (sizeof(struct hash_table_partition));
    assert(p_partition != NULL);
    memset(p_partition, 0, sizeof(struct hash_table_partition));
    
    // malloc some buckets
    p_partition->buckets = (struct partition_bucket*) malloc (total_buckets_size);
    assert(p_partition->buckets != NULL);
    memset(p_partition->buckets, 0, total_buckets_size);
    return p_partition;
}

// Funcation to malloc a hashtable partition.
// [return] : the ptr of partition.
static bool free_one_partition(struct hash_table_partition* partition) 
{
    if (partition == NULL) 
    {
        return false;
    }
    free(partition->buckets);
    return true;
}

// Function to lock bucket.
// bucket : which to be locked.
static void lock_bucket(struct partition_bucket *bucket, Status &status) 
{
#ifdef COLLECT_LOCK
    bool collision = false;
#endif
    while(true) 
    {
        // Modify the lowest bit to 
        uint32_t v = *(volatile uint32_t *)&bucket->version & ~1U;
        uint32_t new_v = v | 1U;
        // compare & swap
        if (__sync_bool_compare_and_swap((volatile uint32_t *)&bucket->version, v, new_v))
			break;
    #ifdef COLLECT_LOCK
        collision = true;
    #endif
    }
#ifdef COLLECT_LOCK
    status.hashtable_collision = collision;
#endif
}

// Function to unlock bucket.
// bucket : which to be unlocked.
static void unlock_bucket(struct partition_bucket *bucket) 
{
    memory_barrier();
    // if value is zero mean unlock before but now has to unlock
    assert( ( *(volatile uint32_t *)&bucket->version & 1U ) == 1U );
    ( *(volatile uint32_t *)&bucket->version )++;
}

// Function to persist KV
static void persist_kv_data(void *p, uint32_t length)
{
// if in_memory_kv, only need to emulate longer write latency of NVM.
#ifdef IN_MEMORY_KV
	double factor = 1 + ((double)length) / 64 * 2 / 16;
	uint64_t cpu_ticks = NVM_WRITE_LATENCY * 2.1 * factor;
	uint64_t start = rdtsc();
	while (rdtsc() < start + cpu_ticks) {}
// if persist kv store, need to flush cache, add longer write latency, and guarantee ordering.
#elif defined(HASHTABLE_PERSISTENCY)
	size_t flush_count = length / 64;
	size_t i = 0;
	for (i = 0; i <= flush_count; i++)
		clflush((uint8_t *)p + i * 64);
	double factor = 1 + ((double)length) / 64 * 2 / 16;
	uint64_t cpu_ticks = NVM_WRITE_LATENCY * 2.1 * factor;
	uint64_t start = rdtsc();
	while (rdtsc() < start + cpu_ticks) {}
	sfence();	
#endif
}

// Function to read a bucket version
// bucket : read version from it
static uint32_t read_begin_version(struct partition_bucket *bucket) 
{
    while(true) 
    {
        uint32_t v = *(volatile uint32_t *)&bucket->version;
		memory_barrier();
        // if is not be locked.
        if ((v & 1U) != 0U)
			continue;
		return v;
    }
}

// Function to read a bucket version
static uint32_t read_end_version(struct partition_bucket *bucket) 
{
    memory_barrier();
    // if is not be locked.
    uint32_t v = *(volatile uint32_t *)&bucket->version;
	return v;
}

// Function to get item from bucket.
// If find return true, [bucket][index] is we need,
// else return false, [bucket][index] is empty index.
static uint32_t find_bucket_item_index(const char *key, size_t key_len, \
                                struct partition_bucket **bucket) 
{
    // Hash64 to compare with bucket signature.
    uint64_t _hash64 = Algorithm::hash64(key, key_len);
    struct partition_bucket *current_bucket = *bucket;
    while(true)
    {
        for (int i = 0; i < NUM_ITEMS_PER_BUCKET; i++) 
        {
            if (current_bucket->items[i].signature != _hash64)
                continue;
            // find a same signature.
            struct hash_table_item* item = (struct hash_table_item*)current_bucket->items[i].addr;
            // compare two keys.
            if (!compare2keys( (uint8_t *)key, key_len, item->data, HIKV_KEY_LENGTH(item->vec_length) ))
                continue;
            *bucket = current_bucket;
            return i;
        }
        if ( current_bucket-> extra_bucket == NULL ) {
            break;
        }
        current_bucket = current_bucket->extra_bucket;
    }
    // 404 No Found.
    return NUM_ITEMS_PER_BUCKET;
}

// Function to get empty item from bucket
// bucket : to locate key-value
static uint32_t find_empty_bucket_item_index(struct partition_bucket **bucket) 
{
    struct partition_bucket *current_bucket = *bucket;
    while(true) 
    {
        for (int i = 0; i < NUM_ITEMS_PER_BUCKET; i++) 
        {
            if (current_bucket->items[i].signature == 0) 
            {
                *bucket = current_bucket;
                return i;
            }
        }
        if ( current_bucket->extra_bucket == NULL ) 
        {
            struct partition_bucket *new_bucket = (struct partition_bucket *) malloc (sizeof(struct partition_bucket));
            memset(new_bucket, 0, sizeof(struct partition_bucket));
            current_bucket->extra_bucket = new_bucket;                            
            *bucket = new_bucket;
            return 0;
        }
        current_bucket = current_bucket->extra_bucket;
    }
    return NUM_ITEMS_PER_BUCKET;
}

// Funcation to set item in bucket, and put key-value.
// [return] : if ok return true, else return false.
static bool put_kv_item(const char *key,  size_t key_len, \
                        const char *value, size_t value_len, \
                        struct partition_bucket *bucket, uint32_t item_index, \
                        Status &status) 
{
    uint64_t signature = Algorithm::hash64(key, key_len);
    bucket->items[item_index].signature = signature;

    // malloc is align 8 bytes to clflush
    uint32_t malloc_size = sizeof(struct hash_table_item) + ( NVMKV_ROUNDUP8(key_len) + NVMKV_ROUNDUP8(value_len) );
    struct hash_table_item* addr = (struct hash_table_item*) malloc (malloc_size);
    
    // memset item 0
    memset(addr, 0, malloc_size);
    bucket->items[item_index].addr = (uint64_t) addr;

    // vopy data to the hashtable item.
    addr->vec_length = HIKV_VEC_KV_LENGTH(key_len, value_len);   
    memcpy(addr->data, (uint8_t *)key, key_len);
    memcpy(addr->data + NVMKV_ROUNDUP8(key_len), (uint8_t *) value, value_len);

    status.v_addr = (uint64_t)addr;
    status.hash64 = signature;
#ifdef PERSIST_DATA
    persist_kv_data(&addr->vec_length, sizeof(uint32_t) + key_len + value_len);
#endif
    return true;
}

// Funcation to update key-value.
// [return] : if ok return true, else return false.
static bool update_kv_item(const char *value, size_t value_len, \
                struct partition_bucket *bucket, uint32_t item_index) 
{
    // Free the old hashtable item.
    struct hash_table_item *addr = (struct hash_table_item*) bucket->items[item_index].addr;
    size_t key_len = HIKV_KEY_LENGTH(addr->vec_length);
    memcpy(addr->data + NVMKV_ROUNDUP8(key_len), (uint8_t *)value, value_len);
    
    // persist data
#ifdef PERSIST_DATA
    persist_data((void *)(addr->data + NVMKV_ROUNDUP8(key_len)), value_len);
#endif
    return true;
}

// Funcation to update key-value.
// [return] : if ok return true, else return false.
static bool delete_kv_item(struct partition_bucket *bucket, uint32_t item_index)
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
HashTable::HashTable(uint32_t num_partitions, uint32_t num_buckets) 
{
    // Ensure num_partions <= define max
    assert(num_partitions <= MAX_PARTITIONS_PER_HT);
    this->num_partitions = num_partitions;
    this->num_buckets = num_buckets;
    
    // malloc partitions
    for(int i = 0; i < num_partitions; i++) 
    {
        this->partitions[i] = malloc_one_partition(num_buckets);
    }
}

// Print table message
void HashTable::Print()
{
    printf("[HashTable]\n");
    printf("[1]partition count : %d, [2] buckets/partition : %d, [3] item count : %d\n", \
            this->num_partitions, this->num_buckets, \
            this->num_partitions * this->num_buckets * NUM_ITEMS_PER_BUCKET);
    printf("[4]hashtable index size : %llu MB\n", \
        (uint64_t)this->num_partitions * this->num_buckets * NUM_ITEMS_PER_BUCKET \
            * sizeof(bucket_item) / (1024 * 1024));
}

// Destruct Function.
HashTable::~HashTable() 
{
    for(int i = 0; i < num_partitions; i++) 
    {
        free_one_partition(partitions[i]);
        free(partitions[i]);
    }
}

// Function to calculate partition index
uint16_t HashTable::calc_partition_index(const char *key, size_t key_len) 
{
    return Algorithm::hash16(key, key_len) % num_partitions;
}

// Function to calculate bucket index
uint16_t HashTable::calc_bucket_index(const char *key, size_t key_len) 
{
    return Algorithm::hash32(key, key_len) % num_buckets;
}

// Put key-value into hashtable
// When we put kv in a bucket, we need to lock the bucket
Status HashTable::Put(Slice &s_key, Slice &s_value) 
{
    Status status;
    // save status
#ifdef COLLECT_RDTSC
    uint64_t st_start, st_end;
#endif
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

 // [0] collect lock bucket time
#ifdef COLLECT_RDTSC
    st_start = rdtsc();
#endif
    // lock bucket prepare to put key-value
    lock_bucket(bucket, status);
#ifdef COLLECT_RDTSC
    st_end = rdtsc();
    status.append_rdt(st_end - st_start);
#endif

// [1] collect find if exist item
#ifdef COLLECT_RDTSC
    st_start = rdtsc();
#endif
    // find item if exist
    uint32_t item_index = find_bucket_item_index(key, key_len, &tmp_bucket);
#ifdef COLLECT_RDTSC
    st_end = rdtsc();
    status.append_rdt(st_end - st_start);
#endif
    bool res;
    if (item_index != NUM_ITEMS_PER_BUCKET) 
    {
        // find so we need to update.
        res = update_kv_item(value, value_len, tmp_bucket, item_index);
    }
    else 
    {
    // [2] collect find empty item time.
    #ifdef COLLECT_RDTSC
        st_start = rdtsc();
    #endif
        // Here to find empty item.
        item_index = find_empty_bucket_item_index(&tmp_bucket);
    #ifdef COLLECT_RDTSC
        st_end = rdtsc();
        status.append_rdt(st_end - st_start);
    #endif
        if (item_index != NUM_ITEMS_PER_BUCKET) 
        {
        // [3] collect put bucket item time
        #ifdef COLLECT_RDTSC
            st_start = rdtsc();
        #endif
            // put bucket item
            res = put_kv_item(key, key_len, value, value_len, tmp_bucket, item_index, status);
        #ifdef COLLECT_RDTSC
            st_end = rdtsc();
            status.append_rdt(st_end - st_start);
        #endif
        } 
        else 
        {
            // TODO no space operator.
            res = false;
        }
    }
    status.set_ok(res);
// [4] collect persist data time
#ifdef COLLECT_RDTSC
    st_start = rdtsc();
#endif
    // Here to clflsh data in nvm to persist data.
#ifdef PERSIST_DATA
    if (status.is_ok()) 
    {
        persist_data((void *)(tmp_bucket->items + item_index), BUCKET_ITEM_LENGTH);
    }
#endif
#ifdef COLLECT_RDTSC
    st_end = rdtsc();
    status.append_rdt(st_end - st_start);
#endif

// [5] collect unlock bucket time
#ifdef COLLECT_RDTSC
    st_start = rdtsc();
#endif
    // Unlock bucket.
    unlock_bucket(bucket);
#ifdef COLLECT_RDTSC
    st_end = rdtsc();
    status.append_rdt(st_end - st_start);
#endif
    return status;
}

// Get key-value into hashtable
// Don't have to lock buclet, we only to use compare & swap to
// ensure the bucket didn't change during we search. ^_^
Status HashTable::Get(Slice &s_key, Slice &s_value) 
{
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
        uint32_t item_index = find_bucket_item_index(key, key_len, &bucket);
        if (item_index == NUM_ITEMS_PER_BUCKET) 
        {
            // If this item have been modified while reading.
            if(begin_version != read_end_version(bucket))
                continue;
            // 404 No Found.
            // status.set_msg("404 Not Found.");
            status.set_ok(false);
            break;
        }
        struct bucket_item bk_item = bucket->items[item_index];
        struct hash_table_item* ht_item = (struct hash_table_item*) bk_item.addr;
        
        uint32_t vec_length   = ht_item->vec_length;
        uint32_t key_length   = HIKV_KEY_LENGTH(vec_length);
        uint32_t value_length = HIKV_VALUE_LENGTH(vec_length);
        
        s_value.set((char *)ht_item->data + NVMKV_ROUNDUP8(key_length), value_length);
        status.set_ok(true);
        // status.set_msg("500 Found.");
        break;
    }
    return status;
}

// Delete key-value into hashtable
// We need to lock the bucket during we delete.
Status HashTable::Delete(Slice &s_key) 
{
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
    lock_bucket(bucket, status);
    uint32_t item_index = find_bucket_item_index(key, key_len, &bucket);

    if (item_index != NUM_ITEMS_PER_BUCKET) {
        bool res = delete_kv_item(bucket, item_index);
        status.set_ok(res);
    } 
    else {
        // 404 No Found.
        status.set_ok(false);
    }
    // Persist data
    if (status.is_ok()) {
        persist_data((void *)(bucket->items + item_index), BUCKET_ITEM_LENGTH);
    }
    // Unlock bucket.
    unlock_bucket(bucket);
    return status; 
}
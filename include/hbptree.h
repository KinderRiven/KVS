/*
 *  Description :
 *  Copyright (C) 2018 Institute of Computing Technology, CAS
 *  B+Tree
 *  Author : KinderRiven <hanshukai@ict.ac.cn>
 *  Date : 2018/3/4
 */

#ifndef HBPTREE_H_
#define HBPTREE_H_

#include "htype.h"
#include "util.h"
#include <queue>
#include <string>
#include <pthread.h>
#include <stack>
#include "bptree/bptree.h"

using namespace nvmkv;
using namespace std;

namespace hikv
{
    // struct : DATA
    // data used for HBTree (BpTree)
    // value_addr is kv_item address
    struct DATA 
    {
        uint64_t value_addr;    // address of value
        DATA() 
        {
            value_addr = 0;
        }
        DATA(uint64_t v) 
        {
            this->value_addr = v;
        }
        bool operator == (DATA &d) {
            return this->value_addr == d.value_addr;
        };
    };
    
    // struct : KEY
    // key used for HBTree (BpTree)
    struct KEY 
    {
        uint32_t key_length;
        uint8_t  key[MAX_KEY_LENGTH];

        KEY()
        {
            key_length = 0;
        }

        KEY(const char *c_str)
        {
            key_length = strlen(c_str);
            memcpy((void *)key, (void *)c_str, key_length);
        }

        KEY(std::string &s) 
        {
            key_length = s.size();
            memcpy((void *)key, (void *)s.data(), key_length);
        }

        void print()
        {
            for(int i = 0; i < key_length; i++) {
                printf("%c", key[i]);
            }
        }

        bool operator < (KEY &k) 
        {
            if (key_length < k.key_length) {
                return memcmp(key, k.key, key_length) <= 0 ? true : false;
            } else {
                return memcmp(key, k.key, k.key_length) < 0 ? true : false;
            }
        };
        
        bool operator <= (KEY &k) 
        {
            if (key_length <= k.key_length) {
                return memcmp(key, k.key, key_length) <= 0 ? true : false;
            } else {
                return memcmp(key, k.key, k.key_length) < 0 ? true : false;
            }
        };
        
        bool operator > (KEY &k) 
        {
            if (key_length <= k.key_length) {
                return memcmp(key, k.key, key_length) > 0 ? true : false;
            } else {
                return memcmp(key, k.key, k.key_length) >= 0 ? true : false;
            }
        };
        
        bool operator >= (KEY &k) 
        {
            if (key_length < k.key_length) {
                return memcmp(key, k.key, key_length) > 0 ? true : false;
            } else {
                return memcmp(key, k.key, k.key_length) >= 0 ? true : false;
            }
        };
        
        bool operator == (KEY &k) 
        {
            return key_length == k.key_length && !memcmp(key, k.key, key_length);
        };
        
        // memcmp two keys.
        friend int key_memcmp(KEY &k1, KEY &k2)
        {
            if(k1.key_length == k2.key_length) 
            {
                return memcmp((void *)k1.key, (void *)k2.key, k1.key_length);
            }
            else if(k1.key_length < k2.key_length)
            {
                return memcmp((void *)k1.key, (void *)k2.key, k1.key_length) <= 0 ? -1 : 1;
            } 
            else
            {
                return memcmp((void *)k1.key, (void *)k2.key, k2.key_length) < 0 ? -1 : 1;
            }
        }
    };
    

    // Class : HBpTree
    // HTM-based b + tree implementation, encapsulated in the previously
    // mature code, add a more concise interface
    class HBpTree
    {
        private:
            Bptree *bp;
        public:
            HBpTree() 
            {
                bp = new Bptree();
            }

            ~HBpTree()
            {
                delete(bp);
            }

            bool is_null()
            {
                return bp->is_null();
            }

            bool Put(KEY &key, DATA &data) 
            {
                bool res = Bptree::bptree_put(this->bp, key.key, key.key_length, data.value_addr);
                return res;
            }

            bool Get(KEY &key, DATA &data) 
            {
                uint64_t value = Bptree::bptree_get(this->bp, key.key, key.key_length);
                if (value == -1L) {
                    return false;
                } else {
                    data.value_addr = value;
                    return true;
                }
            }

            bool Delete(KEY &key)
            {
                bool res = Bptree::bptree_delete(this->bp, key.key, key.key_length);
                return res;
            }
            
            bool Scan(KEY &lower, KEY &upper, std::vector<Slice> &values)
            {
                vector<uint64_t> addrs;
                vector<string> keys;
                bool res = Bptree::bptree_scan(this->bp, lower.key, lower.key_length, \
                                upper.key, upper.key_length, keys, addrs);
                return true;
            }
            
            bool ScanAll(std::vector<Slice> &values)
            {
                vector<uint64_t> addrs;
                vector<string> keys;
                Bptree::bptree_scan_all_leaf(bp, keys, addrs);
                return true;
            }
    };
}


#endif
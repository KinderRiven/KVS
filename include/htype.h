/*
 *  Description :
 *  Copyright (C) 2018 Institute of Computing Technology, CAS
 *  Some type.
 *  Author : KinderRiven <hanshukai@ict.ac.cn>
 *  Date : 2018/2/4
 */
#ifndef HTYPE_H_
#define HTYPE_H_

#include <utility>
#include <string>
#include <cstring>
#include <assert.h>
#include <stdlib.h>
#include <iostream>

#ifndef NULL
    #define NULL 0
#endif
#ifndef TRUE
    #define TRUE 1
    #define FALSE 0
#endif

typedef unsigned long long uint64_t;
typedef long long  int64_t;
typedef unsigned int uint32_t;
typedef int int32_t;
typedef unsigned short uint16_t;
typedef short int16_t;
typedef unsigned char uint8_t;
typedef signed char int8_t;
typedef unsigned long size_t;
typedef std::pair<uint64_t, uint64_t> uint128_t;

namespace hikv{
    class Slice {
        public:
            // Create an empty slice.
            Slice() : data_(""), size_(0){};
            Slice(const char *d, size_t s) : data_(d), size_(s){}
            Slice(std::string &s) : data_(s.data()), size_(s.size()){}
            Slice(const char *s) : data_(s), size_(strlen(s)){}
            // Return value.
            const char* data() const {
                return data_;
            }
            size_t size() const {
                return size_;
            }
            // Set value.
            void set(const char *data, size_t size) {
                data_ = data, size_ = size;
            }
            void set_data(char *data) { 
                data_ = data; 
            }
            void set_size(size_t size) { 
                size_ = size; 
            }
            void set(std::string) {}
            // Overload operator.
            char operator[](size_t n) const {
                assert(n < size());
                return data_[n];
            }
            friend std::ostream& operator << (std::ostream &output, Slice &s) {
                for(int i = 0; i < s.size(); i++) {
                    output << s.data()[i];
                }
                return output;
            }
        private:
            size_t size_;
            const char *data_;
    };
    inline bool operator == (const Slice &x, const Slice &y) {
        return ((x.size() == y.size()) && (memcmp(x.data(), y.data(), x.size()) == 0));
    }
}
#endif
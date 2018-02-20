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
    #define SLICE_SIZE 2048
    class Slice {
        public:
            // Create an empty slice.
            Slice() {
                data_ = (char *)malloc(SLICE_SIZE);
                size_ = 0;
            };
            // Copy
            Slice(const Slice &s) {
                this->size_ = s.size_;
                this->data_ = (char *)malloc(SLICE_SIZE);
                memcpy((void *)this->data_, (void *)s.data_, SLICE_SIZE);
            }
            Slice(const char *d, size_t s) {
                data_ = (char *)malloc(SLICE_SIZE);
                size_ = s;
                memcpy((void *)data_, (void *)d, size_);
            }
            Slice(std::string &s) {
                data_ = (char *)malloc(SLICE_SIZE);
                size_ = s.size();
                memcpy((void *)data_, (void *)s.data(), size_);
            }
            Slice(const char *s) {
                data_ = (char *)malloc(SLICE_SIZE);
                size_ = strlen(s);
                memcpy((void *)data_, (void *)s, size_);
            }
            ~Slice() {
                free(data_);
            }
            // Return value.
            const char* data() const {
                return data_;
            }
            size_t size() const {
                return size_;
            }
            // Set value.
            void set(const char *data, size_t size) {
                size_ = size;
                memcpy((void *)data_, (void *)data, size_);
            }
            void set(const char *data) {
                size_ = strlen(data_);
                memcpy((void *)data_, (void *)data, size_);
            }
            void set(std::string s) {
                size_ = s.size();
                memcpy((void *)data_, (void *)s.data(), size_);
            }
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
            char *data_;
    };
    inline bool operator == (const Slice &x, const Slice &y) {
        return ((x.size() == y.size()) && (memcmp(x.data(), y.data(), x.size()) == 0));
    }
}
#endif
/*
 *  Description :
 *  Copyright (C) 2018 Institute of Computing Technology, CAS
 *  TestFactory.
 *  Author : KinderRiven <hanshukai@ict.ac.cn>
 *  Date : 2018/2/3
 */

#ifndef HTEST_H_
#define HTEST_H_

namespace hikv {
    #ifndef HASH_PUT
        #define HASH_PUT 1
        #define HASH_GET 2
        #define HASH_DEL 3
    #endif
    class TestHashTableFactory {
        public:
            TestHashTableFactory(const char *data_in, int num_partitions, int num_buckets) \
                : data_in(data_in), num_partitions(num_partitions), num_buckets(num_buckets) {};
            ~TestHashTableFactory();
            void single_thread_test();
            void multi_thread_test();
        private:
            const char *data_in;
            int num_partitions;
            int num_buckets;
    };
}

#endif //NVM_KV_TEST_H

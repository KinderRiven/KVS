#include "htest.h"
#include <vector>
using namespace std;
using namespace hikv;

// Function to use single thread test 
void TestHashTableFactory::single_thread_test() 
{
    int opt, opt_count = 0;
    double time_count = 0;
    struct timeval begin_time, end_time;    // count time
    pid_t pid;
    char key[1024], value[1024];
    Status status;
    vector<string> vec_key, vec_value;
    
    HashTable *ht = new HashTable(num_partitions, num_buckets); 
    ifstream in(data_in);
    
    // Show Message.
    pid = getpid();
    printf("[HashTable test is running]\n");
    printf("[Input filename] : %s\n[PID] : %d\n", data_in, pid);
    
    while(in >> opt) 
    {
        in >> key >> value;
        vec_key.push_back(key);
        vec_value.push_back(value);
        Slice s_key = Slice(key), s_value = Slice(value);
        opt_count++;
        gettimeofday(&begin_time, NULL);
        switch(opt) 
        {
            case TEST_PUT:
                status = ht->Put(s_key, s_value);
                break;
            default:
                cout << "Illegal Parameters!" << endl;
                break;
        }
        gettimeofday(&end_time, NULL);
        assert(status.is_ok() == true);
        double exe_time = (double)(1000000.0 * ( end_time.tv_sec - begin_time.tv_sec ) + \
                    end_time.tv_usec - begin_time.tv_usec) / 1000000.0;
        time_count += exe_time;
    }
    // show result
    printf("[Result] opt count : %d, time_count : %.5f s, iops : %.5f\n", \
            opt_count, time_count, (double) opt_count / time_count);
    // validation
    int vec_size = vec_key.size();
    for(int i = vec_size - 1; i >= 0; i--)
    {
        Slice key = Slice(vec_key[i]);
        Slice value;
        status = ht->Get(key, value);
        cout << status << endl;
        assert(status.is_ok() == true);
    }
    delete(ht);
    in.close();
}
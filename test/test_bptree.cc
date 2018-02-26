#include "htest.h"
#include <vector>
using namespace hikv;
using namespace std;

void TestBpTreeFactory::single_thread_test()
{
    int opt, opt_count = 0;
    double time_count = 0;
    struct timeval begin_time, end_time;    // count time
    pid_t pid;
    string key, value;
    vector<string> vec_key, vec_value;

    BpTree *bp = new BpTree();
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
        KEY bp_key = KEY(key);
        DATA bp_data;
        opt_count++;
        gettimeofday(&begin_time, NULL);
        switch(opt) 
        {
            case TEST_PUT:
                bp->Put(bp_key, bp_data);
                break;
            default:
                cout << "Illegal Parameters!" << endl;
                break;
        }
        gettimeofday(&end_time, NULL);
        //Execute time (second).
        double exe_time = (double)(1000000.0 * ( end_time.tv_sec - begin_time.tv_sec ) + \
                    end_time.tv_usec - begin_time.tv_usec) / 1000000.0;
        time_count += exe_time;
    }
    // Show result.
    printf("[Result] opt count : %d, time_count : %.5f s, iops : %.5f\n", \
            opt_count, time_count, (double) opt_count / time_count);
    bp->BFS_Print();
}
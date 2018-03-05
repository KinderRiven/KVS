#include "htest.h"
using namespace hikv;

TestDataSet::TestDataSet()
{

}

void TestDataSet::init()
{
      for(int i = 0; i < MAX_TEST_THREAD;i ++) 
      {
          vec_opt[i].clear();
          vec_key[i].clear();
          vec_value[i].clear();
      }
      data_count = 0;
}

void TestDataSet::read_data(const char *file_name, int num_vector)
{  
    printf("[RUNNING] prepare read data...\n");   
    int opt, count = 0;
    int all = 0;
    string key, value;
    ifstream in(file_name);

    while(in >> opt) 
    {
        in >> key >> value;
        vec_opt[count].push_back(opt);
        vec_key[count].push_back(key);
        vec_value[count].push_back(value);
        count = (count + 1) % num_vector;
        all++;
    }
    in.close(); 
    this->data_count = all;
    printf("[OK] Read data OK (%d)\n", all);
}

void TestDataSet::generate_data(int num_kvs, int key_length, int value_length, int num_vector)
{
    printf("[RUNNING] Prepare gernerate data...\n");  
    int opt, count = 0;
    int all = 0;
    string key, value;
    static const char alphabet[] =
    			"abcdefghijklmnopqrstuvwxyz"
    			"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    			"0123456789";

    random_device rd;
    mt19937 gen(rd());
    uniform_int_distribution<> range1( key_length, key_length);
    uniform_int_distribution<> range2( value_length, value_length );
    uniform_int_distribution<> range3( 0, sizeof(alphabet) / sizeof(*alphabet) - 2 );
    
    for(int i = 0; i < num_kvs; i++) 
    {
        int key_length = range1(gen);
        int value_length = range2(gen);
        string s_key = "", s_value = "";
        
        for(int i = 0; i < key_length; i++) 
        {
            s_key += alphabet[range3(gen)];
        }
        for(int i = 0; i < value_length; i++) 
        {
            s_value += alphabet[range3(gen)];
        }
        vec_opt[count].push_back(1);
        vec_key[count].push_back(s_key);
        vec_value[count].push_back(s_value);
        count = (count + 1) % num_vector;
        all++;
    }
    this->data_count = all;
    printf("[OK] Generate data OK (%d)\n", all);
}
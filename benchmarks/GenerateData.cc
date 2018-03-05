/*
 *  Description :
 *  Copyright (C) 2018 Institute of Computing Technology, CAS
 *  Generate Key-Value Data to test KVS.
 *  Author : KinderRiven <hanshukai@ict.ac.cn>
 *  Date : 2018/2/7
 */
#include <fstream>
#include <iostream>
#include <string>
#include <cstdlib>
#include <limits>
#include <random>
#include <algorithm>
#include <cstring>

using namespace std;

#define HASH_PUT 1
#define HASH_GET 2
#define HASH_DEL 3

static const char alphabet[] =
    			"abcdefghijklmnopqrstuvwxyz"
    			"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    			"0123456789";

struct GenerateDataConfig 
{
    int key_size_shortest;          // key size range  
    int key_size_longest;       
    int value_size_shortest;        // value size range
    int value_size_longest;
    const char *output_filename;    // output filename
    unsigned long long file_size;   // file size
};

// Class to generate data.
class GenerateDataFactory 
{
    private:
        GenerateDataConfig config;
    public:
        GenerateDataFactory() {};
        GenerateDataFactory(GenerateDataConfig config) : config(config) {};
        ~GenerateDataFactory();
        void load_config(GenerateDataConfig config);
        void generate_data();
};

// Function to generate data.
void GenerateDataFactory::generate_data() 
{  
    ofstream os;
    os.open(config.output_filename);
    // std random lib
    random_device rd;
    mt19937 gen(rd());
    uniform_int_distribution<> range1( config.key_size_shortest, config.key_size_longest );
    uniform_int_distribution<> range2( config.value_size_shortest, config.value_size_longest );
    uniform_int_distribution<> range3( 0, sizeof(alphabet) / sizeof(*alphabet) - 2 );

    int current_size = 0, key_len, value_len;
    while(current_size < config.file_size) 
    {
        os << HASH_PUT << ' ';
        key_len = range1(gen), value_len = range2(gen);
        // generate key data.
        for(int i = 0; i < key_len; i++) 
        {
            os << alphabet[range3(gen)];
        }
        os << ' ';
        // generate value data.
        for(int i = 0; i < value_len; i++) 
        {
            os << alphabet[range3(gen)];
        }
        os << endl;
        current_size += (key_len + value_len);
    }
    os.close();
}

GenerateDataConfig config = {16, 16, 128, 128, "data_100M.in", 100 * 1024 * 1024};
int main(int argc, char *argv[]) 
{
    GenerateDataFactory *factory = new GenerateDataFactory(config);
    if(argc == 2) {
        config.output_filename = argv[1];
        config.file_size = atol(argv[2]) * 1024 * 1024; //MB
    }
    factory->generate_data();
}
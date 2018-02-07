#include "htest.h"
#include "htype.h"
#include "hhash.h"
#include <iostream>

using namespace hikv;
using namespace std;

int main(int argc, char *argv[]) {
    TestHashTableFactory *factory = new TestHashTableFactory(argv[1], 16, 128);
    factory->single_thread_test();
    return 0;
}

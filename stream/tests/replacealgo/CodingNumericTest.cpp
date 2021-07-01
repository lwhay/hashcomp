//
// Created by iclab on 6/30/21.
//
#include <iostream>

using namespace std;

int main(int argc, char **argv) {
    float f0 = 0.000000000000000000000000000000000000000000001, f1 = 0.1, f2 = 1.0, f3 = 10., f4 = 100.0;
    float *fp1 = &f1;
    cout << hex << *((int *) &f0) << " " << hex << *((int *) &f1) << " " << hex << *((int *) &f2) << " " << hex
         << *((int *) &f3) << " " << hex << *((int *) &f4) << endl;
}

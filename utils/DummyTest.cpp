//
// Created by iclab on 10/7/19.
//

#include <atomic>
#include <bitset>
#include <iostream>

using namespace std;

int main(int argc, char **argv) {
    std::bitset<128> bs(0);
    for (int i = 0; i < 128; i++) if (i % 2 == 0) bs.set(i);
    for (int i = 0; i < 128; i++) if (bs.test(i)) cout << "1"; else cout << "0";
    cout << endl;
    cout << bs.to_string() << endl;
    atomic<long long> tick(0);
    cout << tick.load() << endl;
    tick.fetch_add(1);
    cout << tick.load() << endl;
    u_char a = 0x3;
    u_char b = 0x6;
    cout << "0x3 and 0x0: " << (a and 0x0) << endl;
    cout << "0x3 and 0x6: " << (a and b) << endl;
    cout << "0x3 & 0x6: " << (a & b) << endl;
    cout << "0x3 or 0x6: " << (a or b) << endl;
    cout << "0x3 | 0x6: " << (a | b) << endl;
    cout << "0x3 xor 0x6: " << (a xor b) << endl;
    cout << "~0x6: " << (0xff & (~b)) << endl;
    u_char c = a;
    c &= b;
    cout << "0x3 &= 0x6: " << (0xff & c) << endl;
    c = a;
    c |= b;
    cout << "0x3 |= 0x6: " << (0xff & c) << endl;
    c = (a and b);
    cout << "0x3 and= 0x6: " << (0xff & c) << endl;
    c = (a or b);
    cout << "0x3 and= 0x6: " << (0xff & c) << endl;
}
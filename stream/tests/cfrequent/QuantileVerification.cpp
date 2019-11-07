//
// Created by Michael on 11/6/19.
//

#include <iostream>
#include <map>
#include <vector>
#include "LossyCount.h"

using namespace std;

class Stats {
public:
    Stats() : dU(0.0), dQ(0.0), dP(0.0), dR(0.0), dF(0.0), dF2(0.0) {}

    double dU, dQ;
    double dP, dR, dF, dF2;
    std::multiset<double> P, R, F, F2;
};

size_t RunExact(uint32_t thresh, std::vector<uint32_t> &exact) {
    size_t hh = 0;

    for (size_t i = 0; i < exact.size(); ++i)
        if (exact[i] >= thresh) ++hh;

    return hh;
}

void CheckOutput(std::map<uint32_t, uint32_t> &res, uint32_t thresh, size_t hh, Stats &S,
                 const std::vector<uint32_t> &exact) {
    if (res.empty()) {
        S.F.insert(0.0);
        S.F2.insert(0.0);
        S.P.insert(100.0);
        S.dP += 100.0;

        if (hh == 0) {
            S.R.insert(100.0);
            S.dR += 100.0;
        } else
            S.R.insert(0.0);

        return;
    }

    size_t correct = 0;
    size_t claimed = res.size();
    size_t falsepositives = 0;
    double e = 0.0, e2 = 0.0;

    std::map<uint32_t, uint32_t>::iterator it;
    for (it = res.begin(); it != res.end(); ++it) {
        if (exact[it->first] >= thresh) {
            ++correct;
            uint32_t ex = exact[it->first];
            double diff = (ex > it->second) ? ex - it->second : it->second - ex;
            e += diff / ex;
        } else {
            ++falsepositives;
            uint32_t ex = exact[it->first];
            double diff = (ex > it->second) ? ex - it->second : it->second - ex;
            e2 += diff / ex;
        }
    }

    if (correct != 0) {
        e /= correct;
        S.F.insert(e);
        S.dF += e;
    } else
        S.F.insert(0.0);

    if (falsepositives != 0) {
        e2 /= falsepositives;
        S.F2.insert(e2);
        S.dF2 += e2;
    } else
        S.F2.insert(0.0);

    double r = 100.0;
    if (hh != 0) r = 100.0 * ((double) correct) / ((double) hh);

    double p = 100.0 * ((double) correct) / ((double) claimed);

    S.R.insert(r);
    S.dR += r;
    S.P.insert(p);
    S.dP += p;
}

void PrintOutput(char *title, size_t size, const Stats &S, size_t u32NumberOfPackets) {
    double p5th = -1.0, p95th = -1.0, r5th = -1.0, r95th = -1.0, f5th = -1.0, f95th = -1.0, f25th = -1.0, f295th = -1.0;
    size_t i5, i95;
    std::multiset<double>::const_iterator it;

    if (!S.P.empty()) {
        it = S.P.begin();
        i5 = S.P.size() * 0.05;
        for (size_t i = 0; i < i5; ++i) ++it;
        p5th = *it;
        i95 = S.P.size() * 0.95;
        for (size_t i = 0; i < (i95 - i5); ++i) ++it;
        p95th = *it;
    }

    if (!S.R.empty()) {
        it = S.R.begin();
        i5 = S.R.size() * 0.05;
        for (size_t i = 0; i < i5; ++i) ++it;
        r5th = *it;
        i95 = S.R.size() * 0.95;
        for (size_t i = 0; i < (i95 - i5); ++i) ++it;
        r95th = *it;
    }

    if (!S.F.empty()) {
        it = S.F.begin();
        i5 = S.F.size() * 0.05;
        for (size_t i = 0; i < i5; ++i) ++it;
        f5th = *it;
        i95 = S.F.size() * 0.95;
        for (size_t i = 0; i < (i95 - i5); ++i) ++it;
        f95th = *it;
    }

    if (!S.F2.empty()) {
        it = S.F2.begin();
        i5 = S.F2.size() * 0.05;
        for (size_t i = 0; i < i5; ++i) ++it;
        f25th = *it;
        i95 = S.F2.size() * 0.95;
        for (size_t i = 0; i < (i95 - i5); ++i) ++it;
        f295th = *it;
    }

    printf("%s\t%1.2f\t\t%d\t\t%1.2f\t%1.2f\t%1.2f\t%1.2f\t%1.2f\t%1.2f\t%1.2f\t%1.2f\t%1.2f\t%1.2f\t%1.2f\t%1.2f\n",
           title, u32NumberOfPackets / S.dU, size,
           S.dR / S.R.size(), r5th, r95th,
           S.dP / S.P.size(), p5th, p95th,
           S.dF / S.F.size(), f5th, f95th,
           S.dF2 / S.F2.size(), f25th, f295th
    );
}

constexpr uint32_t itemsize = 1000000;

constexpr double fPhi = 0.01;

int main(int argc, char **argv) {
    uint32_t u32DomainSize = itemsize;
    std::vector<uint32_t> exact(u32DomainSize + 1, 0);
    uint32_t thresh = fPhi * itemsize;
    LCL_type *lcl = LCL_Init(fPhi);
    for (int i = 0; i < itemsize; i++) {
        exact[i]++;
        LCL_Update(lcl, i, 1);
    }
    map<uint32_t, uint32_t> output = LCL_Output(lcl, thresh);
    map<uint32_t, uint32_t>::iterator iter = output.begin();
    while (iter != output.end()) {
        cout << iter->first << " " << iter->second << endl;
        iter++;
    }
    cout << "***********************" << endl;
    size_t hh = RunExact(thresh, exact);
    Stats S;
    CheckOutput(output, thresh, hh, S, exact);
    printf("\nMethod\tUpdates/ms\tSpace\t\tRecall\t5th\t95th\tPrecis\t5th\t95th\tFreq RE\t5th\t95th\n");
    PrintOutput("LCL", LCL_Size(lcl), S, itemsize);
    return 0;
}
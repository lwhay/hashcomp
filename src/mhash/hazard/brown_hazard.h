//
// Created by iclab on 12/29/19.
//

#ifndef HASHCOMP_BROWN_HAZARD_H
#define HASHCOMP_BROWN_HAZARD_H

#include <atomic>
#include <cassert>
#include "ihazard.h"
#include "brown/reclaimer_hazardptr.h"

class brown_hazard : public ihazard {
private:
    reclaimer_hazardptr<uint64_t, nullptr> bhazard;
};

#endif //HASHCOMP_BROWN_HAZARD_H

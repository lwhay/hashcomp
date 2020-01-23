//
// Created by iclab on 1/23/20.
//

#include <cpuid.h>
#include "pmalloc.h"

#define MASK(msb, lsb) (~((~0) << (msb + 1)) & ((~0) << lsb))
#define EXTRACT(val, msb, lsb) ((MASK(msb, lsb) & val) >> lsb)
#define MODEL(eax) EXTRACT(eax, 7, 4)
#define EXTENDED_MODEL(eax) EXTRACT(eax, 19, 16)
#define MODEL_NUMBER(eax) ((EXTENDED_MODEL(eax) << 4) | MODEL(eax))
#define FAMILY(eax) EXTRACT(eax, 11, 8)
#define Extended_Family(eax) EXTRACT(eax, 27, 20)
#define Family_Number(eax) (FAMILY(eax) + Extended_Family(eax))

void get_family_model(int *family, int *model) {
    unsigned int eax, ebx, ecx, edx;
    int success = __get_cpuid(1, &eax, &ebx, &ecx, &edx);
    if (family != NULL) {
        *family = success ? Family_Number(eax) : 0;
    }

    if (model != NULL) {
        *model = success ? MODEL_NUMBER(eax) : 0;
    }
}

int main(int argc, char **argv) {
    int family, model;
    get_family_model(&family, &model);
    void *ptr = pmalloc(1024);
    pfree(ptr, 1024);
    return 0;
}
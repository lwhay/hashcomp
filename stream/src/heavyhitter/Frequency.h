//
// Created by Michael on 10/28/2019.
//

#ifndef HASHCOMP_FREQUENCY_H
#define HASHCOMP_FREQUENCY_H

#include <map>
#include <cmath>
#include <stdlib.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MOD 2147483647
#define HL 31

#define KK  17
#define NTAB 32

long hash31(int64_t a, int64_t b, int64_t x) {
    int64_t result;
    long lresult;

    // return a hash of x using a and b mod (2^31 - 1)
    // may need to do another mod afterwards, or drop high bits
    // depending on d, number of bad guys
    // 2^31 - 1 = 2147483647

    //  result = ((int64_t) a)*((int64_t) x)+((int64_t) b);
    result = (a * x) + b;
    result = ((result >> HL) + result) & MOD;
    lresult = (long) result;

    return (lresult);
}

typedef struct prng_type {
    int usenric; // which prng to use
    float scale;             /* 2^(- integer size) */
    long floatidum;
    long intidum; // needed to keep track of where we are in the
    // nric random number generators
    long iy;
    long iv[NTAB];
    /* global variables */
    unsigned long randbuffer[KK];  /* history buffer */
    int r_p1, r_p2;          /* indexes into history buffer */
    int iset;
    double gset;
} prng_type;


// following definitions needed for the random number generator
#define IA 16807
#define IM 2147483647
#define AM (1.0/IM)
#define IQ 127773
#define IR 2836
#define NDIV (1+(IM-1)/NTAB)
#define EPS 1.2e-7
#define RNMX (1.0-EPS)

float ran1(prng_type *prng) {

    // A Random Number Generator that picks a uniform [0,1] random number
    // From Numerical Recipes, page 280
    // Should be called with a NEGATIVE value of idum to initialize
    // subsequent calls should not alter idum

    int j;
    long k;
    float temp;

    if (prng->floatidum <= 0 || !prng->iy) {
        if (-(prng->floatidum) < 1) prng->floatidum = 1;
        else prng->floatidum = -(prng->floatidum);
        for (j = NTAB + 7; j >= 0; j--) {
            k = (prng->floatidum) / IQ;
            prng->floatidum = IA * (prng->floatidum - k * IQ) - IR * k;
            if (prng->floatidum < 0) prng->floatidum += IM;
            if (j < NTAB) prng->iv[j] = prng->floatidum;
        }
        prng->iy = prng->iv[0];
    }
    k = (prng->floatidum) / IQ;
    prng->floatidum = IA * (prng->floatidum - k * IQ) - IR * k;
    if (prng->floatidum < 0) prng->floatidum += IM;
    j = prng->iy / NDIV;
    prng->iy = prng->iv[j];
    prng->iv[j] = prng->floatidum;
    if ((temp = AM * prng->iy) > RNMX) return RNMX;
    else return temp;
}

long ran2(prng_type *prng) {
    // A Random Number Generator that picks a uniform random number
    // from the range of long integers.
    // From Numerical Recipes, page 280
    // Should be called with a NEGATIVE value of idum to initialize
    // subsequent calls should not alter idum
    // This is a hacked version of the above procedure, so proceed with
    // caution.

    int j;
    long k;

    if (prng->intidum <= 0 || !prng->iy) {
        if (-(prng->intidum) < 1) prng->intidum = 1;
        else prng->intidum = -(prng->intidum);
        for (j = NTAB + 7; j >= 0; j--) {
            k = (prng->intidum) / IQ;
            prng->intidum = IA * (prng->intidum - k * IQ) - IR * k;
            if (prng->intidum < 0) prng->intidum += IM;
            if (j < NTAB) prng->iv[j] = prng->intidum;
        }
        prng->iy = prng->iv[0];
    }
    k = (prng->intidum) / IQ;
    prng->intidum = IA * (prng->intidum - k * IQ) - IR * k;
    if (prng->intidum < 0) prng->intidum += IM;
    j = prng->iy / NDIV;
    prng->iy = prng->iv[j];
    prng->iv[j] = prng->intidum;
    return prng->iy;
}

/**********************************************************************/

// Following routines are from www.agner.org

/************************* RANROTB.C ******************** AgF 1999-03-03 *
*  Random Number generator 'RANROT' type B                               *
*                                                                        *
*  This is a lagged-Fibonacci type of random number generator with       *
*  rotation of bits.  The algorithm is:                                  *
*  X[n] = ((X[n-j] rotl r1) + (X[n-k] rotl r2)) modulo 2^b               *
*                                                                        *
*  The last k values of X are stored in a circular buffer named          *
*  randbuffer.                                                           *
*                                                                        *
*  This version works with any integer size: 16, 32, 64 bits etc.        *
*  The integers must be unsigned. The resolution depends on the integer  *
*  size.                                                                 *
*                                                                        *
*  Note that the function RanrotAInit must be called before the first    *
*  call to RanrotA or iRanrotA                                           *
*                                                                        *
*  The theory of the RANROT type of generators is described at           *
*  www.agner.org/random/ranrot.htm                                       *
*                                                                        *
*************************************************************************/

// this should be almost verbatim from the above webpage.
// although it's been hacked with a little bit...

unsigned long rotl(unsigned long x, unsigned long r) {
    return (x << r) | (x >> (sizeof(x) * 8 - r));
}

/* define parameters (R1 and R2 must be smaller than the integer size): */
#define JJ  10
#define R1   5
#define R2   3

/* returns some random bits */
unsigned long ran3(prng_type *prng) {
    unsigned long x;

    /* generate next random number */

    x = prng->randbuffer[prng->r_p1] = rotl(prng->randbuffer[prng->r_p2], R1)
                                       + rotl(prng->randbuffer[prng->r_p1], R2);
    /* rotate list pointers */
    if (--prng->r_p1 < 0) prng->r_p1 = KK - 1;
    if (--prng->r_p2 < 0) prng->r_p2 = KK - 1;
    /* conversion to float */
    return x;
}

/* returns a random number between 0 and 1 */
double ran4(prng_type *prng) {

    /* conversion to floating point type */
    return (ran3(prng) * prng->scale);
}

/* this function initializes the random number generator.      */
/* Must be called before the first call to RanrotA or iRanrotA */
void RanrotAInit(prng_type *prng, unsigned long seed) {
    int i;

    /* put semi-random numbers into the buffer */
    for (i = 0; i < KK; i++) {
        prng->randbuffer[i] = seed;
        seed = rotl(seed, 5) + 97;
    }

    /* initialize pointers to circular buffer */
    prng->r_p1 = 0;
    prng->r_p2 = JJ;

    /* randomize */
    for (i = 0; i < 300; i++) ran3(prng);
    prng->scale = ldexp(1.0f, -8.0f * sizeof(unsigned long));
}

long prng_int(prng_type *prng) {
    // returns a pseudo-random long integer.  Initialise the generator
    // before use!

    long response = 0;

    switch (prng->usenric) {
        case 1 :
            response = (ran2(prng));
            break;
        case 2 :
            response = (ran3(prng));
            break;
        case 3 :
            response = (lrand48());
            break;
    }
    return response;
}

float prng_float(prng_type *prng) {
    // returns a pseudo-random float in the range [0.0,1.0].
    // Initialise the generator before use!
    float result = 0;

    switch (prng->usenric) {
        case 1 :
            result = (ran1(prng));
            break;
        case 2 :
            result = (ran4(prng));
            break;
        case 3 :
            result = (drand48());
            break;
    }
    return result;
}

prng_type *prng_Init(long seed, int nric) {
    // Initialise the random number generators.  nric determines
    // which algorithm to use, 1 for Numerical Recipes in C,
    // 0 for the other one.
    prng_type *result;

    result = (prng_type *) calloc(1, sizeof(prng_type));

    result->iy = 0;
    result->usenric = nric;
    result->floatidum = -1;
    result->intidum = -1;
    result->iset = 0;
    // set a global variable to record which algorithm to use
    switch (nric) {
        case 2 :
            RanrotAInit(result, seed);
            break;
        case 1 :
            if (seed > 0) {
                // to initialise the NRiC PRNGs, call it with a negative value
                // so make sure it gets a negative value!
                result->floatidum = -(seed);
                result->intidum = -(seed);
            } else {
                result->floatidum = seed;
                result->intidum = seed;
            }
            break;
        case 3 :
            srand48(seed);
            break;
    }

    prng_float(result);
    prng_int(result);
    // call the routines to actually initialise them
    return (result);
}

void prng_Destroy(prng_type *prng) {
    free(prng);
}

typedef struct itemlist ITEMLIST;
typedef struct group GC_GROUP;

struct group {
    int diff;
    ITEMLIST *items;
    GC_GROUP *previousg, *nextg;
};

struct itemlist {
    int item;
    GC_GROUP *parentg;
    ITEMLIST *previousi, *nexti;
    ITEMLIST *nexting, *previousing;

};

typedef struct freq_type {
    ITEMLIST **hashtable;
    GC_GROUP *groups;
    int k;
    int tblsz;
    int64_t a, b;
} freq_type;

#ifdef __cplusplus
};

void FShowGroups(freq_type *freq) {
    GC_GROUP *g;
    ITEMLIST *i, *first;
    int count;

    g = freq->groups;
    count = 0;
    while (g != NULL) {
        count = count + g->diff;
        printf("Group %d :", count);
        first = g->items;
        i = first;
        if (i != NULL)
            do {
                printf("%d -> ", i->item);
                i = i->nexting;
            } while (i != first);
        else printf(" empty");
        do {
            printf("%d <- ", i->item);
            i = i->previousing;
        } while (i != first);
        printf(")");
        g = g->nextg;
        if ((g != NULL) && (g->previousg->nextg != g))
            printf("Badly linked");
        printf("\n");
    }
}

std::map<uint32_t, uint32_t> Freq_Output(freq_type *freq, int thresh) {
    GC_GROUP *g;
    ITEMLIST *i, *first;
    int count = 0;
    std::map<uint32_t, uint32_t> res;

    g = freq->groups->nextg;

    while (g != NULL) {
        count = count + g->diff;
        first = g->items;
        i = first;
        if (i != NULL) {
            do {
                res.insert(std::pair<uint32_t, uint32_t>(i->item, count));
                i = i->nexting;
            } while (i != first);
        }

        g = g->nextg;
    }

    return res;
}

ITEMLIST *FGetNewCounter(freq_type *freq) {
    ITEMLIST *newi;
    int j;

    newi = freq->groups->items;  // take a counter from the pool
    freq->groups->items = freq->groups->items->nexting;

    newi->nexting->previousing = newi->previousing;
    newi->previousing->nexting = newi->nexting;
    // unhook the new item from the linked list

    // need to remove this item from the hashtable
    j = hash31(freq->a, freq->b, newi->item) % freq->tblsz;
    if (freq->hashtable[j] == newi)
        freq->hashtable[j] = newi->nexti;

    if (newi->nexti != NULL)
        newi->nexti->previousi = newi->previousi;
    if (newi->previousi != NULL)
        newi->previousi->nexti = newi->nexti;

    return (newi);
}

void FInsertIntoHashtable(freq_type *freq, ITEMLIST *newi, int i, int newitem) {
    newi->nexti = freq->hashtable[i];
    newi->item = newitem;
    newi->previousi = NULL;
    // insert item into the hashtable

    if (freq->hashtable[i] != NULL)
        freq->hashtable[i]->previousi = newi;
    freq->hashtable[i] = newi;
}

void FInsertIntoFirstGroup(freq_type *freq, ITEMLIST *newi) {
    GC_GROUP *firstg;

    firstg = freq->groups->nextg;
    newi->parentg = firstg;
    /* overwrite whatever was in the parent pointer */
    newi->nexting = firstg->items;
    newi->previousing = firstg->items->previousing;
    newi->previousing->nexting = newi;
    firstg->items->previousing = newi;
}

void FCreateFirstGroup(freq_type *freq, ITEMLIST *newi) {
    GC_GROUP *newgroup, *firstg;

    firstg = freq->groups->nextg;
    newgroup = (GC_GROUP *) malloc(sizeof(GC_GROUP));
    newgroup->diff = 1;
    newgroup->items = newi;
    newi->nexting = newi;
    newi->previousing = newi;
    newi->parentg = newgroup;
    // overwrite whatever was there before
    newgroup->nextg = firstg;
    newgroup->previousg = freq->groups;
    freq->groups->nextg = newgroup;
    if (firstg != NULL) {
        firstg->previousg = newgroup;
        firstg->diff--;
    }
}

void FPutInNewGroup(ITEMLIST *newi, GC_GROUP *tmpg) {
    GC_GROUP *oldgroup;

    oldgroup = newi->parentg;
    // put item in the tmpg group
    newi->parentg = tmpg;

    if (newi->nexting != newi) { // remove the item from its current group
        newi->nexting->previousing = newi->previousing;
        newi->previousing->nexting = newi->nexting;
        oldgroup->items = oldgroup->items->nexting;
    } else {
        if (oldgroup->diff != 0) {
            if (oldgroup->nextg != NULL) {
                oldgroup->nextg->diff += oldgroup->diff;
                oldgroup->nextg->previousg = oldgroup->previousg;
            }
            oldgroup->previousg->nextg = oldgroup->nextg;
            free(oldgroup);
            /* if we have created an empty group, remove it
               but avoid deleting the first group */
        }
    }
    newi->nexting = tmpg->items;
    newi->previousing = tmpg->items->previousing;
    newi->previousing->nexting = newi;
    newi->nexting->previousing = newi;
}

void FAddNewGroupAfter(ITEMLIST *newi, GC_GROUP *oldgroup) {
    GC_GROUP *newgroup;

    // remove item from old group...
    newi->nexting->previousing = newi->previousing;
    newi->previousing->nexting = newi->nexting;
    oldgroup->items = newi->nexting;
    newgroup = (GC_GROUP *) malloc(sizeof(GC_GROUP));
    newgroup->diff = 1;
    newgroup->items = newi;
    newgroup->previousg = oldgroup;
    newgroup->nextg = oldgroup->nextg;
    oldgroup->nextg = newgroup;
    if (newgroup->nextg != NULL) {
        newgroup->nextg->diff--;
        newgroup->nextg->previousg = newgroup;
    }
    newi->parentg = newgroup;
    newi->nexting = newi;
    newi->previousing = newi;
}

void FAddNewGroupBefore(ITEMLIST *newi, GC_GROUP *oldgroup) {
    GC_GROUP *newgroup;

    // remove item from old group...
    newi->nexting->previousing = newi->previousing;
    newi->previousing->nexting = newi->nexting;
    oldgroup->items = newi->nexting;
    newgroup = (GC_GROUP *) malloc(sizeof(GC_GROUP));
    newgroup->diff = oldgroup->diff - 1;
    oldgroup->diff = 1;

    newgroup->items = newi;
    newgroup->nextg = oldgroup;
    newgroup->previousg = oldgroup->previousg;
    oldgroup->previousg = newgroup;
    if (newgroup->previousg != NULL) {
        newgroup->previousg->nextg = newgroup;
    }
    newi->parentg = newgroup;
    newi->nexting = newi;
    newi->previousing = newi;
}


void FDeleteFirstGroup(freq_type *freq) {
    GC_GROUP *tmpg;

    freq->groups->nextg->items->previousing->nexting = freq->groups->items->nexting;
    freq->groups->items->nexting->previousing = freq->groups->nextg->items->previousing;
    freq->groups->nextg->items->previousing = freq->groups->items;
    freq->groups->items->nexting = freq->groups->nextg->items;
    /* phew!  that has merged the two circular doubly linked lists */

    tmpg = freq->groups->nextg;
    freq->groups->nextg->diff = 0;
    freq->groups->nextg = freq->groups->nextg->nextg;
    if (freq->groups->nextg != NULL)
        freq->groups->nextg->previousg = freq->groups;
    tmpg->nextg = NULL;
    tmpg->previousg = NULL;
}

void FIncrementCounter(ITEMLIST *newi) {
    GC_GROUP *oldgroup;

    oldgroup = newi->parentg;
    if ((oldgroup->nextg != NULL) && (oldgroup->nextg->diff == 1))
        FPutInNewGroup(newi, oldgroup->nextg);
        // if the next group exists
    else {
        // need to create a new group with a differential of one
        if (newi->nexting == newi) {
            newi->parentg->diff++;
            if (newi->parentg->nextg != NULL)
                newi->parentg->nextg->diff--;
        } else
            FAddNewGroupAfter(newi, oldgroup);
    }
}

void SubtractCounter(ITEMLIST *newi) {
    GC_GROUP *oldgroup;

    oldgroup = newi->parentg;
    if ((oldgroup->previousg != NULL) && (oldgroup->diff == 1))
        FPutInNewGroup(newi, oldgroup->previousg);
    else {
        if (newi->nexting == newi) {
            newi->parentg->diff--;
            if (newi->parentg->nextg != NULL)
                newi->parentg->nextg->diff++;
        } else
            FAddNewGroupBefore(newi, oldgroup);
    }
}


void DecrementCounts(freq_type *freq) {
    if ((freq->groups->nextg != NULL) && (freq->groups->nextg->diff > 0)) {
        freq->groups->nextg->diff--;
        if (freq->groups->nextg->diff == 0)
            FDeleteFirstGroup(freq);
        /* need to delete the first group... */
    }
}

void FirstGroup(freq_type *freq, ITEMLIST *newi) {
    if ((freq->groups->nextg != NULL) && (freq->groups->nextg->diff == 1))
        FInsertIntoFirstGroup(freq, newi);
        /* if the first group starts at 1... */
    else
        FCreateFirstGroup(freq, newi);
    /* need to create a new first group */
    /* and we are done, we don't need to decrement */
}

void RecycleCounter(freq_type *freq, ITEMLIST *il) {
    if (il->nexting == il)
        DecrementCounts(freq);
    else {
        if (freq->groups->items == il)
            freq->groups->items = freq->groups->items->nexting;
        /* tidy up here in case we have emptied a defunct group?
         need an item counter in order to do this */
        il->nexting->previousing = il->previousing;
        il->previousing->nexting = il->nexting;
        FirstGroup(freq, il);
        /* Needed to sort out what happens when we insert an item
       which has a counter but its counter is zero
       */
    }
}

void Freq_Update(freq_type *freq, int newitem) {
    int i;
    ITEMLIST *il;
    int diff;

    if (newitem > 0) diff = 1;
    else {
        (newitem = -newitem);
        diff = -1;
    }
    i = hash31(freq->a, freq->b, newitem) % freq->tblsz;
    il = freq->hashtable[i];
    while (il != NULL) {
        if ((il->item) == newitem)
            break;
        il = il->nexti;
    }
    if (il == NULL) {
        if (diff == 1) {
            /* item is not monitored (not in hashtable) */
            if (freq->groups->items->nexting != freq->groups->items) {
                /* if there is space for a new item */
                il = FGetNewCounter(freq);
                /* and put it into the hashtable for the new item */
                FInsertIntoHashtable(freq, il, i, newitem);
                FirstGroup(freq, il);
            } else
                DecrementCounts(freq);
        }
        /* else, delete an item that isnt there, ignore it */
    } else if (diff == 1)
        if (il->parentg->diff == 0)
            RecycleCounter(freq, il);
        else
            FIncrementCounter(il);
        /* if we have an item, we need to increment its counter */
    else if (il->parentg->diff != 0)
        SubtractCounter(il);
}

freq_type *Freq_Init(float phi) {
    ITEMLIST *inititem;
    ITEMLIST *previtem;
    int i, k;
    int hashspace, groupspace, itemspace;
    freq_type *result;
    prng_type *prng;

    k = (int) ceil(1.0 / phi);
    if (k < 1) k = 1;
    result = (freq_type *) calloc(1, sizeof(freq_type));

    // need to init the rng...
    prng = prng_Init(45445, 2);
    prng_int(prng);
    prng_int(prng);
    prng_int(prng);
    prng_int(prng);
    result->a = (int64_t) (prng_int(prng) % MOD);
    result->b = (int64_t) (prng_int(prng) % MOD);
    prng_Destroy(prng);
    result->k = k;

    result->tblsz = 2 * k;
    result->hashtable = (ITEMLIST **) calloc(2 * k + 2, sizeof(ITEMLIST *));
    hashspace = (2 * k + 2) * sizeof(ITEMLIST *);
    for (i = 0; i < 2 * k; i++)
        result->hashtable[i] = NULL;

    result->groups = (GC_GROUP *) malloc(sizeof(GC_GROUP));
    result->groups->diff = 0;
    result->groups->nextg = NULL;
    result->groups->previousg = NULL;
    previtem = (ITEMLIST *) malloc(sizeof(ITEMLIST));
    result->groups->items = previtem;
    previtem->nexti = NULL;
    previtem->previousi = NULL;
    previtem->parentg = result->groups;
    previtem->nexting = previtem;
    previtem->previousing = previtem;
    previtem->item = 0;
    groupspace = k * sizeof(GC_GROUP);

    for (i = 1; i <= k; i++) {
        inititem = (ITEMLIST *) malloc(sizeof(ITEMLIST));
        inititem->item = 0;
        inititem->parentg = result->groups;
        inititem->nexti = NULL;
        inititem->previousi = NULL;
        inititem->nexting = previtem;
        inititem->previousing = previtem->previousing;
        previtem->previousing->nexting = inititem;
        previtem->previousing = inititem;
    }

    itemspace = (k + 1) * sizeof(ITEMLIST);
    return (result);
}

int Freq_Size(freq_type *freq) {
    int size;

    size = 2 * (freq->tblsz) * sizeof(ITEMLIST *) + (freq->k + 1) * sizeof(ITEMLIST) + (freq->k) * sizeof(GC_GROUP);
    return size;
}

void Freq_Destroy(freq_type *freq) {
    // placeholder implementation: need to go through and free
    // all memory associated with the data structure explicitly
    free(freq);
}

#endif
#endif //HASHCOMP_FREQUENCY_H

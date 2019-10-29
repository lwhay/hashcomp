//
// Created by Michael on 10/28/2019.
//

#ifndef HASHCOMP_FREQUENCY_H
#define HASHCOMP_FREQUENCY_H

#include <map>
#include <cmath>
#include <stdlib.h>
#include <stdio.h>
#include "prng.h"

#ifdef __cplusplus
extern "C" {
#endif

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

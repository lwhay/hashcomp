//
// Created by iclab on 10/29/19.
//

#ifndef HASHCOMP_GROUPFREQUENT_H
#define HASHCOMP_GROUPFREQUENT_H

#include <cmath>
#include <cstdint>

class ItemList;

class Group {
private:
    int diff;
    ItemList *items;
    Group *prevg, *nextg;
public:
    void setDiff(int d) { diff = d; }

    int getDiff() { return diff; }

    void setItems(ItemList *its) { items = its; }

    ItemList *getItems() { return items; }

    void setPrevg(Group *g) { prevg = g; }

    Group *getPrevg() { return prevg; }

    void setNextg(Group *g) { nextg = g; }

    Group *getNextg() { return nextg; }
};

class ItemList {
private:
    int item;
    Group *parents;
    ItemList *previ, *nexti;
    ItemList *next, *prev;
public:
    void setItem(int value) { item = value; }

    int getItem() { return item; }

    void setGroup(Group *g) { parents = g; }

    Group *getGroup() { return parents; }

    void setPrevi(ItemList *node) { previ = node; }

    ItemList *getPrevi() { return previ; }

    void setNexti(ItemList *node) { nexti = node; }

    ItemList *getNexti() { return nexti; }

    void setPrev(ItemList *node) { prev = node; }

    ItemList *getPrev() { return prev; }

    void setNext(ItemList *node) { next = node; }

    ItemList *getNext() { return next; }
};

constexpr int MOD = 2147483647;

constexpr int HL = 31;

class GroupFrequent {
private:
    ItemList **hashtable;
    Group *groups;
    int k;
    int tblsz;
    int64_t a, b;

    inline long hash31(int64_t a, int64_t b, int64_t x) {
        int64_t result;
        long lresult;
        result = (a * x) + b;
        result = ((result >> HL) + result) & MOD;
        lresult = (long) result;

        return (lresult);
    }

private:
    ItemList *GetNewCounter() {
        ItemList *newi;
        int j;
        newi = groups->getItems();
        groups->setItems(groups->getItems()->getNext());
        newi->getNext()->setPrev(newi->getPrev());
        newi->getPrev()->setNext(newi->getNext());
        j = hash31(a, b, newi->getItem()) % tblsz;
        if (hashtable[j] == newi) hashtable[j] = newi->getNexti();
        if (newi->getNexti() != nullptr) newi->getNexti()->setPrevi(newi->getPrevi());
        if (newi->getPrevi() != nullptr) newi->getPrevi()->setNexti(newi->getNexti());
        return newi;
    }

    void InsertIntoHashtable(ItemList *newi, int i, int newitem) {
        newi->setNexti(hashtable[i]);
        newi->setItem(newitem);
        newi->setPrevi(nullptr);
        if (hashtable[i] != nullptr) {
            hashtable[i]->setPrevi(newi);
        }
        hashtable[i] = newi;
    }

    void InsertIntoFirstGroup(ItemList *newi, int i, int newitem) {
        Group *firstg = groups->getNextg();
        newi->setGroup(firstg);
        newi->setNext(firstg->getItems());
        newi->setPrev(firstg->getItems()->getPrev());
        newi->getPrev()->setNext(newi);
        firstg->getItems()->setPrev(newi);
    }

    void CreateFirstGroup(ItemList *newi) {

    }

    void PutInNewGroup(ItemList *newi, Group *tmpg) {

    }

    void AddNewGroupAfter(ItemList *newi, Group *oldg) {

    }

    void AddNewGroupBefore(ItemList *newi, Group *oldg) {

    }

    void DeleteFirstGroup() {

    }

    void IncrementCounter(ItemList *newi) {

    }

    void SubtractCounter(ItemList *newi) {

    }

    void DecrementCounters() {

    }

    void FirstGroup(ItemList *newi) {

    }

    void RecycleCounter(ItemList *il) {

    }

public:
    GroupFrequent(double phi) : k((int) ceil(1 / phi)) {}

    ~GroupFrequent() {}

    void put(int item) {
        int i;
        ItemList *il;
        int diff;
        if (item > 0) diff = 1;
        else {
            item = -item;
            diff = -1;
        }

        i = hash31(a, b, item) % tblsz;
        il = hashtable[i];
        while (il != nullptr) {
            if ((il->getItem()) == item) break;
            il = il->getNexti();
        }
        if (il == nullptr) {
            if (diff == 1) {
                if (groups->getItems()->getNext() != groups->getItems()) {
                    il = GetNewCounter();
                    InsertIntoHashtable(il, i, item);
                    FirstGroup(il);
                } else DecrementCounters();
            }
        } else if (diff == 1) {
            if (il->getGroup()->getDiff() == 0) RecycleCounter(il);
            else IncrementCounter(il);
        } else if (il->getGroup()->getDiff() != 0)
            SubtractCounter(il);
    }
};

#endif //HASHCOMP_GROUPFREQUENT_H

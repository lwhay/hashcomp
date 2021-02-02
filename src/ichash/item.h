#ifndef RESEARCH_ITEM_H
#define RESEARCH_ITEM_H

#include "assert_msg.h"


//#define FIXED_128 1

#ifdef FIXED_128

#define ITEM_KEY(item_ptr) ((Item*)item_ptr)->key
#define ITEM_KEY_LEN(item_ptr)  ((Item * )item_ptr)->key_len
#define ITEM_VALUE(item_ptr) ((Item * )item_ptr)->value
#define ITEM_VALUE_LEN(item_ptr)  ((Item * )item_ptr)->value_len


struct Item{
    char key[128];
    char value[128];
    uint32_t key_len;
    uint32_t value_len;
};

Item * allocate_item(char * key,size_t key_len,char * value,size_t value_len){
    Item * p = (Item * )malloc(2 * 128 + 2 * sizeof(uint32_t));
    ASSERT(p!= nullptr,"malloc failure");
    p->key_len = key_len;
    p->value_len = value_len;
    memcpy(ITEM_KEY(p),key,key_len);
    memcpy(ITEM_VALUE(p),value,value_len);
    return p;
}

#else

#define ITEM_KEY(item_ptr) ((Item*)item_ptr)->buf
#define ITEM_KEY_LEN(item_ptr)  ((Item * )item_ptr)->key_len
#define ITEM_VALUE(item_ptr) (((Item * )item_ptr)->buf + key_len)
#define ITEM_VALUE_LEN(item_ptr)  ((Item * )item_ptr)->value_len


struct Item{
    uint32_t key_len;
    uint32_t value_len;
    char buf[];
};

Item * allocate_item(char * key,size_t key_len,char * value,size_t value_len){
    Item * p = (Item * )malloc(key_len + value_len + 2 * sizeof(uint32_t));
    ASSERT(p!= nullptr,"malloc failure");
    p->key_len = key_len;
    p->value_len = value_len;
    memcpy(ITEM_KEY(p),key,key_len);
    memcpy(ITEM_VALUE(p),value,value_len);
    return p;
}

#endif


#endif //RESEARCH_ITEM_H

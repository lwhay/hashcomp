#ifndef MY_RECLAIMER_ITEM_H
#define MY_RECLAIMER_ITEM_H

#include "assert_msg.h"
#include <string.h>

//#define FIX_LEN 1;


typedef  uint32_t ltype;

#define ITEM_KEY_LEN(item_ptr)  ((Item * )item_ptr)->key_len
#define ITEM_VALUE_LEN(item_ptr)  ((Item * )item_ptr)->value_len


#ifdef FIX_LEN

#define KEY_BUF_LEN 32
#define VALUE_BUF_LEN 32
#define ITEM_LEN(item_ptr)  ( KEY_BUF_LEN + VALUE_BUF_LEN + 2* sizeof(ltype))

#define ITEM_KEY(item_ptr) ((Item*)item_ptr)->key
#define ITEM_VALUE(item_ptr) ((Item * )item_ptr)->value

struct Item{
    char key[KEY_BUF_LEN];
    char value[VALUE_BUF_LEN];
    ltype key_len;
    ltype value_len;
    inline uint64_t get_struct_len(){
        return KEY_BUF_LEN + VALUE_BUF_LEN +2* sizeof(ltype);
    }
};

#else

#define ITEM_LEN_ALLOC(kl,vl) (kl+vl+2* sizeof(ltype))

#define ITEM_KEY(item_ptr) ((Item*)item_ptr)->buf
#define ITEM_VALUE(item_ptr) (((Item * )item_ptr)->buf + ITEM_KEY_LEN(item_ptr))
#define ITEM_LEN(item_ptr)  (ITEM_KEY_LEN(item_ptr) + ITEM_VALUE_LEN(item_ptr) + 2* sizeof(ltype))

struct Item{
    ltype key_len;
    ltype value_len;
    char buf[];
    inline uint64_t get_struct_len(){
        return key_len + value_len +2* sizeof(ltype);
    }
};

#endif

//Key length and value length must be assigned firstly during initialzation
//since we need to find the address of value based on its length
static inline void init_item(Item * item,
                             char * key,
                             ltype key_len,
                             char * value,
                             ltype value_len){
    item->key_len = key_len;
    item->value_len = value_len;
    memcpy(ITEM_KEY(item),key,key_len);
    memcpy(ITEM_VALUE(item),value,value_len);
}


#endif //MY_RECLAIMER_ITEM_H
